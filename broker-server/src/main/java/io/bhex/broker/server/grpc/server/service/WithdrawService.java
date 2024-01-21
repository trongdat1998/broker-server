/**********************************
 *@项目名称: broker-server
 *@文件名称: io.bhex.broker.server.grpc.server.service
 *@Date 2018/8/23
 *@Author peiwei.ren@bhex.io
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
package io.bhex.broker.server.grpc.server.service;

import com.github.pagehelper.PageHelper;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;
import io.bhex.base.account.AsyncWithdrawOrderRequest;
import io.bhex.base.account.AsyncWithdrawOrderResponse;
import io.bhex.base.account.*;
import io.bhex.base.proto.DecimalUtil;
import io.bhex.base.quote.Rate;
import io.bhex.base.rc.WithdrawalRequestSign;
import io.bhex.base.token.ChainType;
import io.bhex.base.token.GetTokenRequest;
import io.bhex.base.token.TokenDetail;
import io.bhex.base.token.TokenTypeEnum;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.common.util.CryptoUtil;
import io.bhex.broker.common.util.JsonUtil;
import io.bhex.broker.grpc.bwlist.UserBlackWhiteType;
import io.bhex.broker.grpc.common.BasicRet;
import io.bhex.broker.grpc.common.Header;
import io.bhex.broker.grpc.user.VerifyTradePasswordResponse;
import io.bhex.broker.grpc.user.level.QueryMyLevelConfigResponse;
import io.bhex.broker.grpc.withdraw.*;
import io.bhex.broker.server.domain.*;
import io.bhex.broker.server.grpc.client.service.GrpcTokenService;
import io.bhex.broker.server.grpc.client.service.GrpcWithdrawService;
import io.bhex.broker.server.grpc.server.service.notify.WithdrawNotify;
import io.bhex.broker.server.grpc.server.service.statistics.StatisticsOrgService;
import io.bhex.broker.server.model.WithdrawAddress;
import io.bhex.broker.server.model.WithdrawOrder;
import io.bhex.broker.server.model.*;
import io.bhex.broker.server.primary.mapper.*;
import io.bhex.broker.server.util.GrpcHeaderUtil;
import io.bhex.broker.server.util.GrpcRequestUtil;
import io.bhex.broker.server.util.SignUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import tk.mybatis.mapper.entity.Example;
import tk.mybatis.mapper.util.StringUtil;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@Slf4j
@Service
public class WithdrawService {

    @Resource
    private OtcThirdPartyMapper otcThirdPartyMapper;

    @Resource
    private WithdrawAddressMapper withdrawAddressMapper;

    @Resource
    private WithdrawOrderMapper withdrawOrderMapper;

    @Resource
    private StatisticsOrgService statisticsOrgService;

    @Resource
    private WithdrawPreApplyMapper withdrawPreApplyMapper;

    @Resource
    private GrpcWithdrawService grpcWithdrawService;

    @Resource
    private GrpcTokenService grpcTokenService;

    @Resource
    private BasicService basicService;

    @Resource
    private UserService userService;

    @Resource
    private TokenMapper tokenMapper;

    @Resource
    private UserMapper userMapper;

    @Resource
    private WithdrawWhiteAddressMapper withdrawWhiteAddressMapper;

    @Resource
    private TokenConvertRateMapper tokenConvertRateMapper;
    @Resource
    private UserLevelService userLevelService;
    @Resource
    private UserSecurityService userSecurityService;

    @Resource
    private VerifyCodeService verifyCodeService;

    @Resource
    private AccountService accountService;

    @Resource(name = "stringRedisTemplate")
    private StringRedisTemplate redisTemplate;

    @Resource
    private SignUtils signUtils;

    @Value("${broker.skipTokenAddressCheck:false}")
    private Boolean skipTokenAddressCheck;

    @Resource
    private FrozenUserRecordMapper frozenUserRecordMapper;

    @Resource
    private UserVerifyMapper userVerifyMapper;

    @Resource
    private CommonIniService commonIniService;

    @Resource
    private BaseBizConfigService baseBizConfigService;

    @Resource
    private UserBlackWhiteListConfigService userBlackWhiteListConfigService;

    @Resource
    private RiskControlBalanceService riskControlBalanceService;

    private static final BigDecimal DEFAULT_RATE_UP_RATIO = new BigDecimal("1");
    private static final BigDecimal DEFAULT_USDT_RATE_UP_RATIO = new BigDecimal("1");

    private static final Integer CONVERT_RATE_SCALE = 8;

    private static final String REFRESH_TOKEN_CONVERT_RATE_LOCK_KEY = "REFRESH_TOKEN_CONVERT_RATE_LOCK";

    private static final String GLOBAL_VARIABLES = "GLOBAL";

    @Value("${verify-captcha:true}")
    private Boolean verifyCaptcha;

    @Value("${global-notify-type:1}")
    private Integer globalNotifyType;

    @Scheduled(cron = "0 0 0/1 * * ?")
    public void refreshConvertRate() {
        if (redisTemplate.opsForValue().setIfAbsent(REFRESH_TOKEN_CONVERT_RATE_LOCK_KEY, "" + System.currentTimeMillis(), Duration.ofMinutes(5))) {
            try {
                List<TokenConvertRate> convertRateList = tokenConvertRateMapper.queryAll();
                if (convertRateList != null && convertRateList.size() > 0) {
                    convertRateList.forEach(this::updateTokenConvertRate);
                }
            } catch (Exception e) {
                log.error("token_convert_rate refresh exception", e);
            } finally {
                redisTemplate.delete(REFRESH_TOKEN_CONVERT_RATE_LOCK_KEY);
            }
        }
    }

    public void refreshTokenConvertRate(Long orgId, String tokenId, String convertTokeId) {
        TokenConvertRate tokenConvertRate = tokenConvertRateMapper.getByToken(orgId, tokenId, convertTokeId);
        if (tokenConvertRate != null) {
            updateTokenConvertRate(tokenConvertRate);
        }
    }

    private void updateTokenConvertRate(TokenConvertRate tokenConvertRate) {
        try {
            BigDecimal rateUpRatio = (tokenConvertRate.getRateUpRatio() == null || tokenConvertRate.getRateUpRatio().compareTo(BigDecimal.ONE) < 0)
                    ? (tokenConvertRate.getTokenId().equals("USDT") ? DEFAULT_USDT_RATE_UP_RATIO : DEFAULT_RATE_UP_RATIO) : tokenConvertRate.getRateUpRatio();
            BigDecimal convertRate = rateUpRatio.multiply(
                    basicService.getFXRate(tokenConvertRate.getOrgId(), tokenConvertRate.getConvertTokenId(), tokenConvertRate.getTokenId()))
                    .setScale(CONVERT_RATE_SCALE, BigDecimal.ROUND_HALF_UP);
            if (convertRate.compareTo(BigDecimal.ZERO) == 0) {
                log.error("withdraw convertRate get {}/{}={}. oldItem = {}",
                        tokenConvertRate.getConvertTokenId(), tokenConvertRate.getTokenId(), convertRate.toPlainString(),
                        JsonUtil.defaultGson().toJson(tokenConvertRate)
                );
                return;
            }
            if (tokenConvertRate.getLastConvertRate().compareTo(BigDecimal.ZERO) == 0) {
                tokenConvertRateMapper.update(tokenConvertRate.getId(), convertRate, convertRate, System.currentTimeMillis());
            } else {
                tokenConvertRateMapper.update(tokenConvertRate.getId(), convertRate, tokenConvertRate.getConvertRate(), System.currentTimeMillis());
            }
        } catch (Exception e) {
            log.error("refresh withdraw convert rate error, convertItem:{}", JsonUtil.defaultGson().toJson(tokenConvertRate), e);
        }
    }

    public void updateRateUpRatio(Long orgId, String tokenId, String convertTokenId, BigDecimal rateUpRatio) {
        TokenConvertRate tokenConvertRate = tokenConvertRateMapper.getByToken(orgId, tokenId, convertTokenId);
        if (tokenConvertRate != null) {
            tokenConvertRateMapper.updateRateUpRatio(tokenConvertRate.getId(), rateUpRatio);
            tokenConvertRate.setRateUpRatio(rateUpRatio);
            updateTokenConvertRate(tokenConvertRate);
        }
    }

    /**
     * 校验地址
     *
     * @param tokenId    token
     * @param address    地址
     * @param addressExt 地址扩展信息
     * @return 校验response
     */
    private CheckWithdrawalAddressResponse checkAddress(Long accountId, String tokenId, String chainType, String address, String addressExt) {
        CheckWithdrawalAddressRequest checkAddressRequest = CheckWithdrawalAddressRequest.newBuilder()
                .setAccountId(accountId)
                .setTokenId(tokenId)
                .setChainType(chainType)
                .setAddress(address)
                .setAddressExt(Strings.nullToEmpty(addressExt))
                .build();
        return grpcWithdrawService.checkAddress(checkAddressRequest);
    }

    /**
     * 用户创建提币地址
     *
     * @return
     */
    public CreateAddressResponse createAddress(Header header, String tokenId, String chainType, String address, String addressExt, String remark,
                                               Integer authTypeValue, Long verifyCodeOrderId, String verifyCode) {
        if (Strings.isNullOrEmpty(verifyCode)) {
            throw new BrokerException(BrokerErrorCode.VERIFY_CODE_CANNOT_BE_NULL);
        }
        AuthType authType = AuthType.fromValue(authTypeValue);
        if (authType == AuthType.GA) {
            userSecurityService.validGACode(header, header.getUserId(), verifyCode);
        } else {
            userSecurityService.validVerifyCode(header, authType, verifyCodeOrderId, verifyCode, VerifyCodeType.ADD_WITHDRAW_ADDRESS);
        }
        CheckWithdrawalAddressResponse checkAddressResponse = checkAddress(accountService.getMainAccountId(header), tokenId, chainType, address, addressExt);
        if (!checkAddressResponse.getCheckResult()) {
            throw new BrokerException(BrokerErrorCode.WITHDRAW_ADDRESS_ILLEGAL);
        }
        if (checkAddressResponse.getIsContractAddress()) {
            throw new BrokerException(BrokerErrorCode.UNSUPPORTED_CONTRACT_ADDRESS);
        }
        WithdrawAddress withdrawAddress = WithdrawAddress.builder()
                .orgId(header.getOrgId())
                .userId(header.getUserId())
                .tokenId(tokenId)
                .chainType(chainType)
                .tokenName(basicService.getTokenName(header.getOrgId(), tokenId))
                .address(address)
                .addressExt(addressExt)
                .remark(remark)
                .created(System.currentTimeMillis())
                .build();
        withdrawAddressMapper.insert(withdrawAddress);
        if (authType != AuthType.GA) {
            User user = userService.getUser(header.getUserId());
            if (authType == AuthType.MOBILE) {
                verifyCodeService.invalidVerifyCode(header, VerifyCodeType.ADD_WITHDRAW_ADDRESS, verifyCodeOrderId, user.getNationalCode() + user.getMobile());
            } else {
                verifyCodeService.invalidVerifyCode(header, VerifyCodeType.ADD_WITHDRAW_ADDRESS, verifyCodeOrderId, user.getEmail());
            }
        }
        return CreateAddressResponse.newBuilder().setAddress(getWithdrawAddress(withdrawAddress)).build();
    }

    public DeleteAddressResponse deleteAddress(Header header, Long addressId) {
        withdrawAddressMapper.delete(addressId, header.getUserId(), WithdrawAddressStatus.DELETED.getStatus());
        return DeleteAddressResponse.newBuilder().build();
    }

    private int deleteWithdrawAddress(Long id, Long userId) {
        return withdrawAddressMapper.delete(id, userId, WithdrawAddressStatus.DELETED.getStatus());
    }

    /**
     * 查询提币地址列表
     */
    public QueryAddressResponse queryAddress(Header header, String tokenId, String chainType) {
        List<WithdrawAddress> withdrawAddressList;
        if (Strings.isNullOrEmpty(tokenId)) {
            withdrawAddressList = withdrawAddressMapper.queryByUserId(header.getUserId());
        } else {
            if (tokenId.equals("USDT") && chainType.equals("OMNI")) {
                withdrawAddressList = withdrawAddressMapper.queryOMNIAddress(header.getUserId());
            } else {
                withdrawAddressList = withdrawAddressMapper.queryByTokenId(header.getUserId(), tokenId, chainType);
            }
        }
        return QueryAddressResponse.newBuilder()
                .addAllAddresses(withdrawAddressList.stream().map(this::getWithdrawAddress).collect(Collectors.toList()))
                .build();
    }

    /**
     * 此接口为用户提币的时候校验提币地址是否可用
     */
    public CheckAddressResponse checkAddress(Header header, String tokenId, String chainType, String address, String addressExt) {
        if (address.matches("^\\d{10,}$")) {
            Long userIdAddress = Longs.tryParse(address);
            User user = userMapper.getByOrgAndUserId(header.getOrgId(), userIdAddress);
            if (user != null) {
                return CheckAddressResponse.newBuilder()
                        .setAddressIsUserId(true)
                        .setIsIllegal(false)
                        .setIsInnerAddress(true)
                        .setIsInBlackList(false)
                        .setIsContractAddress(false)
                        .build();
            }
        }
//        if (header.getOrgId() == 6002 && tokenId.equalsIgnoreCase("HBC")) {
//            return CheckAddressResponse.newBuilder()
//                    .setAddressIsUserId(false)
//                    .setIsIllegal(true)
//                    .setIsInnerAddress(false)
//                    .setIsInBlackList(false)
//                    .setIsContractAddress(false)
//                    .build();
//        }
        CheckWithdrawalAddressResponse response = checkAddress(accountService.getMainAccountId(header), tokenId, chainType, address, addressExt);
        return CheckAddressResponse.newBuilder()
                .setAddressIsUserId(false)
                .setIsIllegal(!response.getCheckResult())
                .setIsInnerAddress(response.getIsInnerAddress())
                .setIsInBlackList(response.getInBacklist())
                .setIsContractAddress(response.getIsContractAddress())
                .build();
    }

    /**
     * return frozen reason
     */
    private Integer checkWithdrawFrozen(Long orgId, Long userId, FrozenType frozenType) {
        Long currentTimestamp = System.currentTimeMillis();
        FrozenUserRecord frozenUserRecord = frozenUserRecordMapper.getByUserIdAndFrozenType(orgId, userId, frozenType.type());
        if (frozenUserRecord != null && frozenUserRecord.getStatus() == FrozenStatus.UNDER_FROZEN.status()
                && (currentTimestamp <= frozenUserRecord.getEndTime() && currentTimestamp >= frozenUserRecord.getStartTime())) {
            log.info("user:{} is under withdraw frozen, cannot withdraw", userId);
            return frozenUserRecord.getFrozenReason();
        }
        if (userService.getUser(userId).getIsVirtual() == 1) {
            return 1;
        }
        return 0;
    }

    private boolean isInWithdrawWhiteList(Long orgId, String tokenId, Long userId) {
        if (userId == null) {
            return false;
        }

        return userBlackWhiteListConfigService.inWithdrawWhiteBlackList(orgId, tokenId, userId, UserBlackWhiteType.WHITE_CONFIG);

//        CommonIni globalCommonIni = commonIniService.getCommonIniFromCache(orgId, GLOBAL_VARIABLES + BrokerServerConstants.WITHDRAW_WHITE_CONFIG);
//        String globalConfigValue = globalCommonIni == null ? "" : Strings.nullToEmpty(globalCommonIni.getIniValue());
//        CommonIni commonIni = commonIniService.getCommonIniFromCache(orgId, tokenId + BrokerServerConstants.WITHDRAW_WHITE_CONFIG);
//        String configValue = commonIni == null ? "" : Strings.nullToEmpty(commonIni.getIniValue());
//        return globalConfigValue.contains(userId.toString()) || configValue.contains(userId.toString());
    }


    private boolean isInWithdrawBlackList(Long orgId, String tokenId, Long userId) {
        if (userId == null) {
            return false;
        }
        return userBlackWhiteListConfigService.inWithdrawWhiteBlackList(orgId, tokenId, userId, UserBlackWhiteType.BLACK_CONFIG);
//        CommonIni globalCommonIni = commonIniService.getCommonIniFromCache(orgId, GLOBAL_VARIABLES + BrokerServerConstants.WITHDRAW_BLACK_CONFIG);
//        String globalConfigValue = globalCommonIni == null ? "" : Strings.nullToEmpty(globalCommonIni.getIniValue());
//        CommonIni commonIni = commonIniService.getCommonIniFromCache(orgId, tokenId + BrokerServerConstants.WITHDRAW_BLACK_CONFIG);
//        String configValue = commonIni == null ? "" : Strings.nullToEmpty(commonIni.getIniValue());
//        return globalConfigValue.contains(userId.toString()) || configValue.contains(userId.toString());
    }

    private TokenWithdrawLimitDTO getLimitDTO(Long orgId, String tokenId, String chainTypeName, WithdrawalQuotaResponse response) {
        io.bhex.broker.server.model.Token token = tokenMapper.getToken(orgId, tokenId);
        if (token == null) {
            log.error("withdrawQuota get token=null, orgId:{} tokenId:{}", orgId, tokenId);
            throw new BrokerException(BrokerErrorCode.PARAM_INVALID);
        }
        TokenDetail tokenDetail = grpcTokenService.getToken(GetTokenRequest.newBuilder().setTokenId(tokenId).build());
        boolean chainTypeAllowWithdraw = true;
        if (!Strings.isNullOrEmpty(chainTypeName)) {
            List<ChainType> chainTypesList = tokenDetail.getChainTypesList();
            for (ChainType chainType : chainTypesList) {
                if (chainType.getChainName().equalsIgnoreCase(chainTypeName)) {
                    chainTypeAllowWithdraw = chainType.getAllowWithdraw();
                }
            }
        }
        return TokenWithdrawLimitDTO.builder()
                .tokenId(tokenId)
                .feeTokenId(response.getPlatformFeeTokenId())
                .minerFeeTokenId(response.getMinerFeeTokenId())
                .allowWithdraw(tokenDetail.getAllowWithdraw() && chainTypeAllowWithdraw
                        && token.getAllowWithdraw() == BrokerServerConstants.ALLOW_STATUS.intValue()
                        && token.getStatus() == BrokerServerConstants.ONLINE_STATUS.intValue())
                .precision(tokenDetail.getMinPrecision())
                .minerFeePrecision(response.getMinerFeeTokenIdPrecision())
                .maxWithdrawQuota(token.getMaxWithdrawQuota().compareTo(BigDecimal.ZERO) > 0
                        ? token.getMaxWithdrawQuota().min(DecimalUtil.toBigDecimal(response.getDayMaxQuota()))
                        : DecimalUtil.toBigDecimal(response.getDayMaxQuota()))
                .minWithdrawQuantity(token.getMinWithdrawQuantity().compareTo(BigDecimal.ZERO) > 0
                        ? token.getMinWithdrawQuantity().max(DecimalUtil.toBigDecimal(response.getMinQuantity()))
                        : DecimalUtil.toBigDecimal(response.getMinQuantity()))
                .fee(token.getFee())
                .isEOS(tokenDetail.getType() == TokenTypeEnum.EOS_TOKEN)
                .needAddressTag(tokenDetail.getAddressNeedTag())
                .token(token)
                .tokenDetail(tokenDetail)
                .build();
    }

    public WithdrawQuotaResponse withdrawQuota(Header header, String tokenId, String chainType) {
        WithdrawalQuotaRequest request = WithdrawalQuotaRequest.newBuilder()
                .setBaseRequest(GrpcRequestUtil.getBaseRequest(header))
                .setAccountId(accountService.getMainAccountId(header))
                .setTokenId(tokenId)
                .setChainType(chainType)
                .build();
        WithdrawalQuotaResponse response = grpcWithdrawService.queryWithdrawQuota(request);
        TokenWithdrawLimitDTO withdrawLimitDTO = getLimitDTO(header.getOrgId(), tokenId, chainType, response);

        long currentTimestamp = System.currentTimeMillis();

        BigDecimal fee = withdrawLimitDTO.getFee().add(DecimalUtil.toBigDecimal(response.getPlatformFee()));
        boolean needConvert = true;
        boolean cannotGetConvertRate = false;
        BigDecimal convertFee = BigDecimal.ZERO, convertRate = BigDecimal.ZERO, totalFees = BigDecimal.ZERO;
        if (tokenId.equals(response.getMinerFeeTokenId())) {
            needConvert = false;
            convertRate = BigDecimal.ONE;
            convertFee = fee;
            totalFees = fee.add(DecimalUtil.toBigDecimal(response.getMaxMinerFee()));
        } else {
            TokenConvertRate tokenConvertRate = tokenConvertRateMapper.getByToken(header.getOrgId(), tokenId, response.getMinerFeeTokenId());
            if (tokenConvertRate == null) {
                BigDecimal rateUpRadio = DEFAULT_RATE_UP_RATIO;
                if (tokenId.equals("USDT")) {
                    rateUpRadio = DEFAULT_USDT_RATE_UP_RATIO;
                }
                convertRate = rateUpRadio.multiply(basicService.getFXRate(header.getOrgId(), response.getMinerFeeTokenId(), tokenId))
                        .setScale(CONVERT_RATE_SCALE, BigDecimal.ROUND_HALF_UP);
                if (convertRate.compareTo(BigDecimal.ZERO) == 0) {
                    log.error("{}:{} {} withdraw quotaInfo get a zero convertRate. feeTokenId:{}",
                            header.getOrgId(), header.getUserId(), tokenId, response.getMinerFeeTokenId());
                    tokenConvertRate = TokenConvertRate.builder().convertRate(convertRate).build();
                } else {
                    tokenConvertRate = TokenConvertRate.builder()
                            .orgId(header.getOrgId())
                            .tokenId(tokenId)
                            .convertTokenId(response.getMinerFeeTokenId())
                            .rateUpRatio(rateUpRadio)
                            .convertRate(convertRate)
                            .lastConvertRate(convertRate)
                            .created(System.currentTimeMillis())
                            .updated(System.currentTimeMillis())
                            .build();
                    tokenConvertRateMapper.insert(tokenConvertRate);
                }
            }
            convertRate = tokenConvertRate.getConvertRate();
            if (convertRate.compareTo(BigDecimal.ZERO) <= 0) {
                log.error("{}:{} {} withdraw quota get a zero convert rate, feeTokenId:{} set allowWithdraw = false",
                        header.getOrgId(), header.getUserId(), tokenId, response.getMinerFeeTokenId());
                cannotGetConvertRate = true;
            } else {
                if (tokenId.equals(response.getPlatformFeeTokenId())) {
                    convertFee = fee;
                    totalFees = fee.add(DecimalUtil.toBigDecimal(response.getMaxMinerFee()).multiply(convertRate));
                } else {
                    convertFee = fee.multiply(convertRate);
                    totalFees = fee.add(DecimalUtil.toBigDecimal(response.getMaxMinerFee())).multiply(convertRate);
                }
            }
        }
        // now we set suggestMinerFee as the minMinerFee value
        BigDecimal dayQuota = withdrawLimitDTO.getMaxWithdrawQuota();
        // 白名单用户不受额度限制
        boolean isInWithdrawWhiteList = isInWithdrawWhiteList(header.getOrgId(), tokenId, header.getUserId());
        if (isInWithdrawWhiteList) {
            dayQuota = DecimalUtil.toBigDecimal(response.getDayMaxQuota());
        }


        Integer frozenReason = checkWithdrawFrozen(header.getOrgId(), header.getUserId(), FrozenType.FROZEN_WITHDRAW);
        boolean userUnderFrozen = frozenReason > 0;
        boolean allowWithdraw = withdrawLimitDTO.isAllowWithdraw();
        if (userUnderFrozen) {
            allowWithdraw = false;
        } else if (!withdrawLimitDTO.getTokenDetail().getAllowWithdraw()) { //平台不允许提币
            allowWithdraw = false;
        } else if (isInWithdrawWhiteList) {
            allowWithdraw = true;
        } else {
            WithdrawWhiteAddress.Builder whiteAddressBuilder = WithdrawWhiteAddress.builder()
                    .orgId(header.getOrgId())
                    .userId(header.getUserId())
                    .tokenId(tokenId)
                    .status(1);
            if (!Strings.isNullOrEmpty(chainType)) {
                whiteAddressBuilder.chainType(chainType);
            }
            List<WithdrawWhiteAddress> whiteAddressList = withdrawWhiteAddressMapper.select(whiteAddressBuilder.build());
            if (allowWithdraw && CollectionUtils.isNotEmpty(whiteAddressList)) {
                allowWithdraw = true;
            } else if (withdrawLimitDTO.getToken().getAllowWithdraw() == 0) { //券商不允许提币
                allowWithdraw = isInWithdrawWhiteList;
            } else if (withdrawLimitDTO.getToken().getAllowWithdraw() == 1) { //券商允许提币
                boolean isInWithdrawBlackList = isInWithdrawBlackList(header.getOrgId(), tokenId, header.getUserId());
                SwitchStatus stopWithdrawSwtich = baseBizConfigService.getConfigSwitchStatus(header.getOrgId(),
                        BaseConfigConstants.WHOLE_SITE_CONTROL_SWITCH_GROUP, BaseConfigConstants.STOP_WITHDRAW_KEY);
                if (isInWithdrawBlackList || stopWithdrawSwtich.isOpen()) {
                    allowWithdraw = false;
                } else {
                    allowWithdraw = true;
                }
            }
        }
        if (response.getResult() == WithdrawalResult.FORBIDDEN) {
            allowWithdraw = false;
            frozenReason = 0;
        }

//        if (userUnderFrozen) {
//            allowWithdraw = false;
//        } else if (isInWithdrawWhiteList && withdrawLimitDTO.getToken().getAllowWithdraw() == 0
//                && withdrawLimitDTO.getTokenDetail().getAllowWithdraw()) {
//            allowWithdraw = true;
//        } else if (isInWithdrawBlackList(header.getOrgId(), tokenId, header.getUserId())
//                || baseBizConfigService.getConfigSwitchStatus(header.getOrgId(), BaseConfigConstants.WHOLE_SITE_CONTROL_SWITCH_GROUP, BaseConfigConstants.STOP_WITHDRAW_KEY).isOpen()) {
//            WithdrawWhiteAddress.Builder whiteAddressBuilder = WithdrawWhiteAddress.builder()
//                    .orgId(header.getOrgId())
//                    .userId(header.getUserId())
//                    .tokenId(tokenId)
//                    .status(1);
//            if (!Strings.isNullOrEmpty(chainType)) {
//                whiteAddressBuilder.chainType(chainType);
//            }
//            List<WithdrawWhiteAddress> whiteAddressList = withdrawWhiteAddressMapper.select(whiteAddressBuilder.build());
//            if (allowWithdraw && CollectionUtils.isNotEmpty(whiteAddressList)) {
//                allowWithdraw = true;
//            } else {
//                allowWithdraw = false;
//            }
//        }


        if (cannotGetConvertRate) {
            allowWithdraw = false;
            frozenReason = 12;
        }

        boolean needCheckKyc = true;
        BigDecimal needKycQuotaQuantity = BigDecimal.ZERO;
        String needKycQuotaUnit = "BTC";
        BigDecimal needKycQuantity = BigDecimal.ZERO;
        UserVerify userVerify = UserVerify.decrypt(userVerifyMapper.getByUserId(header.getUserId()));
        if (userVerify != null && userVerify.getVerifyStatus() == UserVerifyStatus.PASSED.value()) {
            needCheckKyc = false;
        } else {
            // 这里先默认就使用BTC做提币限制计价基准单位
            String needKycQuota = "";
            // 判断是否首笔
            int withdrawCount = statisticsOrgService.countByUserId(header.getOrgId(), header.getUserId() + "");
            if (withdrawCount == 0) {
                needKycQuota = commonIniService.getStringValueOrDefault(header.getOrgId(), BrokerServerConstants.FIRST_WITHDRAW_NEED_KYC_CONFIG, "1.BTC");
            } else {
                needKycQuota = commonIniService.getStringValueOrDefault(header.getOrgId(), BrokerServerConstants.WITHDRAW_NEED_KYC_CONFIG, "1.BTC");
            }
//            needKycQuotaQuantity = new BigDecimal(needKycQuota.split("\\.")[0]);
//            needKycQuotaUnit = needKycQuota.split("\\.")[1];
            needKycQuotaQuantity = new BigDecimal(splitWithdrawAmountConfig(needKycQuota));
            needKycQuotaUnit = splitWithdrawUnitConfig(needKycQuota);

            // 判断24小时内的提现，包括提现中和提现成功
            if (withdrawCount > 0) {
                // 查询用户24小时内的提现记录
                List<io.bhex.broker.server.model.WithdrawOrder> withdrawOrderList
                        = withdrawOrderMapper.queryWithdrawOrderWithIn24H(header.getUserId(), currentTimestamp - BrokerServerConstants.MILLISECONDS_WITHIN_24H);
                BigDecimal totalWithdrawBtc = BigDecimal.ZERO;
                for (io.bhex.broker.server.model.WithdrawOrder withdrawOrder : withdrawOrderList) {
                    if (withdrawOrder.getBtcValue() != null && !Strings.isNullOrEmpty(withdrawOrder.getCurrentFxRate())) {
                        totalWithdrawBtc = totalWithdrawBtc.add(withdrawOrder.getBtcValue());
                    } else {
                        Rate fxRate = basicService.getV3Rate(header.getOrgId(), withdrawOrder.getTokenId());
                        if (fxRate == null) {
                            log.error("withdraw quota info: {}:{} calculate total withdrawal quantity cannot get {} rate, use 0 for calculate", header.getOrgId(), header.getUserId(), tokenId);
                            totalWithdrawBtc = totalWithdrawBtc.add(BigDecimal.ZERO);
                        } else {
                            totalWithdrawBtc = totalWithdrawBtc.add(withdrawOrder.getQuantity().multiply(DecimalUtil.toBigDecimal(fxRate.getRatesMap().get("BTC"))));
                        }
                    }
                }

                int withdrawCheckIdCardNoNumberLimit = commonIniService.getIntValueOrDefault(header.getOrgId(), BrokerServerConstants.WITHDRAW_CHECK_ID_CARD_NO_NUMBER_CONFIG, 5);
                // String needCheckIdCardNoConfig = commonIniService.getStringValueOrDefault(header.getOrgId(), BrokerServerConstants.WITHDRAW_CHECK_ID_CARD_NO_CONFIG, "5.BTC");
//                    String needCheckIDCardQuotaQuantity = needKycQuota.split("\\.")[0];
//                    String needCheckIDCardQuotaUnit = needKycQuota.split("\\.")[1]; // BTC
                // String needCheckIDCardQuotaQuantity = splitWithdrawAmountConfig(needCheckIdCardNoConfig);
                // String needCheckIDCardQuotaUnit = splitWithdrawUnitConfig(needCheckIdCardNoConfig); // BTC
                BigDecimal needCheckIDCardQuotaQuantity = getUserNeedCheckIDCardQuotaQuantity(header.getOrgId(), header.getUserId());
                if (withdrawOrderList.size() >= withdrawCheckIdCardNoNumberLimit || totalWithdrawBtc.compareTo(needCheckIDCardQuotaQuantity) >= 0) {
                    // 当次提现一定需要KYC
                    needKycQuotaQuantity = BigDecimal.ZERO;
                } else {
                    if (needCheckIDCardQuotaQuantity.subtract(totalWithdrawBtc).compareTo(needKycQuotaQuantity) < 0) {
                        // 当次提现数量如果大于 needCheckIDCardQuotaQuantity-totalWithdrawBtc 则需要KYC
                        needKycQuotaQuantity = needCheckIDCardQuotaQuantity.subtract(totalWithdrawBtc);
                    }
//                    if (withdrawOrderList.size() >= withdrawCheckIdCardNoNumberLimit) {
//                        needKycQuotaQuantity = BigDecimal.ZERO;
//                    } else if (new BigDecimal(needCheckIDCardQuotaQuantity).subtract(totalWithdrawBtc).compareTo(needKycQuotaQuantity) < 0) {
//                        // 当次提现数量如果大于 needCheckIDCardQuotaQuantity-totalWithdrawBtc 则需要KYC
//                        needKycQuotaQuantity = new BigDecimal(needCheckIDCardQuotaQuantity).subtract(totalWithdrawBtc);
//                    }
                }
            }
            Rate fxRate = basicService.getV3Rate(header.getOrgId(), tokenId);
            if (fxRate == null || DecimalUtil.toBigDecimal(fxRate.getRatesMap().get("BTC")).compareTo(BigDecimal.ZERO) == 0) {
                log.error("withdraw quota info: {}:{} cannot get {} rate, current withdrawal will requires mandatory KYC", header.getOrgId(), header.getUserId(), tokenId);
                needKycQuantity = BigDecimal.ZERO;
            } else {
                needKycQuantity = needKycQuotaQuantity.divide(DecimalUtil.toBigDecimal(fxRate.getRatesMap().get("BTC")),
                        Math.min(8, withdrawLimitDTO.getPrecision()), BigDecimal.ROUND_DOWN);
            }
        }
        String tokenType = withdrawLimitDTO.getTokenDetail().getTokenType().name();
        if (tokenId.equalsIgnoreCase("USDT") && chainType.equalsIgnoreCase("ERC20")) {
            tokenType = TokenTypeEnum.ERC20_TOKEN.name();
        } else if (tokenId.equalsIgnoreCase("USDT") && chainType.equalsIgnoreCase("TRC20")) {
            tokenType = TokenTypeEnum.TRX_TOKEN.name();
        }
        return WithdrawQuotaResponse.newBuilder()
                .setAllowWithdraw(allowWithdraw)
                .setAvailable(DecimalUtil.toBigDecimal(response.getDayAvailable()).stripTrailingZeros().toPlainString())
                .setMinWithdrawQuantity(withdrawLimitDTO.getMinWithdrawQuantity().stripTrailingZeros().toPlainString())
                .setDayQuota(dayQuota.stripTrailingZeros().toPlainString())
                .setUsedQuota(DecimalUtil.toBigDecimal(response.getDayUsedQuota()).stripTrailingZeros().toPlainString())
                .setFeeTokenId(response.getPlatformFeeTokenId())
                .setFeeTokenName(basicService.getTokenName(header.getOrgId(), response.getPlatformFeeTokenId()))
                .setPlatformFee(DecimalUtil.toBigDecimal(response.getPlatformFee()).stripTrailingZeros().toPlainString())
                .setBrokerFee(withdrawLimitDTO.getFee().stripTrailingZeros().toPlainString())
                .setFee(fee.stripTrailingZeros().toPlainString())
                .setMinerFeeTokenId(response.getMinerFeeTokenId())
                .setMinerFeeTokenName(basicService.getTokenName(header.getOrgId(), response.getMinerFeeTokenId()))
                .setMinMinerFee(DecimalUtil.toBigDecimal(response.getMinMinerFee()).stripTrailingZeros().toPlainString())
                .setMaxMinerFee(DecimalUtil.toBigDecimal(response.getMaxMinerFee()).stripTrailingZeros().toPlainString())
                .setSuggestMinerFee(DecimalUtil.toBigDecimal(response.getSuggestMinerFee()).stripTrailingZeros().toPlainString())
                .setConvertRate(convertRate.stripTrailingZeros().toPlainString())
                .setConvertFee(convertFee.stripTrailingZeros().toPlainString())
                .setNeedConvert(needConvert)
                .setIsEos(withdrawLimitDTO.isEOS())
                .setMinPrecision(Math.min(8, withdrawLimitDTO.getPrecision()))
                .setNeedAddressTag(withdrawLimitDTO.isNeedAddressTag())
                .setNeedKycCheck(needCheckKyc)
                .setNeedKycQuotaQuantity(needKycQuotaQuantity.stripTrailingZeros().toPlainString())
                .setNeedKycQuotaUnit(needKycQuotaUnit)
                .setNeedKycQuantity(needKycQuantity.stripTrailingZeros().toPlainString())
                .setRefuseWithdrawReason(frozenReason)
                .setInternalTransferHasFee(response.getChargeInnerWithdrawFee())
                .setInternalTransferFee(DecimalUtil.toBigDecimal(response.getInnerWithdrawFee()).stripTrailingZeros().toPlainString())
                .setTokenType(tokenType)
                .addAllRiskBalance(response.getRiskBalancesList().stream()
                        .map(riskBalance -> WithdrawQuotaResponse.RiskBalance.newBuilder()
                                .setTokenId(riskBalance.getTokenId())
                                .setTokenName(basicService.getTokenName(header.getOrgId(), riskBalance.getTokenId()))
                                .setAmount(riskBalance.getAmount())
                                .setBtcAmount(riskBalance.getAmountBtc())
                                .setReason(riskBalance.getSubject().name())
                                .setCreatedAt(riskBalance.getCreatedAt())
                                .build())
                        .collect(Collectors.toList()))
                .build();
    }

    public BigDecimal getOrgNeedCheckIDCardQuotaQuantity(long orgId) {
        String needCheckIdCardNoConfig = commonIniService.getStringValueOrDefault(orgId, BrokerServerConstants.WITHDRAW_CHECK_ID_CARD_NO_CONFIG, "5.BTC");
        String needCheckIDCardQuotaQuantity = splitWithdrawAmountConfig(needCheckIdCardNoConfig);
        return new BigDecimal(needCheckIDCardQuotaQuantity);
    }

    public BigDecimal getUserNeedCheckIDCardQuotaQuantity(long orgId, long userId) {
        String needCheckIdCardNoConfig = commonIniService.getStringValueOrDefault(orgId, BrokerServerConstants.WITHDRAW_CHECK_ID_CARD_NO_CONFIG, "5.BTC");
        String needCheckIDCardQuotaQuantity = splitWithdrawAmountConfig(needCheckIdCardNoConfig);

        QueryMyLevelConfigResponse myVipLevelConfig = userLevelService.queryMyLevelConfig(orgId, userId, false, false);
        if (myVipLevelConfig != null && new BigDecimal(myVipLevelConfig.getWithdrawUpperLimitInBTC()).compareTo(new BigDecimal(needCheckIDCardQuotaQuantity)) > 0) {
            log.info("broker user:{} config : {} vip config : {}", userId, needCheckIDCardQuotaQuantity, myVipLevelConfig.getWithdrawUpperLimitInBTC());
            return new BigDecimal(myVipLevelConfig.getWithdrawUpperLimitInBTC());
        }
        return new BigDecimal(needCheckIDCardQuotaQuantity);
    }


    private boolean addressInWhiteList(Header header, String tokenId, String chainType, String address, String addressExt) {
        WithdrawWhiteAddress.Builder whiteAddressBuilder = WithdrawWhiteAddress.builder()
                .orgId(header.getOrgId())
                .userId(header.getUserId())
                .tokenId(tokenId)
                .status(1);
        if (!Strings.isNullOrEmpty(chainType)) {
            whiteAddressBuilder.chainType(chainType);
        }
        List<WithdrawWhiteAddress> whiteAddressList = withdrawWhiteAddressMapper.select(whiteAddressBuilder.build());
        for (WithdrawWhiteAddress withdrawWhiteAddress : whiteAddressList) {
            if (withdrawWhiteAddress.getAddress().equals(address)
                    && withdrawWhiteAddress.getAddressExt().equals(addressExt)) {
                return true;
            }
        }
        return false;
    }

    public WithdrawResponse withdraw(Header header, String tokenId, String chainType, String clientOrderId, Long addressId, String address, String addressExt,
                                     String withdrawQuantity, String minerFee, Boolean isAutoConvert, String showConvertRate,
                                     String tradePassword, Integer authTypeValue, Long verifyCodeOrderId, String verifyCode, String remarks,
                                     String userName, String userAddress) {

        if (!isAutoConvert) {
            //正常情况下只能是true
            log.error("Error: isAutoConvert is false！{},{}", header.getOrgId(), header.getUserId());
            throw new BrokerException(BrokerErrorCode.PARAM_INVALID);
        }

        //风控拦截 判断当前用户是否满足风控可提币条件
        riskControlBalanceService.checkRiskControlLimit(header.getOrgId(), header.getUserId(), FrozenType.FROZEN_WITHDRAW);

        // 校验提币数量-日限额
        WithdrawalQuotaRequest request = WithdrawalQuotaRequest.newBuilder()
                .setBaseRequest(GrpcRequestUtil.getBaseRequest(header))
                .setAccountId(accountService.getMainAccountId(header))
                .setTokenId(tokenId)
                .setChainType(chainType)
                .build();
        WithdrawalQuotaResponse response = grpcWithdrawService.queryWithdrawQuota(request);
        TokenWithdrawLimitDTO withdrawLimitDTO = getLimitDTO(header.getOrgId(), tokenId, chainType, response);

        boolean isInWithdrawWhiteList = isInWithdrawWhiteList(header.getOrgId(), tokenId, header.getUserId());
        SwitchStatus stopWithdrawSwitch = baseBizConfigService.getConfigSwitchStatus(header.getOrgId(),
                BaseConfigConstants.WHOLE_SITE_CONTROL_SWITCH_GROUP, BaseConfigConstants.STOP_WITHDRAW_KEY);
        boolean isInWithdrawBlackList = isInWithdrawBlackList(header.getOrgId(), tokenId, header.getUserId());
        Integer frozenReason = checkWithdrawFrozen(header.getOrgId(), header.getUserId(), FrozenType.FROZEN_WITHDRAW);
        boolean userUnderFrozen = frozenReason > 0;
        // 是否禁止提现
        if (response.getResult() == WithdrawalResult.FORBIDDEN
                || userUnderFrozen
                || (!withdrawLimitDTO.isAllowWithdraw() && !isInWithdrawWhiteList)
                || ((isInWithdrawBlackList || stopWithdrawSwitch.isOpen()) && !addressInWhiteList(header, tokenId, chainType, address, addressExt))) {
            return WithdrawResponse.newBuilder().setAllowWithdraw(false).setRefuseWithdrawReason(frozenReason).build();
        }

        WithdrawOrder originWithdrawOrder = withdrawOrderMapper.getByClientIdAndUserId(header.getOrgId(), header.getUserId(), clientOrderId);
        if (originWithdrawOrder != null) {
            log.warn("{}:{} submit withdraw request with repeated clientOrderId:{}", header.getOrgId(), header.getUserId(), clientOrderId);
            throw new BrokerException(BrokerErrorCode.REPEATED_SUBMIT_REQUEST);
        }

        WithdrawAddress withdrawAddress = withdrawAddressMapper.getById(addressId, header.getUserId());
        // 判定用户使用的提现地址是否属于伪造
        if (addressId != 0) {
            if (withdrawAddress == null) {
                log.warn("param addressId:{} cannot find from address records", addressId);
                throw new BrokerException(BrokerErrorCode.WITHDRAW_ADDRESS_ILLEGAL);
            }
            if (!address.equals(withdrawAddress.getAddress()) || !addressExt.equals(Strings.nullToEmpty(withdrawAddress.getAddressExt()))) {
                log.warn("address in db({}) not match with param(addressStr:{}, tag:{})", JsonUtil.defaultGson().toJson(withdrawAddress), address, addressExt);
                throw new BrokerException(BrokerErrorCode.WITHDRAW_ADDRESS_ILLEGAL);
            }
        } else {
            AuthType authType = AuthType.fromValue(authTypeValue);
            if (Strings.isNullOrEmpty(verifyCode)) {
                throw new BrokerException(BrokerErrorCode.VERIFY_CODE_CANNOT_BE_NULL);
            }
            if (authType == AuthType.GA) {
                userSecurityService.validGACode(header, header.getUserId(), verifyCode);
            } else {
                userSecurityService.validVerifyCode(header, authType, verifyCodeOrderId, verifyCode, VerifyCodeType.WITHDRAW);
            }
        }

        CheckAddressResponse checkAddressResponse = checkAddress(header, tokenId, chainType, address, addressExt);
        boolean addressIsUserId = checkAddressResponse.getAddressIsUserId();
        if (checkAddressResponse.getIsIllegal()) {
            log.warn("{}:{} withdraw address:{} is illegal", header.getOrgId(), header.getUserId(), address);
            throw new BrokerException(BrokerErrorCode.WITHDRAW_ADDRESS_ILLEGAL);
        }
        if (checkAddressResponse.getIsContractAddress()) {
            throw new BrokerException(BrokerErrorCode.UNSUPPORTED_CONTRACT_ADDRESS);
        }

        /*
         * 重置提币金额和矿工费的小数位，省得以后报错
         */
        int scale = Math.min(8, withdrawLimitDTO.getPrecision());
        int minerFeeScale = Math.min(8, withdrawLimitDTO.getMinerFeePrecision());
        withdrawQuantity = new BigDecimal(withdrawQuantity).setScale(scale, BigDecimal.ROUND_DOWN).toPlainString();
        minerFee = new BigDecimal(minerFee).setScale(minerFeeScale, BigDecimal.ROUND_UP).toPlainString();

        long currentTimestamp = System.currentTimeMillis();
        Rate fxRate = basicService.getV3Rate(header.getOrgId(), tokenId);
        if (fxRate == null) {
            log.error("{} withdraw: cannot get {} rate", header.getOrgId(), tokenId);
            throw new BrokerException(BrokerErrorCode.WITHDRAW_FAILED);
        }
        String fxRateInfo = "";
        try {
            fxRateInfo = JsonUtil.defaultProtobufJsonPrinter().print(fxRate);
        } catch (Exception e) {
            log.error("withdraw print fxRate error");
            throw new BrokerException(BrokerErrorCode.WITHDRAW_FAILED);
        }
        BigDecimal btcValue = new BigDecimal(withdrawQuantity).multiply(DecimalUtil.toBigDecimal(fxRate.getRatesMap().get("BTC"))).setScale(8, BigDecimal.ROUND_DOWN);

        if (!isInWithdrawWhiteList) {
            BigDecimal usedQuota = DecimalUtil.toBigDecimal(response.getDayUsedQuota());
            if (new BigDecimal(withdrawQuantity).add(usedQuota).compareTo(withdrawLimitDTO.getMaxWithdrawQuota()) > 0) {
                log.warn("{}:{} withdrawal total amount({} + {}) grant then dayQuota({})", header.getOrgId(),
                        header.getUserId(), withdrawQuantity, usedQuota, withdrawLimitDTO.getMaxWithdrawQuota());
                throw new BrokerException(BrokerErrorCode.WITHDRAW_AMOUNT_MAX_LIMIT);
            }
        }

        // 校验提币数量-最小提币数量
        if (new BigDecimal(withdrawQuantity).compareTo(withdrawLimitDTO.getMinWithdrawQuantity()) < 0) {
            log.warn("{}:{} withdrawal amount {} is less than min withdraw quantity {}", header.getOrgId(), header.getUserId(), withdrawQuantity, withdrawLimitDTO.getMinWithdrawQuantity());
            throw new BrokerException(BrokerErrorCode.WITHDRAW_AMOUNT_ILLEGAL);
        }

        // 校验资金密码
        VerifyTradePasswordResponse verifyTradePwdResponse = userSecurityService.verifyTradePassword(header, tradePassword);
        if (verifyTradePwdResponse.getRet() != 0) {
            throw new BrokerException(BrokerErrorCode.fromCode(verifyTradePwdResponse.getRet()));
        }
        BigDecimal convertQuantity, arriveQuantity, convertRate;
        //避免设置不合理的精度问题（收取的费用一律向上取取整，用户到账一律向下取整）
        BigDecimal platformFee = DecimalUtil.toBigDecimal(response.getPlatformFee());
        BigDecimal brokerFee = withdrawLimitDTO.getFee();
        BigDecimal minerFeeBigDecimal = new BigDecimal(minerFee);
        if (checkAddressResponse.getIsInnerAddress()) {
            if (response.getChargeInnerWithdrawFee()) {
                convertRate = BigDecimal.ONE;
                convertQuantity = BigDecimal.ZERO;
                BigDecimal innerWithdrawFee = DecimalUtil.toBigDecimal(response.getInnerWithdrawFee());
                arriveQuantity = new BigDecimal(withdrawQuantity).subtract(innerWithdrawFee);
            } else {
                convertRate = BigDecimal.ONE;
                convertQuantity = BigDecimal.ZERO;
                arriveQuantity = new BigDecimal(withdrawQuantity);
            }
        } else {
            // 计算用户的到账金额，和为了抵扣非同币种的手续费需要额外付出的币金额
            if (tokenId.equals(response.getMinerFeeTokenId())) {
                isAutoConvert = false;
                convertRate = BigDecimal.ONE;
                convertQuantity = BigDecimal.ZERO;
                platformFee = platformFee.setScale(scale, RoundingMode.UP);
                brokerFee = brokerFee.setScale(scale, RoundingMode.UP);
                minerFeeBigDecimal = minerFeeBigDecimal.setScale(scale, RoundingMode.UP);
                arriveQuantity = new BigDecimal(withdrawQuantity)
                        .subtract(platformFee) // 平台手续费
                        .subtract(brokerFee)  // 券商手续费
                        .subtract(minerFeeBigDecimal) // 矿工费
                        .setScale(scale, RoundingMode.DOWN);
            } else {
                TokenConvertRate tokenConvertRate = tokenConvertRateMapper.getByToken(header.getOrgId(), tokenId, response.getMinerFeeTokenId());
                convertRate = tokenConvertRate.getConvertRate();
                if (convertRate.compareTo(BigDecimal.ZERO) <= 0 || tokenConvertRate.getLastConvertRate().compareTo(BigDecimal.ZERO) <= 0) {
                    log.error("{}:{} {} withdraw get a zero convert rate. feeTokenId:{} set allowWithdraw false",
                            header.getOrgId(), header.getUserId(), tokenId, response.getMinerFeeTokenId());
                    return WithdrawResponse.newBuilder().setAllowWithdraw(false).setRefuseWithdrawReason(frozenReason).build();
                }
                if (new BigDecimal(showConvertRate).compareTo(convertRate) != 0 && new BigDecimal(showConvertRate).compareTo(tokenConvertRate.getLastConvertRate()) != 0) {
                    log.error("{}:{} {}  withdraw get convert rate from db {} not equals it showed to user {}",
                            header.getOrgId(), header.getUserId(), tokenId, convertRate.toPlainString(), showConvertRate);
                    throw new BrokerException(BrokerErrorCode.WITHDRAW_FAILED);
                }

                // 这里指的是当手续费token和提币的token不一致的时候，是否有券商垫付手续费, isAutoConvert=true指的是有券商垫付要扣除的费用
                if (isAutoConvert) {
                    if (tokenId.equals(response.getPlatformFeeTokenId())) {
                        convertQuantity = minerFeeBigDecimal.multiply(tokenConvertRate.getConvertRate()).setScale(scale, RoundingMode.UP);
                        platformFee = platformFee.setScale(scale, RoundingMode.UP);
                        brokerFee = brokerFee.setScale(scale, RoundingMode.UP);
                        arriveQuantity = new BigDecimal(withdrawQuantity)
                                .subtract(platformFee) // 平台手续费
                                .subtract(brokerFee) // 券商手续费
                                .subtract(convertQuantity) // 抵扣矿工费 = 矿工费 * 转换比例
                                .setScale(scale, RoundingMode.DOWN);
                    } else {
                        convertQuantity = (platformFee.add(brokerFee).add(minerFeeBigDecimal))
                                .multiply(tokenConvertRate.getConvertRate())
                                .setScale(scale, RoundingMode.UP);
                        arriveQuantity = new BigDecimal(withdrawQuantity)
                                .subtract(convertQuantity) // 抵扣费用 = (平台手续费 + 券商手续费 + 矿工费) * 转换比例
                                .setScale(scale, RoundingMode.DOWN);
                    }
                } else {
                    convertQuantity = BigDecimal.ZERO;
                    if (tokenId.equals(response.getPlatformFeeTokenId())) {
                        platformFee = platformFee.setScale(scale, RoundingMode.UP);
                        brokerFee = brokerFee.setScale(scale, RoundingMode.UP);
                        arriveQuantity = new BigDecimal(withdrawQuantity)
                                .subtract(platformFee) // 平台手续费
                                .subtract(brokerFee) // 券商手续费
                                .setScale(scale, RoundingMode.DOWN);
                    } else {
                        arriveQuantity = new BigDecimal(withdrawQuantity);
                    }
                }
            }
        }
        if (arriveQuantity.compareTo(BigDecimal.ZERO) <= 0) {
            log.error("Error: get a negative withdraw quantity after calculate");
            throw new BrokerException(BrokerErrorCode.WITHDRAW_AMOUNT_ILLEGAL);
        }

        /*
         * withdraw check
         */
        WithdrawCheckResult withdrawCheckResult = withdrawProcessCheck(header, tokenId, withdrawQuantity, fxRate);
        User user = userService.getUser(header.getUserId());
        if (user.getUserType() == UserType.INSTITUTIONAL_USER.value()) {
            withdrawCheckResult.setNeedCheckIdCard(false);
        }

        String requestId = CryptoUtil.getRandomCode(32);
        WithdrawPreApply withdrawPreApply = WithdrawPreApply.builder()
                .requestId(requestId)
                .orgId(header.getOrgId())
                .userId(header.getUserId())
                .accountId(accountService.getMainAccountId(header))
                .addressIsUserId(addressIsUserId ? 1 : 0)
                .addressId(addressId)
                .address(address)
                .addressExt(addressExt)
                .isInnerAddress(checkAddressResponse.getIsInnerAddress() ? 1 : 0)
                .clientOrderId(clientOrderId)
                .tokenId(tokenId)
                .chainType(chainType)
                .feeTokenId(response.getPlatformFeeTokenId())
                .platformFee(platformFee)
                .brokerFee(brokerFee)
                .minerFeeTokenId(response.getMinerFeeTokenId())
                .minerFee(minerFeeBigDecimal)
                .isAutoConvert(isAutoConvert ? 1 : 0) // 这里指的是当手续费token和提币的token不一致的时候，是否由券商垫付手续费
                .convertRate(convertRate)
                .convertQuantity(convertQuantity)
                .quantity(new BigDecimal(withdrawQuantity))
                .arrivalQuantity(arriveQuantity)
                .innerWithdrawFee(DecimalUtil.toBigDecimal(response.getInnerWithdrawFee()))
                .needBrokerAudit(withdrawCheckResult.getNeedBrokerAudit() ? 1 : 0)
                .needCheckIdCardNo(withdrawCheckResult.getNeedCheckIdCard() ? 1 : 0)
                .cardNo("")
                .hasCheckIdCardNo(0)
                .currentFxRate(fxRateInfo)
                .btcValue(btcValue)
                .platform(header.getPlatform().name())
                .ip(header.getRemoteIp())
                .userAgent(header.getUserAgent())
                .language(header.getLanguage())
                .appBaseHeader(GrpcHeaderUtil.getAppBaseInfo(header))
                .created(currentTimestamp)
                .updated(currentTimestamp)
                .expired(currentTimestamp + BrokerServerConstants.WITHDRAW_PRE_APPLY_EFFECTIVE_MILLISECONDS)
                .remarks(buildJsonRemarks(remarks, userName, userAddress))
                .build();
        withdrawPreApplyMapper.insertRecord(withdrawPreApply);

        AuthType authType = AuthType.fromValue(authTypeValue);
        if (authType != AuthType.GA) {
            if (authType == AuthType.MOBILE) {
                verifyCodeService.invalidVerifyCode(header, VerifyCodeType.WITHDRAW, verifyCodeOrderId, user.getNationalCode() + user.getMobile());
            } else if (authType == AuthType.EMAIL) {
                verifyCodeService.invalidVerifyCode(header, VerifyCodeType.WITHDRAW, verifyCodeOrderId, user.getEmail());
            }
        }

        if (withdrawCheckResult.getNeedCheckIdCard()) {
            return WithdrawResponse.newBuilder().setAllowWithdraw(true)
                    .setRequestId(requestId).setAddressIsUserId(addressIsUserId)
                    .setOrder(getWithdrawOrder(header, withdrawPreApply))
                    .setTimeLeft(1800).setNeedCheckIdCardNo(true).build();
        } else {
            WithdrawalRequest withdrawalRequest = WithdrawalRequest.newBuilder()
                    .setBaseRequest(GrpcRequestUtil.getBaseRequest(header))
                    .setAccountId(accountService.getMainAccountId(header))
                    .setTokenId(tokenId)
                    .setChainType(chainType)
                    .setClientWithdrawalId(clientOrderId)
                    .setTargetOrgId(header.getOrgId())
                    .setAddress(address)
                    .setAddressExt(Strings.nullToEmpty(addressExt))
                    .setQuantity(DecimalUtil.fromBigDecimal(new BigDecimal(withdrawQuantity)))
                    .setArriveQuantity(DecimalUtil.fromBigDecimal(arriveQuantity))
                    .setFeeTokenId(response.getPlatformFeeTokenId())
                    .setPlatformFee(DecimalUtil.fromBigDecimal(platformFee))
                    .setBrokerFee(DecimalUtil.fromBigDecimal(brokerFee))
                    .setMinerTokenId(response.getMinerFeeTokenId())
                    .setMinerFee(DecimalUtil.fromBigDecimal(minerFeeBigDecimal))
                    .setIsBrokerAutoConvert(isAutoConvert)
                    .setConvertRate(DecimalUtil.fromBigDecimal(convertRate))
                    .setBrokerConvertPaidQuantity(DecimalUtil.fromBigDecimal(convertQuantity))
                    .setLanguage(header.getLanguage())
                    .setAuditStatus(withdrawCheckResult.getNeedBrokerAudit() ? WithdrawalBrokerAuditEnum.BROKER_AUDITING : WithdrawalBrokerAuditEnum.NO_NEED)
                    .setConfirmationCodeType(user.getRegisterType() == RegisterType.MOBILE.value() ? ConfirmationCodeType.MOBILE : ConfirmationCodeType.EMAIL)
                    .setInnerWithdrawFee(response.getInnerWithdrawFee())
                    .setIsUidWithdraw(addressIsUserId)
                    .setRemarks(buildJsonRemarks(remarks, userName, userAddress))
                    .build();
            WithdrawalResponse withdrawalResponse = grpcWithdrawService.withdraw(withdrawalRequest);
            try {
                // update tb_withdraw_address set request_num = request_num + 1 where id = #{addressId} and user_id=#{userId}
                this.withdrawAddressMapper.updateRequestNum(header.getUserId(), tokenId, address);
            } catch (Exception ex) {
                log.info("Update request num fail !!!! userId {} clientOrderId {}", header.getUserId(), clientOrderId);
            }
            return WithdrawResponse.newBuilder()
                    .setAllowWithdraw(true).setRequestId(requestId)
                    .setAddressIsUserId(addressIsUserId)
                    .setCodeOrderId(withdrawalResponse.getCodeSeqId())
                    .setOrder(getWithdrawOrder(header, withdrawPreApply))
                    .setTimeLeft(1800).setNeedCheckIdCardNo(false).build();
        }
    }

    private io.bhex.broker.grpc.withdraw.WithdrawOrder getWithdrawOrder(Header header, String requestId) {
        WithdrawPreApply preApply = withdrawPreApplyMapper.getByRequestIdAndUserId(requestId, header.getOrgId(), header.getUserId());
        return getWithdrawOrder(header, preApply);
    }

    private io.bhex.broker.grpc.withdraw.WithdrawOrder getWithdrawOrder(Header header, WithdrawPreApply apply) {
        return io.bhex.broker.grpc.withdraw.WithdrawOrder.newBuilder()
                .setUserId(apply.getUserId())
                .setAccountId(apply.getAccountId())
                .setTokenId(apply.getTokenId())
                .setAddress(apply.getAddress())
                .setAddressExt(apply.getAddressExt())
                .setQuantity(apply.getQuantity().stripTrailingZeros().toPlainString())
                .setArriveQuantity(apply.getArrivalQuantity().stripTrailingZeros().toPlainString())
                .setTotalFee(WithdrawFee.newBuilder()
                        .setFeeTokenId(apply.getTokenId())
                        .setFeeTokenName(basicService.getTokenName(header.getOrgId(), apply.getTokenId()))
                        .setFee((apply.getQuantity().subtract(apply.getArrivalQuantity()).stripTrailingZeros().toPlainString()))
                        .build())
                .setChainType(apply.getChainType())
                .build();
    }

    private io.bhex.broker.grpc.withdraw.WithdrawOrder getWithdrawOrder(Header header, WithdrawOrder order, Long orderId) {
        return io.bhex.broker.grpc.withdraw.WithdrawOrder.newBuilder()
                .setOrderId(orderId)
                .setUserId(order.getUserId())
                .setAccountId(order.getAccountId())
                .setTokenId(order.getTokenId())
                .setAddress(order.getAddress())
                .setAddressExt(order.getAddressExt())
                .setQuantity(order.getQuantity().stripTrailingZeros().toPlainString())
                .setArriveQuantity(order.getArrivalQuantity().stripTrailingZeros().toPlainString())
                .setTotalFee(WithdrawFee.newBuilder()
                        .setFeeTokenId(order.getTokenId())
                        .setFeeTokenName(basicService.getTokenName(header.getOrgId(), order.getTokenId()))
                        .setFee((order.getQuantity().subtract(order.getArrivalQuantity()).stripTrailingZeros().toPlainString()))
                        .build())
                .setChainType(order.getChainType())
                .build();
    }

    @WithdrawNotify
    public OrgWithdrawResponse orgWithdraw(Header header, String tokenId, String chainType, String clientOrderId, String address, String addressExt,
                                           String withdrawQuantity, String minerFee, Boolean isAutoConvert, String showConvertRate, String tradePassword, String remarks) {
        if (header.getUserId() == 842145604923681280L) { //1.禁止这个用户一切资金操作 7070的账号
            throw new BrokerException(BrokerErrorCode.FEATURE_SUSPENDED);
        }
        boolean orgApiWithdrawSwitch = commonIniService.getBooleanValueOrDefault(header.getOrgId(), "orgApiWithdraw", false);
        if (!orgApiWithdrawSwitch) {
            throw new BrokerException(BrokerErrorCode.FEATURE_SUSPENDED);
        }

        //风控拦截 判断当前用户是否满足风控可提币条件
        riskControlBalanceService.checkRiskControlLimit(header.getOrgId(), header.getUserId(), FrozenType.FROZEN_WITHDRAW);

        // 校验提币数量-日限额
        WithdrawalQuotaRequest request = WithdrawalQuotaRequest.newBuilder()
                .setBaseRequest(GrpcRequestUtil.getBaseRequest(header))
                .setAccountId(accountService.getMainAccountId(header))
                .setTokenId(tokenId)
                .setChainType(chainType)
                .build();
        WithdrawalQuotaResponse response = grpcWithdrawService.queryWithdrawQuota(request);
        TokenWithdrawLimitDTO withdrawLimitDTO = getLimitDTO(header.getOrgId(), tokenId, chainType, response);

        Integer frozenReason = checkWithdrawFrozen(header.getOrgId(), header.getUserId(), FrozenType.FROZEN_WITHDRAW);
        boolean userUnderFrozen = frozenReason > 0;
        boolean isInWithdrawWhiteList = isInWithdrawWhiteList(header.getOrgId(), tokenId, header.getUserId());
        SwitchStatus stopWithdrawSwitch = baseBizConfigService.getConfigSwitchStatus(header.getOrgId(),
                BaseConfigConstants.WHOLE_SITE_CONTROL_SWITCH_GROUP, BaseConfigConstants.STOP_WITHDRAW_KEY);
        boolean isInWithdrawBlackList = isInWithdrawBlackList(header.getOrgId(), tokenId, header.getUserId());
        // 是否禁止提现
        if (response.getResult() == WithdrawalResult.FORBIDDEN
                || userUnderFrozen
                || (!withdrawLimitDTO.isAllowWithdraw() && !isInWithdrawWhiteList)
                || ((isInWithdrawBlackList || stopWithdrawSwitch.isOpen()) && !addressInWhiteList(header, tokenId, chainType, address, addressExt))) {
            return OrgWithdrawResponse.newBuilder().setAllowWithdraw(false).setRefuseWithdrawReason(frozenReason).build();
        }


        WithdrawOrder originWithdrawOrder = withdrawOrderMapper.getByClientIdAndUserId(header.getOrgId(), header.getUserId(), clientOrderId);
        if (originWithdrawOrder != null) {
            log.warn("orgApiWithdraw {}:{} submit withdraw request with repeated clientOrderId:{}", header.getOrgId(), header.getUserId(), clientOrderId);
            throw new BrokerException(BrokerErrorCode.REPEATED_SUBMIT_REQUEST);
        }

        CheckAddressResponse checkAddressResponse = checkAddress(header, tokenId, chainType, address, addressExt);
        if (checkAddressResponse.getIsIllegal()) {
            log.warn("orgApiWithdraw {}::{} withdraw address:{} is illegal", header.getOrgId(), header.getUserId(), address);
            throw new BrokerException(BrokerErrorCode.WITHDRAW_ADDRESS_ILLEGAL);
        }
        if (checkAddressResponse.getIsContractAddress()) {
            throw new BrokerException(BrokerErrorCode.UNSUPPORTED_CONTRACT_ADDRESS);
        }
        boolean addressIsUserId = checkAddressResponse.getAddressIsUserId();
        /*
         * 重置提币金额和矿工费的小数位，省得以后报错
         */
        int scale = Math.min(8, withdrawLimitDTO.getPrecision());
        int minerFeeScale = Math.min(8, withdrawLimitDTO.getMinerFeePrecision());
        withdrawQuantity = new BigDecimal(withdrawQuantity).setScale(scale, BigDecimal.ROUND_DOWN).toPlainString();
        minerFee = new BigDecimal(minerFee).setScale(minerFeeScale, BigDecimal.ROUND_UP).toPlainString();

        long currentTimestamp = System.currentTimeMillis();
        Rate fxRate = basicService.getV3Rate(header.getOrgId(), tokenId);
        if (fxRate == null) {
            log.error("{} withdraw: cannot get {} rate", header.getOrgId(), tokenId);
            throw new BrokerException(BrokerErrorCode.WITHDRAW_FAILED);
        }
        String fxRateInfo = "";
        try {
            fxRateInfo = JsonUtil.defaultProtobufJsonPrinter().print(fxRate);
        } catch (Exception e) {
            log.error("withdraw print faRate error");
            throw new BrokerException(BrokerErrorCode.WITHDRAW_FAILED);
        }
        BigDecimal btcValue = new BigDecimal(withdrawQuantity).multiply(DecimalUtil.toBigDecimal(fxRate.getRatesMap().get("BTC"))).setScale(8, BigDecimal.ROUND_UP);

        if (!isInWithdrawWhiteList(header.getOrgId(), tokenId, header.getUserId())) {
            BigDecimal usedQuota = DecimalUtil.toBigDecimal(response.getDayUsedQuota());
            if (new BigDecimal(withdrawQuantity).add(usedQuota).compareTo(withdrawLimitDTO.getMaxWithdrawQuota()) > 0) {
                log.warn("orgApiWithdraw {}:{} {} withdrawal total amount({} + {}) grant then dayQuota({})",
                        header.getOrgId(), header.getUserId(), tokenId, withdrawQuantity, usedQuota, withdrawLimitDTO.getMaxWithdrawQuota());
                throw new BrokerException(BrokerErrorCode.WITHDRAW_AMOUNT_MAX_LIMIT);
            }
        }

        // 校验提币数量-最小提币数量
        if (new BigDecimal(withdrawQuantity).compareTo(withdrawLimitDTO.getMinWithdrawQuantity()) < 0) {
            log.warn("orgApiWithdraw {}:{} {} withdrawal amount {} is less than min withdraw quantity {}",
                    header.getOrgId(), header.getUserId(), tokenId, withdrawQuantity, withdrawLimitDTO.getMinWithdrawQuantity());
            throw new BrokerException(BrokerErrorCode.WITHDRAW_AMOUNT_ILLEGAL);
        }

        // 校验资金密码
        VerifyTradePasswordResponse verifyTradePwdResponse = userSecurityService.verifyTradePassword(header, tradePassword);
        if (verifyTradePwdResponse.getRet() != 0) {
            throw new BrokerException(BrokerErrorCode.fromCode(verifyTradePwdResponse.getRet()));
        }

        BigDecimal convertQuantity, arriveQuantity, convertRate;
        if (checkAddressResponse.getIsInnerAddress()) {
            if (response.getChargeInnerWithdrawFee()) {
                convertRate = BigDecimal.ONE;
                convertQuantity = BigDecimal.ZERO;
                BigDecimal innerWithdrawFee = DecimalUtil.toBigDecimal(response.getInnerWithdrawFee());
                arriveQuantity = new BigDecimal(withdrawQuantity).subtract(innerWithdrawFee);
            } else {
                convertRate = BigDecimal.ONE;
                convertQuantity = BigDecimal.ZERO;
                arriveQuantity = new BigDecimal(withdrawQuantity);
            }
        } else {
            // 计算用户的到账金额，和为了抵扣非同币种的手续费需要额外付出的币金额
            if (tokenId.equals(response.getMinerFeeTokenId())) {
                isAutoConvert = false;
                convertRate = BigDecimal.ONE;
                convertQuantity = BigDecimal.ZERO;
                arriveQuantity = new BigDecimal(withdrawQuantity)
                        .subtract(DecimalUtil.toBigDecimal(response.getPlatformFee())) // 平台手续费
                        .subtract(withdrawLimitDTO.getFee())  // 券商手续费
                        .subtract(new BigDecimal(minerFee)) // 矿工费
                        .setScale(scale, BigDecimal.ROUND_DOWN);
            } else {
                TokenConvertRate tokenConvertRate = tokenConvertRateMapper.getByToken(header.getOrgId(), tokenId, response.getMinerFeeTokenId());
                convertRate = tokenConvertRate.getConvertRate();
                if (convertRate.compareTo(BigDecimal.ZERO) <= 0 || tokenConvertRate.getLastConvertRate().compareTo(BigDecimal.ZERO) <= 0) {
                    log.warn("orgApiWithdraw {}:{} {} withdraw get a zero convert rate. set allowWithdraw false",
                            header.getOrgId(), header.getUserId(), tokenId);
                    return OrgWithdrawResponse.newBuilder().setAllowWithdraw(false).setRefuseWithdrawReason(frozenReason).build();
                }
                if (new BigDecimal(showConvertRate).compareTo(convertRate) != 0 && new BigDecimal(showConvertRate).compareTo(tokenConvertRate.getLastConvertRate()) != 0) {
                    log.error("orgApiWithdraw {}:{} {} withdraw get convert rate from db {} not equals it showed to user {}",
                            header.getOrgId(), header.getUserId(), tokenId, convertRate.toPlainString(), showConvertRate);
                    throw new BrokerException(BrokerErrorCode.WITHDRAW_FAILED);
                }

                // 这里指的是当手续费token和提币的token不一致的时候，是否有券商垫付手续费, isAutoConvert=true指的是有券商垫付要扣除的费用
                if (isAutoConvert) {
                    if (tokenId.equals(response.getPlatformFeeTokenId())) {
                        convertQuantity = new BigDecimal(minerFee).multiply(tokenConvertRate.getConvertRate()).setScale(scale, BigDecimal.ROUND_UP);
                        arriveQuantity = new BigDecimal(withdrawQuantity)
                                .subtract(DecimalUtil.toBigDecimal(response.getPlatformFee())) // 平台手续费
                                .subtract(withdrawLimitDTO.getFee()) // 券商手续费
                                .subtract(convertQuantity) // 抵扣矿工费 = 矿工费 * 转换比例
                                .setScale(scale, BigDecimal.ROUND_DOWN);
                    } else {
                        convertQuantity = (DecimalUtil.toBigDecimal(response.getPlatformFee()).add(withdrawLimitDTO.getFee()).add(new BigDecimal(minerFee)))
                                .multiply(tokenConvertRate.getConvertRate())
                                .setScale(scale, BigDecimal.ROUND_UP);
                        arriveQuantity = new BigDecimal(withdrawQuantity)
                                .subtract(convertQuantity) // 抵扣费用 = (平台手续费 + 券商手续费 + 矿工费) * 转换比例
                                .setScale(scale, BigDecimal.ROUND_DOWN);
                    }
                } else {
                    convertQuantity = BigDecimal.ZERO;
                    if (tokenId.equals(response.getPlatformFeeTokenId())) {
                        arriveQuantity = new BigDecimal(withdrawQuantity)
                                .subtract(DecimalUtil.toBigDecimal(response.getPlatformFee())) // 平台手续费
                                .subtract(withdrawLimitDTO.getFee()) // 券商手续费
                                .setScale(scale, BigDecimal.ROUND_DOWN);
                    } else {
                        arriveQuantity = new BigDecimal(withdrawQuantity);
                    }
                }
            }
        }
        if (arriveQuantity.compareTo(BigDecimal.ZERO) <= 0) {
            log.error("Error: get a negative withdraw quantity after calculate");
            throw new BrokerException(BrokerErrorCode.WITHDRAW_AMOUNT_ILLEGAL);
        }

        boolean needBrokerAudit = forceBrokerAudit(header.getOrgId(), header.getUserId());
        WithdrawOrder withdrawOrder = WithdrawOrder.builder()
                .orgId(header.getOrgId())
                .userId(header.getUserId())
                .accountId(accountService.getMainAccountId(header))
                .addressId(0L)
                .address(address)
                .addressExt(addressExt)
                .isInnerAddress(checkAddressResponse.getIsInnerAddress() ? 1 : 0)
                .orderId(0L)
                .tokenId(tokenId)
                .chainType(chainType)
                .clientOrderId(clientOrderId)
                .feeTokenId(response.getPlatformFeeTokenId())
                .platformFee(DecimalUtil.toBigDecimal(response.getPlatformFee()))
                .brokerFee(withdrawLimitDTO.getFee())
                .minerFeeTokenId(response.getMinerFeeTokenId())
                .minerFee(new BigDecimal(minerFee))
                .isAutoConvert(isAutoConvert ? 1 : 0) // 这里指的是当手续费token和提币的token不一致的时候，是否由券商垫付手续费
                .convertRate(convertRate)
                .convertQuantity(convertQuantity)
                .quantity(new BigDecimal(withdrawQuantity))
                .arrivalQuantity(arriveQuantity)
                .status(WithdrawalBrokerAuditEnum.NO_NEED.getNumber())
                .orderStatus(WithdrawalStatus.AUDITING_STATUS_VALUE)
                .needBrokerAudit(needBrokerAudit ? 1 : 0)
                .needCheckCardNo(0)
                .cardNo("")
                .platform(header.getPlatform().name())
                .ip(header.getRemoteIp())
                .userAgent(header.getUserAgent())
                .language(header.getLanguage())
                .appBaseHeader(GrpcHeaderUtil.getAppBaseInfo(header))
                .currentFxRate(fxRateInfo)
                .btcValue(btcValue)
                .created(currentTimestamp)
                .updated(currentTimestamp)
                .remarks(remarks)
                .build();
        withdrawOrderMapper.insertSelective(withdrawOrder);

        // 处理提现请求
        long signTime = System.currentTimeMillis() / 1000;
        String signNonce = UUID.randomUUID().toString();
        WithdrawalRequestSign withdrawSign = WithdrawalRequestSign.newBuilder()
                .setOrganizationId(header.getOrgId())
                .setClientWithdrawalId(clientOrderId)
                .setAccountId(accountService.getMainAccountId(header))
                .setTokenId(tokenId)
                .setMinerTokenId(response.getMinerFeeTokenId())
                .setFeeTokenId(response.getPlatformFeeTokenId())
                .setAddress(address)
                .setArriveQuantity(DecimalUtil.toTrimString(arriveQuantity))
                .setMinerFee(DecimalUtil.toTrimString(new BigDecimal(minerFee)))
                .setPlatformFee(DecimalUtil.toTrimString(response.getPlatformFee()))
                .setBrokerFee(DecimalUtil.toTrimString(withdrawLimitDTO.getFee()))
                .setIsBrokerAutoConvert(isAutoConvert)
                .setBrokerConvertPaidQuantity(DecimalUtil.toTrimString(convertQuantity))
                .setQuantity(DecimalUtil.toTrimString(new BigDecimal(withdrawQuantity)))
                .setConvertRate(DecimalUtil.toTrimString(convertRate))
                .setSignTime(signTime)
                .setSignNonce(signNonce)
                .build();

        String sign = "";
        try {
            sign = signUtils.sign(withdrawSign.toByteArray(), header.getOrgId());
        } catch (Exception e) {
            throw new BrokerException(BrokerErrorCode.WITHDRAW_FAILED);
        }

        WithdrawalRequest withdrawalRequest = WithdrawalRequest.newBuilder()
                .setBaseRequest(GrpcRequestUtil.getBaseRequest(header))
                .setAccountId(accountService.getMainAccountId(header))
                .setTokenId(tokenId)
                .setChainType(chainType)
                .setClientWithdrawalId(clientOrderId)
                .setTargetOrgId(header.getOrgId())
                .setAddress(address)
                .setAddressExt(Strings.nullToEmpty(addressExt))
                .setQuantity(DecimalUtil.fromBigDecimal(new BigDecimal(withdrawQuantity)))
                .setArriveQuantity(DecimalUtil.fromBigDecimal(arriveQuantity))
                .setFeeTokenId(response.getPlatformFeeTokenId())
                .setPlatformFee(response.getPlatformFee())
                .setBrokerFee(DecimalUtil.fromBigDecimal(withdrawLimitDTO.getFee()))
                .setMinerTokenId(response.getMinerFeeTokenId())
                .setMinerFee(DecimalUtil.fromBigDecimal(new BigDecimal(minerFee)))
                .setIsBrokerAutoConvert(isAutoConvert)
                .setConvertRate(DecimalUtil.fromBigDecimal(convertRate))
                .setBrokerConvertPaidQuantity(DecimalUtil.fromBigDecimal(convertQuantity))
                .setLanguage(header.getLanguage())
                // .setAuditStatus(WithdrawalBrokerAuditEnum.NO_NEED)
                .setAuditStatus(needBrokerAudit ? WithdrawalBrokerAuditEnum.BROKER_AUDITING : WithdrawalBrokerAuditEnum.NO_NEED)
                .setNotCheckVerifyCode(true)
                .setInnerWithdrawFee(response.getInnerWithdrawFee())
                .setIsUidWithdraw(addressIsUserId)
                .setSignTime(signTime)
                .setSignNonce(signNonce)
                .setSignBroker(sign)
                .setRemarks(buildJsonRemarks(remarks, "", ""))
                .build();
        WithdrawalResponse withdrawalResponse = grpcWithdrawService.withdraw(withdrawalRequest);
        WithdrawOrder updateObj = WithdrawOrder.builder()
                .id(withdrawOrder.getId())
                .orderId(withdrawalResponse.getWithdrawalOrderId())
                .build();
        if (withdrawalResponse.getStatus() == WithdrawalStatus.BROKER_AUDITING_STATUS) {
            updateObj.setNeedBrokerAudit(1);
            updateObj.setStatus(WithdrawalBrokerAuditEnum.BROKER_AUDITING.getNumber());
            updateObj.setOrderStatus(WithdrawalStatus.BROKER_AUDITING_STATUS_VALUE);
        }
        withdrawOrderMapper.updateByPrimaryKeySelective(updateObj);
        return OrgWithdrawResponse.newBuilder()
                .setAllowWithdraw(true)
                .setAddressIsUserId(addressIsUserId)
                .setNeedBrokerAudit(withdrawalResponse.getStatus() == WithdrawalStatus.BROKER_AUDITING_STATUS)
                .setOrder(getWithdrawOrder(header, withdrawOrder, withdrawalResponse.getWithdrawalOrderId()))
                .setWithdrawOrderId(withdrawalResponse.getWithdrawalOrderId()).build();
    }

    /**
     * 请求BlueHelix短信验证码
     */
    public WithdrawVerifyCodeResponse getWithdrawVerifyCode(Header header, String requestId) {
        WithdrawalRequest request = buildWithdrawalRequest(header, requestId);
        WithdrawalResponse response = grpcWithdrawService.withdraw(request);
        return WithdrawVerifyCodeResponse.newBuilder().setCodeOrderId(response.getCodeSeqId()).setOrder(getWithdrawOrder(header, requestId)).build();
    }

    /**
     * 用户提现确认
     */
    @WithdrawNotify
    @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.READ_COMMITTED)
    public WithdrawVerifyResponse withdrawVerify(Header header, String requestId,
                                                 Boolean skipInputIdCardNo, String idCardNo,
                                                 Long codeOrderId, String verifyCode) {
        WithdrawPreApply preApply = withdrawPreApplyMapper.getByRequestIdAndUserId(requestId, header.getOrgId(), header.getUserId());
        if (preApply == null) {
//            if (preApply == null || System.currentTimeMillis() < preApply.getExpired()) {
            throw new BrokerException(BrokerErrorCode.PARAM_INVALID);
        }
        preApply = withdrawPreApplyMapper.lockById(preApply.getId());
        WithdrawOrder originWithdrawOrder = withdrawOrderMapper.getByClientIdAndUserId(header.getOrgId(), header.getUserId(), preApply.getClientOrderId());
        if (originWithdrawOrder != null) {
            log.warn("{}:{} submit withdrawVerify request with repeated clientOrderId:{}", header.getOrgId(), header.getUserId(), preApply.getClientOrderId());
            throw new BrokerException(BrokerErrorCode.REPEATED_SUBMIT_REQUEST);
        }
        if (preApply.getNeedCheckIdCardNo() == 1 && preApply.getHasCheckIdCardNo() != 1) {
            WithdrawalRequest.Builder builder = buildWithdrawalRequest(header, requestId).toBuilder();
            WithdrawPreApply updatePreApplyObj = new WithdrawPreApply();
            updatePreApplyObj.setId(preApply.getId());
            // 如果用户跳过了输入身份证号检验，则直接进行平台验证码校验，并进入券商人工审核
            if (skipInputIdCardNo) {
                updatePreApplyObj.setNeedCheckIdCardNo(0);
                updatePreApplyObj.setCardNo("");
                updatePreApplyObj.setNeedBrokerAudit(1);
                builder.setAuditStatus(WithdrawalBrokerAuditEnum.BROKER_AUDITING);
            } else {
                if (Strings.isNullOrEmpty(idCardNo)) {
                    throw new BrokerException(BrokerErrorCode.PARAM_INVALID);
                }
                UserVerify userVerify = UserVerify.decrypt(userVerifyMapper.getByUserId(header.getUserId()));
                if (!userVerify.getCardNo().equals(Strings.nullToEmpty(idCardNo))) {
                    throw new BrokerException(BrokerErrorCode.ID_CARD_NO_ERROR);
                }
                updatePreApplyObj.setNeedCheckIdCardNo(1);
                updatePreApplyObj.setCardNo(idCardNo);
                boolean needBrokerAudit = forceBrokerAudit(header.getOrgId(), header.getUserId());
                updatePreApplyObj.setNeedBrokerAudit(needBrokerAudit ? 1 : 0);
                builder.setAuditStatus(needBrokerAudit ? WithdrawalBrokerAuditEnum.BROKER_AUDITING : WithdrawalBrokerAuditEnum.NO_NEED);

            }
            updatePreApplyObj.setHasCheckIdCardNo(1);
            updatePreApplyObj.setUpdated(System.currentTimeMillis());
            withdrawPreApplyMapper.updateByPrimaryKeySelective(updatePreApplyObj);

            WithdrawalRequest request = builder.build();
            WithdrawalResponse response = grpcWithdrawService.withdraw(request);

            return WithdrawVerifyResponse.newBuilder().setRequestId(requestId)
                    .setAddressIsUserId(preApply.getAddressIsUserId() == 1)
                    .setCodeOrderId(response.getCodeSeqId())
                    .setOrder(getWithdrawOrder(header, preApply))
                    .setNeedCheckIdCardNo(false).setProcessIsEnd(false).build();
        }

        if (Strings.isNullOrEmpty(verifyCode)) {
            throw new BrokerException(BrokerErrorCode.PARAM_INVALID);
        }
        Rate fxRate;
        try {
            if (Strings.isNullOrEmpty(preApply.getCurrentFxRate())) {
                log.error("cannot get faRate info in cached withdraw order");
                throw new BrokerException(BrokerErrorCode.GRPC_SERVER_SYSTEM_ERROR);
            }
            Rate.Builder builder = Rate.newBuilder();
            JsonUtil.defaultProtobufJsonParser().merge(preApply.getCurrentFxRate(), builder);
            fxRate = builder.build();
        } catch (Exception e) {
            log.error("cannot get faRate info in cached withdraw order");
//            throw new BrokerException(BrokerErrorCode.GRPC_SERVER_SYSTEM_ERROR);
            fxRate = basicService.getV3Rate(preApply.getOrgId(), preApply.getTokenId());
        }

        // 这里只拦截是否需要用户输入身份校验信息
        WithdrawCheckResult withdrawCheckResult = withdrawProcessCheck(header, preApply.getTokenId(),
                preApply.getQuantity().stripTrailingZeros().toPlainString(), fxRate);
        User user = userService.getUser(header.getUserId());
        if (user.getUserType() == UserType.INSTITUTIONAL_USER.value()) {
            withdrawCheckResult.setNeedCheckIdCard(false);
        }

        log.debug("withdrawVerify check:{}", JsonUtil.defaultGson().toJson(withdrawCheckResult));
        if (withdrawCheckResult.getNeedCheckIdCard() && preApply.getHasCheckIdCardNo() != 1) {
            WithdrawPreApply updatePreApplyObj = new WithdrawPreApply();
            updatePreApplyObj.setId(preApply.getId());
            updatePreApplyObj.setNeedCheckIdCardNo(withdrawCheckResult.getNeedCheckIdCard() ? 1 : 0);
            updatePreApplyObj.setNeedBrokerAudit(withdrawCheckResult.getNeedBrokerAudit() ? 1 : 0);
            return WithdrawVerifyResponse.newBuilder().setRequestId(requestId).setNeedCheckIdCardNo(true).setProcessIsEnd(false).build();
        }

        WithdrawalRequest.Builder builder = buildWithdrawalRequest(header, requestId).toBuilder();
        // 处理提现请求
        long signTime = System.currentTimeMillis() / 1000;
        String signNonce = UUID.randomUUID().toString();
        WithdrawalRequestSign withdrawSign = WithdrawalRequestSign.newBuilder()
                .setOrganizationId(header.getOrgId())
                .setClientWithdrawalId(Strings.nullToEmpty(builder.getClientWithdrawalId()))
                .setAccountId(builder.getAccountId())
                .setTokenId(builder.getTokenId())
                .setMinerTokenId(builder.getMinerTokenId())
                .setFeeTokenId(builder.getFeeTokenId())
                .setAddress(builder.getAddress())
                .setArriveQuantity(DecimalUtil.toTrimString(builder.getArriveQuantity()))
                .setMinerFee(DecimalUtil.toTrimString(builder.getMinerFee()))
                .setPlatformFee(DecimalUtil.toTrimString(builder.getPlatformFee()))
                .setBrokerFee(DecimalUtil.toTrimString(builder.getBrokerFee()))
                .setIsBrokerAutoConvert(builder.getIsBrokerAutoConvert())
                .setBrokerConvertPaidQuantity(DecimalUtil.toTrimString(builder.getBrokerConvertPaidQuantity()))
                .setQuantity(DecimalUtil.toTrimString(builder.getQuantity()))
                .setConvertRate(DecimalUtil.toTrimString(builder.getConvertRate()))
                .setSignTime(signTime)
                .setSignNonce(signNonce)
                .build();

        String sign = "";
        try {
            sign = signUtils.sign(withdrawSign.toByteArray(), header.getOrgId());
        } catch (Exception e) {
            throw new BrokerException(BrokerErrorCode.WITHDRAW_FAILED);
        }
        builder.setSignTime(signTime)
                .setSignNonce(signNonce)
                .setSignBroker(sign);

        WithdrawalRequest request = builder
                .setBaseRequest(GrpcRequestUtil.getBaseRequest(header))
                .setTargetOrgId(header.getOrgId())
                .setCodeSeqId(codeOrderId)
                .setConfirmationCode(verifyCode)
                .setAuditStatus(preApply.getNeedBrokerAudit() == 1 ? WithdrawalBrokerAuditEnum.BROKER_AUDITING : WithdrawalBrokerAuditEnum.NO_NEED)
                .build();
        WithdrawalResponse response = grpcWithdrawService.withdraw(request);
        WithdrawOrder withdrawOrder = buildWithdrawOrder(preApply);
        withdrawOrder.setOrderId(response.getWithdrawalOrderId());
        withdrawOrder.setStatus(request.getAuditStatusValue());
        withdrawOrder.setOrderStatus(preApply.getNeedBrokerAudit() == 1 ? WithdrawalStatus.BROKER_AUDITING_STATUS_VALUE : WithdrawalStatus.AUDITING_STATUS_VALUE);
        if (response.getStatus() == WithdrawalStatus.BROKER_AUDITING_STATUS) {
            withdrawOrder.setNeedBrokerAudit(1);
            withdrawOrder.setStatus(WithdrawalBrokerAuditEnum.BROKER_AUDITING.getNumber());
            withdrawOrder.setOrderStatus(WithdrawalStatus.BROKER_AUDITING_STATUS_VALUE);
        }
        withdrawOrder.setUpdated(System.currentTimeMillis());
        try {
            withdrawOrderMapper.insertRecord(withdrawOrder);
        } catch (Exception e) {
            log.error("insert withdraw order:{} with exception ", JsonUtil.defaultGson().toJson(withdrawOrder), e);
        }

        return WithdrawVerifyResponse.newBuilder()
                .setAddressIsUserId(preApply.getAddressIsUserId() == 1)
                .setWithdrawOrderId(response.getWithdrawalOrderId())
                .setOrder(getWithdrawOrder(header, withdrawOrder, response.getWithdrawalOrderId()))
                .setNeedCheckIdCardNo(false).setProcessIsEnd(true).build();
    }

    private WithdrawOrder buildWithdrawOrder(WithdrawPreApply withdrawPreApply) {
        long currentTimestamp = System.currentTimeMillis();
        return WithdrawOrder.builder()
                .orgId(withdrawPreApply.getOrgId())
                .userId(withdrawPreApply.getUserId())
                .accountId(withdrawPreApply.getAccountId())
                .addressId(withdrawPreApply.getAddressId())
                .address(withdrawPreApply.getAddress())
                .addressExt(withdrawPreApply.getAddressExt())
                .isInnerAddress(withdrawPreApply.getIsInnerAddress())
                .tokenId(withdrawPreApply.getTokenId())
                .chainType(withdrawPreApply.getChainType())
                .clientOrderId(withdrawPreApply.getClientOrderId())
                .feeTokenId(withdrawPreApply.getFeeTokenId())
                .platformFee(withdrawPreApply.getPlatformFee())
                .brokerFee(withdrawPreApply.getBrokerFee())
                .minerFeeTokenId(withdrawPreApply.getMinerFeeTokenId())
                .minerFee(withdrawPreApply.getMinerFee())
                .isAutoConvert(withdrawPreApply.getIsAutoConvert()) // 这里指的是当手续费token和提币的token不一致的时候，是否由券商垫付手续费
                .convertRate(withdrawPreApply.getConvertRate())
                .convertQuantity(withdrawPreApply.getConvertQuantity())
                .quantity(withdrawPreApply.getQuantity())
                .arrivalQuantity(withdrawPreApply.getArrivalQuantity())
                .needBrokerAudit(withdrawPreApply.getNeedBrokerAudit())
                .needCheckCardNo(withdrawPreApply.getNeedCheckIdCardNo())
                .cardNo("")
                .hasCheckIdCardNo(withdrawPreApply.getHasCheckIdCardNo() == 1)
                .currentFxRate(withdrawPreApply.getCurrentFxRate())
                .btcValue(withdrawPreApply.getBtcValue())
                .platform(withdrawPreApply.getPlatform())
                .ip(withdrawPreApply.getIp())
                .userAgent(withdrawPreApply.getUserAgent())
                .language(withdrawPreApply.getLanguage())
                .appBaseHeader(withdrawPreApply.getAppBaseHeader())
                .created(currentTimestamp)
                .updated(currentTimestamp)
                .remarks(getOriginalRemarks(withdrawPreApply.getRemarks()))
                .build();
    }

    private WithdrawalRequest buildWithdrawalRequest(Header header, String requestId) {
        WithdrawPreApply preApply = withdrawPreApplyMapper.getByRequestIdAndUserId(requestId, header.getOrgId(), header.getUserId());
        if (preApply == null) {
//        if (preApply == null || System.currentTimeMillis() < preApply.getExpired()) {
            throw new BrokerException(BrokerErrorCode.PARAM_INVALID);
        }
        User user = userService.getUser(header.getUserId());
        //二阶段预提币
        if (verifyCaptcha && globalNotifyType == 2 && user.getRegisterType() == RegisterType.EMAIL.value()) {
            //如果平台仅支持手机,但是历史原因注册类型为邮箱的，引导客户绑定手机
            throw new BrokerException(BrokerErrorCode.FEATURE_SUSPENDED);
        } else if (verifyCaptcha && globalNotifyType == 3 && user.getRegisterType() == RegisterType.MOBILE.value()) {
            //如果平台仅支持邮箱，但是历史原因注册类型为手机的，引导客户绑定邮箱
            throw new BrokerException(BrokerErrorCode.FEATURE_SUSPENDED);
        }

        return WithdrawalRequest.newBuilder()
                .setBaseRequest(GrpcRequestUtil.getBaseRequest(preApply.getOrgId()))
                .setAccountId(preApply.getAccountId())
                .setTokenId(preApply.getTokenId())
                .setChainType(preApply.getChainType())
                .setClientWithdrawalId(preApply.getClientOrderId())
                .setTargetOrgId(header.getOrgId())
                .setAddress(preApply.getAddress())
                .setAddressExt(Strings.nullToEmpty(preApply.getAddressExt()))
                .setQuantity(DecimalUtil.fromBigDecimal(preApply.getQuantity()))
                .setArriveQuantity(DecimalUtil.fromBigDecimal(preApply.getArrivalQuantity()))
                .setFeeTokenId(preApply.getFeeTokenId())
                .setPlatformFee(DecimalUtil.fromBigDecimal(preApply.getPlatformFee()))
                .setBrokerFee(DecimalUtil.fromBigDecimal(preApply.getBrokerFee()))
                .setMinerTokenId(preApply.getMinerFeeTokenId())
                .setMinerFee(DecimalUtil.fromBigDecimal(preApply.getMinerFee()))
                .setIsBrokerAutoConvert(preApply.getIsAutoConvert() == 1)
                .setConvertRate(DecimalUtil.fromBigDecimal(preApply.getConvertRate()))
                .setBrokerConvertPaidQuantity(DecimalUtil.fromBigDecimal(preApply.getConvertQuantity()))
                .setLanguage(preApply.getLanguage())
                .setAuditStatus(preApply.getNeedBrokerAudit() == 1 ? WithdrawalBrokerAuditEnum.BROKER_AUDITING : WithdrawalBrokerAuditEnum.NO_NEED)
                .setConfirmationCodeType(user.getRegisterType() == RegisterType.MOBILE.value() ? ConfirmationCodeType.MOBILE : ConfirmationCodeType.EMAIL)
                .setInnerWithdrawFee(DecimalUtil.fromBigDecimal(preApply.getInnerWithdrawFee()))
                .setIsUidWithdraw(preApply.getAddressIsUserId() == 1)
                .setRemarks(preApply.getRemarks())
                .build();
    }

    public WithdrawCheckResult withdrawProcessCheck(Header header, String tokenId, String withdrawQuantity, Rate tokenFxRate) {
        WithdrawCheckResult withdrawCheckResult = new WithdrawCheckResult();

        long currentTimestamp = System.currentTimeMillis();
        TokenDetail tokenDetail = grpcTokenService.getToken(GetTokenRequest.newBuilder().setTokenId(tokenId).build());

        // 单笔校验needKyc
        UserVerify userVerify = UserVerify.decrypt(userVerifyMapper.getByUserId(header.getUserId()));

        int withdrawCount = statisticsOrgService.countByUserId(header.getOrgId(), header.getUserId() + "");
        String needKycQuota = "";
        if (userVerify == null || !userVerify.isSeniorVerifyPassed()) {
            if (withdrawCount == 0) {
                needKycQuota = commonIniService.getStringValueOrDefault(header.getOrgId(), BrokerServerConstants.FIRST_WITHDRAW_NEED_KYC_CONFIG, "1.BTC");
            } else {
                needKycQuota = commonIniService.getStringValueOrDefault(header.getOrgId(), BrokerServerConstants.WITHDRAW_NEED_KYC_CONFIG, "1.BTC");
            }

//            String needKycQuotaQuantity = needKycQuota.split("\\.")[0];
//            String needKycQuotaUnit = needKycQuota.split("\\.")[1]; // 这里先设定为BTC
            String needKycQuotaQuantity = splitWithdrawAmountConfig(needKycQuota);
            String needKycQuotaUnit = splitWithdrawUnitConfig(needKycQuota); // 这里先设定为BTC
            // 如果提币数量折合`needKycQuotaUnit`数量大于needKycQuotaQuantity，需要KYC
            if (new BigDecimal(withdrawQuantity).multiply(DecimalUtil.toBigDecimal(tokenFxRate.getRatesMap().get("BTC")))
                    .setScale(Math.min(8, tokenDetail.getMinPrecision()), BigDecimal.ROUND_DOWN)
                    .compareTo(new BigDecimal(needKycQuotaQuantity)) > 0) {
                if (userVerify == null || !userVerify.isSeniorVerifyPassed()) {
                    // withdrawCheckResult.setNeedKyc(true);
                    log.warn("withdrawCheck [ONCE KYC check] orgId:{} userId:{} tokenId:{} quantity:{} rate:{} withdrawCount:{}, kycQuota:{}",
                            header.getOrgId(), header.getUserId(), tokenId, withdrawQuantity, tokenFxRate.toString(), withdrawCount, needKycQuota);
                    throw new BrokerException(BrokerErrorCode.WITHDRAW_NEED_SENIOR_VERIFY);
                }
            }
        }

        if (forceBrokerAudit(header.getOrgId(), header.getUserId())) {
            log.warn("withdrawCheck [broker audit check] orgId:{} userId:{} tokenId:{} quantity:{} rate:{} withdrawCount:{}, kycQuota:{} force broker audit",
                    header.getOrgId(), header.getUserId(), tokenId, withdrawQuantity, tokenFxRate.toString(), withdrawCount, needKycQuota);
            withdrawCheckResult.setNeedBrokerAudit(true);
            return withdrawCheckResult;
        }

        // 判定单笔提现的人工审核条件
        String needBrokerAuditQuota = "";
        if (withdrawCount == 0) {
            // 首次单笔1BTC以上需要转入人工审核
            needBrokerAuditQuota = commonIniService.getStringValueOrDefault(header.getOrgId(), BrokerServerConstants.FIRST_WITHDRAW_BROKER_AUDIT_CONFIG, "1.BTC"); // 1.BTC
        } else {
            // 非首次大于等于5BTC以上，转入人工审核
            needBrokerAuditQuota = commonIniService.getStringValueOrDefault(header.getOrgId(), BrokerServerConstants.WITHDRAW_BROKER_AUDIT_CONFIG, "5.BTC"); // 5.BTC
        }
//        String needBrokerAuditQuotaQuantity = needBrokerAuditQuota.split("\\.")[0];
//        String needBrokerAuditQuotaUnit = needBrokerAuditQuota.split("\\.")[1];
        String needBrokerAuditQuotaQuantity = splitWithdrawAmountConfig(needBrokerAuditQuota);
        String needBrokerAuditQuotaUnit = splitWithdrawUnitConfig(needBrokerAuditQuota);
        if (new BigDecimal(withdrawQuantity).multiply(DecimalUtil.toBigDecimal(tokenFxRate.getRatesMap().get("BTC")))
                .setScale(Math.min(8, tokenDetail.getMinPrecision()), BigDecimal.ROUND_DOWN)
                .compareTo(new BigDecimal(needBrokerAuditQuotaQuantity)) > 0) {
            log.warn("withdrawCheck [ONCE broker audit check] orgId:{} userId:{} tokenId:{} quantity:{} rate:{} withdrawCount:{}, brokerAuditQuota:{} force broker audit",
                    header.getOrgId(), header.getUserId(), tokenId, withdrawQuantity, tokenFxRate.toString(), withdrawCount, needBrokerAuditQuota);
            withdrawCheckResult.setNeedBrokerAudit(true);
            return withdrawCheckResult;
        }

        BigDecimal totalWithdrawUSDT = BigDecimal.ZERO;

        if (withdrawCount > 0) {
            // 30天内提币不超过10个，否则强制KYC
            {
                Long timestampBefore30H = currentTimestamp - 30 * 24 * 3600 * 1000L; // 24小时起始时间
                // 这里应该查询用户30天内处于提现中和提现成功的订单
                List<StatisticsWithdrawOrder> withdrawOrderList = statisticsOrgService.getByUserIdWithGiveTime(header.getOrgId(), header.getUserId(),
                        timestampBefore30H, currentTimestamp);
                //List<io.bhex.broker.server.model.WithdrawOrder> withdrawOrderList = withdrawOrderMapper.getByUserIdWithGiveTime(header.getUserId(), timestampBefore24H, currentTimestamp);
                BigDecimal totalWithdrawBtcIn30Day = BigDecimal.ZERO;
                for (StatisticsWithdrawOrder withdrawOrder : withdrawOrderList) {
                    Rate fxRate = basicService.getV3Rate(header.getOrgId(), withdrawOrder.getTokenId());
                    if (fxRate == null) {
                        log.warn("withdraw: check broker audit, cannot get {} rate, use 0 for calculate", tokenId);
                        totalWithdrawBtcIn30Day = totalWithdrawBtcIn30Day.add(BigDecimal.ZERO);
                    } else {
                        totalWithdrawBtcIn30Day = totalWithdrawBtcIn30Day.add(withdrawOrder.getTotalQuantity().multiply(DecimalUtil.toBigDecimal(fxRate.getRatesMap().get("BTC"))));
                    }
                }

                // total
                needKycQuota = commonIniService.getStringValueOrDefault(header.getOrgId(), BrokerServerConstants.TOTAL_WITHDRAW_BROKER_AUDIT_CONFIG_30D, "10.BTC"); // 10.BTC
                String needKycQuotaQuantity = splitWithdrawAmountConfig(needKycQuota);
                if (totalWithdrawBtcIn30Day.add(new BigDecimal(withdrawQuantity).multiply(DecimalUtil.toBigDecimal(tokenFxRate.getRatesMap().get("BTC"))))
                        .compareTo(new BigDecimal(needKycQuotaQuantity)) > 0) {
                    if (userVerify == null || !userVerify.isSeniorVerifyPassed()) {
                        // withdrawCheckResult.setNeedKyc(true);
                        log.warn("withdrawCheck [30D KYC check] orgId:{} userId:{} tokenId:{} quantity:{} totalBTC30D:{} rate:{} withdrawCount:{}, kycQuota:{}",
                                header.getOrgId(), header.getUserId(), tokenId, withdrawQuantity, totalWithdrawBtcIn30Day.toPlainString(), tokenFxRate.toString(), withdrawCount, needKycQuota);
                        throw new BrokerException(BrokerErrorCode.WITHDRAW_NEED_SENIOR_VERIFY);
                    }
                }
            }

            {
                // 判定多笔提现的问题
                Long timestampBefore24H = currentTimestamp - 24 * 3600 * 1000; // 24小时起始时间
                // 这里应该查询用户24小时内处于提现中和提现成功的订单
                List<StatisticsWithdrawOrder> withdrawOrderList = statisticsOrgService.getByUserIdWithGiveTime(header.getOrgId(), header.getUserId(),
                        timestampBefore24H, currentTimestamp);
                //List<io.bhex.broker.server.model.WithdrawOrder> withdrawOrderList = withdrawOrderMapper.getByUserIdWithGiveTime(header.getUserId(), timestampBefore24H, currentTimestamp);
                BigDecimal totalWithdrawBtc = BigDecimal.ZERO;
                for (StatisticsWithdrawOrder withdrawOrder : withdrawOrderList) {
                    Rate fxRate = basicService.getV3Rate(header.getOrgId(), withdrawOrder.getTokenId());
                    if (fxRate == null) {
                        log.warn("withdraw: check broker audit, cannot get {} rate, use 0 for calculate", tokenId);
                        totalWithdrawBtc = totalWithdrawBtc.add(BigDecimal.ZERO);
                    } else {
                        totalWithdrawBtc = totalWithdrawBtc.add(withdrawOrder.getTotalQuantity().multiply(DecimalUtil.toBigDecimal(fxRate.getRatesMap().get("BTC"))));
                        totalWithdrawUSDT = totalWithdrawUSDT.add(withdrawOrder.getTotalQuantity().multiply(DecimalUtil.toBigDecimal(fxRate.getRatesMap().get("USDT"))));
                    }
                }

                // total
                needBrokerAuditQuota = commonIniService.getStringValueOrDefault(header.getOrgId(), BrokerServerConstants.TOTAL_WITHDRAW_BROKER_AUDIT_CONFIG, "50.BTC"); // 25.BTC
                String totalWithdrawBrokerAuditQuotaQuantity = splitWithdrawAmountConfig(needBrokerAuditQuota);
                if (totalWithdrawBtc.add(new BigDecimal(withdrawQuantity).multiply(DecimalUtil.toBigDecimal(tokenFxRate.getRatesMap().get("BTC"))))
                        .compareTo(new BigDecimal(totalWithdrawBrokerAuditQuotaQuantity)) > 0) {
                    withdrawCheckResult.setNeedBrokerAudit(true);
                    log.warn("withdrawCheck [24H broker audit check] orgId:{} userId:{} tokenId:{} quantity:{} totalBTC24H:{} rate:{} withdrawCount:{}, brokerAuditQuota:{} force broker audit",
                            header.getOrgId(), header.getUserId(), tokenId, withdrawQuantity, totalWithdrawBtc.toPlainString(), tokenFxRate.toString(), withdrawCount, needBrokerAuditQuota);
                    return withdrawCheckResult;
                }

                int withdrawCheckIdCardNoNumberLimit = commonIniService.getIntValueOrDefault(header.getOrgId(), BrokerServerConstants.WITHDRAW_CHECK_ID_CARD_NO_NUMBER_CONFIG, 5);
                //String needCheckIDCardQuotaConfig = commonIniService.getStringValueOrDefault(header.getOrgId(), BrokerServerConstants.WITHDRAW_CHECK_ID_CARD_NO_CONFIG, "5.BTC");
//            String needCheckIDCardQuotaQuantity = needCheckIDCardQuotaConfig.split("\\.")[0];
//            String needCheckIDCardQuotaUnit = needCheckIDCardQuotaConfig.split("\\.")[1]; // BTC
                //String needCheckIDCardQuotaQuantity = splitWithdrawAmountConfig(needCheckIDCardQuotaConfig);
                // String needCheckIDCardQuotaUnit = splitWithdrawUnitConfig(needCheckIDCardQuotaConfig); // BTC

                BigDecimal needCheckIDCardQuotaQuantity = getUserNeedCheckIDCardQuotaQuantity(header.getOrgId(), header.getUserId());
                // 如果24小时提现大于5笔 || 并且提现金额大于25BTC
                if (withdrawOrderList.size() >= withdrawCheckIdCardNoNumberLimit
                        || totalWithdrawBtc.add(new BigDecimal(withdrawQuantity).multiply(DecimalUtil.toBigDecimal(tokenFxRate.getRatesMap().get("BTC"))))
                        .compareTo(needCheckIDCardQuotaQuantity) > 0) {
                    // 先校验用户是否有KYC并且是否通过
                    if (userVerify == null || !userVerify.isSeniorVerifyPassed()) {
                        // withdrawCheckResult.setNeedKyc(true);
                        log.warn("withdrawCheck [24H KYC check] orgId:{} userId:{} tokenId:{} quantity:{} totalBTC24H:{} rate:{} withdrawCount:{}, kycQuota:{}",
                                header.getOrgId(), header.getUserId(), tokenId, withdrawQuantity, totalWithdrawBtc.toPlainString(), tokenFxRate.toString(), withdrawCount, needCheckIDCardQuotaQuantity.toPlainString());
                        throw new BrokerException(BrokerErrorCode.WITHDRAW_NEED_SENIOR_VERIFY);
                    }
                    withdrawCheckResult.setNeedBrokerAudit(true);
                    withdrawCheckResult.setNeedCheckIdCard(true);
                }
            }
        }

        // 风控24小时限额-USDT
        boolean risk24H = riskControl(header, totalWithdrawUSDT, withdrawQuantity, tokenFxRate);
        if (risk24H) {
            withdrawCheckResult.setNeedBrokerAudit(true);
        }

        return withdrawCheckResult;
    }

    /**
     * 风控24小时提币限额 USDT
     *
     * @param header
     * @param totalWithdrawUSDT
     * @param withdrawQuantity
     * @param tokenFxRate
     * @return
     */
    private boolean riskControl(Header header, BigDecimal totalWithdrawUSDT, String withdrawQuantity, Rate tokenFxRate) {
        String needBrokerAuditQuota = commonIniService.getStringValueOrDefault(header.getOrgId()
                , BrokerServerConstants.RISK_CONTROL_TOTAL_WITHDRAW_BROKER_AUDIT_CONFIG_24H, "");
        if (Strings.isNullOrEmpty(needBrokerAuditQuota)) {
            return false;
        } else {
            String totalWithdrawBrokerAuditQuotaQuantity = splitWithdrawAmountConfig(needBrokerAuditQuota);
            if (totalWithdrawUSDT.add(new BigDecimal(withdrawQuantity).multiply(DecimalUtil.toBigDecimal(tokenFxRate.getRatesMap().get("USDT"))))
                    .compareTo(new BigDecimal(totalWithdrawBrokerAuditQuotaQuantity)) > 0) {
                log.warn("withdrawCheck [24H broker audit check_risk_control] orgId:{} userId:{} tokenId:{} quantity:{} totalUSDT24H:{} rate:{} brokerAuditQuota:{} force broker audit",
                        header.getOrgId(), header.getUserId(), tokenFxRate.getToken(), withdrawQuantity, totalWithdrawUSDT.toPlainString(), tokenFxRate.toString(), needBrokerAuditQuota);
                return true;
            }
        }
        return false;
    }

    public boolean forceBrokerAudit(long orgId, long userId) {
        SwitchStatus switchStatus = baseBizConfigService.getConfigSwitchStatus(orgId,
                BaseConfigConstants.WHOLE_SITE_CONTROL_SWITCH_GROUP, BaseConfigConstants.WHOLE_SITE_FORCE_AUDIT_WITHDRAW_KEY);
        if (switchStatus.isOpen()) {
            log.info("org:{} uid:{} whole site withdraw force broker audit", orgId, userId);
            return true;
        }
        switchStatus = baseBizConfigService.getConfigSwitchStatus(orgId,
                BaseConfigConstants.FORCE_AUDIT_USER_WITHDRAW_GROUP, userId + "");
        if (switchStatus.isOpen()) {
            log.info("org:{} uid:{} withdraw force broker audit", orgId, userId);
            return true;
        }
        return false;
    }

    private String splitWithdrawAmountConfig(String config) {
        int lastDot = config.lastIndexOf(".");
        return config.substring(0, lastDot);
    }

    private String splitWithdrawUnitConfig(String config) {
        int lastDot = config.lastIndexOf(".");
        return config.substring(lastDot + 1);
    }

    /**
     * 同步提币结果
     */
    public AsyncWithdrawOrderResponse asyncWithdrawOrder(AsyncWithdrawOrderRequest request) {
        io.bhex.broker.server.model.WithdrawOrder withdrawOrder = withdrawOrderMapper.getByOrderId(request.getOrderId());
        if (withdrawOrder != null) {
            io.bhex.broker.server.model.WithdrawOrder updateObj = io.bhex.broker.server.model.WithdrawOrder.builder()
                    .orderId(withdrawOrder.getOrderId())
                    .orderStatus(request.getOrderStatus())
                    .updated(request.getHandleTime())
                    .build();
            withdrawOrderMapper.update(updateObj);
        } else {
            log.warn("bh async withdraw order, but cannot find order with orderId:{}", request.getOrderId());
        }
        return AsyncWithdrawOrderResponse.newBuilder().build();
    }

    public GetWithdrawOrderDetailResponse getWithdrawOrder(Header header, Long orderId, String clientOrderId) {
        GetWithdrawalOrderRequest request = GetWithdrawalOrderRequest.newBuilder()
                .setBaseRequest(GrpcRequestUtil.getBaseRequest(header))
                .setAccountId(accountService.getMainAccountId(header))
                .setOrderId(orderId)
                .setClientWithdrawalId(clientOrderId)
                .build();
        List<WithdrawalOrderDetail> withdrawOrderDetailList = grpcWithdrawService.queryWithdrawOrders(request).getWithdrawOrderDetailsList();
        if (withdrawOrderDetailList.size() <= 0) {
            throw new BrokerException(BrokerErrorCode.ORDER_NOT_FOUND);
        }
        return GetWithdrawOrderDetailResponse.newBuilder()
                .setOrder(getWithdrawOrder(header.getOrgId(), withdrawOrderDetailList.get(0)))
                .build();
    }


    /**
     * 查询提币订单list
     */
    public QueryWithdrawOrdersResponse queryWithdrawOrder(Header header, String tokenId, Long fromOrderId, Long endOrderId,
                                                          Long startTime, Long endTime, Integer limit) {
        GetWithdrawalOrderRequest request = GetWithdrawalOrderRequest.newBuilder()
                .setBaseRequest(GrpcRequestUtil.getBaseRequest(header))
                .setAccountId(accountService.getMainAccountId(header))
                .addAllTokenId(Strings.isNullOrEmpty(tokenId) ? Lists.newArrayList() : Lists.newArrayList(tokenId))
                .setFromWithdrawalOrderId(fromOrderId)
                .setEndWithdrawalOrderId(endOrderId)
                .setStartTime(startTime)
                .setEndTime(endTime)
                .setLimit(limit)
                .build();
        List<WithdrawalOrderDetail> withdrawOrderDetailList = grpcWithdrawService.queryWithdrawOrders(request).getWithdrawOrderDetailsList();
        List<io.bhex.broker.grpc.withdraw.WithdrawOrder> withdrawOrderList = Lists.newArrayList();
        if (withdrawOrderDetailList != null) {
            withdrawOrderList = withdrawOrderDetailList.stream().map(item -> getWithdrawOrder(header.getOrgId(), item)).collect(Collectors.toList());
        }
        return QueryWithdrawOrdersResponse.newBuilder()
                .addAllOrders(withdrawOrderList)
                .build();
    }

    private io.bhex.broker.grpc.withdraw.WithdrawOrder getWithdrawOrder(Long orgId, WithdrawalOrderDetail withdrawalOrderDetail) {
        Map<String, String> extensionMap = withdrawalOrderDetail.getExtensionMap();
        String kernelId = "";
        String remarks = "";
        if (extensionMap != null) {
            kernelId = extensionMap.getOrDefault("kernel_id", "");
            remarks = extensionMap.getOrDefault("remarks", "");
        }
        return io.bhex.broker.grpc.withdraw.WithdrawOrder.newBuilder()
                .setOrderId(withdrawalOrderDetail.getWithdrawalOrderId())
                .setAccountId(withdrawalOrderDetail.getAccountId())
                .setTokenId(withdrawalOrderDetail.getToken().getTokenId())
                .setTokenName(basicService.getTokenName(orgId, withdrawalOrderDetail.getToken().getTokenId()))
                .setAddress(withdrawalOrderDetail.getAddress())
                .setAddressExt(Strings.nullToEmpty(withdrawalOrderDetail.getAddressExt()))
                .setQuantity(new BigDecimal(withdrawalOrderDetail.getTotalQuantity()).stripTrailingZeros().toPlainString())
                .setArriveQuantity(DecimalUtil.toBigDecimal(withdrawalOrderDetail.getArriveQuantity()).stripTrailingZeros().toPlainString())
                .setStatus(withdrawalOrderDetail.getStatusValue())
                .setStatusCode(withdrawalOrderDetail.getStatus().name())
                .setTime(withdrawalOrderDetail.getWithdrawalTime())
                .setTxid(withdrawalOrderDetail.getTxId())
                .setTxidUrl(withdrawalOrderDetail.getTxExploreUrl())
                .setWalletHandleTime(withdrawalOrderDetail.getWalletHandleTime())
                .setTotalFee(WithdrawFee.newBuilder()
                        .setFeeTokenId(withdrawalOrderDetail.getToken().getTokenId())
                        .setFeeTokenName(withdrawalOrderDetail.getToken().getTokenName())
                        .setFee(DecimalUtil.toBigDecimal(withdrawalOrderDetail.getHandlingFee()).stripTrailingZeros().toPlainString())
                        .build())
                .setKernelId(kernelId)
                .setIsInternalTransfer(!withdrawalOrderDetail.getIsChainWithdraw())
                .setCanCancel(withdrawalOrderDetail.getCanBeCancelled())
                .setFailedReason(withdrawalOrderDetail.getFailedReason())
                .setRemarks(getOriginalRemarks(remarks))
                .setUserName(getUserName(remarks))
                .setUserAddress(getUserAddress(remarks))
                .build();
    }

    private String buildJsonRemarks(String remarks, String userName, String userAddress) {
        return JsonUtil.defaultGson().toJson(JsonRemarks.builder()
                .remarks(remarks).userName(userName).userAddress(userAddress)
                .build());
    }

    private String getOriginalRemarks(String remarks) {
        String originalRemarks = "";
        if (StringUtil.isNotEmpty(remarks)) {
            try {
                JsonRemarks remark = JsonUtil.defaultGson().fromJson(remarks, JsonRemarks.class);
                if (remark != null) {
                    originalRemarks = StringUtil.isNotEmpty(remark.getRemarks()) ? remark.getRemarks() : "";
                }
            } catch (Exception e) {
                log.info("getOriginalRemarks Exception:{}", e.getMessage(), e);
                originalRemarks = remarks;
            }
        }
        return originalRemarks;
    }

    private String getUserName(String remarks) {
        String userName = "";
        if (StringUtil.isNotEmpty(remarks)) {
            try {
                JsonRemarks remark = JsonUtil.defaultGson().fromJson(remarks, JsonRemarks.class);
                if (remark != null) {
                    userName = StringUtil.isNotEmpty(remark.getUserName()) ? remark.getUserName() : "";
                }
            } catch (Exception e) {
                log.info("getUserName Exception:{}", e.getMessage(), e);
            }
        }
        return userName;
    }

    private String getUserAddress(String remarks) {
        String userAddress = "";
        if (StringUtil.isNotEmpty(remarks)) {
            try {
                JsonRemarks remark = JsonUtil.defaultGson().fromJson(remarks, JsonRemarks.class);
                if (remark != null) {
                    userAddress = StringUtil.isNotEmpty(remark.getUserAddress()) ? remark.getUserAddress() : "";
                }
            } catch (Exception e) {
                log.info("getUserAddress Exception:{}", e.getMessage(), e);
            }
        }
        return userAddress;
    }


    private io.bhex.broker.grpc.withdraw.WithdrawAddress getWithdrawAddress(WithdrawAddress withdrawAddress) {
        return io.bhex.broker.grpc.withdraw.WithdrawAddress.newBuilder()
                .setId(withdrawAddress.getId())
                .setTokenId(withdrawAddress.getTokenId())
                .setChainType((withdrawAddress.getTokenId().equals("USDT") && Strings.isNullOrEmpty(withdrawAddress.getChainType()))
                        ? "OMNI" : withdrawAddress.getChainType())
                .setTokenName(basicService.getTokenName(withdrawAddress.getOrgId(), withdrawAddress.getTokenId()))
                .setAddress(withdrawAddress.getAddress())
                .setAddressExt(Strings.nullToEmpty(withdrawAddress.getAddressExt()))
                .setRemark(withdrawAddress.getRemark())
                .build();
    }

    @WithdrawNotify
    public OpenApiWithdrawResponse openApiWithdraw(Header header,
                                                   String clientOrderId,
                                                   String address,
                                                   String addressExt,
                                                   String tokenId,
                                                   String withdrawQuantity,
                                                   String chainType,
                                                   Boolean isAutoConvert,
                                                   Boolean isQuick) {
        if (header.getUserId() == 842145604923681280L) { //1.禁止这个用户一切资金操作 7070的账号
            throw new BrokerException(BrokerErrorCode.FEATURE_SUSPENDED);
        }
        log.info("openApiWithdraw clientOrderId {} address {} addressExt {} tokenId {} withdrawQuantity {} chainType {} isAutoConvert {} isQuick {}", clientOrderId, address, addressExt, tokenId, withdrawQuantity, chainType, isAutoConvert, isQuick);
        // 增加判断调用用户是否是Otc第三方提供者，如果是则不判断提出地址是否为添加过的白名单地址
        boolean isOtcThirdParty = false;
        io.bhex.broker.server.model.OtcThirdParty thirdParty = this.otcThirdPartyMapper.getByUser(header.getOrgId(), header.getUserId());
        if (thirdParty != null) {
            CheckAddressResponse checkAddressResponse = checkAddress(header, tokenId, chainType, address, addressExt);
            if (checkAddressResponse.getIsIllegal()) {
                log.warn("openApiWithdraw {}:{} {} withdraw address:{} is illegal", header.getOrgId(), header.getUserId(), tokenId, address);
                throw new BrokerException(BrokerErrorCode.WITHDRAW_ADDRESS_ILLEGAL);
            }
            if (checkAddressResponse.getIsContractAddress()) {
                throw new BrokerException(BrokerErrorCode.UNSUPPORTED_CONTRACT_ADDRESS);
            }
            if (!checkAddressResponse.getIsInnerAddress()) {
                throw new BrokerException(BrokerErrorCode.WITHDRAW_ADDRESS_ILLEGAL);
            }
            isOtcThirdParty = true;
        }
        if (!isOtcThirdParty) {
            //地址是否存在 并且是否成功进行过提币透传操作操作
            List<WithdrawAddress> withdrawAddress = this.withdrawAddressMapper.getByAddress(header.getUserId(), address, addressExt, tokenId);
            if (CollectionUtils.isEmpty(withdrawAddress)) {
                throw new BrokerException(BrokerErrorCode.WITHDRAW_ADDRESS_NOT_IN_WHITE_LIST);
            }
        }

        //风控拦截 判断当前用户是否满足风控可提币条件
        riskControlBalanceService.checkRiskControlLimit(header.getOrgId(), header.getUserId(), FrozenType.FROZEN_WITHDRAW);

        //获取提币矿工费等数据
        WithdrawalQuotaRequest request = WithdrawalQuotaRequest.newBuilder()
                .setBaseRequest(GrpcRequestUtil.getBaseRequest(header))
                .setAccountId(accountService.getMainAccountId(header))
                .setTokenId(tokenId)
                .setChainType(chainType)
                .build();
        WithdrawalQuotaResponse response = grpcWithdrawService.queryWithdrawQuota(request);

        TokenWithdrawLimitDTO withdrawLimitDTO = getLimitDTO(header.getOrgId(), tokenId, chainType, response);
        Integer frozenReason = checkWithdrawFrozen(header.getOrgId(), header.getUserId(), FrozenType.FROZEN_WITHDRAW);
        boolean userUnderFrozen = frozenReason > 0;
        boolean isInWithdrawWhiteList = isInWithdrawWhiteList(header.getOrgId(), tokenId, header.getUserId());
        SwitchStatus stopWithdrawSwitch = baseBizConfigService.getConfigSwitchStatus(header.getOrgId(),
                BaseConfigConstants.WHOLE_SITE_CONTROL_SWITCH_GROUP, BaseConfigConstants.STOP_WITHDRAW_KEY);
        boolean isInWithdrawBlackList = isInWithdrawBlackList(header.getOrgId(), tokenId, header.getUserId());
        // 是否禁止提现
        if (response.getResult() == WithdrawalResult.FORBIDDEN
                || userUnderFrozen
                || (!withdrawLimitDTO.isAllowWithdraw() && !isInWithdrawWhiteList)
                || ((isInWithdrawBlackList || stopWithdrawSwitch.isOpen()) && !addressInWhiteList(header, tokenId, chainType, address, addressExt))) {
            return OpenApiWithdrawResponse.newBuilder().setAllowWithdraw(false).setRefuseWithdrawReason(frozenReason).build();
        }


        WithdrawOrder originWithdrawOrder = withdrawOrderMapper.getByClientIdAndUserId(header.getOrgId(), header.getUserId(), clientOrderId);
        if (originWithdrawOrder != null) {
            log.warn("openApiWithdraw {}:{} submit {} withdraw request with repeated clientOrderId:{}", header.getOrgId(), header.getUserId(), tokenId, clientOrderId);
            throw new BrokerException(BrokerErrorCode.REPEATED_SUBMIT_REQUEST);
        }

        CheckAddressResponse checkAddressResponse = checkAddress(header, tokenId, chainType, address, addressExt);
        if (checkAddressResponse.getIsIllegal()) {
            log.warn("openApiWithdraw {}:{} {} withdraw address:{} is illegal", header.getOrgId(), header.getUserId(), tokenId, address);
            throw new BrokerException(BrokerErrorCode.WITHDRAW_ADDRESS_ILLEGAL);
        }
        if (checkAddressResponse.getIsContractAddress()) {
            throw new BrokerException(BrokerErrorCode.UNSUPPORTED_CONTRACT_ADDRESS);
        }
        boolean addressIsUserId = checkAddressResponse.getAddressIsUserId();
        //获取矿工费 默认提现手续费 还是加速手续费
        String minerFee;
        if (isQuick) {
            minerFee = response.getMaxMinerFee().getStr();
        } else {
            minerFee = response.getSuggestMinerFee().getStr();
        }

        /*
         * 重置提币金额和矿工费的小数位，省得以后报错
         */
        int scale = Math.min(8, withdrawLimitDTO.getPrecision());
        int minerFeeScale = Math.min(8, withdrawLimitDTO.getMinerFeePrecision());
        withdrawQuantity = new BigDecimal(withdrawQuantity).setScale(scale, BigDecimal.ROUND_DOWN).toPlainString();
        minerFee = new BigDecimal(minerFee).setScale(minerFeeScale, BigDecimal.ROUND_UP).toPlainString();

        long currentTimestamp = System.currentTimeMillis();
        Rate fxRate = basicService.getV3Rate(header.getOrgId(), tokenId);
        if (fxRate == null) {
            log.error("withdraw: cannot get {} rate", tokenId);
            throw new BrokerException(BrokerErrorCode.WITHDRAW_FAILED);
        }
        String fxRateInfo = "";
        try {
            fxRateInfo = JsonUtil.defaultProtobufJsonPrinter().print(fxRate);
        } catch (Exception e) {
            log.error("withdraw print faRate error");
            throw new BrokerException(BrokerErrorCode.WITHDRAW_FAILED);
        }
        BigDecimal btcValue = new BigDecimal(withdrawQuantity).multiply(DecimalUtil.toBigDecimal(fxRate.getRatesMap().get("BTC"))).setScale(8, BigDecimal.ROUND_DOWN);

        if (!isInWithdrawWhiteList(header.getOrgId(), tokenId, header.getUserId())) {
            BigDecimal usedQuota = DecimalUtil.toBigDecimal(response.getDayUsedQuota());
            if (new BigDecimal(withdrawQuantity).add(usedQuota).compareTo(withdrawLimitDTO.getMaxWithdrawQuota()) > 0) {
                log.warn("openapiWithdrawal {}:{} {} total amount({} + {}) grant then dayQuota({})",
                        header.getOrgId(), header.getUserId(), tokenId, withdrawQuantity, usedQuota, withdrawLimitDTO.getMaxWithdrawQuota());
                throw new BrokerException(BrokerErrorCode.WITHDRAW_AMOUNT_MAX_LIMIT);
            }
        }

        // 校验提币数量-最小提币数量
        if (new BigDecimal(withdrawQuantity).compareTo(withdrawLimitDTO.getMinWithdrawQuantity()) < 0) {
            log.warn("openapiWithdrawal {}:{} {} amount {} is less than min withdraw quantity {}",
                    header.getOrgId(), header.getUserId(), tokenId, withdrawQuantity, withdrawLimitDTO.getMinWithdrawQuantity());
            throw new BrokerException(BrokerErrorCode.WITHDRAW_AMOUNT_ILLEGAL);
        }

        BigDecimal convertQuantity, arriveQuantity, convertRate;
        if (checkAddressResponse.getIsInnerAddress()) {
            if (response.getChargeInnerWithdrawFee()) {
                convertRate = BigDecimal.ONE;
                convertQuantity = BigDecimal.ZERO;
                BigDecimal innerWithdrawFee = DecimalUtil.toBigDecimal(response.getInnerWithdrawFee());
                arriveQuantity = new BigDecimal(withdrawQuantity).subtract(innerWithdrawFee);
            } else {
                convertRate = BigDecimal.ONE;
                convertQuantity = BigDecimal.ZERO;
                arriveQuantity = new BigDecimal(withdrawQuantity);
            }
        } else {
            // 计算用户的到账金额，和为了抵扣非同币种的手续费需要额外付出的币金额
            if (tokenId.equals(response.getMinerFeeTokenId())) {
                isAutoConvert = false;
                convertRate = BigDecimal.ONE;
                convertQuantity = BigDecimal.ZERO;
                arriveQuantity = new BigDecimal(withdrawQuantity)
                        .subtract(DecimalUtil.toBigDecimal(response.getPlatformFee())) // 平台手续费
                        .subtract(withdrawLimitDTO.getFee())  // 券商手续费
                        .subtract(new BigDecimal(minerFee)) // 矿工费
                        .setScale(scale, BigDecimal.ROUND_DOWN);
            } else {
                TokenConvertRate tokenConvertRate = tokenConvertRateMapper.getByToken(header.getOrgId(), tokenId, response.getMinerFeeTokenId());
                convertRate = tokenConvertRate.getConvertRate();
                if (convertRate.compareTo(BigDecimal.ZERO) <= 0 || tokenConvertRate.getLastConvertRate().compareTo(BigDecimal.ZERO) <= 0) {
                    log.error("openapiWithdraw {}:{} {} get a zero convert rate. set allowWithdraw false", header.getOrgId(), header.getUserId(), tokenId);
                    return OpenApiWithdrawResponse.newBuilder().setAllowWithdraw(false).setRefuseWithdrawReason(frozenReason).build();
                }

                // 这里指的是当手续费token和提币的token不一致的时候，是否有券商垫付手续费, isAutoConvert=true指的是有券商垫付要扣除的费用
                if (isAutoConvert) {
                    if (tokenId.equals(response.getPlatformFeeTokenId())) {
                        convertQuantity = new BigDecimal(minerFee).multiply(tokenConvertRate.getConvertRate()).setScale(scale, BigDecimal.ROUND_UP);
                        arriveQuantity = new BigDecimal(withdrawQuantity)
                                .subtract(DecimalUtil.toBigDecimal(response.getPlatformFee())) // 平台手续费
                                .subtract(withdrawLimitDTO.getFee()) // 券商手续费
                                .subtract(convertQuantity) // 抵扣矿工费 = 矿工费 * 转换比例
                                .setScale(scale, BigDecimal.ROUND_DOWN);
                    } else {
                        convertQuantity = (DecimalUtil.toBigDecimal(response.getPlatformFee()).add(withdrawLimitDTO.getFee()).add(new BigDecimal(minerFee)))
                                .multiply(tokenConvertRate.getConvertRate())
                                .setScale(scale, BigDecimal.ROUND_UP);
                        arriveQuantity = new BigDecimal(withdrawQuantity)
                                .subtract(convertQuantity) // 抵扣费用 = (平台手续费 + 券商手续费 + 矿工费) * 转换比例
                                .setScale(scale, BigDecimal.ROUND_DOWN);
                    }
                } else {
                    convertQuantity = BigDecimal.ZERO;
                    if (tokenId.equals(response.getPlatformFeeTokenId())) {
                        arriveQuantity = new BigDecimal(withdrawQuantity)
                                .subtract(DecimalUtil.toBigDecimal(response.getPlatformFee())) // 平台手续费
                                .subtract(withdrawLimitDTO.getFee()) // 券商手续费
                                .setScale(scale, BigDecimal.ROUND_DOWN);
                    } else {
                        arriveQuantity = new BigDecimal(withdrawQuantity);
                    }
                }
            }
        }
        if (arriveQuantity.compareTo(BigDecimal.ZERO) <= 0) {
            log.error("Error: get a negative withdraw quantity after calculate");
            throw new BrokerException(BrokerErrorCode.WITHDRAW_AMOUNT_ILLEGAL);
        }

        WithdrawCheckResult withdrawCheckResult = withdrawProcessCheck(header, tokenId, withdrawQuantity, fxRate);
        User user = userService.getUser(header.getUserId());
        if (user.getUserType() == UserType.INSTITUTIONAL_USER.value()) {
            withdrawCheckResult.setNeedCheckIdCard(false);
        }
        boolean needBrokerAudit = forceBrokerAudit(header.getOrgId(), header.getUserId());
        WithdrawOrder withdrawOrder = WithdrawOrder.builder()
                .orgId(header.getOrgId())
                .userId(header.getUserId())
                .accountId(accountService.getMainAccountId(header))
                .addressId(0L)
                .address(address)
                .addressExt(addressExt)
                .isInnerAddress(checkAddressResponse.getIsInnerAddress() ? 1 : 0)
                .orderId(0L)
                .tokenId(tokenId)
                .chainType(chainType)
                .clientOrderId(clientOrderId)
                .feeTokenId(response.getPlatformFeeTokenId())
                .platformFee(DecimalUtil.toBigDecimal(response.getPlatformFee()))
                .brokerFee(withdrawLimitDTO.getFee())
                .minerFeeTokenId(response.getMinerFeeTokenId())
                .minerFee(new BigDecimal(minerFee))
                .isAutoConvert(isAutoConvert ? 1 : 0) // 这里指的是当手续费token和提币的token不一致的时候，是否由券商垫付手续费
                .convertRate(convertRate)
                .convertQuantity(convertQuantity)
                .quantity(new BigDecimal(withdrawQuantity))
                .arrivalQuantity(arriveQuantity)
                .status(WithdrawalBrokerAuditEnum.NO_NEED.getNumber())
                .orderStatus(WithdrawalStatus.AUDITING_STATUS_VALUE)
                .needBrokerAudit(needBrokerAudit ? 1 : 0)
                .needCheckCardNo(0)
                .cardNo("")
                .platform(header.getPlatform().name())
                .ip(header.getRemoteIp())
                .userAgent(header.getUserAgent())
                .language(header.getLanguage())
                .appBaseHeader(GrpcHeaderUtil.getAppBaseInfo(header))
                .currentFxRate(fxRateInfo)
                .btcValue(btcValue)
                .created(currentTimestamp)
                .updated(currentTimestamp)
                .build();
        withdrawOrderMapper.insertSelective(withdrawOrder);

        // 处理提现请求
        long signTime = System.currentTimeMillis() / 1000;
        String signNonce = UUID.randomUUID().toString();
        WithdrawalRequestSign withdrawSign = WithdrawalRequestSign.newBuilder()
                .setOrganizationId(header.getOrgId())
                .setClientWithdrawalId(clientOrderId)
                .setAccountId(accountService.getMainAccountId(header))
                .setTokenId(tokenId)
                .setMinerTokenId(response.getMinerFeeTokenId())
                .setFeeTokenId(response.getPlatformFeeTokenId())
                .setAddress(address)
                .setArriveQuantity(DecimalUtil.toTrimString(arriveQuantity))
                .setMinerFee(DecimalUtil.toTrimString(new BigDecimal(minerFee)))
                .setPlatformFee(DecimalUtil.toTrimString(response.getPlatformFee()))
                .setBrokerFee(DecimalUtil.toTrimString(withdrawLimitDTO.getFee()))
                .setIsBrokerAutoConvert(isAutoConvert)
                .setBrokerConvertPaidQuantity(DecimalUtil.toTrimString(convertQuantity))
                .setQuantity(DecimalUtil.toTrimString(new BigDecimal(withdrawQuantity)))
                .setConvertRate(DecimalUtil.toTrimString(convertRate))
                .setSignTime(signTime)
                .setSignNonce(signNonce)
                .build();

        String sign = "";
        try {
            sign = signUtils.sign(withdrawSign.toByteArray(), header.getOrgId());
        } catch (Exception e) {
            throw new BrokerException(BrokerErrorCode.WITHDRAW_FAILED);
        }

        WithdrawalRequest withdrawalRequest = WithdrawalRequest.newBuilder()
                .setBaseRequest(GrpcRequestUtil.getBaseRequest(header))
                .setAccountId(accountService.getMainAccountId(header))
                .setTokenId(tokenId)
                .setChainType(chainType)
                .setClientWithdrawalId(clientOrderId)
                .setTargetOrgId(header.getOrgId())
                .setAddress(address)
                .setAddressExt(Strings.nullToEmpty(addressExt))
                .setQuantity(DecimalUtil.fromBigDecimal(new BigDecimal(withdrawQuantity)))
                .setArriveQuantity(DecimalUtil.fromBigDecimal(arriveQuantity))
                .setFeeTokenId(response.getPlatformFeeTokenId())
                .setPlatformFee(response.getPlatformFee())
                .setBrokerFee(DecimalUtil.fromBigDecimal(withdrawLimitDTO.getFee()))
                .setMinerTokenId(response.getMinerFeeTokenId())
                .setMinerFee(DecimalUtil.fromBigDecimal(new BigDecimal(minerFee)))
                .setIsBrokerAutoConvert(isAutoConvert)
                .setConvertRate(DecimalUtil.fromBigDecimal(convertRate))
                .setBrokerConvertPaidQuantity(DecimalUtil.fromBigDecimal(convertQuantity))
                .setLanguage(header.getLanguage())
                .setAuditStatus(needBrokerAudit ? WithdrawalBrokerAuditEnum.BROKER_AUDITING : WithdrawalBrokerAuditEnum.NO_NEED)
                //.setAuditStatus(WithdrawalBrokerAuditEnum.NO_NEED)
                .setNotCheckVerifyCode(true)
                .setInnerWithdrawFee(response.getInnerWithdrawFee())
                .setIsUidWithdraw(addressIsUserId)
                .setSignTime(signTime)
                .setSignNonce(signNonce)
                .setSignBroker(sign)
                .build();
        WithdrawalResponse withdrawalResponse = grpcWithdrawService.withdraw(withdrawalRequest);
        WithdrawOrder updateObj = WithdrawOrder.builder()
                .id(withdrawOrder.getId())
                .orderId(withdrawalResponse.getWithdrawalOrderId())
                .build();
        if (withdrawalResponse.getStatus() == WithdrawalStatus.BROKER_AUDITING_STATUS) {
            updateObj.setNeedBrokerAudit(1);
            updateObj.setStatus(WithdrawalBrokerAuditEnum.BROKER_AUDITING.getNumber());
            updateObj.setOrderStatus(WithdrawalStatus.BROKER_AUDITING_STATUS_VALUE);
        }
        withdrawOrderMapper.updateByPrimaryKeySelective(updateObj);
        return OpenApiWithdrawResponse.newBuilder()
                .setAllowWithdraw(true).setNeedBrokerAudit(withdrawalResponse.getStatus() == WithdrawalStatus.BROKER_AUDITING_STATUS)
                .setWithdrawOrderId(withdrawalResponse.getWithdrawalOrderId()).build();
    }

    public CancelWithdrawOrderResponse cancelWithdrawOrder(Header header, Long orderId) {
        Long accountId = accountService.getMainAccountId(header);
        CancelWithdrawRequest request = CancelWithdrawRequest.newBuilder()
                .setBaseRequest(GrpcRequestUtil.getBaseRequest(header))
                .setAccountId(accountId)
                .setOrderId(orderId)
                .build();
        CancelWithdrawResponse response = grpcWithdrawService.cancelWithdrawOrder(request);
        WithdrawalResult result = response.getWithdrawalResult();
        if (result == WithdrawalResult.CANNOT_CANCEL_WITHDRAW) {
            throw new BrokerException(BrokerErrorCode.WITHDRAW_ORDER_CANNOT_BE_CANCELLED);
        }
        return CancelWithdrawOrderResponse.newBuilder().setStatusCode(response.getStatus().name()).build();
    }

    public GetBrokerWithdrawOrderInfoResponse getBrokerWithdrawOrderInfo(GetBrokerWithdrawOrderInfoRequest request) {
        WithdrawOrder withdrawOrder = withdrawOrderMapper.getByOrderId(request.getOrderId());
        if (withdrawOrder == null) {
            return GetBrokerWithdrawOrderInfoResponse.newBuilder()
                    .setBasicRet(BasicRet.newBuilder().setCode(BrokerErrorCode.DB_RECORD_NOT_EXISTS.code()).build())
                    .build();
        }
        return GetBrokerWithdrawOrderInfoResponse.newBuilder()
                .setBrokerWithdrawOrderInfo(GetBrokerWithdrawOrderInfoResponse.BrokerWithdrawOrderInfo.newBuilder()
                        .setId(withdrawOrder.getId())
                        .setUserId(withdrawOrder.getUserId())
                        .setAccountId(withdrawOrder.getAccountId())
                        .setAddress(withdrawOrder.getAddress())
                        .setAddressExt(withdrawOrder.getAddressExt())
                        .setTokenId(withdrawOrder.getTokenId())
                        .setChainType(withdrawOrder.getChainType())
                        .setQuantity(withdrawOrder.getQuantity().stripTrailingZeros().toPlainString())
                        .setArrivalQuantity(withdrawOrder.getArrivalQuantity().stripTrailingZeros().toPlainString())
                        .setIp(withdrawOrder.getIp())
                        .setPlatform(withdrawOrder.getPlatform())
                        .setUserAgent(Strings.nullToEmpty(withdrawOrder.getUserAgent()))
                        .setAppBaseHeader(Strings.nullToEmpty(withdrawOrder.getAppBaseHeader()))
                        .build())
                .build();
    }

    public GetBrokerWithdrawOrderInfoListResponse getBrokerWithdrawOrderInfoList(Header header, String tokenId, Long fromOrderId, Integer limit) {
        Example example = Example.builder(WithdrawOrder.class).orderByDesc("id").build();
        PageHelper.startPage(1, limit > 0 ? limit : 100);
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("orgId", header.getOrgId())
                .andEqualTo("userId", header.getUserId());
        if (StringUtils.isNotEmpty(tokenId)) {
            criteria.andEqualTo("tokenId", tokenId);
        }
        if (fromOrderId > 0) {
            criteria.andLessThan("id", fromOrderId);
        }


        List<WithdrawOrder> orders = withdrawOrderMapper.selectByExample(example);
        if (CollectionUtils.isEmpty(orders)) {
            return GetBrokerWithdrawOrderInfoListResponse.getDefaultInstance();
        }

        List<GetBrokerWithdrawOrderInfoListResponse.BrokerWithdrawOrderInfo> result = orders.stream().map(withdrawOrder -> {
            GetBrokerWithdrawOrderInfoListResponse.BrokerWithdrawOrderInfo.Builder builder = GetBrokerWithdrawOrderInfoListResponse.BrokerWithdrawOrderInfo.newBuilder();
            builder.setId(withdrawOrder.getId())
                    .setUserId(withdrawOrder.getUserId())
                    .setAccountId(withdrawOrder.getAccountId())
                    .setAddress(withdrawOrder.getAddress())
                    .setAddressExt(withdrawOrder.getAddressExt())
                    .setTokenId(withdrawOrder.getTokenId())
                    .setChainType(withdrawOrder.getChainType())
                    .setQuantity(withdrawOrder.getQuantity().stripTrailingZeros().toPlainString())
                    .setArrivalQuantity(withdrawOrder.getArrivalQuantity().stripTrailingZeros().toPlainString())
                    .setIp(withdrawOrder.getIp())
                    .setPlatform(withdrawOrder.getPlatform())
                    .setUserAgent(Strings.nullToEmpty(withdrawOrder.getUserAgent()))
                    .setAppBaseHeader(Strings.nullToEmpty(withdrawOrder.getAppBaseHeader()))
                    .setStatus(withdrawOrder.getStatus())
                    .setCreated(withdrawOrder.getCreated());
            return builder.build();
        }).collect(Collectors.toList());
        return GetBrokerWithdrawOrderInfoListResponse.newBuilder().addAllBrokerWithdrawOrderInfo(result).build();
    }

}
