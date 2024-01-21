package io.bhex.broker.server.grpc.server.service;

import com.google.api.client.util.Lists;
import com.google.common.base.Strings;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.ibatis.session.RowBounds;
import org.springframework.context.ApplicationContext;
import org.springframework.core.task.TaskExecutor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Resource;

import io.bhex.base.account.AccountType;
import io.bhex.base.account.BusinessSubject;
import io.bhex.base.account.SyncTransferRequest;
import io.bhex.base.account.SyncTransferResponse;
import io.bhex.base.idgen.api.ISequenceGenerator;
import io.bhex.base.quote.Rate;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.common.util.JsonUtil;
import io.bhex.broker.core.domain.BaseResult;
import io.bhex.broker.grpc.bwlist.UserBlackWhiteType;
import io.bhex.broker.grpc.common.AccountTypeEnum;
import io.bhex.broker.grpc.common.Header;
import io.bhex.broker.grpc.user.VerifyTradePasswordResponse;
import io.bhex.broker.server.domain.BrokerServerConstants;
import io.bhex.broker.server.domain.FrozenReason;
import io.bhex.broker.server.domain.FrozenStatus;
import io.bhex.broker.server.domain.FrozenType;
import io.bhex.broker.server.domain.FunctionModule;
import io.bhex.broker.server.domain.RedPacketStatus;
import io.bhex.broker.server.domain.RedPacketType;
import io.bhex.broker.server.grpc.client.service.GrpcBatchTransferService;
import io.bhex.broker.server.grpc.server.service.listener.RedPacketReceivedEvent;
import io.bhex.broker.server.model.FrozenUserRecord;
import io.bhex.broker.server.model.RedPacket;
import io.bhex.broker.server.model.RedPacketConfig;
import io.bhex.broker.server.model.RedPacketReceiveDetail;
import io.bhex.broker.server.model.RedPacketTheme;
import io.bhex.broker.server.model.RedPacketTokenConfig;
import io.bhex.broker.server.model.User;
import io.bhex.broker.server.model.UserVerify;
import io.bhex.broker.server.primary.mapper.FrozenUserRecordMapper;
import io.bhex.broker.server.primary.mapper.RedPacketConfigMapper;
import io.bhex.broker.server.primary.mapper.RedPacketMapper;
import io.bhex.broker.server.primary.mapper.RedPacketReceiveDetailMapper;
import io.bhex.broker.server.primary.mapper.RedPacketThemeMapper;
import io.bhex.broker.server.primary.mapper.RedPacketTokenConfigMapper;
import io.bhex.broker.server.primary.mapper.UserMapper;
import io.bhex.broker.server.primary.mapper.UserVerifyMapper;
import io.bhex.broker.server.util.BaseReqUtil;
import io.bhex.broker.server.util.GrpcHeaderUtil;
import lombok.extern.slf4j.Slf4j;
import tk.mybatis.mapper.entity.Example;

@Service
@Slf4j
public class RedPacketService {

    public static final int FROZEN_OPEN_RED_PACKET_TIME = 24 * 3600 * 1000;

    public static final int RED_PACKET_EXPIRED_MILLISECONDS = 24 * 3600 * 1000;

    public static final String EQUIVALENT_TOKEN = "USDT";

    public static final char[] RANDOM_CODE_CHARS = "AB0CD1EF2GH3JK4LM5NP6QR7ST8UV9WXYZ".toCharArray();

    // redPacketIds for those not expired
    // map -> key: redPacketId, value: expiredTime
    public static final String RED_PACKET_IDS_FOR_EXPIRED_SCAN = "RP_EXPIRED_TIME";

    // lpush -> key: OrgId + redPacketId, value: assignedList -> [7, 8, 3, 9, 0]
    public static final String RED_PACKET_ASSIGN_INDEX_QUEUE = "RP_%s_%s";

    // increase -> key: orgId + userId
    private static final String RED_PACKET_ERROR_PASSWORD_COUNT_KEY = "RP_ERROR_PWS_COUNT_%s_%s";

    // add -> key: orgId + redPacketId, value: userId
    public static final String RED_PACKET_RECEIVED_USER_IDS = "RP_RECEIVED_UIDs_%s_%s";

    // map -> key: orgId + password, value: redPacketId
    public static final String RED_PACKET_PASSWORD2ID = "RP_PWD_2_ID_%s_%s";

    private static final Integer RED_PACKET_INDEX_THEME_POSITION = 1;
    private static final Integer SEND_PASSWORD_RED_PACKET_THEME_POSITION = 2;
    private static final Integer SEND_INVITE_RED_PACKET_THEME_POSITION = 3;

    @Resource
    private RedPacketMapper redPacketMapper;

    @Resource
    private RedPacketConfigMapper redPacketConfigMapper;

    @Resource
    private RedPacketTokenConfigMapper redPacketTokenConfigMapper;

    @Resource
    private RedPacketReceiveDetailMapper redPacketReceiveDetailMapper;

    @Resource
    private RedPacketThemeMapper redPacketThemeMapper;

    @Resource
    private ISequenceGenerator<Long> sequenceGenerator;

    @Resource
    private UserMapper userMapper;

    @Resource
    private UserVerifyMapper userVerifyMapper;

    @Resource
    private AccountService accountService;

    @Resource
    private UserSecurityService userSecurityService;

    @Resource
    private BasicService basicService;

    @Resource
    private FrozenUserRecordMapper frozenUserRecordMapper;

    @Resource
    private GrpcBatchTransferService grpcBatchTransferService;

    @Resource(name = "stringRedisTemplate")
    private StringRedisTemplate redisTemplate;

    @Resource
    private ApplicationContext applicationContext;

    @Resource(name = "asyncTaskExecutor")
    private TaskExecutor taskExecutor;

    @Resource
    private BrokerService brokerService;

    @Resource
    private UserBlackWhiteListConfigService userBlackWhiteListConfigService;
    @Resource
    private RiskControlBalanceService riskControlBalanceService;

    public io.bhex.broker.grpc.red_packet.RedPacketConfig queryRedPacketConfig(Header header) {
        if (!BrokerService.checkModule(header, FunctionModule.RED_PACKET) && header.getOrgId() > 0) {
            return io.bhex.broker.grpc.red_packet.RedPacketConfig.getDefaultInstance();
        }
        io.bhex.broker.grpc.red_packet.RedPacketConfig.Builder configBuilder =
                io.bhex.broker.grpc.red_packet.RedPacketConfig.newBuilder();
        RedPacketConfig redPacketConfig = redPacketConfigMapper.getRedPacketConfig(header.getOrgId());
        if (redPacketConfig == null) {
            redPacketConfig = RedPacketConfig.builder()
                    .passwordRedPacket(1)
                    .inviteRedPacket(1)
                    .build();
        }
        configBuilder.setPasswordRedPacket(redPacketConfig.getPasswordRedPacket());
        configBuilder.setInviteRedPacket(redPacketConfig.getInviteRedPacket());
        List<RedPacketTheme> themeList = redPacketThemeMapper.queryOrgRedPacketTheme(header.getOrgId());
        Map<Integer, List<RedPacketTheme>> themesMap = themeList.stream().collect(Collectors.groupingBy(RedPacketTheme::getPosition));
        configBuilder.addAllRedPacketIndexTheme(themesMap.getOrDefault(RED_PACKET_INDEX_THEME_POSITION, Lists.newArrayList()).stream().map(RedPacketTheme::convertGrpcObj).collect(Collectors.toList()));
        configBuilder.addAllSendPasswordRedPacketTheme(themesMap.getOrDefault(SEND_PASSWORD_RED_PACKET_THEME_POSITION, Lists.newArrayList()).stream().map(RedPacketTheme::convertGrpcObj).collect(Collectors.toList()));
        configBuilder.addAllSendInviteRedPacketTheme(themesMap.getOrDefault(SEND_INVITE_RED_PACKET_THEME_POSITION, Lists.newArrayList()).stream().map(RedPacketTheme::convertGrpcObj).collect(Collectors.toList()));
        List<RedPacketTokenConfig> tokenConfigList = redPacketTokenConfigMapper.queryOrgRedPacketTokenConfig(header.getOrgId());
        configBuilder.addAllTokenConfig(tokenConfigList.stream().map(RedPacketTokenConfig::convertGrpcObj).collect(Collectors.toList()));
        return configBuilder.build();
    }

    /**
     * 获取券商的红包Token配置
     *
     * @param header
     * @return
     */
    public List<RedPacketTokenConfig> queryOrgRedPacketTokenConfig(Header header) {
        return redPacketTokenConfigMapper.queryOrgRedPacketTokenConfig(header.getOrgId());
    }

    /**
     * 获取券商的红包Theme配置
     *
     * @param header
     * @return
     */
    public List<RedPacketTheme> queryOrgRedPacketTheme(Header header) {
        return redPacketThemeMapper.queryOrgRedPacketTheme(header.getOrgId());
    }

    public Integer getSendRedPacketThemePosition(Integer receiverUserType) {
        if (receiverUserType == 1) {
            // 邀请红包的theme在tb_red_packet_theme中的posotion配置的是3
            return 3;
        }
        return 2;
    }

    /**
     * 目前限定的是口令红包 接口在没有进行Insert操作之前，可以使用BaseResult返回数据，transfer和update可以throw Exception，以保证事务回滚
     *
     * @param header           请求通用信息
     * @param redPacketType    红包类型 普通红包 随机红包
     * @param themeId          主题Id
     * @param needPassword     是否使用口令红包
     * @param tokenId          红包Token
     * @param totalCount       总共的红包个数
     * @param amount           普通红包单个金额
     * @param totalAmount      随机红包总金额
     * @param receiverUserType 收红包用户类型 0 不限 1 仅限新用户
     * @return 红包信息
     */
    @Transactional(isolation = Isolation.READ_COMMITTED, propagation = Propagation.REQUIRED, rollbackFor = Throwable.class)
    public BaseResult<RedPacket> sendRedPacket(Header header, Integer redPacketType, Long themeId, Boolean needPassword,
                                               String tokenId, Integer totalCount, BigDecimal amount, BigDecimal totalAmount,
                                               Integer receiverUserType, String tradePassword) {
        if (!BrokerService.checkModule(header.getOrgId(), FunctionModule.RED_PACKET)) {
            return BaseResult.fail(BrokerErrorCode.FEATURE_NOT_OPEN);
        }

        if (header.getUserId() == 842145604923681280L) { //1.禁止这个用户一切资金操作 7070的账号
            throw new BrokerException(BrokerErrorCode.FEATURE_SUSPENDED);
        }

        if (userBlackWhiteListConfigService.inWithdrawWhiteBlackList(header.getOrgId(),
                tokenId, header.getUserId(), UserBlackWhiteType.BLACK_CONFIG)) {
            log.info("sendRedPacket inWithdrawBlackList orgId {} userId {}", header.getOrgId(), header.getUserId());
            return BaseResult.fail(BrokerErrorCode.RISK_CONTROL_INTERCEPTION_LIMIT);
        }


        //风控拦截check 如果在风控黑名单则不允许发红包
        riskControlBalanceService.checkRiskControlLimit(header.getOrgId(), header.getUserId(), FrozenType.FROZEN_SEND_RED_PACKET);
//        UserBizPermission userRiskControlConfig = riskControlService.query(header.getOrgId(), header.getUserId());
//        if (userRiskControlConfig != null && userRiskControlConfig.getAssetTransferOut() == 1) {
//            throw new BrokerException(BrokerErrorCode.RISK_CONTROL_INTERCEPTION_LIMIT);
//        }

        VerifyTradePasswordResponse verifyTradePwdResponse = userSecurityService.verifyTradePassword(header, tradePassword);
        if (verifyTradePwdResponse.getRet() != 0) {
            return BaseResult.fail(BrokerErrorCode.fromCode(verifyTradePwdResponse.getRet()));
        }
        // validThemeId
        if (themeId > 0) {
            //
            RedPacketTheme theme = redPacketThemeMapper.selectOne(RedPacketTheme.builder().id(themeId).position(getSendRedPacketThemePosition(receiverUserType)).build());
            if (theme == null || (theme.getOrgId() != header.getOrgId() && theme.getOrgId() != 0)) {
                return BaseResult.fail(BrokerErrorCode.RED_PACKET_INVALID_THEME);
            }
        }
        RedPacketTokenConfig config = redPacketTokenConfigMapper.selectOne(RedPacketTokenConfig.builder().orgId(header.getOrgId()).tokenId(tokenId).status(1).build());
        if (config == null) {
            return BaseResult.fail(BrokerErrorCode.RED_PACKET_INVALID_TOKEN);
        }
        Long currentTimestamp = System.currentTimeMillis();
        User user = userMapper.getByOrgAndUserId(header.getOrgId(), header.getUserId());
        RedPacket redPacket = RedPacket.builder()
                .id(sequenceGenerator.getLong())
                .orgId(header.getOrgId())
                .userId(header.getUserId())
                .accountId(accountService.getMainAccountId(header))
                .chatId(0L)
                .avatar(user.getAvatar())
                .nickname(user.getNickname())
                .username(getUsername(user))
                .inviteUrl("")
                .inviteCode(user.getInviteCode())
                .themeId(themeId)
                .backgroundUrl("")
                .slogan("")
                .redPacketType(redPacketType)
                .needPassword(needPassword ? 1 : 0)
                .password(needPassword ? RandomStringUtils.random(6, RANDOM_CODE_CHARS) : null)
                .tokenId(tokenId)
                .tokenName(basicService.getTokenName(header.getOrgId(), tokenId))
                .totalCount(totalCount)
                .amount(amount)
                .sendTransferId(sequenceGenerator.getLong())
                .refundTransferId(sequenceGenerator.getLong())
                .receiveUserType(receiverUserType)
                .expired(currentTimestamp + RED_PACKET_EXPIRED_MILLISECONDS)
                .remainCount(totalCount)
                .refundAmount(BigDecimal.ZERO)
                .status(RedPacketStatus.DOING.status())
                .ip(header.getRemoteIp())
                .platform(header.getPlatform().name())
                .userAgent(header.getUserAgent())
                .language(header.getLanguage())
                .appBaseHeader(GrpcHeaderUtil.getAppBaseInfo(header))
                .channel("")
                .source("")
                .created(currentTimestamp)
                .updated(currentTimestamp)
                .build();
        if (totalCount <= 0) {
            return BaseResult.fail(BrokerErrorCode.RED_PACKET_TOTAL_COUNT_LESS_THAN_MIN);
        }
        if (totalCount > config.getMaxCount()) {
            log.warn("sendRedPacket: orgId:{} userId:{} redPacketType:{} totalCount:{} > config.maxCount:{}",
                    header.getOrgId(), header.getUserId(), redPacketType, totalCount, config.getMaxCount());
            return BaseResult.fail(BrokerErrorCode.RED_PACKET_TOTAL_COUNT_GREAT_THAN_MIN);
        }
        String[] amountAssignInfos = new String[totalCount];
        if (redPacketType == RedPacketType.NORMAL.type()) {
            // valid amount prevision
            if (amount.remainder(config.getMinAmount()).compareTo(BigDecimal.ZERO) != 0) {
                log.warn("sendRedPacket: orgId:{} userId:{} redPacketType:{} amount:{} scale invalid, config.minAmount:{}",
                        header.getOrgId(), header.getUserId(), "NORMAL", amount.toPlainString(), config.getMinAmount().toPlainString());
                return BaseResult.fail(BrokerErrorCode.RED_PACKET_INVALID_AMOUNT);
            }
            if (amount.compareTo(config.getMinAmount()) < 0) {
                log.warn("sendRedPacket: orgId:{} userId:{} redPacketType:{} amount:{} < config.minAmount:{}",
                        header.getOrgId(), header.getUserId(), "NORMAL", amount.toPlainString(), config.getMinAmount().toPlainString());
                return BaseResult.fail(BrokerErrorCode.RED_PACKET_AMOUNT_LESS_THAN_MIN);
            }
            if (amount.compareTo(config.getMaxAmount()) > 0) {
                log.warn("sendRedPacket: orgId:{} userId:{} redPacketType:{} amount:{} >config.maxAmount:{}",
                        header.getOrgId(), header.getUserId(), "NORMAL", amount.toPlainString(), config.getMaxAmount().toPlainString());
                return BaseResult.fail(BrokerErrorCode.RED_PACKET_AMOUNT_GREAT_THAN_MAX);
            }
            totalAmount = amount.multiply(new BigDecimal(totalCount));
            redPacket.setTotalAmount(totalAmount);
            redPacket.setRemainAmount(totalAmount);
            amountAssignInfos = Collections.nCopies(totalCount, amount.stripTrailingZeros().toPlainString()).toArray(new String[0]);
        } else if (redPacketType == RedPacketType.RANDOM.type()) {
            // valid totalAmount precision
            if (totalAmount.remainder(config.getMinAmount()).compareTo(BigDecimal.ZERO) != 0) {
                log.warn("sendRedPacket: orgId:{} userId:{} redPacketType:{} totalAmount:{} scale invalid, config.minAmount:{}",
                        header.getOrgId(), header.getUserId(), "NORMAL", totalAmount.toPlainString(), config.getMinAmount().toPlainString());
                return BaseResult.fail(BrokerErrorCode.RED_PACKET_INVALID_TOTAL_AMOUNT);
            }
            if (totalAmount.compareTo(config.getMinAmount().multiply(new BigDecimal(totalCount))) < 0) {
                log.warn("sendRedPacket: orgId:{} userId:{} redPacketType:{} totalAmount:{} < config.minAmount:{} * totalCount:{}",
                        header.getOrgId(), header.getUserId(), "NORMAL", totalAmount.toPlainString(), config.getMinAmount().toPlainString(), totalCount);
                return BaseResult.fail(BrokerErrorCode.RED_PACKET_TOTAL_AMOUNT_LESS_THAN_MIN);
            }
            if (totalAmount.compareTo(config.getMaxTotalAmount()) > 0) {
                log.warn("sendRedPacket: orgId:{} userId:{} redPacketType:{} totalAmount:{} > config.maxTotalAmount:{}",
                        header.getOrgId(), header.getUserId(), "NORMAL", totalAmount.toPlainString(), config.getMaxTotalAmount().toPlainString());
                return BaseResult.fail(BrokerErrorCode.RED_PACKET_TOTAL_AMOUNT_GREAT_THAN_MAX);
            }
            redPacket.setTotalAmount(totalAmount);
            redPacket.setRemainAmount(totalAmount);
            if (config.getMinAmount().multiply(new BigDecimal(totalCount)).compareTo(totalAmount) == 0) {
                amountAssignInfos = Collections.nCopies(totalCount, config.getMinAmount().stripTrailingZeros().toPlainString()).toArray(new String[0]);
            } else {
                // 保证分配金额不能超过红包总金额
                do {
                    amountAssignInfos = getRandomAmount(totalAmount, totalCount, config);
                } while (Stream.of(amountAssignInfos).map(BigDecimal::new).reduce(BigDecimal::add).orElse(BigDecimal.ZERO).compareTo(totalAmount) != 0);
            }
        } else {
            return BaseResult.fail(BrokerErrorCode.PARAM_INVALID);
        }
        Rate rate = basicService.getV3Rate(header.getOrgId(), tokenId);
        String fxRateStr = "";
        try {
            fxRateStr = JsonUtil.defaultProtobufJsonPrinter().print(rate);
        } catch (Exception e) {
            fxRateStr = "";
        }
        redPacket.setFxRate(fxRateStr);
        BigDecimal equivalentRate = basicService.getFXRate(header.getOrgId(), tokenId, EQUIVALENT_TOKEN);
        redPacket.setEquivalentUsdtAmount(redPacket.getTotalAmount().multiply(equivalentRate).setScale(8, BigDecimal.ROUND_DOWN));
        // valid totalAmount >= balance.totalAvailable
        String tokenAvailable = accountService.getAvailableBalance(header, AccountTypeEnum.COIN, 0, tokenId);
        if (Strings.isNullOrEmpty(tokenAvailable) || new BigDecimal(tokenAvailable).compareTo(totalAmount) < 0) {
            return BaseResult.fail(BrokerErrorCode.INSUFFICIENT_BALANCE);
        }
//        Balance balance = accountService.queryTokenBalance(header.getOrgId(), redPacket.getAccountId(), tokenId);
//        if (balance == null || new BigDecimal(balance.getFree()).compareTo(totalAmount) < 0) {
//            return BaseResult.fail(BrokerErrorCode.INSUFFICIENT_BALANCE);
//        }
        redPacket.setAmountAssignInfo(Arrays.toString(amountAssignInfos));
        redPacketMapper.insertSelective(redPacket);
        List<String> assignIndexList = Lists.newArrayList();
        List<RedPacketReceiveDetail> receiveDetailList = Lists.newArrayList();
        for (int index = 0; index < amountAssignInfos.length; index++) {
            RedPacketReceiveDetail receiveDetail = RedPacketReceiveDetail.builder()
                    .orgId(header.getOrgId())
                    .redPacketId(redPacket.getId())
                    .themeId(themeId)
                    .backgroundUrl("")
                    .slogan("")
                    .senderUserId(redPacket.getUserId())
                    .senderAccountId(redPacket.getAccountId())
                    .senderAvatar("")
                    .senderNickname(user.getNickname())
                    .senderUsername(getUsername(user))
                    .tokenId(tokenId)
                    .tokenName(basicService.getTokenName(header.getOrgId(), tokenId))
                    .assignIndex(index)
                    .receiveAmount(new BigDecimal(amountAssignInfos[index]))
                    .fxRate(fxRateStr)
                    .equivalentUsdtAmount(new BigDecimal(amountAssignInfos[index]).multiply(equivalentRate).setScale(8, BigDecimal.ROUND_DOWN))
                    .receiverType(receiverUserType)
                    .receiverUserId(null)
                    .receiverAccountId(null)
                    .receiverAvatar("")
                    .receiverUsername("")
                    .receiverNickname("")
                    .transferId(sequenceGenerator.getLong())
                    .status(0)
                    .redPacketHandled(0)
                    .transferStatus(0)
                    .opened(0L)
                    .ip("")
                    .platform("")
                    .userAgent("")
                    .language("")
                    .appBaseHeader("")
                    .channel("")
                    .source("")
                    .created(currentTimestamp)
                    .updated(currentTimestamp)
                    .build();
            receiveDetailList.add(receiveDetail);
            assignIndexList.add(String.valueOf(index));
        }
        Collections.shuffle(receiveDetailList);
        redPacketReceiveDetailMapper.insertList(receiveDetailList);
        redisTemplate.opsForList().leftPushAll(String.format(RED_PACKET_ASSIGN_INDEX_QUEUE, header.getOrgId(), redPacket.getId()), assignIndexList);
        redisTemplate.expire(String.format(RED_PACKET_ASSIGN_INDEX_QUEUE, header.getOrgId(), redPacket.getId()), 24, TimeUnit.HOURS);
        redisTemplate.opsForValue().set(String.format(RED_PACKET_PASSWORD2ID, header.getOrgId(), redPacket.getPassword()), String.valueOf(redPacket.getId()), 24, TimeUnit.HOURS);
        sendRedPacketTransfer(redPacket, 0);
        try {
            redisTemplate.opsForZSet().add(RED_PACKET_IDS_FOR_EXPIRED_SCAN, redPacket.getId().toString(), redPacket.getExpired().doubleValue() + 5);
        } catch (Exception e) {
            // ignore
        }
        return BaseResult.success(redPacket);
    }

    // 生成红包的分配金额
    private String[] getRandomAmount(BigDecimal totalAmount, Integer totalCount, RedPacketTokenConfig redPacketTokenConfig) {
        Integer remainCount = totalCount;
        BigDecimal remainAmount = totalAmount;
        String[] amountAssignInfo = new String[totalCount];
        int scale = redPacketTokenConfig.getMinAmount().stripTrailingZeros().scale();
        SecureRandom random = new SecureRandom();
        while (remainCount > 0) {
            BigDecimal amount;
            if (remainCount == 1) {
                amount = remainAmount;
            } else {
                amount = BigDecimal.valueOf(random.nextDouble() *
                        (remainAmount.divide(new BigDecimal(remainCount), scale, RoundingMode.DOWN).multiply(new BigDecimal("2"))).doubleValue())
                        .setScale(scale, RoundingMode.DOWN);
                if (amount.compareTo(redPacketTokenConfig.getMinAmount()) < 0) {
                    amount = redPacketTokenConfig.getMinAmount();
                }
            }
            remainCount--;
            remainAmount = remainAmount.subtract(amount);
            amountAssignInfo[remainCount] = amount.stripTrailingZeros().toPlainString();
        }
        return amountAssignInfo;
    }

    private void sendRedPacketTransfer(RedPacket redPacket, int invokeIndex) {
        SyncTransferRequest request = SyncTransferRequest.newBuilder()
                .setBaseRequest(BaseReqUtil.getBaseRequest(redPacket.getOrgId()))
                // 转账ID 、TOKEN 、数量
                .setClientTransferId(redPacket.getSendTransferId())
                .setTokenId(redPacket.getTokenId())
                .setAmount(redPacket.getTotalAmount().stripTrailingZeros().toPlainString())
                // 用户
                .setSourceOrgId(redPacket.getOrgId())
                .setSourceAccountId(redPacket.getAccountId())
                .setSourceAccountType(AccountType.GENERAL_ACCOUNT)
                .setSourceFlowSubject(BusinessSubject.SEND_RED_PACKET)
                // 红包账户
                .setTargetOrgId(redPacket.getOrgId())
                .setTargetAccountType(AccountType.RED_PACKET)
                .setTargetFlowSubject(BusinessSubject.SEND_RED_PACKET)
                .build();
        try {
            SyncTransferResponse response = grpcBatchTransferService.syncTransfer(request);
            if (response.getCode() == SyncTransferResponse.ResponseCode.SUCCESS) {
                return;
            } else if (response.getCode() == SyncTransferResponse.ResponseCode.BALANCE_INSUFFICIENT) {
                throw new BrokerException(BrokerErrorCode.INSUFFICIENT_BALANCE);
            }
            log.error("sendRedPacketTransfer failed: orgId:{}, redPacket:{}, errorCode:{}", redPacket.getOrgId(), JsonUtil.defaultGson().toJson(redPacket), response.getCode());
            throw new BrokerException(BrokerErrorCode.RED_PACKET_SEND_FAILED);
        } catch (BrokerException e) {
            log.error("sendRedPacketTransfer error: orgId:{}, redPacket:{}", redPacket.getOrgId(), JsonUtil.defaultGson().toJson(redPacket), e);
            // 超时重试一次
            if (e.getCode() == BrokerErrorCode.GRPC_SERVER_TIMEOUT.code() && invokeIndex == 0) {
                sendRedPacketTransfer(redPacket, ++invokeIndex);
            } else {
                throw e;
            }
        } catch (Exception e) {
            log.error("sendRedPacketTransfer error: orgId:{}, redPacket:{}", redPacket.getOrgId(), JsonUtil.defaultGson().toJson(redPacket), e);
            throw new BrokerException(BrokerErrorCode.RED_PACKET_SEND_FAILED);
        }
    }

    private BaseResult<Long> validRedPacketPassword(Header header, String password) {
        long currentTimestamp = System.currentTimeMillis();
        FrozenUserRecord frozenUserRecord = frozenUserRecordMapper.getByUserIdAndFrozenType(header.getOrgId(), header.getUserId(), FrozenType.FROZEN_RECEIVE_RED_PACKET.type());
        if (frozenUserRecord != null
                && (currentTimestamp <= frozenUserRecord.getEndTime() && currentTimestamp >= frozenUserRecord.getStartTime())) {
            log.info("user:{} is under receive red_packet frozen, cannot receive red packet", header.getUserId());
            return BaseResult.fail(BrokerErrorCode.RED_PACKET_ERROR_PASSWORD6);
        }
        String cachedRedPacketId = redisTemplate.opsForValue().get(String.format(RED_PACKET_PASSWORD2ID, header.getOrgId(), password));
        if (!Strings.isNullOrEmpty(cachedRedPacketId)) {
            return BaseResult.success(Long.valueOf(cachedRedPacketId));
        }
        // 如果DB里面有记录，那么重新cache一下，并且返回
        RedPacket redPacket = redPacketMapper.selectOne(RedPacket.builder().orgId(header.getOrgId()).password(password).build());
        if (redPacket != null) {
            redisTemplate.opsForValue().set(String.format(RED_PACKET_PASSWORD2ID, header.getOrgId(), password), String.valueOf(redPacket.getId()));
            return BaseResult.success(redPacket.getId());
        }
        //设置红包输错的有效期24小时
        String errorRedPacketCacheKey = String.format(RED_PACKET_ERROR_PASSWORD_COUNT_KEY, header.getOrgId(), header.getUserId());
        redisTemplate.opsForValue().setIfAbsent(errorRedPacketCacheKey, "0", 1, TimeUnit.DAYS);
        Long errorPasswordCount = redisTemplate.opsForValue().increment(errorRedPacketCacheKey);
        if (errorPasswordCount == null || errorPasswordCount == 1) {
            return BaseResult.fail(BrokerErrorCode.RED_PACKET_ERROR_PASSWORD1);
        } else if (errorPasswordCount == 2) {
            return BaseResult.fail(BrokerErrorCode.RED_PACKET_ERROR_PASSWORD2);
        } else if (errorPasswordCount == 3) {
            return BaseResult.fail(BrokerErrorCode.RED_PACKET_ERROR_PASSWORD3);
        } else if (errorPasswordCount == 4) {
            return BaseResult.fail(BrokerErrorCode.RED_PACKET_ERROR_PASSWORD4);
        } else if (errorPasswordCount == 5) {
            frozenOpenRedPacket(header);
            return BaseResult.fail(BrokerErrorCode.RED_PACKET_ERROR_PASSWORD5);
        } else {
            frozenUserRecord = frozenUserRecordMapper.getByUserIdAndFrozenType(header.getOrgId(), header.getUserId(), FrozenType.FROZEN_RECEIVE_RED_PACKET.type());
            if (frozenUserRecord == null || currentTimestamp > frozenUserRecord.getEndTime()) {
                frozenOpenRedPacket(header);
            }
            return BaseResult.fail(BrokerErrorCode.RED_PACKET_ERROR_PASSWORD6);
        }
    }

    private void frozenOpenRedPacket(Header header) {
        //因为输错指定次数导致冻结用户时，调整原有输错红包的redis失效时间10s并且置0
        String errorRedPacketCacheKey = String.format(RED_PACKET_ERROR_PASSWORD_COUNT_KEY, header.getOrgId(), header.getUserId());
        redisTemplate.opsForValue().set(errorRedPacketCacheKey, "0", 10, TimeUnit.SECONDS);
        long currentTimestamp = System.currentTimeMillis();
        FrozenUserRecord frozenUserRecord = FrozenUserRecord.builder()
                .orgId(header.getOrgId())
                .userId(header.getUserId())
                .frozenType(FrozenType.FROZEN_RECEIVE_RED_PACKET.type())
                .frozenReason(FrozenReason.RED_PACKET_PASSWORD_ERROR.reason())
                .status(FrozenStatus.UNDER_FROZEN.status())
                .startTime(currentTimestamp)
                .endTime(currentTimestamp + FROZEN_OPEN_RED_PACKET_TIME)
                .created(currentTimestamp)
                .updated(currentTimestamp)
                .build();
        frozenUserRecordMapper.insertRecord(frozenUserRecord);
    }

    @Transactional(isolation = Isolation.READ_COMMITTED, propagation = Propagation.REQUIRED, rollbackFor = Throwable.class)
    public BaseResult<RedPacketReceiveDetail> openPasswordRedPacket(Header header, String password, Boolean passedGeeTestCheck) {
        /* 校验红包密码并且获取redPacketId
         * 如果因为遍历红包密码抢红包被冻结，则直接不允许抢红包
         * 否则给出响应的错误提示
         */
        BaseResult<Long> validPasswordAndGetId = validRedPacketPassword(header, password);
        if (!validPasswordAndGetId.isSuccess()) {
            return BaseResult.fail(BrokerErrorCode.fromCode(validPasswordAndGetId.getCode()));
        }
        Long redPacketId = validPasswordAndGetId.getData();
        // 用户是否已经打开过红包？
        if (redisTemplate.opsForSet().isMember(String.format(RED_PACKET_RECEIVED_USER_IDS, header.getOrgId(), redPacketId), String.valueOf(header.getUserId()))) {
            RedPacketReceiveDetail queryObj = RedPacketReceiveDetail.builder().orgId(header.getOrgId()).redPacketId(redPacketId).receiverUserId(header.getUserId()).build();
            RedPacketReceiveDetail receivedRedPacket = redPacketReceiveDetailMapper.selectOne(queryObj);
            receivedRedPacket.setHasBeenOpened(true);
            return BaseResult.success(receivedRedPacket);
        }
        // Redis 拦截一道: 看红包是否已经抢完
        if (!redisTemplate.hasKey(String.format(RED_PACKET_ASSIGN_INDEX_QUEUE, header.getOrgId(), redPacketId))
                || redisTemplate.opsForList().size(String.format(RED_PACKET_ASSIGN_INDEX_QUEUE, header.getOrgId(), redPacketId)) <= 0) {
            return BaseResult.fail(BrokerErrorCode.RED_PACKET_HAS_BEEN_OVER);
        }

        RedPacket redPacket = redPacketMapper.selectOne(RedPacket.builder().id(redPacketId).orgId(header.getOrgId()).build());
        User user = userMapper.getByOrgAndUserId(header.getOrgId(), header.getUserId());
        /* 如果红包是要求新人专享，那么校验
         * 1、当前抢红包的用户的邀请人是不是发红包的人
         * 2、当前抢红包的用户的注册时间是不是晚于发红包的时间
         * 否则被认为不符合抢红包的条件
         */
        if (redPacket.getReceiveUserType() != 1 && !passedGeeTestCheck) {
            return BaseResult.fail(BrokerErrorCode.RECAPTCHA_INVALID);
        }
        if (redPacket.getReceiveUserType() != 1) {
            UserVerify userVerify = UserVerify.decrypt(userVerifyMapper.getByUserId(header.getUserId()));
            if (userVerify == null || !userVerify.isSeniorVerifyPassed()) {
                throw new BrokerException(BrokerErrorCode.KYC_SENIOR_VERIFY_UNDONE);
            }
        }
        if (redPacket.getReceiveUserType() == 1 &&
                (!user.getInviteUserId().equals(redPacket.getUserId()) || user.getCreated() < redPacket.getCreated())) {
            return BaseResult.fail(BrokerErrorCode.RED_PACKET_RECEIVE_CONDITION_NOT_MATCH);
        }
        // 红包是否失效
        if (redPacket.getExpired() <= System.currentTimeMillis()) {
            return BaseResult.fail(BrokerErrorCode.RED_PACKET_HAS_BEEN_EXPIRED);
        }
        String assignIndex = redisTemplate.opsForList().rightPop(String.format(RED_PACKET_ASSIGN_INDEX_QUEUE, header.getOrgId(), redPacketId));
        if (Strings.isNullOrEmpty(assignIndex)) {
            return BaseResult.fail(BrokerErrorCode.RED_PACKET_HAS_BEEN_OVER);
        }
        try {
            RedPacketReceiveDetail receiveDetail = redPacketReceiveDetailMapper.lockWithIndex(header.getOrgId(), redPacketId, Integer.valueOf(assignIndex));
            if (receiveDetail.getReceiverUserId() != null && (receiveDetail.getReceiverUserId() > 0 || receiveDetail.getStatus() == 1)) {
                log.error("openRedPacket get a assigned red_packet index, so bad, orgId:{}, userId:{}, redPacketId:{}, index:{}", header.getOrgId(), header.getUserId(), redPacketId, assignIndex);
                throw new BrokerException(BrokerErrorCode.RED_PACKET_RECEIVER_FAILED);
            }
            if (receiveDetail.getStatus() == -1) {
                return BaseResult.fail(BrokerErrorCode.RED_PACKET_HAS_BEEN_EXPIRED);
            }
            RedPacketReceiveDetail receiverObj = RedPacketReceiveDetail.builder()
                    .id(receiveDetail.getId())
                    .receiverUserId(header.getUserId())
                    .receiverAccountId(accountService.getMainAccountId(header))
                    .receiverAvatar(user.getAvatar())
                    .receiverNickname(user.getNickname())
                    .receiverUsername(getUsername(user))
                    .status(1)
                    .ip(header.getRemoteIp())
                    .platform(header.getPlatform().name())
                    .userAgent(header.getUserAgent())
                    .language(header.getLanguage())
                    .appBaseHeader(GrpcHeaderUtil.getAppBaseInfo(header))
                    .opened(System.currentTimeMillis())
                    .updated(System.currentTimeMillis())
                    .build();
            redPacketReceiveDetailMapper.updateByPrimaryKeySelective(receiverObj);
            // 这里代表用户已经抢红包成功了，我们确保转账成功，不再抛出异常
            try {
                RedPacketReceivedEvent redPacketReceivedEvent = RedPacketReceivedEvent.builder().receiveId(receiveDetail.getId()).build();
                applicationContext.publishEvent(redPacketReceivedEvent);
                redisTemplate.opsForSet().add(String.format(RED_PACKET_RECEIVED_USER_IDS, header.getOrgId(), redPacketId), String.valueOf(header.getUserId()));
                CompletableFuture.runAsync(() -> receiverRedPacketTransfer(receiveDetail.getId()), taskExecutor);
            } catch (Exception e) {
                redisTemplate.opsForList().leftPush(RedPacketTaskHandleService.RED_PACKET_RECEIVED_QUEUE, String.valueOf(receiveDetail.getId()));
                redisTemplate.opsForSet().add(String.format(RED_PACKET_RECEIVED_USER_IDS, header.getOrgId(), redPacketId), String.valueOf(header.getUserId()));
                log.error("receiveRedPacket transfer error, redPacketReceiverDetail:{}", JsonUtil.defaultGson().toJson(receiveDetail), e);
            }
            receiveDetail.setHasBeenOpened(false);
            return BaseResult.success(receiveDetail);
        } catch (Exception e) {
            log.error("openRedPacket has error, so bad, orgId:{}, userId:{}, redPacketId:{}", header.getOrgId(), header.getUserId(), redPacketId, e);
            redisTemplate.opsForList().rightPush(String.format(RED_PACKET_ASSIGN_INDEX_QUEUE, header.getOrgId(), redPacketId), assignIndex);
            throw new BrokerException(BrokerErrorCode.RED_PACKET_RECEIVER_FAILED);
        }
    }

    public void receiverRedPacketTransfer(Long receiveDetailId) {
        RedPacketReceiveDetail receiveDetail = redPacketReceiveDetailMapper.selectByPrimaryKey(receiveDetailId);
        if (receiveDetail.getReceiverUserId() == null) {
            return;
        }
        receiverRedPacketTransfer(receiveDetail, 0);
    }

    public void receiverRedPacketTransfer(RedPacketReceiveDetail receiveDetail, int invokeIndex) {
        SyncTransferRequest request = SyncTransferRequest.newBuilder()
                .setBaseRequest(BaseReqUtil.getBaseRequest(receiveDetail.getOrgId()))
                // 转账ID 、TOKEN 、数量
                .setClientTransferId(receiveDetail.getTransferId())
                .setTokenId(receiveDetail.getTokenId())
                .setAmount(receiveDetail.getReceiveAmount().stripTrailingZeros().toPlainString())
                // 红包账户
                .setSourceOrgId(receiveDetail.getOrgId())
                .setSourceAccountType(AccountType.RED_PACKET)
                .setSourceFlowSubject(BusinessSubject.RECEIVER_RED_PACKET)
                // 用户
                .setTargetOrgId(receiveDetail.getOrgId())
                .setTargetAccountId(receiveDetail.getReceiverAccountId())
                .setTargetFlowSubject(BusinessSubject.RECEIVER_RED_PACKET)
                .build();
        try {
            SyncTransferResponse response = grpcBatchTransferService.syncTransfer(request);
            if (response.getCode() == SyncTransferResponse.ResponseCode.SUCCESS) {
                RedPacketReceiveDetail updateObj = RedPacketReceiveDetail.builder()
                        .id(receiveDetail.getId())
                        .transferStatus(1)
                        .updated(System.currentTimeMillis())
                        .build();
                redPacketReceiveDetailMapper.updateByPrimaryKeySelective(updateObj);
                return;
            } else if (response.getCode() == SyncTransferResponse.ResponseCode.BALANCE_INSUFFICIENT) {
                log.error("!!! transfer receiveRedPacket has insufficient balance, please check. orgId:{}, detailId:{}", receiveDetail.getOrgId(), receiveDetail.getId());
                log.error("!!! transfer receiveRedPacket has insufficient balance, please check. orgId:{}, detailId:{}", receiveDetail.getOrgId(), receiveDetail.getId());
            } else {
                log.error("receiverRedPacketTransfer failed: orgId:{}, detailId:{}, errorCode:{}", receiveDetail.getId(), receiveDetail.getId(), response.getCode());
            }
        } catch (Exception e) {
            log.error("receiverRedPacketTransfer error: orgId:{}, redPacket:{}", receiveDetail.getOrgId(), JsonUtil.defaultGson().toJson(receiveDetail), e);
        }
        if (invokeIndex == 0) {
            receiverRedPacketTransfer(receiveDetail, ++invokeIndex);
        }
    }

    /**
     * 抢红包-转账成功后处理红包的剩余个数和金额
     *
     * @param receiveId
     */
    @Transactional(isolation = Isolation.READ_COMMITTED, propagation = Propagation.REQUIRED, rollbackFor = Throwable.class)
    public void handleReceiveRedPacket(Long receiveId) {
        RedPacketReceiveDetail receiveDetail = redPacketReceiveDetailMapper.lockWithId(receiveId);
        if (receiveDetail.getReceiverUserId() == null || receiveDetail.getReceiverUserId() <= 0) {
            return;
        }
        if (receiveDetail.getRedPacketHandled() == 1) {
            return;
        }
        RedPacket redPacketLocked = redPacketMapper.lockedWithId(receiveDetail.getRedPacketId());
        int status = redPacketLocked.getStatus();
        if (redPacketLocked.getRemainAmount().compareTo(receiveDetail.getReceiveAmount()) == 0) {
            status = RedPacketStatus.OVER.status();
        }
        // 处理剩余个数和剩余金额
        redPacketMapper.reduceRemainValue(redPacketLocked.getId(), receiveDetail.getReceiveAmount(), status);
        RedPacketReceiveDetail updateObj = RedPacketReceiveDetail.builder()
                .id(receiveDetail.getId())
                .redPacketHandled(1)
                .updated(System.currentTimeMillis())
                .build();
        redPacketReceiveDetailMapper.updateByPrimaryKeySelective(updateObj);
        if (receiveDetail.getTransferStatus() == 0) {
            receiverRedPacketTransfer(receiveDetail, 0);
        }
    }

    /**
     * 查询我发出的红包
     */
    public List<RedPacket> queryRedPacketSendFromMe(Header header, Long fromId, Integer limit) {
        Example example = Example.builder(RedPacket.class).orderByDesc("id").build();
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("orgId", header.getOrgId());
        criteria.andEqualTo("userId", header.getUserId());
        if (fromId > 0) {
            criteria.andLessThan("id", fromId);
        }
        return redPacketMapper.selectByExampleAndRowBounds(example, new RowBounds(0, limit));
    }

    /**
     * 查询我收到的红包
     */
    public List<RedPacketReceiveDetail> queryMyReceivedRedPacket(Header header, Long fromId, Integer limit) {
        Example example = Example.builder(RedPacketReceiveDetail.class).orderByDesc("id").build();
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("orgId", header.getOrgId());
        criteria.andEqualTo("receiverUserId", header.getUserId());
        if (fromId > 0) {
            criteria.andLessThan("id", fromId);
        }
        return redPacketReceiveDetailMapper.selectByExampleAndRowBounds(example, new RowBounds(0, limit));
    }

    /**
     * 这个方法会校验红包是否是当前查询用户所发，或者是否领取过该红包，否则会返回null
     */
    public RedPacket getRedPacketForDetail(Header header, Long redPacketId) {
        if (redPacketMapper.selectOne(RedPacket.builder().id(redPacketId).orgId(header.getOrgId()).userId(header.getUserId()).build()) == null
                && redPacketReceiveDetailMapper.selectOne(RedPacketReceiveDetail.builder().orgId(header.getOrgId()).redPacketId(redPacketId).receiverUserId(header.getUserId()).build()) == null) {
            log.warn("getRedPacketForDetail: orgId:{} userId:{} want to query no-related red_packet receive detail with redPacketId:{}", header.getOrgId(), header.getUserId(), redPacketId);
            return null;
        }
        return redPacketMapper.selectOne(RedPacket.builder().orgId(header.getOrgId()).id(redPacketId).build());
    }

    /**
     * 查询某个红包的领取情况
     */
    public List<RedPacketReceiveDetail> queryRedPacketReceiveDetail(Header header, Long redPacketId, Long fromId, Integer limit) {
        // 如果此红包和当前查询人没任何关系，返回Empty
        if (redPacketMapper.selectOne(RedPacket.builder().id(redPacketId).orgId(header.getOrgId()).userId(header.getUserId()).build()) == null
                && redPacketReceiveDetailMapper.selectOne(RedPacketReceiveDetail.builder().orgId(header.getOrgId()).redPacketId(redPacketId).receiverUserId(header.getUserId()).build()) == null) {
            log.warn("queryReceiveDetail: orgId:{} userId:{} want to query no-related red_packet receive detail with redPacketId:{}", header.getOrgId(), header.getUserId(), redPacketId);
            return Lists.newArrayList();
        }
        Example example = Example.builder(RedPacketReceiveDetail.class).orderByDesc("id").build();
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("orgId", header.getOrgId());
        criteria.andEqualTo("redPacketId", redPacketId);
        criteria.andEqualTo("status", 1);
        if (fromId > 0) {
            criteria.andLessThan("id", fromId);
        }
        return redPacketReceiveDetailMapper.selectByExampleAndRowBounds(example, new RowBounds(0, limit));
    }

    private String getUsername(User user) {
        return user.getRegisterType() == 1 && !Strings.isNullOrEmpty(user.getMobile())
                ? user.getMobile().replaceAll(BrokerServerConstants.MASK_MOBILE_REG, "$1****$2")
                : user.getEmail().replaceAll(BrokerServerConstants.MASK_EMAIL_REG, "*");
    }

    @Transactional(isolation = Isolation.READ_COMMITTED, propagation = Propagation.REQUIRED, rollbackFor = Throwable.class)
    public void refundRedPacket(Long redPacketId) throws Exception {
        RedPacket redPacketLocked = redPacketMapper.lockedWithId(redPacketId);
        if (redPacketLocked.getStatus() == RedPacketStatus.OVER.status()
                || redPacketLocked.getStatus() == RedPacketStatus.REFUNDED.status()) {
            return;
        }
        List<RedPacketReceiveDetail> receiveDetailList = redPacketReceiveDetailMapper.select(RedPacketReceiveDetail.builder().redPacketId(redPacketId).build());
        BigDecimal receivedAmount = receiveDetailList.stream()
                .filter(receiveDetail -> receiveDetail.getReceiverUserId() != null)
                .map(RedPacketReceiveDetail::getReceiveAmount).reduce(BigDecimal::add).orElse(BigDecimal.ZERO);
        // 以下三个条件，任意一个都代表红包已经被抢完
        if (redPacketLocked.getRemainCount() == 0
                || redPacketLocked.getRemainAmount().compareTo(BigDecimal.ZERO) == 0
                || redPacketLocked.getTotalAmount().compareTo(receivedAmount) == 0) {
            // 以下三个条件，任意一个都代表红包的金额处理出问题了
            if (redPacketLocked.getRemainCount() != 0
                    || redPacketLocked.getRemainAmount().compareTo(BigDecimal.ZERO) != 0
                    || redPacketLocked.getTotalAmount().compareTo(receivedAmount) != 0) {
                String errorMsg = String.format("[RedPacketTaskHandle] orgId:%s redPacketId:%s handle 'over' error. totalCount:%s totalAmount:%s remainCount:%s remainAmount:%s",
                        redPacketLocked.getOrgId(), redPacketId, redPacketLocked.getTotalCount(), redPacketLocked.getTotalAmount().toPlainString(),
                        redPacketLocked.getRemainCount(), redPacketLocked.getRemainAmount().toPlainString());
                log.error(errorMsg);
                throw new Exception(errorMsg);
            }
            RedPacket updateObj = RedPacket.builder()
                    .id(redPacketId)
                    .status(RedPacketStatus.OVER.status())
                    .updated(System.currentTimeMillis())
                    .build();
            redPacketMapper.updateByPrimaryKeySelective(updateObj);
        }
        if (redPacketLocked.getTotalAmount().subtract(receivedAmount).compareTo(redPacketLocked.getRemainAmount()) != 0) {
            String errorMsg = String.format("[RedPacketTaskHandle] orgId:%s redPacketId:%s canculate refund error. totalAmount:%s receivedAmount:%s remainAmount:%s",
                    redPacketLocked.getOrgId(), redPacketId, redPacketLocked.getTotalAmount().toPlainString(),
                    receivedAmount.toPlainString(), redPacketLocked.getRemainAmount().toPlainString());
            log.error(errorMsg);
            throw new Exception(errorMsg);
        }
        SyncTransferRequest request = SyncTransferRequest.newBuilder()
                .setBaseRequest(BaseReqUtil.getBaseRequest(redPacketLocked.getOrgId()))
                // 转账ID 、TOKEN 、数量
                .setClientTransferId(redPacketLocked.getRefundTransferId())
                .setTokenId(redPacketLocked.getTokenId())
                .setAmount(redPacketLocked.getRemainAmount().stripTrailingZeros().toPlainString())
                // 红包账户
                .setSourceOrgId(redPacketLocked.getOrgId())
                .setSourceAccountType(AccountType.RED_PACKET)
                .setSourceFlowSubject(BusinessSubject.REFUND_RED_PACKET)
                // 用户
                .setTargetOrgId(redPacketLocked.getOrgId())
                .setTargetAccountId(redPacketLocked.getAccountId())
                .setTargetFlowSubject(BusinessSubject.REFUND_RED_PACKET)
                .build();
        SyncTransferResponse response = grpcBatchTransferService.syncTransfer(request);
        if (response.getCode() == SyncTransferResponse.ResponseCode.SUCCESS) {
            RedPacket updateObj = RedPacket.builder()
                    .id(redPacketId)
                    .refundAmount(redPacketLocked.getRemainAmount())
                    .status(RedPacketStatus.REFUNDED.status())
                    .updated(System.currentTimeMillis())
                    .build();
            redPacketMapper.updateByPrimaryKeySelective(updateObj);
        } else {
            String errorMsg = String.format("[RedPacketTaskHandle] orgId:%s redPacketId:%s handle refund transfer error. refundAmount:%s, errorCOde:%s",
                    redPacketLocked.getOrgId(), redPacketId, redPacketLocked.getRemainAmount().toPlainString(), response.getCode());
            log.error(errorMsg);
            throw new Exception(errorMsg);
        }
    }

}
