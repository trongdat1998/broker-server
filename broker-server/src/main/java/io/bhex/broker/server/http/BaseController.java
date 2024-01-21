/**
 * internal
 */
package io.bhex.broker.server.http;

import com.google.common.base.Splitter;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;

import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.Resource;

import io.bhex.base.idgen.api.ISequenceGenerator;
import io.bhex.base.quote.GetIndicesReply;
import io.bhex.base.quote.Index;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.common.util.JsonUtil;
import io.bhex.broker.grpc.activity.lockInterest.CreateLockInterestOrderReply;
import io.bhex.broker.grpc.activity.lockInterest.CreateLockInterestOrderRequest;
import io.bhex.broker.grpc.activity.lockInterest.QueryActivityProjectInfoRequest;
import io.bhex.broker.grpc.activity.lockInterest.QueryActivityProjectInfoResponse;
import io.bhex.broker.grpc.activity.lockInterest.QueryMyPerformanceResponse;
import io.bhex.broker.grpc.activity.lockInterest.QueryTradeCompetitionInfoReply;
import io.bhex.broker.grpc.basic.QuerySymbolResponse;
import io.bhex.broker.grpc.common.Header;
import io.bhex.broker.server.domain.ActivityMappingStatusType;
import io.bhex.broker.server.grpc.client.service.GrpcQuoteService;
import io.bhex.broker.server.grpc.server.DataImportGrpcService;
import io.bhex.broker.server.grpc.server.SupportService;
import io.bhex.broker.server.grpc.server.service.ActivityLambService;
import io.bhex.broker.server.grpc.server.service.ActivityLockInterestMappingService;
import io.bhex.broker.server.grpc.server.service.ActivityLockInterestService;
import io.bhex.broker.server.grpc.server.service.ActivityUnlockInBatchesService;
import io.bhex.broker.server.grpc.server.service.AgentUserService;
import io.bhex.broker.server.grpc.server.service.BalanceService;
import io.bhex.broker.server.grpc.server.service.BasicService;
import io.bhex.broker.server.grpc.server.service.BrokerAuthService;
import io.bhex.broker.server.grpc.server.service.BrokerFeeService;
import io.bhex.broker.server.grpc.server.service.InviteService;
import io.bhex.broker.server.grpc.server.service.InviteTaskService;
import io.bhex.broker.server.grpc.server.service.OptionPriceService;
import io.bhex.broker.server.grpc.server.service.OptionService;
import io.bhex.broker.server.grpc.server.service.OtcLegalCurrencyService;
import io.bhex.broker.server.grpc.server.service.OtcService;
import io.bhex.broker.server.grpc.server.service.PushDataService;
import io.bhex.broker.server.grpc.server.service.QuoteTokenService;
import io.bhex.broker.server.grpc.server.service.StatisticService;
import io.bhex.broker.server.grpc.server.service.SymbolService;
import io.bhex.broker.server.grpc.server.service.TradeCompetitionService;
import io.bhex.broker.server.grpc.server.service.UserLevelScheduleService;
import io.bhex.broker.server.grpc.server.service.UserSecurityService;
import io.bhex.broker.server.grpc.server.service.UserService;
import io.bhex.broker.server.grpc.server.service.activity.ActivityConfig;
import io.bhex.broker.server.model.AgentUser;
import io.bhex.broker.server.model.OptionInfo;
import io.bhex.broker.server.model.QuoteToken;
import io.bhex.broker.server.model.SimpleUserInfo;
import io.bhex.broker.server.model.TradeCompetition;
import io.bhex.broker.server.model.TradeCompetitionExt;
import io.bhex.broker.server.model.TradeCompetitionLimit;
import io.bhex.broker.server.model.UserVerify;
import io.bhex.broker.server.primary.mapper.AgentUserMapper;
import io.bhex.broker.server.primary.mapper.InviteRelationMapper;
import io.bhex.broker.server.primary.mapper.TradeCompetitionMapper;
import io.bhex.broker.server.primary.mapper.UserVerifyMapper;
import io.bhex.broker.server.util.BrokerMessageProducer;
import io.bhex.broker.server.util.CommonUtil;
import io.bhex.guild.grpc.gtp.GTPEventMessage;
import lombok.extern.slf4j.Slf4j;

@RestController
@Slf4j
public class BaseController {

    @Resource
    private UserVerifyMapper userVerifyMapper;

    @Resource
    private BrokerMessageProducer brokerMessageProducer;

    @Resource
    private SupportService supportService;

    @Resource
    private DataImportGrpcService dataImportGrpcService;

    @Resource
    private UserService userService;

    @Resource
    private OptionPriceService optionPriceService;

    @Resource
    private OptionService optionService;
    @Resource
    private QuoteTokenService quoteTokenService;

    @Resource
    private OtcLegalCurrencyService otcLegalCurrencyService;

    @Resource
    private BalanceService balanceService;

    @Resource
    private StatisticService statisticService;

    @Resource
    private ActivityLockInterestMappingService activityLockInterestMappingService;

    @Resource
    private ActivityLockInterestService activityLockInterestService;

    @Resource
    private ActivityLambService activityLambService;

    @Resource
    GrpcQuoteService grpcQuoteService;

    @Resource
    private InviteService inviteService;

    @Resource
    private TradeCompetitionService tradeCompetitionService;

    @Resource
    private ActivityLockInterestService lockInterestService;

    @Resource
    private BasicService basicService;

    @Resource
    private InviteTaskService inviteTaskService;

    @Resource
    private PushDataService pushDataService;

    @Resource
    private AgentUserService agentUserService;

    @Resource
    private AgentUserMapper agentUserMapper;

    @Resource
    private InviteRelationMapper inviteRelationMapper;

    @Resource
    private ActivityUnlockInBatchesService activityUnlockInBatchesService;

    @Resource
    private ISequenceGenerator sequenceGenerator;

    @Resource
    private BrokerFeeService brokerFeeService;

    @Resource
    private UserLevelScheduleService userLevelScheduleService;

    @Resource
    BrokerAuthService brokerAuthService;
    @Resource
    SymbolService symbolService;

    /**
     * 压测下单接口
     *
     * @return string
     */
    @RequestMapping(value = "/internal/stress_activity_order")
    public String stressActivityOrder() {
        Long start = System.currentTimeMillis();
        Header header = Header.newBuilder()
                .setOrgId(6001L)
                .setUserId(214995263184699392L)
                .build();

        CreateLockInterestOrderRequest request = CreateLockInterestOrderRequest.newBuilder()
                .setHeader(header)
                .setAmount(BigDecimal.valueOf(1L).toPlainString())
                .setClientOrderId(Math.round(new SecureRandom().nextDouble() * 100000))
                .setProjectId(13L)
                .setProjectCode("yant_0723")
                .build();
        CreateLockInterestOrderReply reply = lockInterestService.createOrder(request);
        Long end = System.currentTimeMillis();
        log.info("stressActivityOrder: ret:{}, duration: {} ms.", reply.getRet(), end - start);
        return reply.getRet() + "";
    }

    /**
     * 同步KYC 算力和状态
     *
     * @return string
     */
    @RequestMapping(value = "/internal/kyc_gtp_history")
    public String kycGtpHistory() {
        List<UserVerify> userVerifies = userVerifyMapper.getAllKycPassUserList();

        if (CollectionUtils.isEmpty(userVerifies)) {
            return "user list is null";
        } else {
            log.info("userVerifies list size : {}", userVerifies.size());
        }

        userVerifies.stream().forEach(u -> {
            //KYC 认证通过增加算力值
            GTPEventMessage gtpEventMessage = GTPEventMessage
                    .newBuilder()
                    .setUserId(u.getUserId())
                    .setType(ActivityConfig.GtpType.KYC.getType())
                    .build();

            brokerMessageProducer
                    .sendMessage(ActivityConfig.CARD_GTP_TOPIC, String.valueOf(ActivityConfig.KYC), gtpEventMessage);
        });
        return "success";
    }

    @RequestMapping(value = "/internal/check_user_info")
    public String checkUserInfo(@RequestParam Long orgId,
                                @RequestParam Long userId,
                                @RequestParam(name = "symbol_id", required = false) String symbolId) {
        if (orgId == null || userId == null) {
            return "check orgId and userId not null";
        }

        return new Gson().toJson(supportService.checkUserInfo(orgId, userId, symbolId));
    }


    @RequestMapping(value = "/internal/import/user_reg")
    public String importUserData(@RequestParam(name = "uid") Long originUid,
                                 @RequestParam(name = "orgid") Long orgId,
                                 @RequestParam(name = "national_code") String nationalCode,
                                 @RequestParam(name = "mobile") String mobile,
                                 @RequestParam(name = "email") String email,
                                 @RequestParam(name = "invite_uid") Long inviteUserId,
                                 @RequestParam(name = "check_invite_info", required = false, defaultValue = "false") Boolean checkInviteInfo,
                                 @RequestParam(name = "comment") String comment,
                                 @RequestParam(name = "import_kyc", required = false, defaultValue = "false") Boolean importKyc,
                                 @RequestParam(name = "is_kyc", required = false, defaultValue = "false") Boolean isKyc,
                                 @RequestParam(name = "force_check_email", required = false, defaultValue = "true") Boolean forceCheckEmail) {
        if (Objects.isNull(originUid) || originUid.longValue() < 1L) {
            return "invalid originUid";
        }

        if (Objects.isNull(orgId) || orgId.longValue() < 1L) {
            return "invalid orgId";
        }

        if (Strings.isNullOrEmpty(mobile) && Strings.isNullOrEmpty(email)) {
            return "Mobile or Email cannot be null,both";
        }

        if (!Strings.isNullOrEmpty(mobile) && Strings.isNullOrEmpty(nationalCode)) {
            return "NationalCode cannot be null";
        }

        return new Gson().toJson(dataImportGrpcService.importUser(originUid, orgId, nationalCode, mobile, email, inviteUserId, checkInviteInfo, comment, importKyc, isKyc, forceCheckEmail));
    }

    @RequestMapping(value = "/internal/account/create_option_account")
    public String createOptionAccount(@RequestParam(name = "orgId") Long orgId,
                                      @RequestParam(name = "userId") Long userId) {
        if (orgId == null || orgId <= 0) {
            return "invalid orgId";
        }

        if (userId == null || userId <= 0) {
            return "invalid userId";
        }
        boolean success = userService.createOptionAccount(orgId, userId);
        return new Gson().toJson(ImmutableMap.of("success", success));

    }

    /**
     * 获取最新价
     *
     * @param exchangeId exchangeId
     * @param symbolId   symbolId
     * @return string
     */
    @RequestMapping(value = "/internal/option/last_price")
    public String getOptionLastPrice(@RequestParam(name = "exchangeId") Long exchangeId,
                                     @RequestParam(name = "symbolId") String symbolId) {
        Long orgId = getDefaultOrgId();
        return optionPriceService.getCurrentOptionPrice(symbolId, exchangeId, orgId).toPlainString();
    }

    /**
     * 获取指数
     *
     * @param symbolId symbolId
     * @return string
     */
    @RequestMapping(value = "/internal/option/indices")
    public String createOptionAccount(@RequestParam(name = "symbolId") String symbolId) {
        Long orgId = getDefaultOrgId();
        return optionPriceService.getIndices(symbolId, orgId).toPlainString();
    }

    /**
     * internal创建期权
     *
     * @param tokenId           tokenId
     * @param tokenName         tokenName
     * @param strikePrice       strikePrice
     * @param issueDate         issueDate
     * @param settlementDate    settlementDate
     * @param isCall            isCall
     * @param maxPayOff         maxPayOff
     * @param positionLimit     positionLimit
     * @param indexToken        indexToken
     * @param minTradeQuantity  minTradeQuantity
     * @param minTradeAmount    minTradeAmount
     * @param minPricePrecision minPricePrecision
     * @param digitMergeList    digitMergeList
     * @param basePrecision     basePrecision
     * @param quotePrecision    quotePrecision
     * @param category          category
     * @param type              type
     * @param exchangeId        exchangeId
     * @param brokerId          brokerId
     * @param makerFeeRate      makerFeeRate
     * @param takerFeeRate      takerFeeRate
     * @return string
     */
    @PostMapping("/internal/create/option")
    public String createOption(@RequestParam(name = "tokenId") String tokenId,
                               @RequestParam(name = "tokenName") String tokenName,
                               @RequestParam(name = "strikePrice") BigDecimal strikePrice,
                               @RequestParam(name = "issueDate") Long issueDate,
                               @RequestParam(name = "settlementDate") Long settlementDate,
                               @RequestParam(name = "isCall") Integer isCall,
                               @RequestParam(name = "maxPayOff") BigDecimal maxPayOff,
                               @RequestParam(name = "positionLimit") Integer positionLimit,
                               @RequestParam(name = "coinToken") String coinToken,
                               @RequestParam(name = "indexToken") String indexToken,
                               @RequestParam(name = "underlyingId") String underlyingId,
                               @RequestParam(name = "minTradeQuantity") BigDecimal minTradeQuantity,
                               @RequestParam(name = "minTradeAmount") BigDecimal minTradeAmount,
                               @RequestParam(name = "minPricePrecision") BigDecimal minPricePrecision,
                               @RequestParam(name = "digitMergeList") String digitMergeList,
                               @RequestParam(name = "basePrecision") BigDecimal basePrecision,
                               @RequestParam(name = "quotePrecision") BigDecimal quotePrecision,
                               @RequestParam(name = "category") Integer category,
                               @RequestParam(name = "type") Integer type,
                               @RequestParam(name = "exchangeId") Long exchangeId,
                               @RequestParam(name = "brokerId") Long brokerId,
                               @RequestParam(name = "makerFeeRate") BigDecimal makerFeeRate,
                               @RequestParam(name = "takerFeeRate") BigDecimal takerFeeRate,
                               @RequestParam(name = "minPrecision") BigDecimal minPrecision) {
        OptionInfo optionInfo = OptionInfo
                .builder()
                .tokenId(tokenId)
                .tokenName(tokenName)
                .strikePrice(strikePrice)
                .issueDate(issueDate)
                .settlementDate(settlementDate)
                .isCall(isCall)
                .maxPayOff(maxPayOff)
                .positionLimit(positionLimit)
                .coinToken(coinToken)
                .indexToken(indexToken)
                .underlyingId(underlyingId)
                .minTradeAmount(minTradeAmount)
                .minTradeQuantity(minTradeQuantity)
                .minPricePrecision(minPricePrecision)
                .digitMergeList(digitMergeList)
                .basePrecision(basePrecision)
                .quotePrecision(quotePrecision)
                .category(category)
                .type(type)
                .exchangeId(exchangeId)
                .orgId(brokerId)
                .makerFeeRate(makerFeeRate)
                .takerFeeRate(takerFeeRate)
                .minPrecision(minPrecision)
                .build();
        return optionService.createOption(optionInfo);
    }


    @PostMapping("/internal/create/bhop_option")
    public String createBHopOption(@RequestParam(name = "tokenId") String tokenId,
                                   @RequestParam(name = "tokenName") String tokenName,
                                   @RequestParam(name = "strikePrice") BigDecimal strikePrice,
                                   @RequestParam(name = "issueDate") Long issueDate,
                                   @RequestParam(name = "settlementDate") Long settlementDate,
                                   @RequestParam(name = "isCall") Integer isCall,
                                   @RequestParam(name = "maxPayOff") BigDecimal maxPayOff,
                                   @RequestParam(name = "positionLimit") Integer positionLimit,
                                   @RequestParam(name = "coinToken") String coinToken,
                                   @RequestParam(name = "indexToken") String indexToken,
                                   @RequestParam(name = "minTradeQuantity") BigDecimal minTradeQuantity,
                                   @RequestParam(name = "minTradeAmount") BigDecimal minTradeAmount,
                                   @RequestParam(name = "minPricePrecision") BigDecimal minPricePrecision,
                                   @RequestParam(name = "digitMergeList") String digitMergeList,
                                   @RequestParam(name = "basePrecision") BigDecimal basePrecision,
                                   @RequestParam(name = "quotePrecision") BigDecimal quotePrecision,
                                   @RequestParam(name = "category") Integer category,
                                   @RequestParam(name = "type") Integer type,
                                   @RequestParam(name = "exchangeId") Long exchangeId,
                                   @RequestParam(name = "brokerId") Long brokerId,
                                   @RequestParam(name = "makerFeeRate") BigDecimal makerFeeRate,
                                   @RequestParam(name = "takerFeeRate") BigDecimal takerFeeRate,
                                   @RequestParam(name = "minPrecision") BigDecimal minPrecision) {
        OptionInfo optionInfo = OptionInfo
                .builder()
                .tokenId(tokenId)
                .tokenName(tokenName)
                .strikePrice(strikePrice)
                .issueDate(issueDate)
                .settlementDate(settlementDate)
                .isCall(isCall)
                .maxPayOff(maxPayOff)
                .positionLimit(positionLimit)
                .coinToken(coinToken)
                .indexToken(indexToken)
                .minTradeAmount(minTradeAmount)
                .minTradeQuantity(minTradeQuantity)
                .minPricePrecision(minPricePrecision)
                .digitMergeList(digitMergeList)
                .basePrecision(basePrecision)
                .quotePrecision(quotePrecision)
                .category(category)
                .type(type)
                .exchangeId(exchangeId)
                .orgId(brokerId)
                .makerFeeRate(makerFeeRate)
                .takerFeeRate(takerFeeRate)
                .minPrecision(minPrecision)
                .build();
        return optionService.createBhopOption(optionInfo);
    }

    @RequestMapping(value = "/internal/quote_token/handle")
    public String getOptionLastPrice(@RequestParam(name = "orgId") Long orgId,
                                     @RequestParam(name = "tokenId") String tokenId,
                                     @RequestParam(name = "tokenIcon") String tokenIcon,
                                     @RequestParam(name = "orders") String orders) {
        List<String> tokenOrders = StringUtils.isEmpty(orders)
                ? new ArrayList<>()
                : Arrays.asList(orders.split(","));
        List<QuoteToken> list = quoteTokenService.handleQuoteToken(orgId, tokenId, tokenIcon, tokenOrders);
        return new Gson().toJson(list);
    }

    @RequestMapping(value = "/internal/create/legal/currency")
    public String bindingUserLegalCurrency() {
        otcLegalCurrencyService.initUserLegalCurrency();
        return "success";
    }

    @RequestMapping(value = "/internal/create_otc_currency")
    public String createTokenIcon(@RequestParam(name = "orgId") Long orgId,
                                  @RequestParam(name = "code") String code,
                                  @RequestParam(name = "language") String language,
                                  @RequestParam(name = "name") String name,
                                  @RequestParam(name = "minQuote") String minQuote,
                                  @RequestParam(name = "maxQuote") String maxQuote,
                                  @RequestParam(name = "scale") Integer scale) {
/*        OtcCurrency otcCurrency = new OtcCurrency();
        otcCurrency.setOrgId(orgId);
        otcCurrency.setCode(code);
        otcCurrency.setLanguage(language);
        otcCurrency.setMaxQuote(new BigDecimal(maxQuote));
        otcCurrency.setMinQuote(new BigDecimal(minQuote));
        otcCurrency.setName(name);
        otcCurrency.setScale(scale);
        otcCurrency.setStatus(1);
        otcCurrency.setCreateDate(new Date());
        otcCurrency.setUpdateDate(new Date());
        otcCurrencyService.createOtcCurrency(otcCurrency);
        return "create success";*/

        return "Be Moved to exchange otc service...";
    }

    @RequestMapping(value = "/internal/update_currency_code")
    public String modifyTokenIcon(@RequestParam(name = "code") String code,
                                  @RequestParam(name = "orgId") Long orgId,
                                  @RequestParam(name = "userId") Long userId) {
        otcLegalCurrencyService.updateCodeByOrgIdAndUserId(code, orgId, userId);
        return "success";
    }


    /**
     * 锁仓资产
     *
     * @param userId     用户ID
     * @param orgId      券商ID
     * @param lockAmount 锁定金额
     * @param tokenId    tokenID
     * @param reason     锁仓原因
     */
    @RequestMapping(value = "/internal/lock/balance")
    public String lockBalance(@RequestParam(name = "userId", required = true) Long userId,
                              @RequestParam(name = "orgId", required = true) Long orgId,
                              @RequestParam(name = "lockAmount", required = true) String lockAmount,
                              @RequestParam(name = "tokenId", required = true) String tokenId,
                              @RequestParam(name = "reason", required = true) String reason) {
        if (userId == null || orgId == null || StringUtils.isEmpty(lockAmount) || StringUtils.isEmpty(tokenId) || StringUtils.isEmpty(reason)) {
            throw new BrokerException(BrokerErrorCode.BAD_REQUEST);
        }

        if (new BigDecimal(lockAmount).compareTo(BigDecimal.ZERO) <= 0) {
            throw new BrokerException(BrokerErrorCode.BAD_REQUEST);
        }

        balanceService.lockBalance(userId, orgId, lockAmount, tokenId, sequenceGenerator.getLong(), reason);
        return "success";
    }

    /**
     * 解锁资产 从锁仓到可用
     *
     * @param userId       用户ID
     * @param orgId        券商ID
     * @param unLockAmount 解锁金额
     * @param tokenId      tokenID
     * @param reason       解锁原因
     */
    @RequestMapping(value = "/internal/unlock/balance")
    public String unLockBalance(@RequestParam(name = "userId", required = true) Long userId,
                                @RequestParam(name = "orgId", required = true) Long orgId,
                                @RequestParam(name = "unLockAmount", required = true) String unLockAmount,
                                @RequestParam(name = "tokenId", required = true) String tokenId,
                                @RequestParam(name = "reason", required = true) String reason) {
        if (userId == null || orgId == null || StringUtils.isEmpty(unLockAmount) || StringUtils.isEmpty(tokenId) || StringUtils.isEmpty(reason)) {
            throw new BrokerException(BrokerErrorCode.BAD_REQUEST);
        }

        if (new BigDecimal(unLockAmount).compareTo(BigDecimal.ZERO) <= 0) {
            throw new BrokerException(BrokerErrorCode.BAD_REQUEST);
        }

        balanceService.unLockBalance(userId, orgId, unLockAmount, tokenId, reason, sequenceGenerator.getLong());
        return "success";
    }

    /**
     * 从用户可用或者锁仓转到运营账户
     *
     * @param userId       用户ID
     * @param orgId        券商ID
     * @param amount       转账金额
     * @param tokenId      tokneID
     * @param fromPosition 从锁仓或者可用转  0:从可用 非0:从锁仓
     * @return string
     */
    @RequestMapping(value = "/internal/sync/transfer")
    public String syncTransfer(@RequestParam(name = "userId", required = true) Long userId,
                               @RequestParam(name = "orgId", required = true) Long orgId,
                               @RequestParam(name = "amount", required = true) String amount,
                               @RequestParam(name = "tokenId", required = true) String tokenId,
                               @RequestParam(name = "fromPosition", required = true) Integer fromPosition) {

        if (userId == null || orgId == null || StringUtils.isEmpty(amount) || StringUtils.isEmpty(tokenId) || fromPosition == null) {
            throw new BrokerException(BrokerErrorCode.BAD_REQUEST);
        }

        if (new BigDecimal(amount).compareTo(BigDecimal.ZERO) <= 0) {
            throw new BrokerException(BrokerErrorCode.BAD_REQUEST);
        }

        balanceService.syncTransfer(userId, orgId, amount, tokenId, fromPosition);
        return "success";
    }

    @RequestMapping(value = "/internal/sync/transfer/by/account/type")
    public String syncTransferByAccountType(@RequestParam(name = "userId", required = true) Long userId,
                                            @RequestParam(name = "orgId", required = true) Long orgId,
                                            @RequestParam(name = "amount", required = true) String amount,
                                            @RequestParam(name = "tokenId", required = true) String tokenId,
                                            @RequestParam(name = "fromPosition", required = true) Integer fromPosition) {

        if (userId == null || orgId == null || StringUtils.isEmpty(amount) || StringUtils.isEmpty(tokenId) || fromPosition == null) {
            throw new BrokerException(BrokerErrorCode.BAD_REQUEST);
        }

        if (new BigDecimal(amount).compareTo(BigDecimal.ZERO) <= 0) {
            throw new BrokerException(BrokerErrorCode.BAD_REQUEST);
        }

        balanceService.syncTransferByAccountType(userId, orgId, amount, tokenId, fromPosition);
        return "success";
    }

    /**
     * 手动执行login分析任务
     */
    @RequestMapping(value = "/internal/analysis/log")
    public String analysisLog(@RequestParam(name = "time", required = true) String time) {
        if (StringUtils.isEmpty(time)) {
            throw new BrokerException(BrokerErrorCode.BAD_REQUEST);
        }
        DateTime nowTime = new DateTime(time);
        DateTime start = nowTime.withTimeAtStartOfDay();
        DateTime end = nowTime.millisOfDay().withMaximumValue();
        statisticService.handelLoginStatisticTask(time, start.getMillis(), end.getMillis());

        //汇总login count
        statisticService.handelLoginStatisticCountTask(nowTime.toString("yyyy-MM-dd"));
        return "success";
    }

    /**
     * 生成获奖结果
     */
    @RequestMapping(value = "/internal/activity/result")
    public String handelActivityResult(@RequestParam(name = "projectId", required = true) Long projectId,
                                       @RequestParam(name = "orgId", required = true) Long orgId) {
        if (projectId == null || orgId == null) {
            throw new BrokerException(BrokerErrorCode.BAD_REQUEST);
        }
        activityLockInterestMappingService.saveActivityResult(orgId, projectId);
        return "success";
    }

    /**
     * 分批解锁 - 记录生成 Unlock in batches
     */
    @RequestMapping(value = "/internal/activity/unlock_in_batches/result")
    public String unlockInBatchesResult(@RequestParam(name = "projectId", required = true) Long projectId,
                                        @RequestParam(name = "orgId", required = true) Long orgId) {
        // hydax 分批解锁，默认每次百分之十
        // 这里必须用字符串0.1
        BigDecimal unlockRate = new BigDecimal("0.10");
        if (projectId == null || orgId == null) {
            throw new BrokerException(BrokerErrorCode.BAD_REQUEST);
        }
        return activityUnlockInBatchesService.unlockInBatchesResult(projectId, orgId, unlockRate);
    }

    /**
     * 分批解锁 - 按记录解锁 Unlock in batches
     */
    @RequestMapping(value = "/internal/activity/unlock_in_batches/unlock")
    public String unlockInBatchesUnlock(@RequestParam(name = "projectId", required = true) Long projectId,
                                        @RequestParam(name = "orgId", required = true) Long orgId) {
        if (projectId == null || orgId == null) {
            throw new BrokerException(BrokerErrorCode.BAD_REQUEST);
        }
        return activityUnlockInBatchesService.unlockInBatchesUnlock(projectId, orgId);
    }

    /**
     * 生成锦鲤获奖结果
     */
    @RequestMapping(value = "/internal/activity/luck_result")
    public String handelActivityLuckResult(@RequestParam(name = "projectId", required = true) Long projectId,
                                           @RequestParam(name = "orgId", required = true) Long orgId) {
        if (projectId == null || orgId == null) {
            throw new BrokerException(BrokerErrorCode.BAD_REQUEST);
        }
        activityLockInterestMappingService.saveActivityLuckResult(orgId, projectId);
        return "success";
    }

    /**
     * 锁仓活动，到期解锁
     */
    @RequestMapping(value = "/internal/activity/expire_unlock")
    public String activityExpiredUnlock(@RequestParam(name = "projectId", required = true) Long projectId,
                                        @RequestParam(name = "brokerId", required = true) Long brokerId) {
        if (projectId == null || brokerId == null) {
            throw new BrokerException(BrokerErrorCode.BAD_REQUEST);
        }
        String result = activityLockInterestService.activityExpiredUnlock(brokerId, projectId);
        return "call success. unlock => " + result;
    }

    /**
     * lamb 锁仓活动，到期解锁
     */
    @RequestMapping(value = "/internal/lamb/expire_unlock")
    public String lambExpiredUnlock(@RequestParam(name = "projectId", required = true) Long projectId,
                                    @RequestParam(name = "brokerId", required = true) Long brokerId) {
        if (projectId == null || brokerId == null) {
            throw new BrokerException(BrokerErrorCode.BAD_REQUEST);
        }
        String result = activityLambService.activityExpiredUnlock(brokerId, projectId);
        return "call success. unlock => " + result;
    }

    /**
     * 处理活动结果发放
     */
    @RequestMapping(value = "/internal/activity/transfer")
    public String handelLambActivityTransfer(@RequestParam(name = "projectId", required = true) Long projectId,
                                             @RequestParam(name = "orgId", required = true) Long orgId) {
        if (projectId == null || orgId == null) {
            throw new BrokerException(BrokerErrorCode.BAD_REQUEST);
        }
        activityLockInterestMappingService.handelLambActivityTransfer(orgId, projectId, ActivityMappingStatusType.PENDING);
        return "success";
    }

    /**
     * 释放活动剩余冻结
     */
    @RequestMapping(value = "/internal/activity/unlock")
    public String handelActivityUnlock(@RequestParam(name = "projectId", required = true) Long projectId,
                                       @RequestParam(name = "orgId", required = true) Long orgId) {
        if (projectId == null || orgId == null) {
            throw new BrokerException(BrokerErrorCode.BAD_REQUEST);
        }
        activityLockInterestMappingService.handelActivityUnLockBalance(orgId, projectId, ActivityMappingStatusType.PENDING);
        return "success";
    }

    /**
     * 释放活动剩余冻结
     */
    @RequestMapping(value = "/internal/get/indices")
    public String handelActivityUnlock(@RequestParam(name = "symbolId", required = true) String symbolId) {
        if (StringUtils.isEmpty(symbolId)) {
            throw new BrokerException(BrokerErrorCode.BAD_REQUEST);
        }
        Long orgId = getDefaultOrgId();
        GetIndicesReply reply = grpcQuoteService.getIndices(symbolId, orgId);
        if (Objects.isNull(reply) || Objects.isNull(reply.getIndicesMapMap())) {
            return "reply is null";
        }
        Map<String, Index> replyIndicesMapMap = reply.getIndicesMapMap();
        Map<String, String> resultMap = new HashMap<>();
        replyIndicesMapMap.keySet().forEach(key -> {
            resultMap.put(key, replyIndicesMapMap.get(key).getIndex().getStr());
        });
        return JsonUtil.defaultGson().toJson(resultMap);

    }

    /**
     * 当前orgId在平台处无效（随机取一个作为通行证）
     */
    private Long getDefaultOrgId() {
        return 0L;
    }

    /**
     * 初始化邀请返佣微信配置
     */
    @RequestMapping(value = "/internal/invite/config")
    public String initInviteWechatConfig(@RequestParam(name = "orgId", required = true) Long orgId) {
        if (orgId == null) {
            throw new BrokerException(BrokerErrorCode.BAD_REQUEST);
        }
        inviteService.initInviteWechatConfig(orgId);
        return "success";
    }

    /**
     * 解禁用户登录
     */
    @RequestMapping(value = "/internal/unfreeze/user")
    public String unfreezeUsers(@RequestParam(name = "orgId", required = true) Long orgId,
                                @RequestParam(name = "userId", required = true) Long userId,
                                @RequestParam(name = "type", required = true) Integer type) {
        if (orgId == null) {
            throw new BrokerException(BrokerErrorCode.BAD_REQUEST);
        }
        userService.unfreezeUsers(orgId, userId, type);
        return "success";
    }

    /**
     * 初始化otc message
     *
     * @param orgId
     * @param fromOrgId
     * @return
     */
//    @RequestMapping(value = "/internal/init/otc/message")
//    public String otcMessage(@RequestParam(name = "orgId", required = true) Long orgId,
//                             @RequestParam(name = "fromOrgId", required = true) Long fromOrgId) {
//        if (orgId == null || fromOrgId == null) {
//            throw new BrokerException(BrokerErrorCode.BAD_REQUEST);
//        }
//        commonIniService.initOtcMessage(orgId, fromOrgId);
//        return "success";
//    }

    /**
     * IEO活动锁仓数据汇总 供给解仓用
     */
    @RequestMapping(value = "/internal/init/ieo/lock/record")
    public String otcMessage(@RequestParam(name = "orgId", required = true) Long orgId,
                             @RequestParam(name = "activityIds", required = true) String activityIds,
                             @RequestParam(name = "project", required = true) String project) {
        if (orgId == null || StringUtils.isEmpty(project)) {
            throw new BrokerException(BrokerErrorCode.BAD_REQUEST);
        }
        activityLockInterestMappingService.createActivityUserLockRecord(orgId, activityIds, project);
        return "success";
    }

    @RequestMapping(value = "/internal/test/symbol")
    public String testSymbolList() {
        QuerySymbolResponse response = basicService.querySymbolsWithTokenDetail(0L, Arrays.asList(1, 2, 3));
        response.getSymbolList().forEach(s -> {
            if (s.getTokenOption() != null && s.getCategory() == 3) {
                log.info("option symbolId {}, settlement time {}", s.getSymbolId(), new DateTime(s.getTokenOption().getSettlementDate()).toString("yyyy-MM-dd HH:mm:ss"));
            }
        });
        return "success";
    }

    @RequestMapping(value = "/internal/account/create_futures_account")
    public String createFuturesAccount(@RequestParam(name = "orgId") Long orgId,
                                       @RequestParam(name = "userId") Long userId) {
        if (orgId == null || orgId <= 0) {
            return "invalid orgId";
        }

        if (userId == null || userId <= 0) {
            return "invalid userId";
        }

        Long accountId = userService.createFuturesAccount(orgId, userId);
        return new Gson().toJson(ImmutableMap.of("accountId", accountId));

    }


    /**
     * 交易大赛配置信息
     */
    @RequestMapping(value = "/internal/trade/competition")
    public String competition(@RequestParam(name = "orgId", required = true) Long orgId,
                              @RequestParam(name = "symbolId", required = true) String symbolId,
                              @RequestParam(name = "startTime", required = true) Long startTime,
                              @RequestParam(name = "endTime", required = true) Long endTime,
                              @RequestParam(name = "type", required = true) Integer type,
                              @RequestParam(name = "status", required = false) Integer status,
                              @RequestParam(name = "effectiveQuantity", required = true) Integer effectiveQuantity,
                              @RequestParam(name = "receiveTokenId", required = true) String receiveTokenId,
                              @RequestParam(name = "receiveTokenName", required = true) String receiveTokenName,
                              @RequestParam(name = "receiveTokenAmount", required = true) String receiveTokenAmount,
                              //逗号分隔字符串,1=收益率,2=收益金额
                              @RequestParam(name = "rankTypes", required = false, defaultValue = "1") String rankTypes) {
        TradeCompetition competition = new TradeCompetition();
        competition.setOrgId(orgId);
        //competition.setCompetitionCode(competitionCode);
        competition.setSymbolId(symbolId);
        competition.setStartTime(new Date(startTime));
        competition.setEndTime(new Date(endTime));
        competition.setType(type);
        competition.setStatus(status);
        competition.setReceiveTokenId(receiveTokenId);
        competition.setReceiveTokenName(receiveTokenName);
        competition.setReceiveTokenAmount(new BigDecimal(receiveTokenAmount));
        competition.setEffectiveQuantity(effectiveQuantity);
        competition.setRankTypes(rankTypes);
        tradeCompetitionService.saveTradeCompetition(competition);
        return "success";
    }


    /**
     * 交易大赛频率限制接口
     */
    @RequestMapping(value = "/internal/trade/competition/limit")
    public String saveTradeCompetitionLimit(@RequestParam(name = "orgId", required = true) Long orgId,
                                            @RequestParam(name = "competitionCode", required = true) String competitionCode,
                                            //exp 12345=laowang,23456=zhangsan
                                            @RequestParam(name = "enterIdStr", required = false, defaultValue = "") String enterIdStr,
                                            @RequestParam(name = "enterStatus", required = true) Integer enterStatus,
                                            //exp 12345=laowang,23456=zhangsan
                                            @RequestParam(name = "enterWhiteStr", required = false, defaultValue = "") String enterWhiteStr,
                                            @RequestParam(name = "enterWhiteStatus", required = true) Integer enterWhiteStatus,
                                            @RequestParam(name = "limitStr", required = true) String limitStr,
                                            @RequestParam(name = "limitStatus", required = true) Integer limitStatus) {

        TradeCompetitionLimit tradeCompetitionLimit = new TradeCompetitionLimit();
        tradeCompetitionLimit.setOrgId(orgId);
        tradeCompetitionLimit.setCompetitionCode(competitionCode);
        //tradeCompetitionLimit.setEnterIdStr(enterIdStr);
        tradeCompetitionLimit.setEnterStatus(enterStatus);
        //tradeCompetitionLimit.setEnterWhiteStr(enterWhiteStr);
        tradeCompetitionLimit.setEnterWhiteStatus(enterWhiteStatus);
        tradeCompetitionLimit.setLimitStr(limitStr);
        tradeCompetitionLimit.setLimitStatus(limitStatus);
        tradeCompetitionService.saveTradeCompetitionLimit(tradeCompetitionLimit, enterIdStr, enterWhiteStr);
        return "success";
    }


    @RequestMapping(value = "/internal/trade/competition/paticipant/add")
    public String addParticipant(@RequestParam(name = "orgId", required = true) Long orgId,
                                 @RequestParam(name = "competitionCode", required = true) String competitionCode,
                                 @RequestParam(name = "enterIdStr", required = false, defaultValue = "") String enterIdStr,
                                 @RequestParam(name = "enterWhiteStr", required = false, defaultValue = "") String enterWhiteStr) {

        TradeCompetitionLimit tradeCompetitionLimit = new TradeCompetitionLimit();
        tradeCompetitionLimit.setOrgId(orgId);
        tradeCompetitionLimit.setCompetitionCode(competitionCode);
        //tradeCompetitionLimit.setEnterIdStr(enterIdStr);
        //tradeCompetitionLimit.setEnterWhiteStr(enterWhiteStr);
        tradeCompetitionService.addParticipant(tradeCompetitionLimit, enterIdStr, enterWhiteStr);
        return "success";
    }


    /**
     * 交易大赛国际化信息
     */
    @RequestMapping(value = "/internal/trade/competition/ext")
    public String saveTradeCompetitionExt(@RequestParam(name = "orgId", required = true) Long orgId,
                                          @RequestParam(name = "competitionCode", required = true) String competitionCode,
                                          @RequestParam(name = "language", required = true) String language,
                                          @RequestParam(name = "name", required = true) String name,
                                          @RequestParam(name = "description", required = true) String description,
                                          @RequestParam(name = "bannerUrl", required = true) String bannerUrl,
                                          @RequestParam(name = "mobileBannerUrl", required = true) String mobileBannerUrl) {
        TradeCompetitionExt tradeCompetitionLimit = new TradeCompetitionExt();
        tradeCompetitionLimit.setOrgId(orgId);
        tradeCompetitionLimit.setCompetitionCode(competitionCode);
        tradeCompetitionLimit.setLanguage(language);
        tradeCompetitionLimit.setName(name);
        tradeCompetitionLimit.setBannerUrl(bannerUrl);
        tradeCompetitionLimit.setDescription(description);
        tradeCompetitionLimit.setMobileBannerUrl(mobileBannerUrl);
        tradeCompetitionService.saveTradeCompetitionExt(tradeCompetitionLimit);
        return "success";
    }

    /**
     * 派发活动奖励
     */
    @RequestMapping(value = "/internal/trade/handel/result")
    public String handelTradeCompetitionResult(@RequestParam(name = "orgId", required = true) Long orgId,
                                               @RequestParam(name = "competitionCode", required = true) String competitionCode) {
        tradeCompetitionService.handelTradeCompetitionResult(orgId, competitionCode);
        return "success";
    }

    /**
     * 生成单个券商活动记录
     */
    @RequestMapping(value = "/internal/create/invite/bonus/record")
    public String generateAdminInviteBonusRecord(@RequestParam(name = "orgId", required = true) Long orgId,
                                                 @RequestParam(name = "time", required = true) Long time) {
        inviteTaskService.generateAdminInviteBonusRecord(orgId, time);
        return "success";
    }

    /**
     * 生成所有券商bonus record
     */
    @RequestMapping(value = "/internal/all/create/invite/bonus/record")
    public String generateAllInviteBonusRecord(@RequestParam(name = "yesterdayTime", required = true) Long yesterdayTime) {
        if (yesterdayTime == null || yesterdayTime.equals(0l)) {
            return "yesterdayTime is null";
        }
        inviteTaskService.manualCompensationCreateBonusRecord(yesterdayTime);
        return "success";
    }

    /**
     * kyc 推送测试
     */
    @RequestMapping(value = "/internal/push/kyc/message")
    public String kycPushMessage(@RequestParam(name = "orgId", required = true) Long orgId,
                                 @RequestParam(name = "userId", required = true) Long userId) {
        pushDataService.userKycMessage(orgId, userId);
        return "success";
    }

    /**
     * 用户注册推送测试
     */
    @RequestMapping(value = "/internal/push/user/register/message")
    public String userRegisterMessage(@RequestParam(name = "orgId", required = true) Long orgId,
                                      @RequestParam(name = "userId", required = true) Long userId) {
        pushDataService.userRegisterMessage(orgId, userId);
        return "success";
    }


    /**
     * kyc历史数据补偿
     */
    @RequestMapping(value = "/internal/kyc/history/data")
    public String kycHistoryData(@RequestParam(name = "orgId", required = true) Long orgId) {
        pushDataService.kycHistoryData(orgId);
        return "success";
    }

    /**
     * 注册历史数据补偿
     */
    @RequestMapping(value = "/internal/register/history/data")
    public String registerHistoryData(@RequestParam(name = "orgId", required = true) Long orgId) {
        pushDataService.registerHistoryData(orgId);
        return "success";
    }

    /**
     * 交易历史数据补偿
     */
    @RequestMapping(value = "/internal/trade/history/data")
    public String tradeHistoryData(@RequestParam(name = "orgId", required = true) Long orgId,
                                   @RequestParam(name = "time", required = true) String time) {
        pushDataService.tradeHistoryData(orgId, time);
        return "success";
    }


    @RequestMapping(value = "/internal/trade/competition/my_performance")
    public String queryMyCompetitionPerformance(@RequestParam(name = "orgId", required = true) Long orgId,
                                                @RequestParam(name = "competitionCode", required = true) String competitionCode,
                                                @RequestParam(name = "userId", required = true) Long userId) {

        try {
            QueryMyPerformanceResponse resp = tradeCompetitionService.queryMyPerformance(orgId, userId, competitionCode);
            return CommonUtil.formatMessage(resp);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return "fail";
        }

    }

    @RequestMapping(value = "/internal/trade/competition/top")
    public String queryMyCompetitionPerformance(@RequestParam(name = "orgId", required = true) Long orgId,
                                                @RequestParam(name = "competitionCode", required = true) String competitionCode,
                                                @RequestParam(name = "userId", required = true) Long userId,
                                                @RequestParam(name = "language", required = true) String language,
                                                @RequestParam(name = "rankType", required = true) Integer rankType) {

        try {
            QueryTradeCompetitionInfoReply reply = tradeCompetitionService.queryTradeCompetitionDetail(orgId, userId, language, competitionCode, rankType);
            return CommonUtil.formatMessage(reply);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return "fail";
        }
    }

    @RequestMapping(value = "/internal/user/list")
    public String queryChangedUserInfo(@RequestParam(name = "orgId", required = true) Long orgId,
                                       @RequestParam(name = "fromId", required = true) Long fromId,
                                       @RequestParam(name = "toId", required = true) Long toId,
                                       @RequestParam(name = "startTime", required = true) Long startTime,
                                       @RequestParam(name = "toTime", required = true) Long toTime) {

        Stopwatch sw = Stopwatch.createStarted();
        try {
            List<SimpleUserInfo> list = userService.queryChangedSimpleUserInfoList(orgId, null, fromId, toId, startTime, toTime, 200);
            return list.size() + "";
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return "fail";
        } finally {
            log.info("queryChangedUserInfo consume {} mills", sw.stop().elapsed(TimeUnit.MILLISECONDS));
        }
    }

    @RequestMapping(value = "/internal/trade/competition/top/refresh")
    public String refreshTradeCompetitionTop() {
        try {
            tradeCompetitionService.releaseTradeRankListSnapshotKey();
            tradeCompetitionService.refreshTradeRankListSnapshotTask();
            tradeCompetitionService.refreshTradeRankListSnapshotDailyTask();
            return "success";
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return e.getMessage();
        }
    }

    @RequestMapping(value = "/internal/trade/competition/lock/release")
    public String releaseLock() {
        try {
            tradeCompetitionService.releaseTradeRankListSnapshotKey();
            return "success";
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return e.getMessage();
        }
    }

    @RequestMapping(value = "/internal/trade/competition/snapshot")
    public String getUserSnapshot(@RequestParam(name = "orgId", required = true) Long orgId,
                                  @RequestParam(name = "competitionId", required = true) Long competitionId,
                                  @RequestParam(name = "startStr", required = true) String startStr,
                                  @RequestParam(name = "endStr", required = true) String endStr,
                                  @RequestParam(name = "userId", required = true) Long userId) {

        try {
            tradeCompetitionService.getUserSnapshot(orgId, competitionId, startStr, endStr, userId);
            return "success";
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return e.getMessage();
        }

    }

    @RequestMapping(value = "/internal/agent/invite/user")
    public String agentInviteUserHandle(@RequestParam(name = "orgId", required = true) Long orgId,
                                        @RequestParam(name = "userId", required = true) Long userId,
                                        @RequestParam(name = "inviteId", required = true) Long inviteId) {
        try {
            AgentUser agentUser = this.agentUserMapper.queryAgentUserByUserId(orgId, inviteId);
            if (agentUser == null) {
                return "fail";
            }
            agentUserService.agentInviteUserHandle(orgId, userId, agentUser);
            this.inviteRelationMapper.updateInviteRelationStatusByUserId(userId);
            return "success";
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return e.getMessage();
        }
    }

    @RequestMapping(value = "/internal/history/invitation/handel")
    public String historyInvitationHandel(@RequestParam(name = "orgId", required = true) Long orgId,
                                          @RequestParam(name = "level", required = true) Integer level) {
        try {
            agentUserService.historyInvitationHandel(orgId, level);
            return "success";
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return e.getMessage();
        }
    }

    @RequestMapping(value = "/internal/rebind/agent/user")
    public String rebindAgentUser(@RequestParam(name = "orgId", required = true) Long orgId,
                                  @RequestParam(name = "userId", required = true) Long userId,
                                  @RequestParam(name = "targetUserId", required = true) Long targetUserId) {
        try {
            agentUserService.rebindAgentUser(orgId, userId, targetUserId);
            return "success";
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return e.getMessage();
        }
    }

    @RequestMapping(value = "/internal/user/verify/update_card_no_hash")
    public String updateUserVerifyCardNoHash() {
        try {
            ForkJoinPool.commonPool().execute(this::updateUserVerifyCardNoHashByList);
            return "success";
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return e.getMessage();
        }
    }

    @RequestMapping(value = "/internal/handel/init/contract/temporary")
    public String initContractTemporaryOrderId() {
        try {
            tradeCompetitionService.initContractTemporaryOrderId();
            return "success";
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return e.getMessage();
        }
    }

    private void updateUserVerifyCardNoHashByList() {
        Long verifyId = 0L;

        while (true) {
            List<UserVerify> userVerifys = userVerifyMapper.getUserVerifyList(verifyId, 1000);
            if (userVerifys == null || userVerifys.isEmpty()) {
                break;
            }

            for (UserVerify userVerify : userVerifys) {
                verifyId = userVerify.getId();

                if (!Strings.isNullOrEmpty(userVerify.getCardNoHash())) {
                    continue;
                }

                try {
                    UserVerify decryptedUserVerify = UserVerify.decrypt(userVerify);
                    String cardNoHash = UserVerify.hashCardNo(decryptedUserVerify.getCardNo());

                    UserVerify dbUpdUserVerify = UserVerify.builder()
                            .userId(userVerify.getUserId())
                            .cardNoHash(cardNoHash)
                            .updated(System.currentTimeMillis())
                            .build();
                    userVerifyMapper.update(dbUpdUserVerify);
                    log.info("Index: {} userId: {} cardNo: {} update cardNoHash: {} success", verifyId,
                            decryptedUserVerify.getUserId(), decryptedUserVerify.getCardNo(), cardNoHash);
                } catch (Throwable e) {
                    log.warn("userId; {} decrypted error", userVerify.getUserId());
                }
            }
        }
    }

    /**
     * 刷新用户等级数据
     *
     * @param orgId         券商ID
     * @param levelConfigId levelConfigId
     * @return string
     */
    @RequestMapping(value = "/internal/user_level/refresh")
    public String refreshUserLevelData(@RequestParam(name = "orgId", required = true) Long orgId,
                                       @RequestParam(name = "levelConfigId", required = true) Long levelConfigId) {
        try {
            userLevelScheduleService.refreshUserLevelData(orgId, levelConfigId);
            return Boolean.TRUE + "";
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return e.getMessage();
        }
    }

    /**
     * 删除已废弃券商的所有币对
     *
     * @param orgId 券商ID
     * @return string
     */
    @RequestMapping(value = "/internal/symbol/remove_unused_broker_symbols")
    public String removeUnusedBrokerSymbols(@RequestParam(name = "orgId", required = true) Long orgId) {
        try {

            return symbolService.removeUnusedBrokerSymbols(orgId);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return e.getMessage();
        }
    }

    /**
     * 绝对值模式-经纪人历史数据处理
     *
     * @param orgId
     * @return
     */
    @RequestMapping(value = "/internal/clean/agent/rate")
    public String cleanAgentRate(@RequestParam(name = "orgId", required = true) Long orgId) {
        try {
            agentUserService.cleanAgentRate(orgId);
            return Boolean.TRUE + "";
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return e.getMessage();
        }
    }

    @RequestMapping(value = "/internal/query/activity/project/info")
    public String queryActivityProjectInfo() {
        try {
            QueryActivityProjectInfoResponse response = lockInterestService.queryActivityProjectInfo(QueryActivityProjectInfoRequest.newBuilder()
                    .setOrgId(6001L)
                    .setProejctId(1156)
                    .build());
            return response.getUseAmount() + "/" + response.getLuckyAmount() + "/" + response.getBackAmount();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return e.getMessage();
        }
    }


    @Resource
    private TradeCompetitionMapper tradeCompetitionMapper;

    @RequestMapping(value = "/internal/refresh/forward/trade/snapshot")
    public String refreshForwardTradeRankListSnapshot(@RequestParam(name = "id", required = true) Long id) {
        try {
            TradeCompetition tradeCompetition = this.tradeCompetitionMapper.selectByPrimaryKey(id);
            if (tradeCompetition != null) {
                tradeCompetitionService.refreshForwardTradeRankListSnapshot(tradeCompetition);
            } else {
                return "tradeCompetition is null";
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return e.getMessage();
        }
        return Boolean.TRUE + "";
    }

    @Resource
    private OtcService otcService;

    @RequestMapping(value = "/internal/clean/otc/payment/term")
    public String cleanOtcPaymentTerm() {
        try {
            otcService.cleanOtcPaymentTerm();
            return Boolean.TRUE + "";
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return e.getMessage();
        }
    }

    @Resource
    private UserSecurityService userSecurityService;

    @RequestMapping(value = "/internal/init/user/invite/info")
    public String initUserInviteInfo(@RequestParam(name = "orgId", required = true) Long orgId,
                                     @RequestParam(name = "userId", required = true) Long userId,
                                     @RequestParam(name = "accountId", required = true) Long accountId) {
        try {
            inviteService.initUserInviteInfo(orgId, userId, accountId);
            return Boolean.TRUE + "";
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return e.getMessage();
        }
    }

    @RequestMapping(value = "/internal/forever/frozen/user")
    public String initUserInviteInfo(@RequestParam(name = "orgId", required = true) Long orgId,
                                     @RequestParam(name = "lockDay", required = true, defaultValue = "100") Integer lockDay,
                                     @RequestParam(name = "userId", required = true) String userId) {
        try {
            if (StringUtils.isEmpty(userId)) {
                throw new BrokerException(BrokerErrorCode.USER_NOT_EXIST);
            }
            List<String> ids = Splitter.on(",").omitEmptyStrings().splitToList(userId);
            if (CollectionUtils.isEmpty(ids)) {
                throw new BrokerException(BrokerErrorCode.USER_NOT_EXIST);
            }
            userSecurityService.foreverFrozenUser(orgId, lockDay, ids.stream().map(Long::parseLong).collect(Collectors.toList()));
            return Boolean.TRUE + "";
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return e.getMessage();
        }
    }

    //批量处理做市账户
    @RequestMapping(value = "/internal/batch/broker/account/fee")
    public String batchBrokerAccountFee(@RequestParam(name = "orgId", required = true) Long orgId,
                                        @RequestParam(name = "exchangeId", required = true) Long exchangeId,
                                        @RequestParam(name = "userId", required = true) Long userId,
                                        @RequestParam(name = "account", required = true) String account,
                                        @RequestParam(name = "symbols", required = true) String symbols,
                                        @RequestParam(name = "takerBuyFee", required = true) String takerBuyFee,
                                        @RequestParam(name = "takerSellFee", required = true) String takerSellFee,
                                        @RequestParam(name = "makerBuyFee", required = true) String makerBuyFee,
                                        @RequestParam(name = "makerSellFee", required = true) String makerSellFee) {
        try {
            brokerFeeService.batchBrokerAccountFee(orgId, exchangeId, userId, account, symbols, takerBuyFee, takerSellFee, makerBuyFee, makerSellFee);
            return Boolean.TRUE + "";
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return e.getMessage();
        }
    }
}