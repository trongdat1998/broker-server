package io.bhex.broker.server.grpc.server.service;

import io.bhex.base.DateUtil;
import io.bhex.base.account.BalanceDetail;
import io.bhex.base.account.GetBalanceDetailRequest;
import io.bhex.base.margin.GetAvailWithdrawAmountRequest;
import io.bhex.base.margin.LoanRequest;
import io.bhex.base.margin.TokenConfig;
import io.bhex.base.margin.*;
import io.bhex.base.margin.cross.CrossLoanPosition;
import io.bhex.base.margin.cross.LoanOrderStatusEnum;
import io.bhex.base.margin.cross.*;
import io.bhex.base.proto.BaseRequest;
import io.bhex.base.proto.DecimalUtil;
import io.bhex.base.quote.Rate;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.grpc.account.Balance;
import io.bhex.broker.grpc.common.Header;
import io.bhex.broker.grpc.margin.CrossLoanOrder;
import io.bhex.broker.grpc.margin.LoanResponse;
import io.bhex.broker.grpc.margin.*;
import io.bhex.broker.server.domain.FunctionModule;
import io.bhex.broker.server.grpc.client.service.GrpcBalanceService;
import io.bhex.broker.server.grpc.client.service.GrpcMarginPositionService;
import io.bhex.broker.server.grpc.client.service.GrpcMarginService;
import io.bhex.broker.server.model.*;
import io.bhex.broker.server.primary.mapper.*;
import io.bhex.broker.server.statistics.statistics.mapper.StatisticsBalanceSnapshotMapper;
import io.bhex.broker.server.util.RedisLockUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.ibatis.session.RowBounds;
import org.springframework.core.task.TaskExecutor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tk.mybatis.mapper.entity.Example;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author JinYuYuan
 * @description
 * @date 2020-06-10 17:14
 */
@Slf4j
@Service
public class MarginPositionService {

    @Resource
    AccountService accountService;

    @Resource
    BasicService basicService;

    @Resource
    private BrokerService brokerService;

    @Resource
    GrpcMarginPositionService grpcMarginPositionService;

    @Resource
    MarginService marginService;

    @Resource
    MarginAssetMapper marginAssetMapper;

    @Resource
    GrpcBalanceService grpcBalanceService;

    @Resource
    GrpcMarginService grpcMarginService;

    @Resource
    BrokerMapper brokerMapper;

    @Resource(name = "stringRedisTemplate")
    private StringRedisTemplate redisTemplate;

    @Resource
    private MarginActivityMapper marginActivityMapper;

    @Resource
    private UserVerifyMapper userVerifyMapper;

    @Resource
    private MarginProfitActivityMapper marginProfitActivityMapper;

    @Resource
    private StatisticsBalanceSnapshotMapper statisticsBalanceSnapshotMapper;

    @Resource(name = "asyncTaskExecutor")
    private TaskExecutor taskExecutor;

    //借币上限key = userId + tokenId
    private final static String MARGIN_LOAN_LIMIT_KEY = "margin_loan_limit_%s_%s";

    public static final String CAL_MARGIN_ASSET_ORG_LOCK_KEY = "cal_margin_asset_org_lock_%s";
    public static final String CAL_MARGIN_ASSET_ORG_ACCOUNT_LOCK_KEY = "cal_margin_asset_org_account_lock_%s_%s";

    private static final String SORT_MARGIN_PROFIT_ORG_LOCK_KEY = "sort_margin_profit_org_lock_%s";

    private static final String ADMIN_RECAL_PROFIT_ORG_LOCK_KEY = "admin_recal_profit_org_lock_%s";
    // 记录重置收益率的次数 recal_profit_count_aid_time
    private static final String RECAL_PROFIT_COUNT_KEY = "recal_profit_count_%s_%s";

    public LoanResponse loan(Header header, String clientId, String tokenId, String loanAmount, Long accountId) {
        if (!BrokerService.checkModule(header, FunctionModule.MARGIN)) {
            return LoanResponse.getDefaultInstance();
        }
        accountId = accountService.checkMarginAccountId(header, accountId);
        TokenConfig tokenConfig = basicService.getOrgMarginToken(header.getOrgId(), tokenId);
        if (tokenConfig == null || !tokenConfig.getCanBorrow() || tokenConfig.getIsOpen() != 1) {
            throw new BrokerException(BrokerErrorCode.MARGIN_TOKEN_NOT_BORROW);

        }
        GetMarginPositionStatusReply positionStatusReply = marginService.getMarginPositionStatus(header, accountId);
        if (positionStatusReply.getCurStatus() == MarginCrossPositionStatusEnum.POSITION_FORCE_CLOSING) {
            log.warn("loan orgId:{} userId:{} accountId:{} status is FORCE_CLOSING cannot loan", header.getOrgId(), header.getUserId(), accountId);
            throw new BrokerException(BrokerErrorCode.MARGIN_ACCOUNT_IS_FORCE_CLOSE);
        }
        BigDecimal amount = new BigDecimal(loanAmount);
        if (amount.compareTo(DecimalUtil.toBigDecimal(tokenConfig.getMaxQuantity())) > 0
                || amount.compareTo(DecimalUtil.toBigDecimal(tokenConfig.getMinQuantity())) < 0) {
            throw new BrokerException(BrokerErrorCode.MARGIN_LOAN_AMOUNT_TOO_BIG_OR_SMALL);
        }
        if (amount.remainder(new BigDecimal(10).pow(-1 * tokenConfig.getQuantityPrecision(), new MathContext(18))).compareTo(BigDecimal.ZERO) != 0) {
            throw new BrokerException(BrokerErrorCode.MARGIN_LOAN_AMOUNT_PRECISION_TOO_LONG);
        }

        GetLevelInterestByUserIdResponse userInterest = marginService.getLevelInterestByUserId(header, tokenId);
        BigDecimal loanInterest = new BigDecimal(userInterest.getInterestConfig().getInterest());


        boolean tryLock = RedisLockUtils.tryLock(redisTemplate, String.format(MARGIN_LOAN_LIMIT_KEY, header.getUserId(), tokenId), 15000L);
        if (!tryLock) {
            log.error("loan can not get redis lock,request too fast ,userId:{}, tokenId:{}", header.getUserId(), tokenId);
            throw new BrokerException(BrokerErrorCode.REQUEST_TOO_FAST);
        }

        try {
            BigDecimal remainTokenLoanLimit = marginService.getRemainTokenLoanLimit(header.getOrgId(), header.getUserId(), accountId, tokenId, amount);

            if (remainTokenLoanLimit.compareTo(amount) == -1) {//剩余币种借贷上限小于借币数量
                log.error("loan remainTokenLoanLimit is less then loanAmount, orgId:{}. userId:{}, accountId:{}, tokenId:{}, remain:{}, loanAmount:{}", header.getOrgId(), header.getUserId(), accountId, tokenConfig, remainTokenLoanLimit, loanAmount);
                throw new BrokerException(BrokerErrorCode.MARGIN_LOAN_FAILED);
            }

            LoanRequest request = LoanRequest.newBuilder()
                    .setBaseRequest(BaseRequest.newBuilder().setOrganizationId(header.getOrgId()).build())
                    .setClientId(clientId)
                    .setOrgId(header.getOrgId())
                    .setExchangeId(tokenConfig.getExchangeId())
                    .setTokenId(tokenId)
                    .setLoanAmount(DecimalUtil.fromBigDecimal(amount))
                    .setAccountId(accountId)
                    .setLoanInterest(DecimalUtil.fromBigDecimal(loanInterest))
                    .build();
            LoanReply reply = grpcMarginPositionService.loan(request);
            //借币成功后更新保证金记录，用于后续收益计算
            calLoanedMarginAsset(header.getOrgId(), header.getUserId(), accountId, 0);
            return LoanResponse.newBuilder()
                    .setLoanOrderId(reply.getLoanOrderId())
                    .build();
        } finally {
            RedisLockUtils.releaseLock(redisTemplate, String.format(MARGIN_LOAN_LIMIT_KEY, header.getUserId(), tokenId));
        }
    }

    public RepayByLoanIdResponse repayByLoanId(Header header, String clientId, String repayAmount, Long accountId,
                                               Long loanOrderId, MarginRepayTypeEnum repayType) {
        if (!BrokerService.checkModule(header, FunctionModule.MARGIN)) {
            return RepayByLoanIdResponse.getDefaultInstance();
        }
        accountId = accountService.checkMarginAccountId(header, accountId);
        //获取借币记录
        List<CrossLoanOrder> loanOrders = marginService.getCrossLoanOrder(header, accountId, "", loanOrderId, 1, 0L, 0L, 1);
        if (loanOrders == null || loanOrders.isEmpty() || loanOrders.get(0).getStatus() == 2) {
            throw new BrokerException(BrokerErrorCode.MARGIN_LOAN_ORDER_NOT_EXIST);
        }
        CrossLoanOrder loanOrder = loanOrders.get(0);
        TokenConfig tokenConfig = basicService.getOrgMarginToken(header.getOrgId(), loanOrder.getTokenId());
        if (tokenConfig == null) {
            throw new BrokerException(BrokerErrorCode.MARGIN_TOKEN_NOT_BORROW);
        }
        if (repayType != MarginRepayTypeEnum.ALL_REPAY) {
            BigDecimal unpaidAmount = new BigDecimal(loanOrder.getUnpaidAmount()).setScale(tokenConfig.getQuantityPrecision(), RoundingMode.DOWN);
            BigDecimal amount = new BigDecimal(repayAmount).setScale(tokenConfig.getQuantityPrecision(), RoundingMode.DOWN);
            if (unpaidAmount.compareTo(amount) > 0
                    && amount.compareTo(DecimalUtil.toBigDecimal(tokenConfig.getRepayMinQuantity())) < 0) {
                throw new BrokerException(BrokerErrorCode.MARGIN_LOAN_REPAY_AMOUNT_IS_SMALL);
            }
        }
        RepayRequest request = RepayRequest.newBuilder()
                .setBaseRequest(BaseRequest.newBuilder().setOrganizationId(header.getOrgId()).build())
                .setClientId(clientId)
                .setTokenId(loanOrder.getTokenId())
                .setRepayAmount(DecimalUtil.fromBigDecimal(new BigDecimal(repayAmount)))
                .setAccountId(accountId)
                .setLoanOrderId(loanOrderId)
                .setRepayType(MarginCrossRepayTypeEnum.forNumber(repayType.getNumber()))
                .setLenderAccountId(loanOrder.getLenderAccountId())
                .build();
        RepayResponse result = grpcMarginPositionService.repayByLoanId(request);
        //还币成功计算安全度
        calculateSafeByAccount(header.getOrgId(), accountId);
        calRepaidMarginAsset(header.getOrgId(), header.getUserId(), accountId, 0);
        return RepayByLoanIdResponse.newBuilder()
                .setRepayOrderId(result.getRepayOrderId())
                .build();
    }

    public GetAvailWithdrawAmountResponse getAvailWithdrawAmount(Header header, String tokenId, Long accountId) {
        if (!BrokerService.checkModule(header, FunctionModule.MARGIN)) {
            return GetAvailWithdrawAmountResponse.getDefaultInstance();
        }
        accountId = accountService.checkMarginAccountId(header, accountId);
        TokenConfig tokenConfig = basicService.getOrgMarginToken(header.getOrgId(), tokenId);
        if (tokenConfig == null) {
            throw new BrokerException(BrokerErrorCode.MARGIN_TOKEN_NOT_BORROW);
        }
        GetAvailWithdrawAmountRequest request = GetAvailWithdrawAmountRequest.newBuilder()
                .setBaseRequest(BaseRequest.newBuilder().setOrganizationId(header.getOrgId()).build())
                .setOrgId(header.getOrgId())
                .setAccountId(accountId)
                .setTokenId(tokenId)
                .build();
        GetAvailWithdrawAmountReply reply = grpcMarginPositionService.getAvailWithdrawAmount(request);
        return GetAvailWithdrawAmountResponse.newBuilder()
                .setAvailWithdrawAmount(DecimalUtil.toTrimString(reply.getAvailWithdrawAmount()))
                .build();
    }

    public GetAllCrossLoanPositionResponse getAllCrossLoanPosition(Header header, Long accountId, String tokenId) {
        if (!BrokerService.checkModule(header, FunctionModule.MARGIN)) {
            return GetAllCrossLoanPositionResponse.getDefaultInstance();
        }
        accountId = accountService.checkMarginAccountIdNoThrow(header, accountId);
        if (accountId == null) {
            return GetAllCrossLoanPositionResponse.newBuilder().getDefaultInstanceForType();
        }
        TokenConfig tokenConfig = null;
        if (StringUtils.isNotEmpty(tokenId)) {
            tokenConfig = basicService.getOrgMarginToken(header.getOrgId(), tokenId);
            if (tokenConfig == null || !tokenConfig.getCanBorrow()) {
                throw new BrokerException(BrokerErrorCode.MARGIN_TOKEN_NOT_BORROW);
            }
        }
        CrossLoanPositionRequest request = CrossLoanPositionRequest.newBuilder()
                .setBaseRequest(BaseRequest.newBuilder().setOrganizationId(header.getOrgId()))
                .setAccountId(accountId)
                .setTokenId(tokenId)
                .build();

        CrossLoanPositionReply reply = grpcMarginPositionService.getCrossLoanPosition(request);
        List<CrossLoanPosition> loanPositions = reply.getCrossLoanPositionList();
        List<io.bhex.broker.grpc.margin.CrossLoanPosition> list = new ArrayList<>();
        if (loanPositions != null) {
            list = loanPositions.stream().filter(position -> DecimalUtil.toBigDecimal(position.getLoanTotal()).compareTo(BigDecimal.ZERO) > 0 || DecimalUtil.toBigDecimal(position.getInterestUnpaid()).compareTo(BigDecimal.ZERO) > 0)
                    .map(position -> getCrossPostion(header.getOrgId(), position))
                    .collect(Collectors.toList());
        }
        return GetAllCrossLoanPositionResponse.newBuilder()
                .addAllCrossLoanPosition(list)
                .build();

    }


    public io.bhex.broker.grpc.margin.CrossLoanPosition getCrossPostion(Long orgId, CrossLoanPosition position) {
        Rate rate = basicService.getV3Rate(orgId, position.getTokenId());
        BigDecimal btcRate = rate == null ? BigDecimal.ZERO : DecimalUtil.toBigDecimal(rate.getRatesMap().get("BTC"));
        BigDecimal usdtRate = rate == null ? BigDecimal.ZERO : DecimalUtil.toBigDecimal(rate.getRatesMap().get("USDT"));
        return io.bhex.broker.grpc.margin.CrossLoanPosition.newBuilder()
                .setAccountId(position.getAccountId())
                .setOrgId(orgId)
                .setTokenId(position.getTokenId())
                .setLoanTotal(DecimalUtil.toTrimString(position.getLoanTotal()))
                .setLoanBtcValue(DecimalUtil.toBigDecimal(position.getLoanTotal()).multiply(btcRate)
                        .setScale(18, RoundingMode.DOWN).stripTrailingZeros().toPlainString())
                .setInterestPaid(DecimalUtil.toTrimString(position.getInterestPaid()))
                .setInterestUnpaid(DecimalUtil.toTrimString(position.getInterestUnpaid()))
                .setUnpaidBtcValue(DecimalUtil.toBigDecimal(position.getInterestUnpaid()).multiply(btcRate)
                        .setScale(18, RoundingMode.DOWN).stripTrailingZeros().toPlainString())
                .setLoanUsdtVaule(DecimalUtil.toBigDecimal(position.getLoanTotal()).multiply(usdtRate)
                        .setScale(18, RoundingMode.DOWN).stripTrailingZeros().toPlainString())
                .setUnpaidUsdtValue(DecimalUtil.toBigDecimal(position.getInterestUnpaid()).multiply(usdtRate)
                        .setScale(18, RoundingMode.DOWN).stripTrailingZeros().toPlainString())
                .build();
    }

    public GetAllPositionResponse getAllPosition(Header header, Long accountId) {
        accountId = accountService.checkMarginAccountId(header, accountId);
        GetLoanAccountPositionRequest request = GetLoanAccountPositionRequest.newBuilder()
                .setBaseRequest(BaseRequest.newBuilder().setOrganizationId(header.getOrgId()).build())
                .setOrgId(header.getOrgId())
                .setAccountId(accountId)
                .build();
        GetLoanAccountPositionReply reply = grpcMarginPositionService.getLoanAccountPosition(request);
        return GetAllPositionResponse.newBuilder()
                .setAccountId(accountId)
                .setOrgId(header.getOrgId())
                .setTotal(DecimalUtil.toTrimString(reply.getTotal()))
                .setLoanAmount(DecimalUtil.toTrimString(reply.getLoanAmount()))
                .setMarginAmount(DecimalUtil.toTrimString(reply.getMarginAmount()))
                .setOccupyMargin(DecimalUtil.toTrimString(reply.getOccupyMargin()))
                .setUsdtTotal(DecimalUtil.toTrimString(reply.getUsdtTotal()))
                .setUsdtLoanAmount(DecimalUtil.toTrimString(reply.getUsdtLoanAmount()))
                .setUsdtMarginAmount(DecimalUtil.toTrimString(reply.getUsdtMarginAmount()))
                .setUsdtOccupyMargin(DecimalUtil.toTrimString(reply.getUsdtOccupyMargin()))
                .build();
    }

    //未使用，后续需修改
    public RepayAllByLoanIdResponse repayAllByLoanId(Header header, Long accountId, String clientOrderId, Long loanOrderId, Long exchangeId) {
        if (!BrokerService.checkModule(header, FunctionModule.MARGIN)) {
            return RepayAllByLoanIdResponse.getDefaultInstance();
        }
        accountId = accountService.checkMarginAccountId(header, accountId);
        BatchRepayRequest request = BatchRepayRequest.newBuilder()
                .setBaseRequest(BaseRequest.newBuilder().setOrganizationId(header.getOrgId()).build())
                .setAccountId(accountId)
                .setClientId(clientOrderId)
                .addLoanOrderIds(loanOrderId)
                .build();
        BatchRepayResponse response = grpcMarginPositionService.batchRepay(request);
        return RepayAllByLoanIdResponse.newBuilder().setLoanOrderId(loanOrderId).build();
    }

    @Async
    public void calculateSafeByAccount(Long orgId, Long accountId) {
        try {
            CalculateSafeByAccountReqeust reqeust = CalculateSafeByAccountReqeust.newBuilder()
                    .setBaseRequest(BaseRequest.newBuilder().setOrganizationId(orgId).build())
                    .setAccountId(accountId)
                    .build();
            CalculateSafeByAccountReply reply = grpcMarginPositionService.calculateSafeByAccount(reqeust);
        } catch (Exception e) {
            log.warn("calculateSafeByAccount error orgId{} accountId{}", orgId, accountId, e);
        }
    }

    public RecalculationRoundProfitResponse recalculationRoundProfit(Header header, boolean adminRecal) {
        if (!BrokerService.checkModule(header, FunctionModule.MARGIN)) {
            return RecalculationRoundProfitResponse.getDefaultInstance();
        }
        Long accountId = accountService.checkMarginAccountId(header, 0L);
        boolean needUpdateCoount = false;
        String key = "";
        int updateCount = 0;
        //活动期间校验重置次数是否达到上限
        if (!adminRecal) {//用户主动重置
            //判断活动是否开启
            Long time = System.currentTimeMillis();
            MarginActivity marginActivity = marginActivityMapper.getByOrgIdAndId(header.getOrgId(), 842711774229992200L);
            if (marginActivity != null && marginActivity.getStatus() == 1 && time >= marginActivity.getStartTime() && time <= marginActivity.getEndTime()) {
                TimeZone timeZone = TimeZone.getTimeZone("GMT+8:00");
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
                simpleDateFormat.setTimeZone(timeZone);
                Long date = Long.parseLong(simpleDateFormat.format(time));
                //晒收益活动期间生效
                key = String.format(RECAL_PROFIT_COUNT_KEY, accountId, date);
                needUpdateCoount = true;
                if (Boolean.TRUE.equals(redisTemplate.hasKey(key))) {
                    redisTemplate.opsForValue().set(key, "0", Duration.ofHours(24));
                }
                updateCount = Integer.parseInt(Objects.requireNonNull(redisTemplate.opsForValue().get(key)));
                if (updateCount >= 10) {
                    //超过限制次数
                    throw new BrokerException(BrokerErrorCode.MARGIN_RECALCULATION_PROFIT_LIMIT_ERROR);
                }
            }
        }
        //获取未归还的借贷记录
        CrossLoanOrderListRequest request = CrossLoanOrderListRequest.newBuilder()
                .setBaseRequest(BaseRequest.newBuilder().setOrganizationId(header.getOrgId()).build())
                .setStatus(LoanOrderStatusEnum.LOAN_ORDER_ACTIVE_VALUE)
                .setAccountId(accountId)
                .setLimit(1)
                .build();
        CrossLoanOrderListReply response = grpcMarginService.getCrossLoanOrder(request);
        List<io.bhex.base.margin.cross.CrossLoanOrder> crossLoanOrders = response.getCrossLoanOrdersList();
        //当前保证金
        //计算保证金资产
        Map<String, BigDecimal> rateMap = new HashMap<>();
        BigDecimal margin = calMarginAsset(header.getOrgId(), accountId, rateMap);
        BigDecimal todayProfit = BigDecimal.ZERO;
        BigDecimal todayProfitRate = BigDecimal.ZERO;
        if (crossLoanOrders.size() == 0) {//不存在借贷未还的借贷记录
            //获取保证金记录表中的记录
            MarginAssetRecord record = marginAssetMapper.getMarginAssetByAccount(header.getOrgId(), accountId);
            if (record != null) {//更新对应记录到无借贷状态
                record.setBeginMarginAsset(BigDecimal.ZERO);
                record.setIsLoaning(2);
                record.setUpdated(System.currentTimeMillis());
                marginAssetMapper.updateByPrimaryKeySelective(record);
                todayProfit = margin.subtract(record.getTodayBeginMarginAsset()).setScale(2, RoundingMode.DOWN);
                todayProfitRate = record.getTodayBeginMarginAsset().compareTo(BigDecimal.ZERO) == 0 ? BigDecimal.ZERO : todayProfit.divide(record.getTodayBeginMarginAsset(), 4, RoundingMode.DOWN);
            }
        } else {
            //存在借贷，更新初始保证金
            MarginAssetRecord marginAssetRecord = marginAssetMapper.getMarginAssetByAccount(header.getOrgId(), accountId);
            boolean insterFlag = false;
            //保证金计算记录不存在
            if (marginAssetRecord == null) {
                insterFlag = true;
                marginAssetRecord = new MarginAssetRecord();
                marginAssetRecord.setOrgId(header.getOrgId());
                marginAssetRecord.setUserId(header.getUserId());
                marginAssetRecord.setAccountId(accountId);
                marginAssetRecord.setCreated(System.currentTimeMillis());
                marginAssetRecord.setTodayBeginMarginAsset(BigDecimal.ZERO);
                marginAssetRecord.setIsLoaning(1);
            }
            int oldIsLoaning = marginAssetRecord.getIsLoaning();
            marginAssetRecord.setIsLoaning(1);

            marginAssetRecord.setBeginMarginAsset(margin);
            //表中不存在对应记录 || 当日初始保证金=0 更新当日初始保证金
            if (insterFlag) {
                marginAssetRecord.setTodayBeginMarginAsset(margin);
            }
            marginAssetRecord.setUpdated(System.currentTimeMillis());
            if (insterFlag) {//新增记录
                marginAssetMapper.insertSelective(marginAssetRecord);
            } else {
                Example example = new Example(MarginAssetRecord.class);
                Example.Criteria criteria = example.createCriteria();
                criteria.andEqualTo("orgId", header.getOrgId());
                criteria.andEqualTo("accountId", accountId);
                criteria.andEqualTo("isLoaning", oldIsLoaning);
                marginAssetMapper.updateByExample(marginAssetRecord, example);
            }
            todayProfit = margin.subtract(marginAssetRecord.getTodayBeginMarginAsset()).setScale(2, RoundingMode.DOWN);
            todayProfitRate = marginAssetRecord.getTodayBeginMarginAsset().compareTo(BigDecimal.ZERO) == 0 ? BigDecimal.ZERO : todayProfit.divide(marginAssetRecord.getTodayBeginMarginAsset(), 4, RoundingMode.DOWN);

        }
        if (needUpdateCoount) {//需要跟下redis的重置次数
            updateCount++;
            redisTemplate.opsForValue().set(key, updateCount + "");
        }
        return RecalculationRoundProfitResponse.newBuilder()
                .setRet(0)
                .setBroundProfit("0")
                .setBroundProfitRate("0")
                .setTodayProfit(todayProfit.stripTrailingZeros().toPlainString())
                .setTodayProfitRate(todayProfitRate.stripTrailingZeros().toPlainString())
                .build();
    }

    public GetMarginProfitResponse getMarginProfit(Header header) {
        if (!BrokerService.checkModule(header, FunctionModule.MARGIN)) {
            return GetMarginProfitResponse.getDefaultInstance();
        }
        Long accountId = accountService.checkMarginAccountIdNoThrow(header, 0L);
        if (accountId == null) {
            return GetMarginProfitResponse.getDefaultInstance();
        }
        //获取保证金记录
        MarginAssetRecord record = marginAssetMapper.getMarginAssetByAccount(header.getOrgId(), accountId);
        BigDecimal broundProfit = BigDecimal.ZERO;
        BigDecimal broundProfitRate = BigDecimal.ZERO;
        BigDecimal todayProfit = BigDecimal.ZERO;
        BigDecimal todayProfitRate = BigDecimal.ZERO;
        if (record != null) {
            //计算当前保证金
            Map<String, BigDecimal> rateMap = new HashMap<>();
            BigDecimal margin = calMarginAsset(header.getOrgId(), accountId, rateMap);
            if (record.getTodayBeginMarginAsset().compareTo(BigDecimal.ZERO) > 0 || record.getIsLoaning() == 1) {
                //当日发生过借贷
                todayProfit = margin.subtract(record.getTodayBeginMarginAsset()).setScale(2, RoundingMode.DOWN);
                todayProfitRate = record.getTodayBeginMarginAsset().compareTo(BigDecimal.ZERO) == 0 ? BigDecimal.ZERO : todayProfit.divide(record.getTodayBeginMarginAsset(), 4, RoundingMode.DOWN);
            }
            if (record.getIsLoaning() == 1) {
                //借贷中，本轮借贷未结束
                broundProfit = margin.subtract(record.getBeginMarginAsset()).setScale(2, RoundingMode.DOWN);
                broundProfitRate = record.getBeginMarginAsset().compareTo(BigDecimal.ZERO) == 0 ? BigDecimal.ZERO : broundProfit.divide(record.getBeginMarginAsset(), 4, RoundingMode.DOWN);
            }
        }

        return GetMarginProfitResponse.newBuilder()
                .setBroundProfit(broundProfit.stripTrailingZeros().toPlainString())
                .setBroundProfitRate(broundProfitRate.stripTrailingZeros().toPlainString())
                .setTodayProfit(todayProfit.stripTrailingZeros().toPlainString())
                .setTodayProfitRate(todayProfitRate.stripTrailingZeros().toPlainString())
                .build();
    }

    //借币后计算保证金
    @Async
    public void calLoanedMarginAsset(Long orgId, Long userId, Long accountId, int index) {
        try {
            //判断当次借币之前是否存在借币
            MarginAssetRecord marginAssetRecord = marginAssetMapper.getMarginAssetByAccount(orgId, accountId);
            BigDecimal margin = BigDecimal.ZERO;
            if (marginAssetRecord == null || marginAssetRecord.getIsLoaning() == 2) {
                boolean insterFlag = false;
                //保证金计算记录不存在
                if (marginAssetRecord == null) {
                    insterFlag = true;
                    marginAssetRecord = new MarginAssetRecord();
                    marginAssetRecord.setOrgId(orgId);
                    marginAssetRecord.setUserId(userId);
                    marginAssetRecord.setAccountId(accountId);
                    marginAssetRecord.setCreated(System.currentTimeMillis());
                    marginAssetRecord.setTodayBeginMarginAsset(BigDecimal.ZERO);
                    marginAssetRecord.setIsLoaning(1);
                }
                int oldIsLoaning = marginAssetRecord.getIsLoaning();
                marginAssetRecord.setIsLoaning(1);
                Map<String, BigDecimal> rateMap = new HashMap<>();
                //计算保证金资产
                margin = calMarginAsset(orgId, accountId, rateMap);
                marginAssetRecord.setBeginMarginAsset(margin);
                //表中不存在对应记录 || 当日初始保证金=0 更新当日初始保证金
                if (insterFlag || marginAssetRecord.getTodayBeginMarginAsset().compareTo(BigDecimal.ZERO) == 0) {
                    marginAssetRecord.setTodayBeginMarginAsset(margin);
                }
                marginAssetRecord.setUpdated(System.currentTimeMillis());
                if (insterFlag) {//新增记录
                    marginAssetMapper.insertSelective(marginAssetRecord);
                } else {
                    Example example = new Example(MarginAssetRecord.class);
                    Example.Criteria criteria = example.createCriteria();
                    criteria.andEqualTo("orgId", orgId);
                    criteria.andEqualTo("accountId", accountId);
                    criteria.andEqualTo("isLoaning", oldIsLoaning);
                    marginAssetMapper.updateByExample(marginAssetRecord, example);
                }
            }
        } catch (Exception e) {
            log.warn("calLoanedMarginAsset orgId:{} userId:{} error {}", orgId, userId, e.getMessage(), e);
            if (index == 0) {
                calLoanedMarginAsset(orgId, userId, accountId, ++index);
            }
        }

    }

    /**
     * 还币后计算保证金
     *
     * @param orgId
     * @param userId
     * @param accountId
     */
    @Async
    public void calRepaidMarginAsset(Long orgId, Long userId, Long accountId, int index) {
        try {
            //获取未归还的借币记录
            CrossLoanOrderListRequest request = CrossLoanOrderListRequest.newBuilder()
                    .setBaseRequest(BaseRequest.newBuilder().setOrganizationId(orgId).build())
                    .setStatus(LoanOrderStatusEnum.LOAN_ORDER_ACTIVE_VALUE)
                    .setAccountId(accountId)
                    .setLimit(1)
                    .build();
            CrossLoanOrderListReply response = grpcMarginService.getCrossLoanOrder(request);
            List<io.bhex.base.margin.cross.CrossLoanOrder> crossLoanOrders = response.getCrossLoanOrdersList();
            if (crossLoanOrders.size() == 0) {//不存在借贷未还的借贷记录，更新初始保证金
                MarginAssetRecord marginAssetRecord = marginAssetMapper.getMarginAssetByAccount(orgId, accountId);
                if (marginAssetRecord != null) {
                    int oldIsLoaning = marginAssetRecord.getIsLoaning();
                    marginAssetRecord.setBeginMarginAsset(BigDecimal.ZERO);
                    marginAssetRecord.setIsLoaning(2);
                    marginAssetRecord.setUpdated(System.currentTimeMillis());
                    Example example = new Example(MarginAssetRecord.class);
                    Example.Criteria criteria = example.createCriteria();
                    criteria.andEqualTo("orgId", orgId);
                    criteria.andEqualTo("accountId", accountId);
                    criteria.andEqualTo("isLoaning", oldIsLoaning);
                    marginAssetMapper.updateByExampleSelective(marginAssetRecord, example);
                }
            }
        } catch (Exception e) {
            log.warn("calRepaidMarginAsset orgId:{} userId:{} error {}", orgId, userId, e.getMessage(), e);
            if (index == 0) {
                calRepaidMarginAsset(orgId, userId, accountId, ++index);
            }
        }

    }

    /**
     * 入金后计算保证金
     *
     * @param orgId
     * @param userId
     * @param accountId
     * @param tokenId
     * @param qty
     */
    @Async
    public void calTransferMarginAsset(Long orgId, Long userId, Long accountId, String tokenId, BigDecimal qty,
                                       int index) {
        try {
            MarginAssetRecord marginAssetRecord = marginAssetMapper.lockMarginAssetByAccount(orgId, accountId);
            if (marginAssetRecord != null) {
                //用户处于借贷中，更新保证金
                if (marginAssetRecord.getTodayBeginMarginAsset().compareTo(BigDecimal.ZERO) != 0 || marginAssetRecord.getIsLoaning() == 1) {
                    Rate rate = basicService.getV3Rate(orgId, tokenId);
                    //汇率不存在，或测试币不进行计算
                    if (rate == null || Stream.of(AccountService.TEST_TOKENS)
                            .anyMatch(testToken -> testToken.equalsIgnoreCase(tokenId))) {
                        return;
                    }
                    BigDecimal usdtRate = DecimalUtil.toBigDecimal(rate.getRatesMap().get("USDT"));
                    BigDecimal margin = qty.multiply(usdtRate);
                    marginAssetRecord.setTodayBeginMarginAsset(marginAssetRecord.getTodayBeginMarginAsset().add(margin));
                    //借贷中更新初始保证金
                    if (marginAssetRecord.getIsLoaning() == 1) {
                        marginAssetRecord.setBeginMarginAsset(marginAssetRecord.getBeginMarginAsset().add(margin));
                    }
                    marginAssetRecord.setUpdated(System.currentTimeMillis());
                    marginAssetMapper.updateByPrimaryKeySelective(marginAssetRecord);
                }
            }
        } catch (Exception e) {
            log.warn("calTransferMarginAsset orgId:{} userId:{} error {}", orgId, userId, e.getMessage(), e);
            if (index == 0) {
                calTransferMarginAsset(orgId, userId, accountId, tokenId, qty, ++index);
            }
        }

    }

    /**
     * 每日更新 每日初始保证金
     */
    @Scheduled(cron = "0 0 0 1/1 * ?")
    public void calTodayMarginAssetTask() {
        if (!BasicService.marginOrgIds.isEmpty()) {
            for (Long orgId : BasicService.marginOrgIds) {
                try {
                    //判断当前机构是否可用
                    Broker broker = brokerMapper.getByOrgIdAndStatus(orgId);
                    if (broker == null) {
                        continue;
                    }
                    String orgLockKey = String.format(CAL_MARGIN_ASSET_ORG_LOCK_KEY, orgId);
                    Boolean lock = RedisLockUtils.tryLock(redisTemplate, orgLockKey, 60 * 60 * 1000);
                    if (lock) {
                        calTodayMarginAssetByOrg(orgId, 0);
                    }
                } catch (Exception e) {
                    log.warn("calTodayMarginAssetTask error orgId:{} {}", orgId, e.getMessage(), e);
                }
            }
        }
    }

    /**
     * 通过ORGID获取计算当日初始保证金所需要的AID信息
     *
     * @param orgId
     */
    public void calTodayMarginAssetByOrg(Long orgId, int index) {
        try {
            //获取所有未还的借币记录
            int size = 500;
            int limit = 500;
            Long toOrderId = 1L;
            List<io.bhex.base.margin.cross.CrossLoanOrder> orders = new ArrayList<>();
            //获取所有的借币订单 todo 需做优化，业务量大时这里会比较耗性能
            while (size >= 500) {
                CrossLoanOrderListRequest request = CrossLoanOrderListRequest.newBuilder()
                        .setBaseRequest(BaseRequest.newBuilder().setOrganizationId(orgId).build())
                        .setStatus(LoanOrderStatusEnum.LOAN_ORDER_ACTIVE_VALUE)
                        .setToOrderId(toOrderId)
                        .setLimit(limit)
                        .build();
                CrossLoanOrderListReply response = grpcMarginService.getCrossLoanOrder(request);
                List<io.bhex.base.margin.cross.CrossLoanOrder> crossLoanOrders = response.getCrossLoanOrdersList();
                size = crossLoanOrders.size();
                if (size == 0) {
                    break;
                }
                toOrderId = crossLoanOrders.stream().map(io.bhex.base.margin.cross.CrossLoanOrder::getLoanOrderId)
                        .max(Long::compareTo).get();
                orders.addAll(crossLoanOrders);
            }
            Set<Long> loanAccountSet = orders.stream()
                    .map(io.bhex.base.margin.cross.CrossLoanOrder::getAccountId)
                    .collect(Collectors.toSet());
            //单次计算 相同org下使用的汇率相同
            Map<String, BigDecimal> rateMap = new HashMap<>();
            //更新有借贷的用户保证金
            for (Long accountId : loanAccountSet) {
                String lockKey = String.format(CAL_MARGIN_ASSET_ORG_ACCOUNT_LOCK_KEY, orgId, accountId);
                Boolean lock = false;
                lock = RedisLockUtils.tryLock(redisTemplate, lockKey, 5 * 60 * 1000);
                if (lock) {
                    calTodayMarginAssetByAccountId(orgId, accountId, rateMap, 0);
                }
            }
            //更新无借贷用户的保证金记录
            updateNoBorrowMarginAsset(orgId, loanAccountSet);
        } catch (Exception e) {
            log.warn("calTodayMarginAssetByOrg error orgId:{} {}", orgId, e.getMessage(), e);
            if (index == 0) {
                calTodayMarginAssetByOrg(orgId, ++index);
            } else {
                throw e;
            }
        }
    }

    public void calTodayMarginAssetByAccountId(Long orgId, Long
            accountId, Map<String, BigDecimal> rateMap, Integer index) {
        try {
            //计算保证金
            BigDecimal margin = calMarginAsset(orgId, accountId, rateMap);
            MarginAssetRecord marginAssetRecord = marginAssetMapper.getMarginAssetByAccount(orgId, accountId);
            if (marginAssetRecord == null) {//记录不存在
                Long userId = null;
                try {
                    userId = accountService.getUserIdByAccountId(orgId, accountId);
                } catch (Exception e) {
                    log.warn("calTodayMarginAssetByAccountId get userId error aid:{}-{} {}", orgId, accountId, e.getMessage(), e);
                }
                if (userId == null) {
                    return;
                }
                marginAssetRecord = new MarginAssetRecord();
                marginAssetRecord.setOrgId(orgId);
                marginAssetRecord.setUserId(userId);
                marginAssetRecord.setAccountId(accountId);
                marginAssetRecord.setBeginMarginAsset(margin);
                marginAssetRecord.setTodayBeginMarginAsset(margin);
                marginAssetRecord.setIsLoaning(1);
                marginAssetRecord.setCreated(System.currentTimeMillis());
                marginAssetRecord.setUpdated(System.currentTimeMillis());
                marginAssetMapper.insertSelective(marginAssetRecord);
            } else {
                marginAssetRecord.setTodayBeginMarginAsset(margin);
                marginAssetRecord.setIsLoaning(1);
                marginAssetRecord.setUpdated(System.currentTimeMillis());
                marginAssetMapper.updateByPrimaryKeySelective(marginAssetRecord);
            }
        } catch (Exception e) {
            log.error("calTodayMarginAssetByAccountId orgId:{} accountId:{} {}", orgId, accountId, e.getMessage(), e);
            if (index == 0) {
                calTodayMarginAssetByAccountId(orgId, accountId, rateMap, ++index);
            }
        }
    }

    /**
     * 更新无借贷用户的每日保证金
     *
     * @param orgId
     */
    public void updateNoBorrowMarginAsset(Long orgId, Set<Long> loanAids) {
        //获取保证金记录表中的AID，与借贷AID集合比较。更新无借贷的记录
        List<MarginAssetRecord> records = marginAssetMapper.queryMarginAssetByOrgId(orgId);
        for (MarginAssetRecord record : records) {
            try {
                //aid不存在当前杠杆借贷AID中
                if (!loanAids.contains(record.getAccountId())) {
                    record.setTodayBeginMarginAsset(BigDecimal.ZERO);
                    record.setBeginMarginAsset(BigDecimal.ZERO);
                    record.setIsLoaning(2);
                    marginAssetMapper.updateByPrimaryKeySelective(record);
                }
            } catch (Exception e) {
                log.warn("updateNoBorrowMarginAsset  orgId:{} uid:{} aid:{} error {}", orgId, record.getUserId(), record.getAccountId(), e.getMessage(), e);
            }
        }
    }

    //计算保证金资产
    public BigDecimal calMarginAsset(Long orgId, Long accountId, Map<String, BigDecimal> rateMap) {
        //获取总资产
        BigDecimal allTotal = BigDecimal.ZERO;
        GetBalanceDetailRequest balanceDetailRequest = GetBalanceDetailRequest.newBuilder()
                .setBaseRequest(BaseRequest.newBuilder().setOrganizationId(orgId))
                .setAccountId(accountId)
                .build();
        List<BalanceDetail> balanceDetails = grpcBalanceService.getBalanceDetail(balanceDetailRequest).getBalanceDetailsList();
        for (BalanceDetail detail : balanceDetails) {
            BigDecimal total = DecimalUtil.toBigDecimal(detail.getTotal());
            if (total.compareTo(BigDecimal.ZERO) > 0) {
                BigDecimal usdtRate = getCalMarginAssetRate(orgId, detail.getTokenId(), rateMap);
                allTotal = allTotal.add(total.multiply(usdtRate));
            }
        }
        //计算总借贷资产
        BigDecimal allLoan = BigDecimal.ZERO;
        CrossLoanPositionRequest request = CrossLoanPositionRequest.newBuilder()
                .setBaseRequest(BaseRequest.newBuilder().setOrganizationId(orgId))
                .setAccountId(accountId)
                .build();
        CrossLoanPositionReply reply = grpcMarginPositionService.getCrossLoanPosition(request);
        List<CrossLoanPosition> loanPositions = reply.getCrossLoanPositionList();
        for (CrossLoanPosition position : loanPositions) {
            BigDecimal loanTotal = DecimalUtil.toBigDecimal(position.getLoanTotal()).add(DecimalUtil.toBigDecimal(position.getInterestUnpaid()));
            if (loanTotal.compareTo(BigDecimal.ZERO) > 0) {
                BigDecimal usdtRate = getCalMarginAssetRate(orgId, position.getTokenId(), rateMap);
                allLoan = allLoan.add(loanTotal.multiply(usdtRate));
            }
        }
        //保证金资产: 当前客户总资产 - 当前已借资产 - 当前未还利息
        return allTotal.subtract(allLoan);
    }

    private BigDecimal getCalMarginAssetRate(Long orgId, String tokenId, Map<String, BigDecimal> rateMap) {
        String key = orgId + "#" + tokenId;
        if (rateMap.containsKey(key)) {
            return rateMap.get(key);
        } else {
            Rate rate = basicService.getV3Rate(orgId, tokenId);
            if (rate == null || Stream.of(AccountService.TEST_TOKENS)
                    .anyMatch(testToken -> testToken.equalsIgnoreCase(tokenId))) {
                return BigDecimal.ZERO;
            }
            BigDecimal usdtRate = DecimalUtil.toBigDecimal(rate.getRatesMap().get("USDT"));
            rateMap.put(key, usdtRate);
            return usdtRate;
        }
    }

    /**
     * 参加收益活动
     *
     * @param header
     * @return
     */
    public SubmitProfitActivityResponse submitProfitActivity(Header header) {
        if (!BrokerService.checkModule(header, FunctionModule.MARGIN)) {
            return SubmitProfitActivityResponse.getDefaultInstance();
        }
        //判断活动是否开启
        Long time = System.currentTimeMillis();
        MarginActivity marginActivity = marginActivityMapper.getByOrgIdAndId(header.getOrgId(), 842711774229992200L);
        if (marginActivity == null || marginActivity.getStatus() != 1 || time < marginActivity.getStartTime() || time > marginActivity.getEndTime()) {
            throw new BrokerException(BrokerErrorCode.FEATURE_NOT_OPEN);
        }
        Long accountId = accountService.checkMarginAccountId(header, 0L);
        //判断是否存在借贷
        MarginAssetRecord marginAssetRecord = marginAssetMapper.getMarginAssetByAccount(header.getOrgId(), accountId);
        if (marginAssetRecord == null || marginAssetRecord.getIsLoaning() == 2) {
            throw new BrokerException(BrokerErrorCode.NEED_TO_LOAN_ERROR);
        }
        //获取KYC
        UserVerify userVerify = userVerifyMapper.getKycPassUserByUserId(header.getUserId());
        if (userVerify == null || userVerify.getKycLevel() < 20) {
            throw new BrokerException(BrokerErrorCode.KYC_SENIOR_VERIFY_NEED);
        }
        //校验资产
        List<Balance> balances = accountService.queryBalance(header, accountId, new ArrayList<>());
        BigDecimal allPosi = BigDecimal.ZERO;
        for (Balance balance : balances) {
            allPosi = allPosi.add(new BigDecimal(balance.getUsdtValue()));
        }
        if (allPosi.compareTo(new BigDecimal("2000")) < 0) {
            throw new BrokerException(BrokerErrorCode.INSUFFICIENT_BALANCE);
        }
        //获取收益率
        GetMarginProfitResponse profitResponse = getMarginProfit(header);
        TimeZone timeZone = TimeZone.getTimeZone("GMT+8:00");
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
        simpleDateFormat.setTimeZone(timeZone);
        Long joinDate = Long.parseLong(simpleDateFormat.format(time));
        MarginProfitActivity activity = marginProfitActivityMapper.getProfitRecord(header.getOrgId(), joinDate, accountId);
        if (activity == null) {
            activity = new MarginProfitActivity();
            activity.setOrgId(header.getOrgId());
            activity.setJoinDate(joinDate);
            activity.setUserId(header.getUserId());
            activity.setAccountId(accountId);
            activity.setSubmitTime(time);
            activity.setKycLevel(userVerify.getKycLevel());
            activity.setProfitRate(new BigDecimal(profitResponse.getBroundProfitRate()));
            activity.setAllPositionUsdt(allPosi);
            activity.setTodayPositionUsdt(BigDecimal.ZERO);
            activity.setJoinStatus(2);
            activity.setDayRanking(0);
            activity.setUpdates(1L);
            activity.setCreated(time);
            activity.setUpdated(time);
            marginProfitActivityMapper.insertSelective(activity);
        } else {
            //是否超过更新次数
            if (activity.getUpdates() >= 10) {
                throw new BrokerException(BrokerErrorCode.MARGIN_SUBMIT_TIMES_LIMIT_ERROR);
            }
            BigDecimal profitRate = new BigDecimal(profitResponse.getBroundProfitRate());
            if (profitRate.compareTo(activity.getProfitRate()) < 0) {
                profitRate = activity.getProfitRate();
            }
            activity.setAllPositionUsdt(allPosi);
            activity.setProfitRate(profitRate);
            activity.setSubmitTime(time);
            activity.setUpdates(activity.getUpdates() + 1);
            activity.setUpdated(time);
            marginProfitActivityMapper.updateByPrimaryKeySelective(activity);
        }
        return SubmitProfitActivityResponse.newBuilder()
                .setRet(0)
                .setProfitRate(activity.getProfitRate().stripTrailingZeros().toPlainString())
                .setUpdates(activity.getUpdates())
                .build();
    }

    /**
     * 交易端查询收益列表
     *
     * @param header
     * @param joinDate
     * @param accountId
     * @param limit
     * @return
     */
    public QueryProfitActivityRankResponse queryProfitActivityRank(Header header, Long joinDate, Long accountId, Integer limit) {
        if (!BrokerService.checkModule(header, FunctionModule.MARGIN)) {
            return QueryProfitActivityRankResponse.getDefaultInstance();
        }
        if (header.getUserId() > 0 || accountId > 0) {
            accountId = accountService.checkMarginAccountIdNoThrow(header, accountId);
        }
        Example example = new Example(MarginProfitActivity.class);
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("orgId", header.getOrgId());
        criteria.andEqualTo("joinDate", joinDate);
        if (accountId != null && accountId > 0) {
            criteria.andEqualTo("accountId", accountId);
        }
        example.setOrderByClause("day_ranking ASC,profit_rate DESC");
        List<MarginProfitActivity> activities = marginProfitActivityMapper.selectByExampleAndRowBounds(example, new RowBounds(0, limit));

        return QueryProfitActivityRankResponse.newBuilder()
                .addAllData(activities.stream().map(this::getProfitActivity).collect(Collectors.toList()))
                .build();
    }

    /**
     * 管理端查询收益活动记录
     *
     * @param header
     * @param beginDate
     * @param endDate
     * @param joinStatus
     * @param accountId
     * @param limit
     * @return
     */
    public AdminQueryProfitActivityResponse adminQueryProfitActivity(Header header, Long beginDate, Long endDate, Integer joinStatus, Long fromId, Long accountId, Integer limit) {
        if (!BrokerService.checkModule(header, FunctionModule.MARGIN)) {
            return AdminQueryProfitActivityResponse.getDefaultInstance();
        }
        if (header.getUserId() > 0 || accountId > 0) {
            accountId = accountService.checkMarginAccountIdNoThrow(header, accountId);
        }
        Example example = new Example(MarginProfitActivity.class);
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("orgId", header.getOrgId());
        if (beginDate > 0) {
            criteria.andGreaterThanOrEqualTo("joinDate", beginDate);
        }
        if (endDate > 0) {
            criteria.andLessThanOrEqualTo("joinDate", endDate);
        }
        if (accountId != null && accountId > 0) {
            criteria.andEqualTo("accountId", accountId);
        }
        if (joinStatus > 0) {
            criteria.andEqualTo("joinStatus", joinStatus);
        }
        if (fromId > 0) {
            criteria.andLessThan("id", fromId);
        }
        example.setOrderByClause("id");
        List<MarginProfitActivity> activities = marginProfitActivityMapper.selectByExampleAndRowBounds(example, new RowBounds(0, limit));
        return AdminQueryProfitActivityResponse.newBuilder()
                .addAllData(activities.stream().map(this::getProfitActivity).collect(Collectors.toList()))
                .build();
    }

    /**
     * 检查上一日提交的收益活动用户资产是否满足
     */
    public void sortProfitActivity(Header header) {
        Broker broker = brokerMapper.getByOrgIdAndStatus(header.getOrgId());
        if (broker == null) {
            return;
        }
        String key = String.format(SORT_MARGIN_PROFIT_ORG_LOCK_KEY, header.getOrgId());
        boolean lock = false;
        try {
            lock = RedisLockUtils.tryLock(redisTemplate, key, 60 * 60 * 1000);
            if (lock) {
                CompletableFuture.runAsync(() -> {
                    try {
                        sortProfitActivityByOrgId(header.getOrgId());
                    } catch (Exception e) {
                        log.error(" sortProfitActivity error", e);
                    }
                }, taskExecutor);
            }
        } finally {
            if (lock) {
                RedisLockUtils.releaseLock(redisTemplate, key);
            }
        }

    }

    /**
     * 上一日收益率进行排序
     *
     * @param orgId
     */
    @Transactional
    void sortProfitActivityByOrgId(Long orgId) {
        long date = DateUtil.startOfDay(System.currentTimeMillis());
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        Long joinDate = Long.parseLong(sdf.format(date));
        //获取上一日的申请记录
        List<MarginProfitActivity> activities = marginProfitActivityMapper.queryProfitRecordToCheck(orgId, joinDate);
        Set<Long> accountIds = activities.stream().map(MarginProfitActivity::getAccountId).collect(Collectors.toSet());
        int ranking = 0;
        for (MarginProfitActivity activity : activities) {
            try {
                activity.setDayRanking(++ranking);
                marginProfitActivityMapper.updateByPrimaryKeySelective(activity);
            } catch (Exception e) {
                log.error("update profit margin account balance error orgId:{}  accountId:{} {}", orgId, accountIds, e.getMessage(), e);
            }
        }

    }

    /**
     * 管理端重置收益前几的本轮收益
     *
     * @param header
     * @param joinDate
     * @param top
     */
    public void adminRecalculationTopProfit(Header header, Long joinDate, Integer top) {
        String key = String.format(ADMIN_RECAL_PROFIT_ORG_LOCK_KEY, header.getOrgId());
        boolean lock = false;
        try {
            lock = RedisLockUtils.tryLock(redisTemplate, key, 60 * 60 * 1000);
            if (!lock) {
                return;
            }
            Long time = System.currentTimeMillis();
            if (joinDate == 0) {
                TimeZone timeZone = TimeZone.getTimeZone("GMT+8:00");
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
                simpleDateFormat.setTimeZone(timeZone);
                joinDate = Long.parseLong(simpleDateFormat.format(time));
            }
            if (top == 0) {
                top = 20;
            }
            //获取排名记录
            List<MarginProfitActivity> activities = marginProfitActivityMapper.queryTopProfitRecord(header.getOrgId(), joinDate, top);
            CompletableFuture.runAsync(() -> {
                for (MarginProfitActivity activity : activities) {
                    try {
                        recalculationRoundProfit(Header.newBuilder().setOrgId(header.getOrgId()).setUserId(activity.getUserId()).build(), true);
                    } catch (Exception e) {
                        log.error("adminRecalculationTopProfit recalculationRoundProfit error", e);
                    }
                }
            }, taskExecutor);
        } catch (Exception e) {
            log.warn("adminRecalculationTopProfit error ", e);
        } finally {
            if (lock) {
                RedisLockUtils.releaseLock(redisTemplate, key);
            }
        }
    }

    /**
     * 管理员修改排名
     *
     * @param header
     * @param joinDate
     * @param ranking
     * @return
     */
    public AdminSetProfitRankingResponse adminSetProfitRanking(Header header, Long joinDate, Integer ranking) {
        marginProfitActivityMapper.updateRanking(header.getOrgId(), joinDate, header.getUserId(), ranking);
        return AdminSetProfitRankingResponse.getDefaultInstance();
    }

    public ProfitActivity getProfitActivity(MarginProfitActivity activity) {
        return ProfitActivity.newBuilder()
                .setId(activity.getId())
                .setOrgId(activity.getOrgId())
                .setJoinDate(activity.getJoinDate())
                .setUserId(activity.getUserId())
                .setAccountId(activity.getAccountId())
                .setSubmitTime(activity.getSubmitTime())
                .setKycLevel(activity.getKycLevel())
                .setProfitRate(activity.getProfitRate().stripTrailingZeros().toPlainString())
                .setAllPositionUsdt(activity.getAllPositionUsdt().stripTrailingZeros().toPlainString())
                .setTodayPositionUsdt(activity.getTodayPositionUsdt().stripTrailingZeros().toPlainString())
                .setJoinStatus(activity.getJoinStatus())
                .setDayRanking(activity.getDayRanking())
                .setUpdates(activity.getUpdates())
                .setUpdated(activity.getUpdated())
                .setCreated(activity.getCreated())
                .build();
    }
}

