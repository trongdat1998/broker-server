package io.bhex.broker.server.grpc.server.service;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import io.bhex.base.DateUtil;
import io.bhex.base.idgen.api.ISequenceGenerator;
import io.bhex.base.margin.AddMarginRiskBlackListRequest;
import io.bhex.base.margin.DelMarginRiskBlackListRequest;
import io.bhex.base.margin.DeleteAccountLoanLimitVIPRequest;
import io.bhex.base.margin.ForceCloseRequest;
import io.bhex.base.margin.GetInterestConfigRequest;
import io.bhex.base.margin.GetRiskConfigRequest;
import io.bhex.base.margin.GetTokenConfigRequest;
import io.bhex.base.margin.InterestConfig;
import io.bhex.base.margin.QueryAccountLoanLimitVIPRequest;
import io.bhex.base.margin.QueryInterestByLevelRequest;
import io.bhex.base.margin.QueryMarginRiskBlackListRequest;
import io.bhex.base.margin.SetAccountLoanLimitVIPRequest;
import io.bhex.base.margin.SetInterestConfigRequest;
import io.bhex.base.margin.SetRiskConfigRequest;
import io.bhex.base.margin.SetTokenConfigRequest;
import io.bhex.base.margin.*;
import io.bhex.base.margin.cross.CrossLoanPosition;
import io.bhex.base.margin.cross.LoanOrderStatusEnum;
import io.bhex.base.margin.cross.*;
import io.bhex.base.proto.BaseRequest;
import io.bhex.base.proto.DecimalUtil;
import io.bhex.base.quote.Rate;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.common.util.JsonUtil;
import io.bhex.broker.grpc.account.Balance;
import io.bhex.broker.grpc.basic.Symbol;
import io.bhex.broker.grpc.common.Header;
import io.bhex.broker.grpc.margin.CrossLoanOrder;
import io.bhex.broker.grpc.margin.ForceRecord;
import io.bhex.broker.grpc.margin.FundingCross;
import io.bhex.broker.grpc.margin.RiskConfig;
import io.bhex.broker.grpc.margin.RiskSum;
import io.bhex.broker.grpc.margin.SpecialLoanLimit;
import io.bhex.broker.grpc.margin.TokenConfig;
import io.bhex.broker.grpc.margin.UserRisk;
import io.bhex.broker.grpc.margin.*;
import io.bhex.broker.grpc.user.SaveUserContractRequest;
import io.bhex.broker.grpc.user.level.ListUserLevelConfigsRequest;
import io.bhex.broker.grpc.user.level.UserLevelConfigObj;
import io.bhex.broker.server.domain.*;
import io.bhex.broker.server.grpc.client.service.GrpcMarginService;
import io.bhex.broker.server.grpc.client.service.GrpcQuoteService;
import io.bhex.broker.server.model.MarginLoanLimit;
import io.bhex.broker.server.model.MarginUserLoanLimit;
import io.bhex.broker.server.model.RptMarginPool;
import io.bhex.broker.server.model.RptMarginTrade;
import io.bhex.broker.server.model.RptMarginTradeDetail;
import io.bhex.broker.server.model.SpecialInterest;
import io.bhex.broker.server.model.*;
import io.bhex.broker.server.model.staking.StakingBalanceSnapshot;
import io.bhex.broker.server.primary.mapper.*;
import io.bhex.broker.server.statistics.statistics.mapper.StatisticsBalanceSnapshotMapper;
import io.bhex.broker.server.statistics.statistics.mapper.StatisticsOdsMapper;
import io.bhex.broker.server.util.RedisLockUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.ibatis.session.RowBounds;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.task.TaskExecutor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tk.mybatis.mapper.entity.Example;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author JinYuYuan
 * @description
 * @date 2020-06-09 13:48
 */
@Slf4j
@Service
public class MarginService {
    private final static String default_lang = "en_US";

    @Resource
    GrpcMarginService grpcMarginService;

    @Resource
    StatisticsOdsMapper statisticsOdsMapper;

    @Resource
    AccountService accountService;

    @Resource
    AccountMapper accountMapper;

    @Resource
    BasicService basicService;

    @Resource(name = "asyncTaskExecutor")
    private TaskExecutor taskExecutor;

    @Resource
    private UserService userService;

    @Autowired
    private SymbolMapper symbolMapper;

    @Resource
    private BaseBizConfigService baseBizConfigService;

    @Resource
    private UserLevelMapper userLevelMapper;

    @Resource
    UserLevelService userLevelService;

    @Resource
    UserSecurityService userSecurityService;

    @Resource
    private SpecialInterestMapper specialInterestMapper;

    //计算&强平黑名单
    public static final String MARGIN_RISK_CALCULATION = "margin.risk.calculation";

    @Resource(name = "stringRedisTemplate")
    private StringRedisTemplate redisTemplate;

    @Resource
    private RptMarginPoolMapper rptMarginPoolMapper;

    @Resource
    private OrgStatisticsService orgStatisticsService;

    @Resource
    private GrpcQuoteService grpcQuoteService;

    @Resource
    private RptMarginTradeMapper rptMarginTradeMapper;

    @Resource
    private RptMarginTradeDetailMapper rptMarginTradeDetailMapper;

    @Resource
    private BrokerMapper brokerMapper;

    @Resource
    private MarginActivityMapper marginActivityMapper;

    @Resource
    private MarginActivityLocalMapper marginActivityLocalMapper;

    public static final String CAL_MARGIN_POOL_LOCK_KEY = "cal_margin_pool_lock_%s";

    //key = orgid
    public static final String CAL_MARGIN_TRADE_LOCK_KEY = "cal_margin_trade_lock_key_%s";

    //key = orgid
    private static final String MARGIN_STATISTICS_RISK_SAVE_KEY = "margin_statistics_risk_save_key_orgid_%s";

    //key = orgid
    private static final String MARGIN_DAILY_STATISTICS_RISK_KEY = "margin_daily_statistics_risk_key_%s";

    //key = orgid
    private static final String OPEN_MARGIN_ACTIVITY_CHECK_LOCK = "open_marin_activity_check_lock_%s";

    private static final String OPEN_MARGIN_ACTIVITY_MONTH_CHECK_LOCK = "open_marin_activity_month_check_lock_%s";

    //key = orgid
    private static final String SUMBIT_OPEN_MARGIN_ACTIVITY_LOCK = "sumbit_open_marin_activity_lock_%s";

    @Resource
    private MarginDailyRiskStatisticsMapper marginDailyRiskStatisticsMapper;

    @Resource
    private RptMarginTradeDetailBatchMapper rptMarginTradeDetailBatchMapper;

    @Resource
    private ISequenceGenerator sequenceGenerator;

    @Resource
    private MarginOpenActivityMapper marginOpenActivityMapper;

    @Resource
    private UserVerifyMapper userVerifyMapper;

    @Resource
    private StatisticsBalanceSnapshotMapper statisticsBalanceSnapshotMapper;

    @Resource
    private MarginLoanLimitMapper marginLoanLimitMapper;

    @Resource
    private MarginUserLoanLimitMapper marginUserLoanLimitMapper;

    @Resource
    MarginDailyOpenActivityMapper marginDailyOpenActivityMapper;


    @Scheduled(cron = "0 0/30 * * * ? ")
    public void calRptMarginPool() {
        if (!BasicService.marginOrgIds.isEmpty()) {
            for (Long orgId : BasicService.marginOrgIds) {
                String key = String.format(CAL_MARGIN_POOL_LOCK_KEY, orgId);
                boolean lock = false;
                try {
                    lock = RedisLockUtils.tryLock(redisTemplate, key, 30 * 60 * 1000);
                    if (!lock) {
                        continue;
                    }
                    log.info("{} start calculation margin pool rpt", orgId);
                    //查询借币记录
                    int size = 500;
                    int limit = 500;
                    Long toOrderId = 1L;
                    List<io.bhex.base.margin.cross.CrossLoanOrder> orders = new ArrayList<>();
                    //获取所有的借币订单 todo 需做优化，业务量大时这里会比较耗性能
                    while (size >= 500) {
                        CrossLoanOrderListRequest request = CrossLoanOrderListRequest.newBuilder()
                                .setBaseRequest(BaseRequest.newBuilder().setOrganizationId(orgId).build())
                                // .setStatus(LoanOrderStatusEnum.LOAN_ORDER_ACTIVE_VALUE)
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
                                .max((o1, o2) -> o1.compareTo(o2)).get();
                        orders.addAll(crossLoanOrders);
                    }
                    FundingCrossRequest request = FundingCrossRequest.newBuilder()
                            .setBaseRequest(BaseRequest.newBuilder().setOrganizationId(orgId).build())
                            .build();
                    FundingCrossReply reply = grpcMarginService.getFundingCross(request);
                    Map<String, RptMarginPool> marginPoolMap = reply.getFundingCrossList().stream()
                            .map(data -> {
                                RptMarginPool pool = new RptMarginPool();
                                pool.setTokenId(data.getTokenId());
                                pool.setOrgId(orgId);
                                pool.setAvailable(DecimalUtil.toBigDecimal(data.getAvailable()));
                                pool.setUnpaidAmount(BigDecimal.ZERO);
                                pool.setInterestUnpaid(BigDecimal.ZERO);
                                pool.setInterestPaid(BigDecimal.ZERO);
                                return pool;
                            }).collect(Collectors.toMap(RptMarginPool::getTokenId, rptMarginPool -> rptMarginPool, (p, q) -> {
                                p.setAvailable(p.getAvailable().add(q.getAvailable()));
                                return p;
                            }));
                    for (io.bhex.base.margin.cross.CrossLoanOrder order : orders) {
                        if (marginPoolMap.containsKey(order.getTokenId())) {
                            RptMarginPool pool = marginPoolMap.get(order.getTokenId());
                            pool.setUnpaidAmount(DecimalUtil.toBigDecimal(order.getUnpaidAmount()).add(pool.getUnpaidAmount()));
                            pool.setInterestUnpaid(DecimalUtil.toBigDecimal(order.getInterestUnpaid()).add(pool.getInterestUnpaid()));
                            pool.setInterestPaid(DecimalUtil.toBigDecimal(order.getInterestPaid()).add(pool.getInterestPaid()));
                        } else {
                            RptMarginPool pool = new RptMarginPool();
                            pool.setTokenId(order.getTokenId());
                            pool.setOrgId(orgId);
                            pool.setAvailable(BigDecimal.ZERO);
                            pool.setUnpaidAmount(DecimalUtil.toBigDecimal(order.getUnpaidAmount()));
                            pool.setInterestUnpaid(DecimalUtil.toBigDecimal(order.getInterestUnpaid()));
                            pool.setInterestPaid(DecimalUtil.toBigDecimal(order.getInterestPaid()));
                            marginPoolMap.put(order.getTokenId(), pool);
                        }
                    }
                    for (Map.Entry<String, RptMarginPool> entry : marginPoolMap.entrySet()) {
                        RptMarginPool pool = entry.getValue();
                        RptMarginPool oldRecord = rptMarginPoolMapper.getMarginPoolRecord(pool.getOrgId(), pool.getTokenId());
                        if (oldRecord == null) {
                            pool.setCreated(System.currentTimeMillis());
                            pool.setUpdated(System.currentTimeMillis());
                            rptMarginPoolMapper.insertSelective(pool);
                        } else {
                            pool.setId(oldRecord.getId());
                            pool.setUpdated(System.currentTimeMillis());
                            rptMarginPoolMapper.updateByPrimaryKeySelective(pool);
                        }
                    }
                } catch (Exception e) {
                    log.warn("calRptMarginPool error :{}", e.getMessage(), e);
                } finally {
                    if (lock) {
                        RedisLockUtils.releaseLock(redisTemplate, key);
                    }
                }
            }
        }
    }


    @Scheduled(cron = "1 0,30,59 0 1/1 * ?")
    public void dailyStatisticsRisk() {
        if (!BasicService.marginOrgIds.isEmpty()) {

            long endDate = DateUtil.startOfDay(System.currentTimeMillis());
            long date = endDate - 86400_000L;

            for (Long orgId : BasicService.marginOrgIds) {

                Broker broker = brokerMapper.getByOrgIdAndStatus(orgId);
                if (broker == null) {
                    continue;
                }

                boolean lock = RedisLockUtils.tryLock(redisTemplate, String.format(MARGIN_DAILY_STATISTICS_RISK_KEY, orgId), 10 * 60 * 1000);
                if (!lock) {
                    continue;
                }
                try {
                    log.info("{} start dailyStatisticsRisk", orgId);

                    MarginDailyRiskStatistics record = marginDailyRiskStatisticsMapper.getByOrgIdAndDate(orgId, date);
                    if (record != null) { //已存在记录则跳过
                        continue;
                    }

                    Rate rate = basicService.getV3Rate(orgId, "BTC");
                    //获取btcusdt汇率
                    BigDecimal rateValue = rate == null ? BigDecimal.ZERO : DecimalUtil.toBigDecimal(rate.getRatesMap().get("USDT"));

                    SummaryRiskRequest request = SummaryRiskRequest.newBuilder()
                            .setBaseRequest(BaseRequest.newBuilder().setOrganizationId(orgId).build())
                            .build();
                    SummaryRiskReply reply = grpcMarginService.summaryRisk(request);

                    MarginDailyRiskStatistics data = new MarginDailyRiskStatistics();
                    data.setOrgId(orgId);
                    data.setDate(date);
                    data.setAverageSafety(new BigDecimal(reply.getRiskSum().getAverageSafety()));
                    data.setLoanValue(new BigDecimal(reply.getRiskSum().getLoanValue()));
                    data.setUsdtLoanValue(new BigDecimal(reply.getRiskSum().getLoanValue()).multiply(rateValue).setScale(8, RoundingMode.DOWN));
                    data.setAllValue(new BigDecimal(reply.getRiskSum().getAllValue()));
                    data.setUsdtAllValue(new BigDecimal(reply.getRiskSum().getAllValue()).multiply(rateValue).setScale(8, RoundingMode.DOWN));
                    data.setUserNum(reply.getRiskSum().getUserNum());

                    //借贷笔数
                    int orderSize = 0;
                    //借贷人数
                    int userSize = 0;
                    //查询借币记录  todo 需优化，提供统计接口，目前的方式业务量大时效率会很差
                    int size = 500;
                    int limit = 500;
                    Long toOrderId = 1L;
                    List<io.bhex.base.margin.cross.CrossLoanOrder> orders = new ArrayList<>();
                    //获取所有的借币未还订单
                    while (size >= 500) {
                        CrossLoanOrderListRequest loanOrderListRequest = CrossLoanOrderListRequest.newBuilder()
                                .setBaseRequest(BaseRequest.newBuilder().setOrganizationId(data.getOrgId()).build())
                                .setStatus(LoanOrderStatusEnum.LOAN_ORDER_ACTIVE_VALUE)
                                .setToOrderId(toOrderId)
                                .setLimit(limit)
                                .build();
                        CrossLoanOrderListReply response = grpcMarginService.getCrossLoanOrder(loanOrderListRequest);
                        List<io.bhex.base.margin.cross.CrossLoanOrder> crossLoanOrders = response.getCrossLoanOrdersList();
                        size = crossLoanOrders.size();
                        if (size == 0) {
                            break;
                        }
                        toOrderId = crossLoanOrders.stream().map(io.bhex.base.margin.cross.CrossLoanOrder::getLoanOrderId)
                                .max((o1, o2) -> o1.compareTo(o2)).get();
                        orders.addAll(crossLoanOrders);
                    }

                    orderSize = orders.size();
//                    todaySize = (int) orders.stream().filter(order -> order.getCreatedAt() >= date && order.getCreatedAt() < endDate).count();
                    userSize = orders.stream().collect(Collectors.groupingBy(io.bhex.base.margin.cross.CrossLoanOrder::getAccountId)).size();

                    data.setLoanUserNum(userSize);
                    data.setLoanOrderNum(orderSize);
//                    data.setTodayLoanOrderNum(todaySize);
                    data.setTodayPayNum(0);//todo 后续提供

                    //查询借币记录  todo 需优化，提供统计接口，目前的方式业务量大时效率会很差
                    size = 500;
                    limit = 500;
                    Long fromOrderId = 0L;
                    List<io.bhex.base.margin.cross.CrossLoanOrder> allOrders = new ArrayList<>();
                    //存在借币创建时间小于date时结束
                    boolean stopflag = false;
                    //获取所有的借币未还订单
                    while (size >= 500 && !stopflag) {
                        CrossLoanOrderListRequest loanOrderListRequest = CrossLoanOrderListRequest.newBuilder()
                                .setBaseRequest(BaseRequest.newBuilder().setOrganizationId(data.getOrgId()).build())
                                .setFromOrderId(fromOrderId)
                                .setLimit(limit)
                                .build();
                        CrossLoanOrderListReply response = grpcMarginService.getCrossLoanOrder(loanOrderListRequest);
                        List<io.bhex.base.margin.cross.CrossLoanOrder> crossLoanOrders = response.getCrossLoanOrdersList();
                        size = crossLoanOrders.size();
                        if (size == 0) {
                            break;
                        }
                        for (io.bhex.base.margin.cross.CrossLoanOrder order : response.getCrossLoanOrdersList()) {
                            if (fromOrderId == 0 || fromOrderId > order.getLoanOrderId()) {
                                fromOrderId = order.getLoanOrderId();
                            }
                            if (order.getCreatedAt() >= date && order.getCreatedAt() < endDate) {
                                allOrders.add(order);
                            }
                            //存在小于date的记录，结束查询
                            if (order.getCreatedAt() < date) {
                                stopflag = true;
                            }
                        }
                    }
                    //当日借贷笔数
                    int todaySize = allOrders.size();
                    //当日借贷人数
                    int todayUserSize = allOrders.stream().collect(Collectors.groupingBy(io.bhex.base.margin.cross.CrossLoanOrder::getAccountId)).size();
                    data.setTodayLoanOrderNum(todaySize);
                    data.setTodayLoanUserNum(todayUserSize);
                    data.setCreated(System.currentTimeMillis());
                    data.setUpdated(System.currentTimeMillis());

                    marginDailyRiskStatisticsMapper.insertSelective(data);

                } catch (Exception e) {
                    log.error("dailyStatisticsRisk summaryRisk error orgId:{} {}", orgId, e.getMessage(), e);
                } finally {
                    RedisLockUtils.releaseLock(redisTemplate, String.format(MARGIN_DAILY_STATISTICS_RISK_KEY, orgId));
                }
            }


        }
    }

    @Scheduled(cron = "0 40 0/1 * * ?")
    public void calMarginTradeRpt() {

        if (!BasicService.marginOrgIds.isEmpty()) {
            for (Long orgId : BasicService.marginOrgIds) {

                Broker broker = brokerMapper.getByOrgIdAndStatus(orgId);
                if (broker == null) {
                    continue;
                }

                boolean lock = RedisLockUtils.tryLock(redisTemplate, String.format(CAL_MARGIN_TRADE_LOCK_KEY, orgId), 10 * 60 * 1000);
                if (!lock) {
                    return;
                }

                try {
                    //获取开始时间戳
                    Long time = System.currentTimeMillis();
                    Calendar cal = Calendar.getInstance();
                    cal.setTimeInMillis(time);
                    cal.set(Calendar.HOUR_OF_DAY, 0);
                    cal.set(Calendar.MINUTE, 0);
                    cal.set(Calendar.SECOND, 0);
                    cal.set(Calendar.MILLISECOND, 0);
                    cal.add(Calendar.DATE, -1);

                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                    String yesterday = sdf.format(cal.getTime());
                    Long date = cal.getTimeInMillis();

                    calMarginTradeRptDeal(orgId, date, yesterday);
                } catch (Exception e) {
                    log.error("calMarginTradeRpt error orgId:{} {}", orgId, e.getMessage(), e);
                } finally {
                    RedisLockUtils.releaseLock(redisTemplate, String.format(CAL_MARGIN_TRADE_LOCK_KEY, orgId));
                }
            }
        }

    }

    @Transactional(rollbackFor = Exception.class)
    public void calMarginTradeRptDeal(Long orgId, Long date, String yesterday) {
        Example example = new Example(RptMarginTrade.class);
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("orgId", orgId);
        criteria.andEqualTo("createTime", date);
        //当前日期已计算，不在进行统计
        List<RptMarginTrade> marginTrades = rptMarginTradeMapper.selectByExample(example);
        if (marginTrades.size() > 0) {
            return;
        }

        if (null == statisticsOdsMapper.getOneTradeSummaryDayByOrgId(yesterday, orgId)) {
            log.info("{} ,{} calculation margin trade rpt has no summary data", orgId, yesterday);
            return;
        }

        log.info("{} start calculation margin trade rpt", orgId);

        //记录编号，关联详情表记录
        Long rptId = sequenceGenerator.getLong();
        //获取杠杆aid
        List<Account> accounts = accountMapper.queryAccountByType(orgId, AccountType.MARGIN.value());

        List<Long> accountIds = new ArrayList<>();
        List<StatisticsAccountTradeSummaryDay> accountTradeSummaryDayList = new ArrayList<>();
        for (Account account : accounts) {
            accountIds.add(account.getAccountId());

            if (accountIds.size() >= 500) {
                List<StatisticsAccountTradeSummaryDay> list = statisticsOdsMapper.queryTradeSummaryDayByAccountIds(yesterday, orgId, accountIds);
                accountTradeSummaryDayList.addAll(list);
                accountIds.clear();
            }
        }

        if (accountIds.size() > 0) {
            List<StatisticsAccountTradeSummaryDay> list = statisticsOdsMapper.queryTradeSummaryDayByAccountIds(yesterday, orgId, accountIds);
            accountTradeSummaryDayList.addAll(list);
        }

        //统计成交报表明细
        Map<String, RptMarginTradeDetail> marginTradeDetailMap = new HashMap<>();

        //总交易账户集合
        Set<Long> tradeAccountIdSet = new HashSet<>();
        //买入交易账户集合
        Set<Long> buyTradeAccountIdSet = new HashSet<>();
        //卖出交易账户集合
        Set<Long> sellTradeAccountIdSet = new HashSet<>();


        Map<String, Set<Long>> symbolBuyAccountIdMap = new HashMap<>();

        Map<String, Set<Long>> symbolSellAccountIdMap = new HashMap<>();

        for (StatisticsAccountTradeSummaryDay data : accountTradeSummaryDayList) {
            RptMarginTradeDetail detail;

            Set<Long> symbolBuyAccountIdSet = symbolBuyAccountIdMap.get(data.getSymbolId());
            if (symbolBuyAccountIdSet == null) {
                symbolBuyAccountIdSet = new HashSet<>();
                symbolBuyAccountIdMap.put(data.getSymbolId(), symbolBuyAccountIdSet);
            }

            Set<Long> symbolSellAccountIdSet = symbolSellAccountIdMap.get(data.getSymbolId());
            if (symbolSellAccountIdSet == null) {
                symbolSellAccountIdSet = new HashSet<>();
                symbolSellAccountIdMap.put(data.getSymbolId(), symbolSellAccountIdSet);
            }

            if (marginTradeDetailMap.containsKey(data.getSymbolId())) {
                detail = marginTradeDetailMap.get(data.getSymbolId());
            } else {
                detail = new RptMarginTradeDetail();
                detail.setRelationId(rptId);
                detail.setOrgId(orgId);
                detail.setSymbolId(data.getSymbolId());
                detail.setTradePeopleNum(0L);
                detail.setBuyPeopleNum(0L);
                detail.setSellPeopleNum(0L);
                detail.setTradeNum(0L);
                detail.setBuyTradeNum(0L);
                detail.setSellTradeNum(0L);
                detail.setBaseFee(BigDecimal.ZERO);
                detail.setQuoteFee(BigDecimal.ZERO);
                detail.setFee(BigDecimal.ZERO);
                detail.setAmount(BigDecimal.ZERO);

                marginTradeDetailMap.put(detail.getSymbolId(), detail);
            }

            //交易总笔数
            detail.setTradeNum(detail.getTradeNum() + data.getBuyOrderNum() + data.getSellOrderNum());

            tradeAccountIdSet.add(data.getAccountId());

            //买入笔数
            detail.setBuyTradeNum(detail.getBuyTradeNum() + data.getBuyOrderNum());
            if (data.getBuyOrderNum() > 0) {//有买入交易数量
                symbolBuyAccountIdSet.add(data.getAccountId());

                buyTradeAccountIdSet.add(data.getAccountId());
            }
            //基础币手续费 不做计算
            //detail.setBaseFee(detail.getBaseFee().add(data.getBuyFeeTotal()));

            //卖出笔数
            detail.setSellTradeNum(detail.getSellTradeNum() + data.getSellOrderNum());
            if (data.getSellOrderNum() > 0) { //有卖出交易数量
                symbolSellAccountIdSet.add(data.getAccountId());

                sellTradeAccountIdSet.add(data.getAccountId());
            }
            //计价币手续费 不做计算
            //detail.setQuoteFee(detail.getQuoteFee().add(data.getSellFeeTotal()));

            //手续费折合USDT
            detail.setFee(detail.getFee().add(data.getBuyFeeTotalUsdt()).add(data.getSellFeeTotalUsdt()));
            //交易额折合USDT
            detail.setAmount(detail.getAmount().add(data.getBuyAmountUsdt()).add(data.getSellAmountUsdt()));

        }

        RptMarginTrade marginTrade = new RptMarginTrade();
        marginTrade.setCreateTime(date);
        marginTrade.setOrgId(orgId);
        marginTrade.setId(rptId);
        marginTrade.setTradePeopleNum((long) tradeAccountIdSet.size());
        marginTrade.setBuyPeopleNum((long) buyTradeAccountIdSet.size());
        marginTrade.setSellPeopleNum((long) sellTradeAccountIdSet.size());

        List<RptMarginTradeDetail> detailList = new ArrayList<>();

        for (Map.Entry<String, RptMarginTradeDetail> entry : marginTradeDetailMap.entrySet()) {
            RptMarginTradeDetail detail = entry.getValue();
            detail.setCreated(System.currentTimeMillis());

            Set<Long> symbolBuyAccountIdSet = symbolBuyAccountIdMap.get(detail.getSymbolId());
            detail.setBuyPeopleNum(symbolBuyAccountIdSet == null ? 0L : symbolBuyAccountIdSet.size());

            Set<Long> symbolSellAccountIdSet = symbolSellAccountIdMap.get(detail.getSymbolId());
            detail.setSellPeopleNum(symbolSellAccountIdSet == null ? 0L : symbolSellAccountIdSet.size());
            //交易总人数
            symbolBuyAccountIdSet.addAll(symbolSellAccountIdSet);
            detail.setTradePeopleNum((long) symbolBuyAccountIdSet.size());

            detailList.add(detail);

            marginTrade.setTradeNum(marginTrade.getTradeNum() + detail.getTradeNum());
            marginTrade.setBuyTradeNum(marginTrade.getBuyTradeNum() + detail.getBuyTradeNum());
            marginTrade.setSellTradeNum(marginTrade.getSellTradeNum() + detail.getSellTradeNum());
            marginTrade.setFee(marginTrade.getFee().add(detail.getFee()));
            marginTrade.setAmount(marginTrade.getAmount().add(detail.getAmount()));
            //添加数据库，保存记录

            if (detailList.size() > 2000) {
                rptMarginTradeDetailBatchMapper.insertList(detailList);
                detailList.clear();
            }
        }
        if (detailList.size() > 0) {
            rptMarginTradeDetailBatchMapper.insertList(detailList);
            detailList.clear();
        }
        //添加数据库，保存记录
        rptMarginTradeMapper.insertSelective(marginTrade);
    }

    /**
     * 检查参加杠杆开户活动的用户资产是否满足要求. --活动结束已停用
     *
     * @param header
     */
    public void checkOpenMarginActivityBalance(Header header) {
        Broker broker = brokerMapper.getByOrgIdAndStatus(header.getOrgId());
        if (broker == null) {
            return;
        }
        CompletableFuture.runAsync(() -> {
            String key = String.format(OPEN_MARGIN_ACTIVITY_CHECK_LOCK, header.getOrgId());
            boolean lock = false;
            try {
                lock = RedisLockUtils.tryLock(redisTemplate, key, 30 * 60 * 1000);
                if (lock) {
                    checkOpenMarginActivityBalanceByOrgId(header.getOrgId(), 2, new BigDecimal("500"));
                }
            } catch (Exception e) {
                log.error("checkOpenMarginActivityBalanceByOrgId orgId:{} error {}", header.getOrgId(), e.getMessage(), e);
            } finally {
                if (lock) {
                    RedisLockUtils.releaseLock(redisTemplate, key);
                }
            }
        }, taskExecutor);

    }

    /**
     * 检查用户是否满足月抽奖条件. --活动结束已停用
     *
     * @param header
     */
    public void checkMonthOpenMarginActivityBalance(Header header) {
        Broker broker = brokerMapper.getByOrgIdAndStatus(header.getOrgId());
        if (broker == null) {
            return;
        }
        CompletableFuture.runAsync(() -> {
            String key = String.format(OPEN_MARGIN_ACTIVITY_MONTH_CHECK_LOCK, header.getOrgId());
            boolean lock = false;
            try {
                lock = RedisLockUtils.tryLock(redisTemplate, key, 30 * 60 * 1000);
                if (lock) {
                    checkOpenMarginActivityBalanceByOrgId(header.getOrgId(), 3, new BigDecimal("2000"));
                }
            } catch (Exception e) {
                log.error("checkOpenMarginActivityBalanceByOrgId orgId:{} error {}", header.getOrgId(), e.getMessage(), e);
            } finally {
                if (lock) {
                    RedisLockUtils.releaseLock(redisTemplate, key);
                }
            }
        }, taskExecutor);
    }

    /**
     * 检查资产是否满足要求
     *
     * @param orgId
     * @param targetJoinStatus
     * @param checkBalaceFlag
     */
    @Transactional
    public void checkOpenMarginActivityBalanceByOrgId(Long orgId, Integer targetJoinStatus, BigDecimal checkBalaceFlag) {
        Map<String, BigDecimal> rateMap = new HashMap<>();
        long endDate = DateUtil.startOfDay(System.currentTimeMillis());
        long date = endDate - 86400_000L;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        String yesterday = sdf.format(date);
        List<MarginOpenActivity> activities = new ArrayList<>();
        if (targetJoinStatus == 2) {
            //查询上一日报名的用户
            activities = marginOpenActivityMapper.queryMarginOpenActivityByTime(orgId, date, endDate);
        } else {
            //获取所有满足日抽奖的用户
            activities = marginOpenActivityMapper.queryJoinDayMarginOpenActivity(orgId);
        }
        if (activities.isEmpty()) {
            return;
        }
        Set<Long> accountIds = activities.stream().map(MarginOpenActivity::getAccountId).collect(Collectors.toSet());
        //获取资产快照
        List<StakingBalanceSnapshot> snapshots = statisticsBalanceSnapshotMapper.queryBalanceSnapshotByAccountIds(yesterday, accountIds);
        Map<Long, List<StakingBalanceSnapshot>> accountSnapshot = snapshots.stream().collect(Collectors.groupingBy(StakingBalanceSnapshot::getAccountId));
        for (Map.Entry<Long, List<StakingBalanceSnapshot>> entry : accountSnapshot.entrySet()) {
            try {
                Long accountId = entry.getKey();
                List<StakingBalanceSnapshot> balanceSnapshots = entry.getValue();
                //获取总资产
                BigDecimal allPosi = openMarinActivityCalAllPosi(orgId, balanceSnapshots, rateMap);
                if (allPosi.compareTo(checkBalaceFlag) < 0) {
                    continue;
                }
                MarginOpenActivity activity = marginOpenActivityMapper.getMarginOpenActivityByAid(orgId, accountId);
                //参与日抽奖或月抽奖
                activity.setJoinStatus(targetJoinStatus);
                if (targetJoinStatus == 2) {
                    //记录当日资产
                    activity.setTodayPositionUsdt(allPosi);
                } else {
                    //统计月底资产
                    activity.setMonthPositionUsdt(allPosi);
                }
                activity.setTodayPositionUsdt(allPosi);
                activity.setUpdated(System.currentTimeMillis());
                marginOpenActivityMapper.updateByPrimaryKeySelective(activity);
            } catch (Exception e) {
                log.error("check open margin account balance error orgId:{}  accountId:{} {}", orgId, accountIds, e.getMessage(), e);
            }
        }
    }

    public BigDecimal openMarinActivityCalAllPosi(Long orgId, List<StakingBalanceSnapshot> balanceSnapshots, Map<String, BigDecimal> rateMap) {
        BigDecimal allPosi = BigDecimal.ZERO;
        //资产折合为USDT
        for (StakingBalanceSnapshot balance : balanceSnapshots) {
            BigDecimal price = null;
            if (rateMap.containsKey(balance.getTokenId())) {
                price = rateMap.get(balance.getTokenId());
            } else {
                Rate rate = basicService.getV3Rate(orgId, balance.getTokenId());
                if (rate == null || Stream.of(AccountService.TEST_TOKENS)
                        .anyMatch(testToken -> testToken.equalsIgnoreCase(balance.getTokenId()))) {
                    continue;
                }
                price = DecimalUtil.toBigDecimal(rate.getRatesMap().get("USDT"));
                rateMap.put(balance.getTokenId(), price);
            }
            allPosi = allPosi.add(balance.getTotal().multiply(price));
        }
        return allPosi;
    }

    //获取币种对USDT的收盘价
//    private BigDecimal getUsdtClosePrice(Long orgId, String tokenId, Long startTime, Long endTime, Map<String, BigDecimal> priceMap) {
//        if (tokenId.equals("USDT")) {
//            return BigDecimal.ONE;
//        } else if (priceMap.containsKey(tokenId)) {
//            return priceMap.get(tokenId);
//        } else {
//            Symbol symbol = basicService.getOrgSymbol(orgId, tokenId + "USDT");
//            if (symbol == null) {
//                return BigDecimal.ZERO;
//            }
//            GetKLineRequest request = GetKLineRequest.newBuilder()
//                    .setBaseRequest(BaseReqUtil.getBaseRequest(orgId))
//                    .setExchangeId(symbol.getExchangeId())
//                    .setSymbol(symbol.getSymbolId())
//                    .setStartTime(startTime - 1)
//                    .setEndTime(endTime - 1)
//                    .setInterval("1d")
//                    .setLimitCount(1)
//                    .build();
//            GetKLineReply klineReply = grpcQuoteService.getKline(request);
//            if (klineReply.getKlineCount() <= 0) {
//                return BigDecimal.ZERO;
//            }
//            priceMap.put(tokenId, DecimalUtil.toBigDecimal(klineReply.getKline(0).getClose()));
//            return priceMap.get(tokenId);
//        }
//    }

    public List<RiskConfig> getRiskConfig(Header header) {
        List<RiskConfig> list = Lists.newArrayList();
        if (header.getOrgId() != 0L && !BrokerService.checkModule(header, FunctionModule.MARGIN)) {
            return list;
        }
        GetRiskConfigRequest request = GetRiskConfigRequest.newBuilder()
                .setBaseRequest(BaseRequest.newBuilder().setOrganizationId(header.getOrgId()).build()).build();
        GetRiskConfigReply result = grpcMarginService.getRisk(request);
        if (result.getRet() == 0) {
            io.bhex.base.margin.RiskConfig config = result.getRiskConfig();
            io.bhex.broker.grpc.margin.RiskConfig risk = RiskConfig.newBuilder()
                    .setOrgId(config.getOrgId())
                    .setWithdrawLine(DecimalUtil.toTrimString(config.getWithdrawLine()))
                    .setWarnLine(DecimalUtil.toTrimString(config.getWarnLine()))
                    .setAppendLine(DecimalUtil.toTrimString(config.getAppendLine()))
                    .setStopLine(DecimalUtil.toTrimString(config.getStopLine()))
                    .setMaxLoanLimiit(DecimalUtil.toTrimString(config.getMaxLoanLimit()))
                    .setNotifyType(config.getNotifyType())
                    .setNotifyNumber(config.getNotifyNumber())
                    .setMaxLoanLimitVip1(DecimalUtil.toTrimString(config.getMaxLoanLimitVip1()))
                    .setMaxLoanLimitVip2(DecimalUtil.toTrimString(config.getMaxLoanLimitVip2()))
                    .setMaxLoanLimitVip3(DecimalUtil.toTrimString(config.getMaxLoanLimitVip3()))
                    .build();
            list.add(risk);
        }
        return list;
    }

    public List<TokenConfig> getTokenConfig(Header header, String tokenId) {
        if (header.getOrgId() != 0L && !BrokerService.checkModule(header, FunctionModule.MARGIN)) {
            return new ArrayList<>();
        }
        GetTokenConfigRequest request = GetTokenConfigRequest.newBuilder()
                .setBaseRequest(BaseRequest.newBuilder().setOrganizationId(header.getOrgId()).build())
                .setTokenId(tokenId)
                .build();
        GetTokenConfigReply response = grpcMarginService.getToken(request);
        return response.getTokenConfigList().stream().map(this::getTokenConfigResult).collect(
                Collectors.toList());
    }

    private TokenConfig getTokenConfigResult(io.bhex.base.margin.TokenConfig tokenConfig) {
        return TokenConfig.newBuilder()
                .setOrgId(tokenConfig.getOrgId())
                .setExchangeId(tokenConfig.getExchangeId())
                .setTokenId(tokenConfig.getTokenId())
                .setConvertRate(DecimalUtil.toTrimString(tokenConfig.getConvertRate()))
                .setLeverage(tokenConfig.getLeverage())
                .setCanBorrow(tokenConfig.getCanBorrow())
                .setMaxQuantity(DecimalUtil.toTrimString(tokenConfig.getMaxQuantity()))
                .setMinQuantity(DecimalUtil.toTrimString(tokenConfig.getMinQuantity()))
                .setQuantityPrecision(tokenConfig.getQuantityPrecision())
                .setRepayMinQuantity(DecimalUtil.toTrimString(tokenConfig.getRepayMinQuantity()))
                .setCreated(tokenConfig.getCreated())
                .setUpdated(tokenConfig.getUpdated())
                .setIsOpen(tokenConfig.getIsOpen())
                .setShowInterestPeriod(tokenConfig.getShowInterestPrecision())
                .build();
    }

    public List<io.bhex.broker.grpc.margin.InterestConfig> getInterestConfig(Header header, String tokenId) {
        if (header.getOrgId() != 0L && !BrokerService.checkModule(header, FunctionModule.MARGIN)) {
            return new ArrayList<>();
        }
        GetInterestConfigRequest request = GetInterestConfigRequest.newBuilder()
                .setBaseRequest(BaseRequest.newBuilder().setOrganizationId(header.getOrgId()).build())
                .setTokenId(tokenId)
                .build();
        GetInterestConfigReply response = grpcMarginService.getInterest(request);
        return response.getInterestConfigList().stream().map(this::getInterestConfigResult).collect(
                Collectors.toList());
    }

    private io.bhex.broker.grpc.margin.InterestConfig getInterestConfigResult(io.bhex.base.margin.InterestConfig
                                                                                      interestConfig) {
        return io.bhex.broker.grpc.margin.InterestConfig.newBuilder()
                .setOrgId(interestConfig.getOrgId())
                .setTokenId(interestConfig.getTokenId())
                .setInterest(DecimalUtil.toTrimString(interestConfig.getInterest()))
                .setInterestPeriod(interestConfig.getInterestPeriod())
                .setCalculationPeriod(interestConfig.getCalculationPeriod())
                .setSettlementPeriod(interestConfig.getSettlementPeriod())
                .setShowInterest(DecimalUtil.toBigDecimal(interestConfig.getShowInterest()).stripTrailingZeros().toPlainString())
                .setCreated(interestConfig.getCreated())
                .setUpdated(interestConfig.getUpdated())
                .build();
    }

    public void setRiskConfig(Header header, String withdrawLine, String warnLine, String appendLine, String
            stopLine, String maxLoanLimit,
                              Integer notifyType, String notifyNumber, String maxLoanLimitVip1, String maxLoanLimitVip2, String
                                      maxLoanLimitVip3) {
        if (!BrokerService.checkModule(header, FunctionModule.MARGIN)) {
            return;
        }
        if (notifyNumber.length() > 128) {
            throw new BrokerException(BrokerErrorCode.PARAM_ERROR);
        }
        BigDecimal withdraw = new BigDecimal(withdrawLine);
        BigDecimal warn = new BigDecimal(warnLine);
        BigDecimal append = new BigDecimal(appendLine);
        BigDecimal stop = new BigDecimal(stopLine);
        BigDecimal loanLimit = new BigDecimal(maxLoanLimit);
        BigDecimal loanLimitVip1 = new BigDecimal(maxLoanLimitVip1);
        BigDecimal loanLimitVip2 = new BigDecimal(maxLoanLimitVip2);
        BigDecimal loanLimitVip3 = new BigDecimal(maxLoanLimitVip3);
        if (withdraw.compareTo(new BigDecimal(2)) < 0
                || warn.compareTo(BigDecimal.ONE) < 0
                || append.compareTo(BigDecimal.ONE) < 0
                || stop.compareTo(BigDecimal.ONE) < 0
                || withdraw.compareTo(warn) < 0
                || warn.compareTo(append) < 0
                || append.compareTo(stop) < 0
                || loanLimit.compareTo(BigDecimal.ZERO) < 0
                || loanLimitVip1.compareTo(BigDecimal.ZERO) < 0
                || loanLimitVip2.compareTo(BigDecimal.ZERO) < 0
                || loanLimitVip3.compareTo(BigDecimal.ZERO) < 0) {
            log.warn("set risk config error orgId:{} withdrawLine:{} warnLine:{} appendLine:{} stopLine:{}  loanLimit:{}",
                    header.getOrgId(), withdrawLine, warnLine, appendLine, stopLine, loanLimit);
            throw new BrokerException(BrokerErrorCode.PARAM_ERROR);
        }
//        List<RiskConfig> curRisk = basicService.getOrgMarginRiskConfig(header.getOrgId());
        SetRiskConfigRequest request = SetRiskConfigRequest.newBuilder()
                .setBaseRequest(BaseRequest.newBuilder().setOrganizationId(header.getOrgId()).build())
                .setWithdrawLine(DecimalUtil.fromBigDecimal(new BigDecimal(withdrawLine)))
                .setWarnLine(DecimalUtil.fromBigDecimal(new BigDecimal(warnLine)))
                .setAppendLine(DecimalUtil.fromBigDecimal(new BigDecimal(appendLine)))
                .setStopLine(DecimalUtil.fromBigDecimal(new BigDecimal(stopLine)))
                .setMaxLoanLimit(DecimalUtil.fromBigDecimal(new BigDecimal(maxLoanLimit)))
                .setNotifyType(notifyType)
                .setNotifyNumber(notifyNumber)
                .setMaxLoanLimitVip1(DecimalUtil.fromBigDecimal(new BigDecimal(maxLoanLimitVip1)))
                .setMaxLoanLimitVip2(DecimalUtil.fromBigDecimal(new BigDecimal(maxLoanLimitVip2)))
                .setMaxLoanLimitVip3(DecimalUtil.fromBigDecimal(new BigDecimal(maxLoanLimitVip3)))
                .build();
        grpcMarginService.setRisk(request);
        //首次配置分控时开通 杠杆子账户。
        /*if (curRisk.isEmpty()) {
            openDefaultMarginAccount(header.getOrgId());
        }*/
    }

    public void setTokenConfig(Header header, String tokenId, String convertRate, int leverage, Boolean
            canBorrow, String maxQuantity, String minQuantity,
                               int quantityPrecision, String repayMinQuantity, int isOpen, int show_interest_precision) {
        if (!BrokerService.checkModule(header, FunctionModule.MARGIN)) {
            return;
        }
        if (leverage <= 0 || StringUtils.isEmpty(convertRate)) {
            throw new BrokerException(BrokerErrorCode.PARAM_ERROR);
        }
        BigDecimal convert = new BigDecimal(convertRate);
        if (convert.compareTo(BigDecimal.ZERO) <= 0) {
            throw new BrokerException(BrokerErrorCode.PARAM_ERROR);
        }
        BigDecimal maxQty = StringUtils.isEmpty(maxQuantity) ? BigDecimal.ZERO : new BigDecimal(maxQuantity);
        BigDecimal minQty = StringUtils.isEmpty(minQuantity) ? BigDecimal.ZERO : new BigDecimal(minQuantity);
        if (minQty.compareTo(BigDecimal.ZERO) < 0 || maxQty.compareTo(BigDecimal.ZERO) < 0) {
            throw new BrokerException(BrokerErrorCode.PARAM_ERROR);
        }
        if (minQty.compareTo(maxQty) > 0) {
            throw new BrokerException(BrokerErrorCode.PARAM_ERROR);
        }
        repayMinQuantity = StringUtils.isEmpty(repayMinQuantity) ? "0" : repayMinQuantity;
        if (new BigDecimal(repayMinQuantity).compareTo(BigDecimal.ZERO) <= 0) {
            throw new BrokerException(BrokerErrorCode.PARAM_ERROR);
        }
        Token token = basicService.getOrgTokenDetail(header.getOrgId(), tokenId);
        if (token == null) {
            throw new BrokerException(BrokerErrorCode.TOKEN_NOT_FOUND);
        }
        if (isOpen != 1) {
            canBorrow = false;
        }
        SetTokenConfigRequest request = SetTokenConfigRequest.newBuilder()
                .setBaseRequest(BaseRequest.newBuilder().setOrganizationId(header.getOrgId()).build())
                .setTokenId(tokenId)
                .setConvertRate(DecimalUtil.fromBigDecimal(convert))
                .setLeverage(leverage)
                .setCanBorrow(canBorrow)
                .setMaxQuantity(DecimalUtil.fromBigDecimal(maxQty))
                .setMinQuantity(DecimalUtil.fromBigDecimal(minQty))
                .setQuantityPrecision(quantityPrecision)
                .setExchangeId(token.getExchangeId())
                .setRepayMinQuantity(DecimalUtil.fromBigDecimal(new BigDecimal(repayMinQuantity)))
                .setIsOpen(isOpen)
                .setShowInterestPrecision(show_interest_precision)
                .build();
        grpcMarginService.setToken(request);
    }

    public void setInterestConfig(Header header, String tokenId, String interest, int interestPeriod,
                                  int calculationPeriod, int settlementPeriod) {
        if (!BrokerService.checkModule(header, FunctionModule.MARGIN)) {
            return;
        }
        interest = StringUtils.isEmpty(interest) ? "0" : interest;
        if (new BigDecimal(interest).compareTo(BigDecimal.ZERO) < 0) {
            throw new BrokerException(BrokerErrorCode.PARAM_ERROR);
        }
        SetInterestConfigRequest request = SetInterestConfigRequest.newBuilder()
                .setBaseRequest(BaseRequest.newBuilder().setOrganizationId(header.getOrgId()).build())
                .setTokenId(tokenId)
                .setInterest(DecimalUtil.fromBigDecimal(new BigDecimal(interest).divide(new BigDecimal(86400 / interestPeriod), 18, RoundingMode.DOWN)))
                .setInterestPeriod(interestPeriod)
                .setCalculationPeriod(calculationPeriod)
                .setSettlementPeriod(settlementPeriod)
                .setShowInterest(DecimalUtil.fromBigDecimal(new BigDecimal(interest)))
                .build();
        grpcMarginService.setInterest(request);
    }

    public void setMarginSymbol(Header header, String symbolId, Boolean allowMargin) {
        if (!BrokerService.checkModule(header, FunctionModule.MARGIN)) {
            return;
        }
        Symbol symbol = basicService.getOrgSymbol(header.getOrgId(), symbolId);
        if (symbol == null) {
            throw new BrokerException(BrokerErrorCode.MARGIN_SYMBOL_NOT_FOUND);
        }
        if (allowMargin && (basicService.getOrgMarginToken(header.getOrgId(), symbol.getBaseTokenId()) == null
                || basicService.getOrgMarginToken(header.getOrgId(), symbol.getQuoteTokenId()) == null)) {
            log.error("set Margin Symbol error with {}  baseToken or quoteToken is not margin token ", symbolId);
            throw new BrokerException(BrokerErrorCode.PARAM_INVALID);
        }
        SetSymbolConfigRequest setSymbolConfigRequest = SetSymbolConfigRequest.newBuilder()
                .setBaseRequest(BaseRequest.newBuilder().setOrganizationId(header.getOrgId()).build())
                .setExchangeId(symbol.getExchangeId())
                .setSymbolId(symbolId)
                .setAllowTrade(allowMargin ? 1 : 2)
                .setMinTradeQuantity(DecimalUtil.fromBigDecimal(new BigDecimal(symbol.getMinTradeQuantity())))
                .setMinTradeAmount(DecimalUtil.fromBigDecimal(new BigDecimal(symbol.getMinTradeAmount())))
                .setMinPricePrecision(DecimalUtil.fromBigDecimal(new BigDecimal(symbol.getMinPricePrecision())))
                .setBasePrecision(DecimalUtil.fromBigDecimal(new BigDecimal(symbol.getBasePrecision())))
                .setQuotePrecision(DecimalUtil.fromBigDecimal(new BigDecimal(symbol.getQuotePrecision())))
                .build();
        grpcMarginService.setSymbolConfig(setSymbolConfigRequest);
        symbolMapper.allowMargin(symbol.getExchangeId(), symbolId, allowMargin ? 1 : 0, header.getOrgId());
    }

    public PoolAccount getPoolAccount(Header header) {
        if (!BrokerService.checkModule(header, FunctionModule.MARGIN)) {
            return PoolAccount.newBuilder()
                    .build();
        }
        // 获取币池可用可借
        FundingCrossRequest request = FundingCrossRequest.newBuilder()
                .setBaseRequest(BaseRequest.newBuilder().setOrganizationId(header.getOrgId()).build())
                .build();
        FundingCrossReply reply = grpcMarginService.getFundingCross(request);
        List<PoolAccountTokenInfo> tokenInfoList = reply.getFundingCrossList()
                .stream()
                .map(cross -> PoolAccountTokenInfo.newBuilder()
                        .setTokenId(cross.getTokenId())
                        .setLoanable(DecimalUtil.toTrimString(cross.getAvailable()))
                        .setBorrowed(DecimalUtil.toTrimString(DecimalUtil.toBigDecimal(cross.getTotal()).subtract(DecimalUtil.toBigDecimal(cross.getAvailable()))))
                        .build())
                .collect(Collectors.toList());
        return PoolAccount.newBuilder()
                .setOrgId(header.getOrgId())
//                .setAccountId((Long.valueOf(accountId + "")))
//                .setAccountName(account.getAccountName())
//                .setAccountType(account.getAccountType())
//                .setAccountIndex(account.getAccountIndex())
//                .setAuthorizedOrg(account.getIsAuthorizedOrg() == 0 ? false : true)
                .addAllToken(tokenInfoList)
                .build();
    }

    public List<CrossLoanOrder> getCrossLoanOrder(Header header, Long accountId, String tokenId,
                                                  Long loanId, Integer status, Long fromOrderId,
                                                  Long toOrderId, Integer limit) {
        if (!BrokerService.checkModule(header, FunctionModule.MARGIN)) {
            return new ArrayList<>();
        }
        if (header.getUserId() > 0 || accountId > 0) {
            accountId = accountService.checkMarginAccountIdNoThrow(header, accountId);
            if (accountId == null) {
                return new ArrayList<>();
            }
        }

        CrossLoanOrderListRequest request = CrossLoanOrderListRequest.newBuilder()
                .setBaseRequest(BaseRequest.newBuilder().setOrganizationId(header.getOrgId()).build())
                .setAccountId(accountId)
                .setTokenId(tokenId)
                .setLoanOrderId(loanId)
                .setStatus(status)
                .setFromOrderId(fromOrderId)
                .setToOrderId(toOrderId)
                .setLimit(limit)
                .build();
        log.info("getCrossLoanOrder:{}", request);
        CrossLoanOrderListReply response = grpcMarginService.getCrossLoanOrder(request);
        return response.getCrossLoanOrdersList().stream().map(this::getCrossLoanOrderResult).collect(
                Collectors.toList());
    }

    private CrossLoanOrder getCrossLoanOrderResult(io.bhex.base.margin.cross.CrossLoanOrder crossLoanOrder) {
        return CrossLoanOrder.newBuilder()
                .setLoanOrderId(crossLoanOrder.getLoanOrderId())
                .setClientId(crossLoanOrder.getClientId())
                .setOrgId(crossLoanOrder.getOrgId())
                .setBalanceId(crossLoanOrder.getBalanceId())
                .setLenderAccountId(crossLoanOrder.getLenderAccountId())
                .setTokenId(crossLoanOrder.getTokenId())
                .setLoanAmount(DecimalUtil.toTrimString(crossLoanOrder.getLoanAmount()))
                .setRepaidAmount(DecimalUtil.toTrimString(crossLoanOrder.getRepaidAmount()))
                .setUnpaidAmount(DecimalUtil.toTrimString(crossLoanOrder.getUnpaidAmount()))
                .setInterestRate1(DecimalUtil.toTrimString(crossLoanOrder.getInterestRate1()))
                .setInterestStart(crossLoanOrder.getInterestStart1())
                .setStatus(crossLoanOrder.getStatus())
                .setInterestPaid(DecimalUtil.toTrimString(crossLoanOrder.getInterestPaid()))
                .setInterestUnpaid(DecimalUtil.toTrimString(crossLoanOrder.getInterestUnpaid()))
                .setCreatedAt(crossLoanOrder.getCreatedAt())
                .setUpdatedAt(crossLoanOrder.getUpdatedAt())
                .setAccountId(crossLoanOrder.getAccountId())
                .build();
    }

    public List<RepayRecord> getRepayRecord(Header header, Long accountId, String tokenId,
                                            Long loanOrderId, Long fromOrderId, Long toOrderId,
                                            Integer limit) {
        if (!BrokerService.checkModule(header, FunctionModule.MARGIN)) {
            return new ArrayList<>();
        }
        accountId = accountService.checkMarginAccountIdNoThrow(header, accountId);
        if (accountId == null) {
            return new ArrayList<>();
        }
        RepayOrderListRequest request = RepayOrderListRequest.newBuilder()
                .setBaseRequest(BaseRequest.newBuilder().setOrganizationId(header.getOrgId()).build())
                .setAccountId(accountId)
                .setLoanOrderId(loanOrderId)
                .setFromOrderId(fromOrderId)
                .setToOrderId(toOrderId)
                .setTokenId(tokenId)
                .setLimit(limit)
                .build();
        RepayOrderListReply response = grpcMarginService.getRepayRecord(request);
        return response.getRepayOrdersList().stream().map(this::getRepayRecordResult).collect(
                Collectors.toList());
    }

    private RepayRecord getRepayRecordResult(RepayOrder repayOrder) {
        return RepayRecord.newBuilder()
                .setRepayOrderId(repayOrder.getRepayOrderId())
                .setOrgId(repayOrder.getOrgId())
                .setTokenId(repayOrder.getTokenId())
                .setAccountId(repayOrder.getAccountId())
                .setClientId(repayOrder.getClientId())
                .setBalanceId(repayOrder.getBalanceId())
                .setLoanOrderId(repayOrder.getLoanOrderId())
                .setAmount(DecimalUtil.toTrimString(repayOrder.getAmount()))
                .setInterest(DecimalUtil.toTrimString(repayOrder.getInterest()))
                .setCreatedAt(repayOrder.getCreatedAt())
                .build();
    }

    public MarginSafety getMarginSafety(Header header, Long accountId) {
        if (!BrokerService.checkModule(header, FunctionModule.MARGIN)) {
            return MarginSafety.newBuilder().build();
        }
        accountId = accountService.checkMarginAccountIdNoThrow(header, accountId);
        if (accountId == null) {
            return MarginSafety.newBuilder().build();
        }

        GetSafetyRequest request = GetSafetyRequest.newBuilder()
                .setBaseRequest(BaseRequest.newBuilder().setOrganizationId(header.getOrgId()).build())
                .setAccountId(accountId)
                .build();
        GetSafetyReply response = grpcMarginService.getMarginSafety(request);
        return MarginSafety.newBuilder()
                .setOrgId(header.getOrgId())
                .setAccountId(accountId)
                .setSafety(DecimalUtil.toTrimString(response.getSaftety() == null ? BigDecimal.ZERO : DecimalUtil.toBigDecimal(response.getSaftety())))
                .build();
    }

    public List<MarginAsset> getMarginAsset(Header header, Integer accountIndex, String tokenIds) {
        if (!BrokerService.checkModule(header, FunctionModule.MARGIN)) {
            return new ArrayList<>();
        }
        Long accountId = accountService.checkMarginAccountIdNoThrow(header, 0L);
        if (accountId == null) {
            return new ArrayList<>();
        }
        /*if (Account.getAccountType().compareTo(AccountType.MARGIN.value()) != 0) {
            throw new BrokerException(BrokerErrorCode.ACCOUNT_NOT_EXIST);
        }*/
        List<MarginAsset> marginAssetList = Lists.newArrayList();

        List<String> tokenList = Lists.newArrayList(tokenIds.split(",")).stream().filter(s -> !Strings.isNullOrEmpty(s)).collect(Collectors.toList());
        List<Balance> balanceList = accountService.queryBalance(header, accountId, tokenList);
        CrossLoanPositionRequest request = CrossLoanPositionRequest.newBuilder()
                .setBaseRequest(BaseRequest.newBuilder().setOrganizationId(header.getOrgId()).build())
                .setAccountId(accountId)
                .build();

        CrossLoanPositionReply response = grpcMarginService.getCrossLoanPosition(request);
        List<CrossLoanPosition> loanPositionList = response.getCrossLoanPositionList();
        Map<String, CrossLoanPosition> positionMap = new ConcurrentHashMap<>();
        for (CrossLoanPosition position : loanPositionList) {
            if (tokenList.isEmpty() || tokenList.contains(position.getTokenId())) {
                positionMap.put(position.getTokenId(), position);
            }
        }
        getMarginAsset(marginAssetList, balanceList, positionMap);
        /*if (tokenList.size() > 0) {
            //如果指定查询tokenIds，则只返回指定的数据
            getMarginAsset(marginAssetList, balanceList, positionMap);
        } else {
            getMarginAsset(marginAssetList, balanceList, positionMap);
            // 遍历已借的map，如果有资产队列里不存在的数据，需要新增到队列中
            for (String key : positionMap.keySet()) {
                Boolean isFind = false;
                for (MarginAsset marginAsset : marginAssetList) {
                    if (key.compareToIgnoreCase(marginAsset.getTokenId()) == 0) {
                        isFind = true;
                        break;
                    }
                }
                if (isFind == false) {
                    CrossLoanPosition position = positionMap.get(key);
                    String tokenName = basicService.getTokenName(position.getOrgId(), position.getTokenId());
                    MarginAsset marginAsset = MarginAsset.newBuilder()
                            .setTokenId(position.getTokenId())
                            .setTokenName(tokenName)
                            .setTokenFullName(tokenName)
                            .setTotal("0")
                            .setFree("0")
                            .setLocked("0")
                            .setBtcValue("0")
                            .setLoanAmount(DecimalUtil.toTrimString(position.getLoanTotal()))
                            .build();
                    marginAssetList.add(marginAsset);
                }
            }
        }*/
        return marginAssetList;
    }

    private void getMarginAsset(List<MarginAsset> marginAssetList, List<Balance> balanceList,
                                Map<String, CrossLoanPosition> positionMap) {
        Set<String> tokenOld = new HashSet<>();
        for (Balance balance : balanceList) {
            CrossLoanPosition position = positionMap.get(balance.getTokenId());
            tokenOld.add(balance.getTokenId());
            if (position != null) {
                MarginAsset marginAsset = MarginAsset.newBuilder()
                        .setTokenId(balance.getTokenId())
                        .setTokenName(balance.getTokenName())
                        .setTokenFullName(balance.getTokenName())
                        .setTotal(balance.getTotal())
                        .setFree(balance.getFree())
                        .setLocked(balance.getLocked())
                        .setBtcValue(new BigDecimal(balance.getBtcValue()).setScale(8, BigDecimal.ROUND_DOWN).stripTrailingZeros().toPlainString())
                        .setLoanAmount(DecimalUtil.toTrimString(position.getLoanTotal()))
                        .setUsdtValue(new BigDecimal(balance.getUsdtValue()).setScale(2, BigDecimal.ROUND_DOWN).stripTrailingZeros().toPlainString())
                        .build();
                marginAssetList.add(marginAsset);
            } else {
                MarginAsset marginAsset = MarginAsset.newBuilder()
                        .setTokenId(balance.getTokenId())
                        .setTokenName(balance.getTokenName())
                        .setTokenFullName(balance.getTokenName())
                        .setTotal(balance.getTotal())
                        .setFree(balance.getFree())
                        .setLocked(balance.getLocked())
                        .setBtcValue(new BigDecimal(balance.getBtcValue()).setScale(8, BigDecimal.ROUND_DOWN).stripTrailingZeros().toPlainString())
                        .setUsdtValue(new BigDecimal(balance.getUsdtValue()).setScale(2, BigDecimal.ROUND_DOWN).stripTrailingZeros().toPlainString())
                        .setLoanAmount("0")
                        .build();
                marginAssetList.add(marginAsset);
            }
        }
        positionMap.forEach((k, v) -> {
            if (!tokenOld.contains(k) && DecimalUtil.toBigDecimal(v.getLoanTotal()).compareTo(BigDecimal.ZERO) > 0) {
                String tokenName = basicService.getTokenName(v.getOrgId(), v.getTokenId());
                MarginAsset marginAsset = MarginAsset.newBuilder()
                        .setTokenId(v.getTokenId())
                        .setTokenName(tokenName)
                        .setTokenFullName(tokenName)
                        .setTotal("0")
                        .setFree("0")
                        .setLocked("0")
                        .setBtcValue("0")
                        .setUsdtValue("0")
                        .setLoanAmount(DecimalUtil.toTrimString(v.getLoanTotal()))
                        .build();
                marginAssetList.add(marginAsset);
            }
        });
    }

    public List<FundingCross> getFundingCross(Header header, Long accountId, String tokenId) {
        if (!BrokerService.checkModule(header, FunctionModule.MARGIN)) {
            return new ArrayList<>();
        }
        accountId = accountService.checkMarginAccountIdNoThrow(header, accountId);
        if (accountId == null) {
            return new ArrayList<>();
        }

        List<FundingCross> fundingCrossList = Lists.newArrayList();
        CrossLoanPositionRequest request = CrossLoanPositionRequest.newBuilder()
                .setBaseRequest(BaseRequest.newBuilder().setOrganizationId(header.getOrgId()).build())
                .setAccountId(accountId)
                .setTokenId(tokenId)
                .build();
        CrossLoanPositionReply response = grpcMarginService.getCrossLoanPosition(request);
        BigDecimal borrowed = BigDecimal.ZERO;
        if (!response.getCrossLoanPositionList().isEmpty()) {
            borrowed = DecimalUtil.toBigDecimal(response.getCrossLoanPosition(0).getLoanTotal());
        }
        GetAvailableAmountRequest request1 = GetAvailableAmountRequest.newBuilder()
                .setBaseRequest(BaseRequest.newBuilder().setOrganizationId(header.getOrgId()).build())
                .setAccountId(accountId)
                .setTokenId(tokenId)
                .setOrgId(header.getOrgId())
                .build();
        GetAvailableAmountReply response1 = grpcMarginService.getAvailableAmount(request1);
        BigDecimal availableAmount = DecimalUtil.toBigDecimal(response1.getAvailableAmount());

        //获取币种借币剩余上限
        BigDecimal remainLoanLimit = this.getRemainTokenLoanLimit(header.getOrgId(), header.getUserId(), accountId, tokenId, availableAmount);
        BigDecimal loanable = remainLoanLimit.compareTo(availableAmount) == -1 ? remainLoanLimit : availableAmount;

        FundingCross fundingCross = FundingCross.newBuilder()
                .setTokenId(tokenId)
                .setAccountId(accountId)
                .setOrgId(header.getOrgId())
                .setBorrowed(borrowed.stripTrailingZeros().toPlainString())
                .setLoanable(loanable.stripTrailingZeros().toPlainString())
                .build();
        fundingCrossList.add(fundingCross);

        return fundingCrossList;
    }

    public List<CoinPool> queryCoinPool(Header header) {
        if (!BrokerService.checkModule(header, FunctionModule.MARGIN)) {
            return new ArrayList<>();
        }
        FundingCrossRequest request = FundingCrossRequest.newBuilder()
                .setBaseRequest(BaseRequest.newBuilder().setOrganizationId(header.getOrgId()).build())
                .build();
        FundingCrossReply reply = grpcMarginService.getFundingCross(request);
        BigDecimal hundred = new BigDecimal("100");
        List<CoinPool> coinPoolList = reply.getFundingCrossList()
                .stream()
                .map(cross -> {
//                    log.info("queryCoinPool :{}", JsonUtil.defaultGson().toJson(cross));
                    return CoinPool.newBuilder()
                            .setTokenId(cross.getTokenId())
                            .setTotal(DecimalUtil.toTrimString(cross.getTotal()))
                            .setBorrowed(DecimalUtil.toTrimString(DecimalUtil.toBigDecimal(cross.getTotal()).subtract(DecimalUtil.toBigDecimal(cross.getAvailable()))))
                            // 借出比例保留6位小数
                            .setRate(DecimalUtil.toTrimString(DecimalUtil.toBigDecimal(cross.getTotal()).subtract(DecimalUtil.toBigDecimal(cross.getAvailable())).divide(DecimalUtil.toBigDecimal(cross.getTotal()), 4, BigDecimal.ROUND_DOWN).multiply(hundred)))
                            .build();
                }).collect(Collectors.toList());
        return coinPoolList;
    }

    public List<UserRisk> queryUserRisk(Header header) {
        if (!BrokerService.checkModule(header, FunctionModule.MARGIN)) {
            return new ArrayList<>();
        }
        GetUserRiskRequest request = GetUserRiskRequest.newBuilder()
                .setBaseRequest(BaseRequest.newBuilder().setOrganizationId(header.getOrgId()).build())
                .build();
        GetUserRiskReply reply = grpcMarginService.queryUserRisk(request);
        List<io.bhex.base.margin.UserRisk> dealingList = reply.getUserRiskList();
        if (dealingList == null || dealingList.isEmpty()) {
            return new ArrayList<>();
        }
        if (reply.getUserRiskList().size() > 100) {
            //前端展示最前面的100条安全度最低的
            dealingList = reply.getUserRiskList().subList(0, 100);
        }
        List<UserRisk> userRiskList = dealingList
                .stream()
                .map(risk -> {
                    //log.info("queryUserRisk :{}", JsonUtil.defaultGson().toJson(risk));
                    Long userId = 0L;
                    try {
                        userId = accountService.getUserIdByAccountId(header.getOrgId(), risk.getAccountId());
                    } catch (Exception e) {
                        log.warn(e.getMessage(), e);
                    }

                    return UserRisk.newBuilder()
                            .setAccountId(risk.getAccountId())
                            .setTotal(risk.getTotal())
                            .setBorrowed(risk.getBorrowed())
                            .setSafety(risk.getSafety())
                            .setUserId(userId)
                            .build();
                }).collect(Collectors.toList());
        return userRiskList;
    }

    public UserRiskSum statisticsUserRisk(Header header) {
        if (!BrokerService.checkModule(header, FunctionModule.MARGIN)) {
            return UserRiskSum.newBuilder().build();
        }
        GetUserRiskRequest request = GetUserRiskRequest.newBuilder()
                .setBaseRequest(BaseRequest.newBuilder().setOrganizationId(header.getOrgId()).build())
                .build();
        GetUserRiskReply reply = grpcMarginService.queryUserRisk(request);
        BigDecimal hundred = new BigDecimal("100");

        // 获取风控配置
        List<RiskConfig> riskConfigList = getRiskConfig(header);
        if (riskConfigList.isEmpty()) {
            return UserRiskSum.newBuilder()
                    .setWarmNum(0)
                    .setAppendNum(0)
                    .setCloseNum(0)
                    .build();
        }
        BigDecimal warnLine = new BigDecimal(riskConfigList.get(0).getWarnLine()).multiply(hundred);
        BigDecimal appendLine = new BigDecimal(riskConfigList.get(0).getAppendLine()).multiply(hundred);
        BigDecimal stopLine = new BigDecimal(riskConfigList.get(0).getStopLine()).multiply(hundred);

        int warmNum = 0;
        int appendNum = 0;
        int closeNum = 0;
        BigDecimal safety;
        // 遍历集合统计人数
        for (io.bhex.base.margin.UserRisk item : reply.getUserRiskList()) {
            safety = new BigDecimal(item.getSafety());
            if (safety.compareTo(stopLine) < 1) {
                // 安全度小于等于止损线，平仓
                closeNum = closeNum + 1;
            } else if (safety.compareTo(appendLine) < 1) {
                // 安全度小于等于追加线，平仓
                appendNum = appendNum + 1;
            } else if (safety.compareTo(warnLine) < 1) {
                // 安全度小于等于预警线
                warmNum = warmNum + 1;
            }
        }
        return UserRiskSum.newBuilder()
                .setWarmNum(warmNum)
                .setAppendNum(appendNum)
                .setCloseNum(closeNum)
                .build();
    }

    public void forceClose(Header header, Long accountId, Long adminUserId, String desc) {
        if (!BrokerService.checkModule(header, FunctionModule.MARGIN)) {
            return;
        }
        Account account = accountMapper.getAccountByAccountId(accountId);
        if (account == null) {
            throw new BrokerException(BrokerErrorCode.ACCOUNT_NOT_EXIST);
        }
        if (account.getAccountType().compareTo(AccountType.MARGIN.value()) != 0) {
            throw new BrokerException(BrokerErrorCode.ACCOUNT_NOT_EXIST);
        }
        ForceCloseRequest request = ForceCloseRequest.newBuilder()
                .setBaseRequest(BaseRequest.newBuilder().setOrganizationId(header.getOrgId()).build())
                .setAccountId(accountId)
                .setAdminUserId(adminUserId)
                .setDesc(desc)
                .build();
        CompletableFuture.runAsync(() -> {
            try {
                grpcMarginService.forceClose(request);
            } catch (Exception e) {
                log.error(" submit forceClose error", e);
            }
        }, taskExecutor);

    }

    public RiskSum statisticsRisk(Header header) {
        if (!BrokerService.checkModule(header, FunctionModule.MARGIN)) {
            return RiskSum.newBuilder().build();
        }

        Rate rate = basicService.getV3Rate(header.getOrgId(), "BTC");
        //获取btcusdt汇率
        BigDecimal rateValue = rate == null ? BigDecimal.ZERO : DecimalUtil.toBigDecimal(rate.getRatesMap().get("USDT"));

        SummaryRiskRequest request = SummaryRiskRequest.newBuilder()
                .setBaseRequest(BaseRequest.newBuilder().setOrganizationId(header.getOrgId()).build())
                .build();
        SummaryRiskReply reply = grpcMarginService.summaryRisk(request);

        String saveValue = redisTemplate.opsForValue().get(String.format(MARGIN_STATISTICS_RISK_SAVE_KEY, header.getOrgId()));

        //借贷笔数
        int orderSize = 0;
        //当日借贷笔数
        int todaySize = 0;
        //借贷人数
        int userSize = 0;
        if (StringUtils.isEmpty(saveValue)) {
            //查询借币记录  todo 需优化，提供统计接口，目前的方式业务量大时效率会很差
            int size = 500;
            int limit = 500;
            Long toOrderId = 1L;
            List<io.bhex.base.margin.cross.CrossLoanOrder> orders = new ArrayList<>();
            //获取所有的借币未还订单
            while (size >= 500) {
                CrossLoanOrderListRequest loanOrderListRequest = CrossLoanOrderListRequest.newBuilder()
                        .setBaseRequest(BaseRequest.newBuilder().setOrganizationId(header.getOrgId()).build())
                        .setStatus(LoanOrderStatusEnum.LOAN_ORDER_ACTIVE_VALUE)
                        .setToOrderId(toOrderId)
                        .setLimit(limit)
                        .build();
                CrossLoanOrderListReply response = grpcMarginService.getCrossLoanOrder(loanOrderListRequest);
                List<io.bhex.base.margin.cross.CrossLoanOrder> crossLoanOrders = response.getCrossLoanOrdersList();
                size = crossLoanOrders.size();
                if (size == 0) {
                    break;
                }
                toOrderId = crossLoanOrders.stream().map(io.bhex.base.margin.cross.CrossLoanOrder::getLoanOrderId)
                        .max((o1, o2) -> o1.compareTo(o2)).get();
                orders.addAll(crossLoanOrders);
            }

            long startOfDay = DateUtil.startOfDay(System.currentTimeMillis());

            orderSize = orders.size();
            todaySize = (int) orders.stream().filter(order -> order.getCreatedAt() >= startOfDay).count();
            userSize = orders.stream().collect(Collectors.groupingBy(io.bhex.base.margin.cross.CrossLoanOrder::getAccountId)).size();

            //有效期1min 值的格式用 ; 拼接，注意顺序
            redisTemplate.opsForValue().set(String.format(MARGIN_STATISTICS_RISK_SAVE_KEY, header.getOrgId()), orderSize + ";" + todaySize + ";" + userSize, 60000L, TimeUnit.MILLISECONDS);
        } else {
            String[] split = saveValue.split(";");
            orderSize = Integer.parseInt(split[0]);
            todaySize = Integer.parseInt(split[1]);
            userSize = Integer.parseInt(split[2]);
        }

        return RiskSum.newBuilder()
                .setUserNum(reply.getRiskSum().getUserNum())
                .setLoanValue(new BigDecimal(reply.getRiskSum().getLoanValue()).multiply(rateValue).setScale(2, RoundingMode.DOWN).stripTrailingZeros().toPlainString())
                .setAllValue(new BigDecimal(reply.getRiskSum().getAllValue()).multiply(rateValue).setScale(2, RoundingMode.DOWN).stripTrailingZeros().toPlainString())
                .setAverageSafety(reply.getRiskSum().getAverageSafety())
                .setLoanOrderNum(orderSize)
                .setTodayLoanOrderNum(todaySize)
                .setLoanUserNum(userSize)
                .build();
    }

    public QueryRptDailyStatisticsRiskResponse queryRptDailyStatisticsRisk(Header header, Long toTime, Integer limit) {
        if (!BrokerService.checkModule(header, FunctionModule.MARGIN)) {
            return QueryRptDailyStatisticsRiskResponse.newBuilder().build();
        }

        Example example = new Example(MarginDailyRiskStatistics.class);
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("orgId", header.getOrgId());

        if (toTime > 0) {
            criteria.andLessThan("date", toTime);
        }

        if (limit < 0) {
            limit = 20;
        }
        if (limit > 100) {
            limit = 100;
        }

        example.orderBy("date").desc();

        List<MarginDailyRiskStatistics> statisticsList = marginDailyRiskStatisticsMapper.selectByExampleAndRowBounds(example, new RowBounds(0, limit));

        List<QueryRptDailyStatisticsRiskResponse.DailyStatisticsRisk> statisticsRisks = statisticsList.stream().map(record -> QueryRptDailyStatisticsRiskResponse.DailyStatisticsRisk.newBuilder().setDate(record.getDate())
                .setAverageSafety(record.getAverageSafety().stripTrailingZeros().toPlainString())
                .setLoanValue(record.getLoanValue().setScale(8, RoundingMode.DOWN).stripTrailingZeros().toPlainString())
                .setUsdtLoanValue(record.getUsdtLoanValue().setScale(2, RoundingMode.DOWN).stripTrailingZeros().toPlainString())
                .setAllValue(record.getAllValue().setScale(8, RoundingMode.DOWN).stripTrailingZeros().toPlainString())
                .setUsdtAllValue(record.getUsdtAllValue().setScale(2, RoundingMode.DOWN).stripTrailingZeros().toPlainString())
                .setUserNum(record.getUserNum())
                .setLoanUserNum(record.getLoanUserNum())
                .setLoanOrderNum(record.getLoanOrderNum())
                .setTodayLoanOrderNum(record.getTodayLoanOrderNum())
                .setTodayPayNum(record.getTodayPayNum())
                .setTodayLoanUserNum(record.getTodayLoanUserNum())
                .build()).collect(Collectors.toList());

        return QueryRptDailyStatisticsRiskResponse.newBuilder().addAllDailyList(statisticsRisks).build();

    }

    public List<PositionDetail> queryPositionDetail(Header header, Long accountId) {
        if (!BrokerService.checkModule(header, FunctionModule.MARGIN)) {
            return new ArrayList<>();
        }
        accountId = accountService.checkMarginAccountIdNoThrow(header, accountId);
        if (accountId == null) {
            return new ArrayList<>();
        }

        List<String> tokenList = Lists.newArrayList("");
        List<Balance> balanceList = accountService.queryBalance(header, accountId, tokenList);
        CrossLoanPositionRequest request = CrossLoanPositionRequest.newBuilder()
                .setBaseRequest(BaseRequest.newBuilder().setOrganizationId(header.getOrgId()).build())
                .setAccountId(accountId)
                .build();

        CrossLoanPositionReply response = grpcMarginService.getCrossLoanPosition(request);
        List<CrossLoanPosition> loanPositionList = response.getCrossLoanPositionList();
        Map<String, CrossLoanPosition> positionMap = new ConcurrentHashMap<>();
        for (CrossLoanPosition position : loanPositionList) {
            positionMap.put(position.getTokenId(), position);
        }

        List<MarginAsset> marginAssetList = Lists.newArrayList();
        getMarginAsset(marginAssetList, balanceList, positionMap);
        // 遍历已借的map，如果有资产队列里不存在的数据，需要新增到队列中
        for (String key : positionMap.keySet()) {
            Boolean isFind = false;
            for (MarginAsset marginAsset : marginAssetList) {
                if (key.compareToIgnoreCase(marginAsset.getTokenId()) == 0) {
                    isFind = true;
                    break;
                }
            }
            if (isFind == false) {
                CrossLoanPosition position = positionMap.get(key);
                String tokenName = basicService.getTokenName(position.getOrgId(), position.getTokenId());
                MarginAsset marginAsset = MarginAsset.newBuilder()
                        .setTokenId(position.getTokenId())
                        .setTokenName(tokenName)
                        .setTokenFullName(tokenName)
                        .setTotal("0")
                        .setFree("0")
                        .setLocked("0")
                        .setBtcValue("0")
                        .setLoanAmount(DecimalUtil.toTrimString(position.getLoanTotal()))
                        .build();
                marginAssetList.add(marginAsset);
            }
        }
        List<PositionDetail> positionDetailList = marginAssetList
                .stream()
                .map(marginAsset -> {
                    log.info("queryPositionDetail :{}", JsonUtil.defaultGson().toJson(marginAsset));
                    return PositionDetail.newBuilder()
                            .setTokenId(marginAsset.getTokenId())
                            .setTotal(marginAsset.getTotal())
                            .setAvailable(marginAsset.getFree())
                            .setLocked(marginAsset.getLocked())
                            .setBorrow(marginAsset.getLoanAmount())
                            .build();
                }).collect(Collectors.toList());

        return positionDetailList;
    }

    public List<ForceClose> queryForceClose(Header header, Long accountId) {
        if (!BrokerService.checkModule(header, FunctionModule.MARGIN)) {
            return new ArrayList<>();
        }
        accountId = accountService.checkMarginAccountId(header, accountId);
        List<ForceClose> forceCloseList = Lists.newArrayList();
        return forceCloseList;
    }

    public GetMarginPositionStatusReply getMarginPositionStatus(Header header, Long accountId) {
        if (!BrokerService.checkModule(header, FunctionModule.MARGIN)) {
            return GetMarginPositionStatusReply.getDefaultInstance();
        }
        GetMarginPositionStatusRequest request = GetMarginPositionStatusRequest.newBuilder()
                .setBaseRequest(BaseRequest.newBuilder().setOrganizationId(header.getOrgId()).build())
                .setAccountId(accountId)
                .build();
        return grpcMarginService.getMarginPositionStatus(request);
    }

    public GetLoanableResponse getLoanable(Header header, Long accountId, String tokenId) {
        if (!BrokerService.checkModule(header, FunctionModule.MARGIN)) {
            return GetLoanableResponse.getDefaultInstance();
        }
        io.bhex.base.margin.TokenConfig token = basicService.getOrgMarginToken(header.getOrgId(), tokenId);
        String loanable = "0";
        if (token != null && token.getCanBorrow()) {
            accountId = accountService.checkMarginAccountId(header, accountId);
            GetAvailableAmountRequest request = GetAvailableAmountRequest.newBuilder()
                    .setBaseRequest(BaseRequest.newBuilder().setOrganizationId(header.getOrgId()).build())
                    .setAccountId(accountId)
                    .setTokenId(tokenId)
                    .setOrgId(header.getOrgId())
                    .build();
            GetAvailableAmountReply response = grpcMarginService.getAvailableAmount(request);
            BigDecimal availableAmount = DecimalUtil.toBigDecimal(response.getAvailableAmount());

            //获取币种借币剩余上限
            BigDecimal remainLoanLimit = this.getRemainTokenLoanLimit(header.getOrgId(), header.getUserId(), accountId, tokenId, availableAmount);
            loanable = DecimalUtil.toTrimString(remainLoanLimit.compareTo(availableAmount) == -1 ? remainLoanLimit : availableAmount);

        }
        return GetLoanableResponse.newBuilder()
                .setAccountId(accountId)
                .setLoanable(loanable)
                .setOrgId(header.getOrgId())
                .setTokenId(tokenId)
                .build();
    }

    @Async
    public void openDefaultMarginAccount(Long orgId) {
        try {
            Long marginAccountId;
            //获取币池账户
            FundingCrossRequest request = FundingCrossRequest.newBuilder()
                    .setBaseRequest(BaseRequest.newBuilder().setOrganizationId(orgId).build())
                    .build();
            FundingCrossReply reply = grpcMarginService.getFundingCross(request);
            if (!reply.getFundingCrossList().isEmpty()) {
                io.bhex.base.margin.cross.FundingCross fundingCross = reply.getFundingCross(0);
                marginAccountId = Long.valueOf(fundingCross.getAccounts().split(",")[0]);
                Long userId = accountService.getUserIdByAccountId(orgId, marginAccountId);
                Long accountId = accountService.getMarginAccountIdNoThrow(orgId, userId, 0);
                //为开通杠杆账户
                if (accountId == null) {
                    SaveUserContractRequest save = SaveUserContractRequest.newBuilder()
                            .setOrgId(orgId)
                            .setUserId(userId)
                            .setOpen(1)
                            .setName("margin").build();
                    userService.saveUserContract(save);
                }
            }
        } catch (Exception e) {

        }
    }

    public GetLevelInterestByUserIdResponse getLevelInterestByUserId(Header header, String tokenId) {
        if (!BrokerService.checkModule(header, FunctionModule.MARGIN)) {
            return GetLevelInterestByUserIdResponse.getDefaultInstance();
        }
        if (!tokenId.isBlank()) {
            Long accountId = accountService.checkMarginAccountIdNoThrow(header, 0L);
            if (accountId != null) {
                //获取特殊利率
                io.bhex.broker.server.model.SpecialInterest specialInterest = specialInterestMapper.getEffectiveSpecialInterest(header.getOrgId(), accountId, tokenId);
                //利率和特殊利率取小值作为借币利率
                if (specialInterest != null) {
                    List<InterestConfig> configs = basicService.getMarginInterestByLevels(header.getOrgId(), tokenId, Collections.singletonList(0L));
                    InterestConfig config = configs.get(0);
                    return GetLevelInterestByUserIdResponse.newBuilder()
                            .setInterestConfig(io.bhex.broker.grpc.margin.InterestConfig.newBuilder()
                                    .setOrgId(config.getOrgId())
                                    .setTokenId(config.getTokenId())
                                    .setInterest(specialInterest.getInterest().compareTo(BigDecimal.ZERO) <= 0 ? "-1" : specialInterest.getInterest().stripTrailingZeros().toPlainString())
                                    .setInterestPeriod(config.getInterestPeriod())
                                    .setCalculationPeriod(config.getCalculationPeriod())
                                    .setSettlementPeriod(config.getSettlementPeriod())
                                    .setShowInterest(specialInterest.getShowInterest().compareTo(BigDecimal.ZERO) <= 0 ? "-1" : specialInterest.getShowInterest().stripTrailingZeros().toPlainString())
                                    .setLevelConfigId(0)
                                    .build())
                            .build();
                }
            }

        }
        //不存在特殊利率。获取对应的等级利率
        GetLevelInterestByUserIdResponse.Builder response = GetLevelInterestByUserIdResponse.newBuilder();
        SwitchStatus switchStatus = baseBizConfigService.getConfigSwitchStatus(header.getOrgId(),
                BaseConfigConstants.USER_LEVEL_CONFIG_GROUP, "open.switch");
        InterestConfig config = null;
        List<UserLevel> userLevels = switchStatus.isOpen() ? userLevelMapper.queryMyAvailableConfigs(header.getOrgId(), header.getUserId()) : Lists.newArrayList();
        if (userLevels.isEmpty()) {//开关未开
            List<InterestConfig> configs = basicService.getMarginInterestByLevels(header.getOrgId(), tokenId, Collections.singletonList(0L));
            if (configs.isEmpty()) {
                //利息未配置。
                throw new BrokerException(BrokerErrorCode.MARGIN_DEFUALT_INTEREST_NOT_EXIST);
            }
            config = configs.get(0);
        } else {
            List<Long> myConfigs = userLevels.stream()
                    .map(UserLevel::getLevelConfigId)
                    .collect(Collectors.toList());
            List<InterestConfig> configs = basicService.getMarginInterestByLevels(header.getOrgId(), tokenId, myConfigs);
            if (configs.isEmpty()) {//对应等级未配置利率，获取默认利率
                configs = basicService.getMarginInterestByLevels(header.getOrgId(), tokenId, Collections.singletonList(0L));
                if (configs.isEmpty()) {
                    //利息未配置。
                    throw new BrokerException(BrokerErrorCode.MARGIN_DEFUALT_INTEREST_NOT_EXIST);
                }
            }
            config = configs.get(0);
            for (InterestConfig interestConfig : configs) {
                if (DecimalUtil.toBigDecimal(interestConfig.getInterest()).compareTo(DecimalUtil.toBigDecimal(config.getInterest())) < 0) {
                    config = interestConfig;
                }
            }
            //获取利率折扣
            BigDecimal orginalDiscount = userLevelService.calculateDiscount(header.getOrgId(), header.getUserId(), AccountType.MARGIN, BigDecimal.ONE);
            BigDecimal interest = DecimalUtil.toBigDecimal(config.getInterest()).compareTo(BigDecimal.ZERO) < 0 ? DecimalUtil.toBigDecimal(config.getInterest()) :
                    DecimalUtil.toBigDecimal(config.getInterest()).multiply(orginalDiscount).stripTrailingZeros();
            BigDecimal showInterest = DecimalUtil.toBigDecimal(config.getShowInterest()).compareTo(BigDecimal.ZERO) < 0 ? DecimalUtil.toBigDecimal(config.getShowInterest()) :
                    DecimalUtil.toBigDecimal(config.getShowInterest()).multiply(orginalDiscount).stripTrailingZeros();
            config = config.toBuilder().setInterest(DecimalUtil.fromBigDecimal(interest)).setShowInterest(DecimalUtil.fromBigDecimal(showInterest)).build();
        }

        return response.setInterestConfig(
                io.bhex.broker.grpc.margin.InterestConfig.newBuilder()
                        .setOrgId(config.getOrgId())
                        .setTokenId(config.getTokenId())
                        .setInterest(DecimalUtil.toBigDecimal(config.getInterest()).stripTrailingZeros().toPlainString())
                        .setInterestPeriod(config.getInterestPeriod())
                        .setCalculationPeriod(config.getCalculationPeriod())
                        .setSettlementPeriod(config.getSettlementPeriod())
                        .setShowInterest(DecimalUtil.toBigDecimal(config.getShowInterest()).stripTrailingZeros().toPlainString())
                        .setLevelConfigId(config.getLevelConfigId())
                        .build())
                .build();
    }

    public QueryInterestByLevelResponse queryInterestByLevel(Header header, String tokenId, Long levelConfigId) {
        if (!BrokerService.checkModule(header, FunctionModule.MARGIN)) {
            return QueryInterestByLevelResponse.getDefaultInstance();
        }
        List<InterestConfig> list = new ArrayList<>();
        if (levelConfigId != -1L) {//查询某一等级的利率，从库中获取
            List<InterestConfig> configs = basicService.getMarginInterestByLevels(header.getOrgId(), tokenId, Collections.singletonList(0L));
            if (configs.isEmpty()) {
                //todo 错误号后续修改  利息未配置。
                throw new BrokerException(BrokerErrorCode.MARGIN_DEFUALT_INTEREST_NOT_EXIST);
            }
            QueryInterestByLevelRequest request = QueryInterestByLevelRequest.newBuilder()
                    .setBaseRequest(BaseRequest.newBuilder().setOrganizationId(header.getOrgId()).build())
                    .setTokenId(tokenId)
                    .setLevelConfigId(levelConfigId)
                    .build();
            QueryInterestByLevelReply reply = grpcMarginService.queryInterestByLevel(request);
            configs.addAll(reply.getInterestConfigList());
            Map<String, InterestConfig> map = configs.stream()
                    .collect(Collectors.toMap(InterestConfig::getTokenId, interestConfig -> interestConfig, (p, q) -> p.getLevelConfigId() == 0L ? q : p));
            list = map.values().stream().map(
                    interestConfig -> interestConfig.toBuilder()
                            .setLevelConfigId(levelConfigId)
                            .build())
                    .collect(Collectors.toList());
        } else {
            List<InterestConfig> configs = basicService.getMarginInterestByLevels(header.getOrgId(), tokenId, new ArrayList<>());
            Map<String, InterestConfig> defaultInterest = configs.stream()
                    .filter(config -> config.getLevelConfigId() == 0L)
                    .collect(Collectors.toMap(InterestConfig::getTokenId, interestConfig -> interestConfig));
            Map<Long, Map<String, InterestConfig>> map = configs.stream()
                    .collect(Collectors.groupingBy(InterestConfig::getLevelConfigId))
                    .entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
                            entry -> entry.getValue().stream().collect(Collectors.toMap(InterestConfig::getTokenId, interestConfig -> interestConfig))));
            if (!map.containsKey(0L)) {
                return QueryInterestByLevelResponse.getDefaultInstance();
            }
            map.forEach((levelId, tokenMap) -> {
                defaultInterest.forEach((defaultTokenId, interestConfig) -> {
                    if (!tokenMap.containsKey(defaultTokenId)) {
                        InterestConfig config = interestConfig.toBuilder().setLevelConfigId(levelId).build();
                        tokenMap.put(defaultTokenId, config);
                    }
                });
            });
            ListUserLevelConfigsRequest configsRequest = ListUserLevelConfigsRequest.newBuilder()
                    .setOrgId(header.getOrgId()).build();
            List<UserLevelConfigObj> levelConfigObjs = userLevelService.listUserLevelConfigs(configsRequest);
            for (UserLevelConfigObj configObj : levelConfigObjs) {
                if (configObj.getIsBaseLevel() == 1) {
                    Map<String, InterestConfig> defaultConfig = map.get(0L);
                    for (String token : defaultConfig.keySet()) {
                        InterestConfig config = defaultConfig.get(token);
                        config = config.toBuilder().setLevelConfigId(configObj.getLevelConfigId()).build();
                        defaultConfig.put(token, config);
                    }
                } else {
                    if (!map.containsKey(configObj.getLevelConfigId())) {
                        Map<String, InterestConfig> curConfig = new HashMap<>();
                        defaultInterest.forEach((defaultTokenId, interestConfig) -> {
                            InterestConfig config = interestConfig.toBuilder().setLevelConfigId(configObj.getLevelConfigId()).build();
                            curConfig.put(defaultTokenId, config);
                        });
                        map.put(configObj.getLevelConfigId(), curConfig);
                    }
                }
            }
            List<InterestConfig> finalList1 = list;
            map.values().forEach(configMap -> {
                finalList1.addAll(new ArrayList<>(configMap.values()));
            });
        }
        return QueryInterestByLevelResponse.newBuilder()
                .addAllInterestConfig(
                        list.stream().map(config -> io.bhex.broker.grpc.margin.InterestConfig.newBuilder()
                                .setOrgId(config.getOrgId())
                                .setTokenId(config.getTokenId())
                                .setInterest(DecimalUtil.toBigDecimal(config.getInterest()).stripTrailingZeros().toPlainString())
                                .setInterestPeriod(config.getInterestPeriod())
                                .setCalculationPeriod(config.getCalculationPeriod())
                                .setSettlementPeriod(config.getSettlementPeriod())
                                .setShowInterest(DecimalUtil.toBigDecimal(config.getShowInterest()).stripTrailingZeros().toPlainString())
                                .setLevelConfigId(config.getLevelConfigId())
                                .build())
                                .collect(Collectors.toList()))
                .build();
    }

    public List<UserRisk> queryLoanUserList(Header header, Integer isAll) {
        if (!BrokerService.checkModule(header, FunctionModule.MARGIN)) {
            return new ArrayList<>();
        }
        GetUserRiskRequest request = GetUserRiskRequest.newBuilder()
                .setBaseRequest(BaseRequest.newBuilder().setOrganizationId(header.getOrgId()).build())
                .build();
        GetUserRiskReply reply = grpcMarginService.queryUserRisk(request);
        List<io.bhex.base.margin.UserRisk> dealingList = reply.getUserRiskList();
        if (dealingList == null || dealingList.isEmpty()) {
            return new ArrayList<>();
        }
        List<UserRisk> userRiskList = dealingList
                .stream()
                .filter(risk -> isAll == 1 || new BigDecimal(risk.getBorrowed()).compareTo(BigDecimal.ZERO) > 0)
                .map(risk -> {
                    //log.info("queryUserRisk :{}", JsonUtil.defaultGson().toJson(risk));
                    Long userId = 0L;
                    try {
                        userId = accountService.getUserIdByAccountId(header.getOrgId(), risk.getAccountId());
                    } catch (Exception e) {
                        log.warn(e.getMessage(), e);
                    }
                    return UserRisk.newBuilder()
                            .setAccountId(risk.getAccountId())
                            .setTotal(risk.getTotal())
                            .setBorrowed(risk.getBorrowed())
                            .setSafety(risk.getSafety())
                            .setUserId(userId)
                            .build();
                }).collect(Collectors.toList());
        return userRiskList;
    }

    public List<ForceRecord> adminQueryForceRecord(Header header, Long accountId, Long fromId, Long toId, Long
            startTime, Long endTime, Integer limit) {
        if (header.getUserId() > 0 || accountId > 0) {
            accountId = accountService.checkMarginAccountIdNoThrow(header, accountId);
            if (accountId == null) {
                return new ArrayList<>();
            }
        }
        QueryForceRecordRequest request = QueryForceRecordRequest.newBuilder()
                .setBaseRequest(BaseRequest.newBuilder().setOrganizationId(header.getOrgId()).build())
                .setAccountId(accountId)
                .setFromId(fromId)
                .setToId(toId)
                .setStartTime(startTime)
                .setEndTime(endTime)
                .setLimit(limit)
                .build();
        QueryForceRecordReply reply = grpcMarginService.queryForceRecord(request);
        return reply.getForceRecordsList().stream()
                .map(record -> {
                    //log.info("queryUserRisk :{}", JsonUtil.defaultGson().toJson(risk));
                    Long userId = 0L;
                    try {
                        userId = accountService.getUserIdByAccountId(header.getOrgId(), record.getAccountId());
                    } catch (Exception e) {
                        log.warn(e.getMessage(), e);
                    }
                    return ForceRecord.newBuilder()
                            .setId(record.getId())
                            .setForceId(record.getForceId())
                            .setOrgId(record.getOrgId())
                            .setAdminUserId(record.getAdminUserId())
                            .setAccountId(record.getAccountId())
                            .setUserId(userId)
                            .setSafety(DecimalUtil.toBigDecimal(record.getSafety()).stripTrailingZeros().toPlainString())
                            .setAllPosition(DecimalUtil.toBigDecimal(record.getAllPosition()).stripTrailingZeros().toPlainString())
                            .setAllLoan(DecimalUtil.toBigDecimal(record.getAllLoan()).stripTrailingZeros().toPlainString())
                            .setForceType(record.getForceType())
                            .setDealStatus(record.getDealStatus())
                            .setForceDesc(record.getForceDesc())
                            .setCreated(record.getCreated())
                            .setUpdated(record.getUpdated())
                            .build();
                }).collect(Collectors.toList());
    }

    public AdminQueryAccountStatusResponse adminQueryAccountStatus(Header header) {
        if (!BrokerService.checkModule(header, FunctionModule.MARGIN)) {
            return AdminQueryAccountStatusResponse.getDefaultInstance();
        }
        Long accountId = accountService.checkMarginAccountIdNoThrow(header, 0L);
        if (accountId == null) {
            return AdminQueryAccountStatusResponse.getDefaultInstance();
        }
        GetMarginPositionStatusReply positionStatusReply = getMarginPositionStatus(header, accountId);
        return AdminQueryAccountStatusResponse.newBuilder()
                .setUserId(header.getUserId())
                .setAccountId(accountId)
                .setCurStatus(positionStatusReply.getCurStatusValue())
                .build();

    }

    public AdminChangeMarginPositionStatusResponse adminChangeMarginPositionStatus(Header header, Integer
            change_to_status, Integer cur_status) {
        if (!BrokerService.checkModule(header, FunctionModule.MARGIN)) {
            return AdminChangeMarginPositionStatusResponse.getDefaultInstance();
        }
        Long accountId = accountService.checkMarginAccountId(header, 0L);
        //解冻需先校验，账户是否在杠杆风控黑名单中
        if (change_to_status == MarginCrossPositionStatusEnum.POSITION_NORMAL_VALUE && !checkAccountNotInMarginBlack(header.getOrgId(), header.getUserId())) {
            // 存在于黑名单中，禁止解冻
            throw new BrokerException(BrokerErrorCode.IN_MARGIN_RISK_BLACK);
        }
        ChangeMarginPositionStatusRequest request = ChangeMarginPositionStatusRequest.newBuilder()
                .setBaseRequest(BaseRequest.newBuilder().setOrganizationId(header.getOrgId()).build())
                .setAccountId(accountId)
                .setCurStatus(MarginCrossPositionStatusEnum.forNumber(cur_status))
                .setChangeToStatus(MarginCrossPositionStatusEnum.forNumber(change_to_status))
                .build();
        ChangeMarginPositionStatusReply reply = grpcMarginService.changeMarginPositionStatus(request);
        if (reply.getCurStatusValue() != change_to_status) {
            //状态更改失败
            throw new BrokerException(BrokerErrorCode.CHANGE_MARGIN_POSITION_STATUS_ERROR);
        }
        return AdminChangeMarginPositionStatusResponse.newBuilder()
                .setCurStatus(reply.getCurStatusValue())
                .build();
    }

    public List<AccountLoanLimitLevel> queryAccountLoanLimitLevel(Header header, Integer vipLevel) {
        if (!BrokerService.checkModule(header, FunctionModule.MARGIN)) {
            return new ArrayList<>();
        }
        Long accountId = 0L;
        if (header.getUserId() > 0) {
            accountId = accountService.checkMarginAccountId(header, 0L);
        }
        QueryAccountLoanLimitVIPRequest request = QueryAccountLoanLimitVIPRequest.newBuilder()
                .setBaseRequest(BaseRequest.newBuilder().setOrganizationId(header.getOrgId()).build())
                .setAccountId(accountId)
                .setVipLevel(vipLevel)
                .build();
        QueryAccountLoanLimitVIPReply reply = grpcMarginService.queryAccountLoanLimitVIP(request);
        if (reply.getRet() != 0) {
            log.warn("queryAccountLoanLimitLevel error {}", reply.getRet());
            return new ArrayList<>();
        }
        return reply.getDatasList().stream().map(data -> AccountLoanLimitLevel.newBuilder()
                .setId(data.getId())
                .setAccountId(data.getAccountId())
                .setUserId(data.getBrokerUserId())
                .setVipLevel(data.getVipLevel())
                .setCreated(data.getCreated())
                .setUpdated(data.getUpdated())
                .build()).collect(Collectors.toList());
    }

    public SetAccountLoanLimitVIPResponse setAccountLoanLimitVIP(Header header, Integer vipLevel, String userIds) {
        if (!BrokerService.checkModule(header, FunctionModule.MARGIN)) {
            return SetAccountLoanLimitVIPResponse.getDefaultInstance();
        }
        if (vipLevel <= 0 || vipLevel > 3) {
            throw new BrokerException(BrokerErrorCode.PARAM_INVALID);
        }
        String[] uids = userIds.split(",");
        if (uids.length == 0) {
            return SetAccountLoanLimitVIPResponse.getDefaultInstance();
        }
        List<SetAccountLoanLimitVIPRequest.SetLoanLimitData> list = new ArrayList<>();
        for (String uid : uids) {
            Long userId = Long.parseLong(uid);
            Long accountId = accountService.getAccountId(header.getOrgId(), userId, AccountType.MARGIN, 0);
            SetAccountLoanLimitVIPRequest.SetLoanLimitData data = SetAccountLoanLimitVIPRequest.SetLoanLimitData.newBuilder()
                    .setBrokerUserId(userId)
                    .setAccountId(accountId)
                    .build();
            list.add(data);
        }
        SetAccountLoanLimitVIPRequest request = SetAccountLoanLimitVIPRequest.newBuilder()
                .setBaseRequest(BaseRequest.newBuilder().setOrganizationId(header.getOrgId()).build())
                .setVipLevel(vipLevel)
                .addAllData(list)
                .build();
        grpcMarginService.setAccountLoanLimitVIP(request);
        return SetAccountLoanLimitVIPResponse.getDefaultInstance();
    }

    public DeleteAccountLoanLimitVIPResponse deleteAccountLoanLimitVIP(Header header) {
        if (!BrokerService.checkModule(header, FunctionModule.MARGIN)) {
            return DeleteAccountLoanLimitVIPResponse.getDefaultInstance();
        }
        Long accountId = accountService.checkMarginAccountId(header, 0L);
        DeleteAccountLoanLimitVIPRequest request = DeleteAccountLoanLimitVIPRequest.newBuilder()
                .setBaseRequest(BaseRequest.newBuilder().setOrganizationId(header.getOrgId()).build())
                .setAccountId(accountId)
                .build();
        DeleteAccountLoanLimitVIPReply reply = grpcMarginService.deleteAccountLoanLimitVIP(request);
        return DeleteAccountLoanLimitVIPResponse.getDefaultInstance();
    }

    public QueryRptMarginPoolResponse queryRptMarginPool(Header header, String tokenId) {
        if (!BrokerService.checkModule(header, FunctionModule.MARGIN)) {
            return QueryRptMarginPoolResponse.getDefaultInstance();
        }
        Example example = new Example(RptMarginPool.class);
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("orgId", header.getOrgId());
        if (!StringUtils.isBlank(tokenId)) {
            criteria.andEqualTo("tokenId", tokenId);
        }
        example.setOrderByClause("id");
        List<RptMarginPool> pools = rptMarginPoolMapper.selectByExample(example);
        List<io.bhex.broker.grpc.margin.RptMarginPool> list = pools.stream()
                .map(pool -> io.bhex.broker.grpc.margin.RptMarginPool.newBuilder()
                        .setId(pool.getId())
                        .setTokenId(pool.getTokenId())
                        .setAvailable(pool.getAvailable().setScale(8, RoundingMode.DOWN).stripTrailingZeros().toPlainString())
                        .setUnpaidAmount(pool.getUnpaidAmount().setScale(8, RoundingMode.DOWN).stripTrailingZeros().toPlainString())
                        .setInterestUnpaid(pool.getInterestUnpaid().setScale(8, RoundingMode.DOWN).stripTrailingZeros().toPlainString())
                        .setInterestPaid(pool.getInterestPaid().setScale(8, RoundingMode.DOWN).stripTrailingZeros().toPlainString())
                        .setUpdated(pool.getUpdated())
                        .setCreated(pool.getCreated())
                        .build()).collect(Collectors.toList());
        return QueryRptMarginPoolResponse.newBuilder().addAllData(list).build();
    }

    public List<MarginRiskBlack> queryMarginRiskBlack(Header header, String confGroup) {
        if (!BrokerService.checkModule(header, FunctionModule.MARGIN)) {
            return new ArrayList<>();
        }
        Long accountId = 0L;
        if (header.getUserId() > 0) {
            accountId = accountService.checkMarginAccountIdNoThrow(header, 0L);
            if (accountId == null) {
                return new ArrayList<>();
            }
        }
        QueryMarginRiskBlackListRequest request = QueryMarginRiskBlackListRequest.newBuilder()
                .setBaseRequest(BaseRequest.newBuilder().setOrganizationId(header.getOrgId()).build())
                .setAccountId(accountId)
                .setConfGroup(confGroup)
                .build();
        QueryMarginRiskBlackListReply reply = grpcMarginService.queryMarginRiskBlackList(request);
        if (reply.getRet() != 0) {
            log.warn("queryMarginRiskBlack error ret:{}", reply.getRet());
            return new ArrayList<>();
        }
        return reply.getDataList().stream().map(data -> MarginRiskBlack.newBuilder()
                .setId(data.getId())
                .setOrgId(data.getOrgId())
                .setUserId(data.getUserId())
                .setAccountId(data.getAccountId())
                .setAdminUserName(data.getAdminUserName())
                .setConfGroup(data.getConfGroup())
                .setReason(data.getReason())
                .setCreated(data.getCreated())
                .setUpdated(data.getUpdated())
                .build()).collect(Collectors.toList());

    }

    public AddMarginRiskBlackListResponse addMarginRiskBlackList(Header header, String confGroup, String
            adminUserName, String reason) {
        if (!BrokerService.checkModule(header, FunctionModule.MARGIN)) {
            return AddMarginRiskBlackListResponse.getDefaultInstance();
        }
        Long accountId = accountService.checkMarginAccountId(header, 0L);
        boolean flag = checkAccountNotInMarginBlack(header.getOrgId(), header.getUserId());
        //判断是否已存在于黑名单中
        if (!flag) {
            return AddMarginRiskBlackListResponse.getDefaultInstance();
        }
        //冻结账户
        adminChangeMarginPositionStatus(header, MarginCrossPositionStatusEnum.POSITION_FORCE_CLOSING_VALUE, MarginCrossPositionStatusEnum.POSITION_NORMAL_VALUE);
        //禁止提币
        userSecurityService.forbidWithdraw24Hour(header, FrozenReason.MARGIN_BLACK_LIST);

        AddMarginRiskBlackListRequest request = AddMarginRiskBlackListRequest.newBuilder()
                .setBaseRequest(BaseRequest.newBuilder().setOrganizationId(header.getOrgId()).build())
                .setUserId(header.getUserId())
                .setAccountId(accountId)
                .setConfGroup(confGroup)
                .setAdminUserName(adminUserName)
                .setReason(reason)
                .build();
        grpcMarginService.addMarginRiskBlackList(request);
        return AddMarginRiskBlackListResponse.getDefaultInstance();
    }

    public DelMarginRiskBlackListResponse delMarginRiskBlackList(Header header, String confGroup) {
        if (!BrokerService.checkModule(header, FunctionModule.MARGIN)) {
            return DelMarginRiskBlackListResponse.getDefaultInstance();
        }
        Long accountId = accountService.checkMarginAccountId(header, 0L);
        DelMarginRiskBlackListRequest request = DelMarginRiskBlackListRequest.newBuilder()
                .setBaseRequest(BaseRequest.newBuilder().setOrganizationId(header.getOrgId()).build())
                .setAccountId(accountId)
                .setConfGroup(confGroup)
                .build();
        grpcMarginService.delMarginRiskBlackList(request);
        return DelMarginRiskBlackListResponse.getDefaultInstance();
    }


    /**
     * 新系统可暂不支持
     */
    public GetMarginActivityInfoResponse getMarginActivityInfo(Header header, Long activityId, String language) {

        GetMarginActivityInfoResponse.Builder responseBuilder = GetMarginActivityInfoResponse.newBuilder();
        MarginActivity marginActivity = marginActivityMapper.getByOrgIdAndId(header.getOrgId(), activityId);
        if (marginActivity == null || marginActivity.getStatus() != 1) {
            return responseBuilder.build();
        }
        responseBuilder.setActivityId(activityId);
        responseBuilder.setIcon(marginActivity.getIcon());
        responseBuilder.setStartTime(marginActivity.getStartTime());
        responseBuilder.setEndTime(marginActivity.getEndTime());
        responseBuilder.setSort(marginActivity.getSort());

        MarginActivityLocal marginActivityLocal = marginActivityLocalMapper.getByActivityIdAndLanguage(activityId, language);

        if (marginActivityLocal == null) {
            marginActivityLocal = marginActivityLocalMapper.getByActivityIdAndLanguage(activityId, default_lang);
            if (marginActivityLocal == null) {
                return responseBuilder.build();
            }
        }

        responseBuilder.setTitle(marginActivityLocal.getTitle());
        responseBuilder.setTitleColor(marginActivityLocal.getTitleColor());
        responseBuilder.setActivityUrl(marginActivityLocal.getActivityUrl());
        return responseBuilder.build();

    }

    /**
     * 新系统可暂不支持
     */
    public GetMarginActivityListResponse getMarginActivityList(Header header, Long activityId, String language) {

        GetMarginActivityListResponse.Builder responseBuilder = GetMarginActivityListResponse.newBuilder();

        long currentTimeMillis = System.currentTimeMillis();

        Example example = new Example(MarginActivity.class);
        Example.Criteria criteria = example.createCriteria().andEqualTo("orgId", header.getOrgId()).andEqualTo("status", 1)
                .andLessThanOrEqualTo("startTime", currentTimeMillis).andGreaterThanOrEqualTo("endTime", currentTimeMillis);
        if (activityId > 0) {
            criteria.andEqualTo("id", activityId);
        }

        example.setOrderByClause("sort,id");


        List<MarginActivity> activities = marginActivityMapper.selectByExample(example);
        if (activities.isEmpty()) {
            return responseBuilder.build();
        }


        Example localExample = new Example(MarginActivityLocal.class);
        localExample.createCriteria().andIn("activityId", activities.stream().map(MarginActivity::getId).collect(Collectors.toList()))
                .andEqualTo("status", 1);


        List<MarginActivityLocal> locals = marginActivityLocalMapper.selectByExample(localExample);

        Map<String, MarginActivityLocal> localMap = locals.stream().collect(Collectors.toMap(marginActivityLocal -> marginActivityLocal.getActivityId() + marginActivityLocal.getLanguage(), p -> p));

        List<GetMarginActivityListResponse.MarginActivity> activityList = new ArrayList<>();

        for (MarginActivity activity : activities) {

            MarginActivityLocal local = localMap.get(activity.getId() + language);
            if (local == null) {
                local = localMap.get(activity.getId() + default_lang);
                if (local == null) {
                    continue;
                }
            }

            GetMarginActivityListResponse.MarginActivity marginActivity = GetMarginActivityListResponse.MarginActivity.newBuilder()
                    .setActivityId(activity.getId())
                    .setTitle(local.getTitle())
                    .setTitleColor(local.getTitleColor())
                    .setIcon(activity.getIcon())
                    .setStartTime(activity.getStartTime())
                    .setEndTime(activity.getEndTime())
                    .setActivityUrl(local.getActivityUrl())
                    .setSort(activity.getSort())
                    .build();

            activityList.add(marginActivity);
        }

        return responseBuilder.addAllActivities(activityList).build();

    }


    public boolean checkAccountNotInMarginBlack(Long orgId, Long userId) {
        Long accountId = accountService.getMarginAccountIdNoThrow(orgId, userId, 0);
        if (accountId == null) {
            return true;
        }
        QueryMarginRiskBlackListRequest request = QueryMarginRiskBlackListRequest.newBuilder()
                .setBaseRequest(BaseRequest.newBuilder().setOrganizationId(orgId).build())
                .setAccountId(accountId)
                .setConfGroup(MARGIN_RISK_CALCULATION)
                .build();
        QueryMarginRiskBlackListReply reply = grpcMarginService.queryMarginRiskBlackList(request);
        if (reply.getRet() != 0 || reply.getDataCount() > 0) {
            // 存在于黑名单中，禁止解冻
            return false;
        }
        return true;
    }

    public QueryRptMarginTradeResponse queryRptMarginTrade(Header header, Long toTime, Integer limit) {
        if (!BrokerService.checkModule(header, FunctionModule.MARGIN)) {
            return QueryRptMarginTradeResponse.getDefaultInstance();
        }

        if (limit < 0) {
            limit = 20;
        }
        if (limit > 100) {
            limit = 100;
        }

        Example example = new Example(RptMarginTrade.class);
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("orgId", header.getOrgId());
        if (toTime > 0) {
            criteria.andLessThan("createTime", toTime);
        }
        example.setOrderByClause("create_time DESC");
        List<RptMarginTrade> marginTrades = rptMarginTradeMapper.selectByExampleAndRowBounds(example, new RowBounds(0, limit));
        List<io.bhex.broker.grpc.margin.RptMarginTrade> trades = marginTrades.stream()
                .map(trade -> io.bhex.broker.grpc.margin.RptMarginTrade.newBuilder()
                        .setId(trade.getId())
                        .setOrgId(trade.getOrgId())
                        .setCreateTime(trade.getCreateTime())
                        .setTradePeopleNum(trade.getTradePeopleNum())
                        .setBuyPeopleNum(trade.getBuyPeopleNum())
                        .setSellPeopleNum(trade.getSellPeopleNum())
                        .setTradeNum(trade.getTradeNum())
                        .setBuyTradeNum(trade.getBuyTradeNum())
                        .setSellTradeNum(trade.getSellTradeNum())
                        .setFee(trade.getFee().setScale(2, RoundingMode.DOWN).stripTrailingZeros().toPlainString())
                        .setAmount(trade.getAmount().setScale(2, RoundingMode.DOWN).stripTrailingZeros().toPlainString())
                        .build())
                .collect(Collectors.toList());
        return QueryRptMarginTradeResponse.newBuilder().addAllData(trades).setRet(0).build();
    }

    public QueryRptMarginTradeDetailResponse queryRptMarginTradeDetail(Header header, Long relationId) {
        if (!BrokerService.checkModule(header, FunctionModule.MARGIN)) {
            return QueryRptMarginTradeDetailResponse.getDefaultInstance();
        }

        Example example = new Example(RptMarginTradeDetail.class);
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("orgId", header.getOrgId());
        criteria.andEqualTo("relationId", relationId);
        List<RptMarginTradeDetail> marginTradeDetails = rptMarginTradeDetailMapper.selectByExample(example);
        List<io.bhex.broker.grpc.margin.RptMarginTradeDetail> details = marginTradeDetails.stream()
                .map(detail -> io.bhex.broker.grpc.margin.RptMarginTradeDetail.newBuilder()
                        .setRelationId(detail.getRelationId())
                        .setOrgId(detail.getOrgId())
                        .setSymbolId(detail.getSymbolId())
                        .setTradePeopleNum(detail.getTradePeopleNum())
                        .setBuyPeopleNum(detail.getBuyPeopleNum())
                        .setSellPeopleNum(detail.getSellPeopleNum())
                        .setTradeNum(detail.getTradeNum())
                        .setBuyTradeNum(detail.getBuyTradeNum())
                        .setSellTradeNum(detail.getSellTradeNum())
                        .setBaseFee(detail.getBaseFee().setScale(8, RoundingMode.DOWN).stripTrailingZeros().toPlainString())
                        .setQuoteFee(detail.getQuoteFee().setScale(8, RoundingMode.DOWN).stripTrailingZeros().toPlainString())
                        .setFee(detail.getFee().setScale(2, RoundingMode.DOWN).stripTrailingZeros().toPlainString())
                        .setAmount(detail.getAmount().setScale(2, RoundingMode.DOWN).stripTrailingZeros().toPlainString())
                        .setCreated(detail.getCreated())
                        .build()).collect(Collectors.toList());
        return QueryRptMarginTradeDetailResponse.newBuilder().addAllData(details).setRet(0).build();
    }

    public List<CrossLoanPosition> queryCrossLoanPosition(Long orgId, Long accountId, String tokenId) {
        CrossLoanPositionRequest request = CrossLoanPositionRequest.newBuilder()
                .setBaseRequest(BaseRequest.newBuilder().setOrganizationId(orgId).build())
                .setAccountId(accountId)
                .setTokenId(tokenId)
                .build();

        return grpcMarginService.getCrossLoanPosition(request).getCrossLoanPositionList();
    }

    public SetSpecialInterestResponse setSpecialInterest(Header header, String tokenId, String
            showInterest, Integer effectiveFlag) {
        if (!BrokerService.checkModule(header, FunctionModule.MARGIN)) {
            return SetSpecialInterestResponse.getDefaultInstance();
        }
        Long accountId = accountService.checkMarginAccountId(header, 0L);
        SpecialInterest specialInterest = specialInterestMapper.getSpecialInterest(header.getOrgId(), accountId, tokenId);

        if (specialInterest != null) {
            showInterest = StringUtils.isEmpty(showInterest) ? "0" : showInterest;
            //接受值为百分之多少
            BigDecimal si = new BigDecimal(showInterest).divide(new BigDecimal("100"), 18, RoundingMode.DOWN);
            if (si.compareTo(BigDecimal.ZERO) < 0) {
                si = BigDecimal.ZERO;
            }
            specialInterest.setEffectiveFlag(effectiveFlag);
            specialInterest.setInterest(si.divide(new BigDecimal("86400"), 18, RoundingMode.DOWN));
            specialInterest.setShowInterest(si);
            specialInterest.setUpdated(System.currentTimeMillis());
            specialInterestMapper.updateByPrimaryKeySelective(specialInterest);
        } else {
            showInterest = StringUtils.isEmpty(showInterest) ? "0" : showInterest;
            //接受值为百分之多少
            BigDecimal si = new BigDecimal(showInterest).divide(new BigDecimal("100"), 18, RoundingMode.DOWN);
            if (si.compareTo(BigDecimal.ZERO) < 0) {
                si = BigDecimal.ZERO;
            }
            specialInterest = new SpecialInterest();
            specialInterest.setOrgId(header.getOrgId());
            specialInterest.setUserId(header.getUserId());
            specialInterest.setAccountId(accountId);
            specialInterest.setTokenId(tokenId);
            specialInterest.setInterest(si.divide(new BigDecimal("86400"), 18, RoundingMode.DOWN));
            specialInterest.setShowInterest(si);
            specialInterest.setEffectiveFlag(effectiveFlag);
            specialInterest.setCreated(System.currentTimeMillis());
            specialInterest.setUpdated(System.currentTimeMillis());
            specialInterestMapper.insert(specialInterest);
        }
        return SetSpecialInterestResponse.newBuilder().setRet(0).build();
    }

    public QuerySpecialInterestResponse querySpecialInterest(Header header, Long userId, Long accountId, String
            tokenId) {
        if (!BrokerService.checkModule(header, FunctionModule.MARGIN)) {
            return QuerySpecialInterestResponse.getDefaultInstance();
        }
        Example example = new Example(SpecialInterest.class);
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("orgId", header.getOrgId());
        if (userId != null && userId > 0) {
            criteria.andEqualTo("userId", userId);
        }

        if (accountId != null && accountId > 0) {
            criteria.andEqualTo("accountId", accountId);
        }
        if (!tokenId.isBlank()) {
            criteria.andEqualTo("tokenId", tokenId);
        }
        List<SpecialInterest> specialInterests = specialInterestMapper.selectByExample(example);
        List<io.bhex.broker.grpc.margin.SpecialInterest> interests = specialInterests.stream()
                .map(interest -> io.bhex.broker.grpc.margin.SpecialInterest.newBuilder()
                        .setId(interest.getId())
                        .setOrgId(interest.getOrgId())
                        .setUserId(interest.getUserId())
                        .setTokenId(interest.getTokenId())
                        .setAccountId(interest.getAccountId())
                        .setInterest(interest.getInterest().stripTrailingZeros().toPlainString())
                        .setShowInterest(interest.getShowInterest().stripTrailingZeros().toPlainString())
                        .setEffectiveFlag(interest.getEffectiveFlag())
                        .setCreated(interest.getCreated())
                        .setUpdated(interest.getUpdated())
                        .build())
                .collect(Collectors.toList());
        return QuerySpecialInterestResponse.newBuilder().addAllData(interests).setRet(0).build();

    }

    public DeleteSpecialInterestResponse deleteSpecialInterest(Header header, String tokenId) {
        if (!BrokerService.checkModule(header, FunctionModule.MARGIN)) {
            return DeleteSpecialInterestResponse.getDefaultInstance();
        }
        Long accountId = accountService.checkMarginAccountIdNoThrow(header, 0L);
        if (accountId == null) {
            return DeleteSpecialInterestResponse.getDefaultInstance();
        }
        specialInterestMapper.delSpecialInterest(header.getOrgId(), accountId, tokenId);
        return DeleteSpecialInterestResponse.newBuilder().setRet(0).build();
    }

    public SubmitOpenMarginActivityResponse submitOpenMarginActivity(Header header) {
        boolean lock = false;
        String key = String.format(SUMBIT_OPEN_MARGIN_ACTIVITY_LOCK, header.getOrgId());
        try {
            if (!BrokerService.checkModule(header, FunctionModule.MARGIN)) {
                return SubmitOpenMarginActivityResponse.getDefaultInstance();
            }
            //判断活动是否开启
            long time = System.currentTimeMillis();
            MarginActivity marginActivity = marginActivityMapper.getByOrgIdAndId(header.getOrgId(), 842711774229992192L);
            if (marginActivity == null || marginActivity.getStatus() != 1 || time < marginActivity.getStartTime() || time > marginActivity.getEndTime()) {
                throw new BrokerException(BrokerErrorCode.FEATURE_NOT_OPEN);
            }

            Long accountId = accountService.checkMarginAccountId(header, 0L);
            //redis 加锁
            lock = RedisLockUtils.tryLockAlways(redisTemplate, key, 5000, 5);
            if (!lock) {
                throw new BrokerException(BrokerErrorCode.ORDER_ACTIVITY_SYSTEM_BUSY);
            }
            //上午开始时间
            long morningBeginTime = DateUtil.startOfDay(time);
            //下午开始时间
            long afternoonBeginTime = morningBeginTime + 12L * 60 * 60 * 1000;

            long endTime = morningBeginTime + 86400 * 1000;
            long submitTime = morningBeginTime;
            if (time >= afternoonBeginTime) {
                submitTime = afternoonBeginTime;
            }
            //校验记录是否存在
            MarginDailyOpenActivity activity = marginDailyOpenActivityMapper.getMarginOpenActivityByAid(header.getOrgId(), submitTime, accountId);

            if (activity != null) {
                //请勿重复提交
                throw new BrokerException(BrokerErrorCode.MARGIN_OPEN_ACTIVITY_RESUMBIT_ERROR);
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
            //获取表中最大的抽奖Id
            Integer lotteryNo = marginDailyOpenActivityMapper.getMaxLotteryNo(header.getOrgId(), morningBeginTime, endTime);

            activity = new MarginDailyOpenActivity();
            activity.setOrgId(header.getOrgId());
            activity.setUserId(header.getUserId());
            activity.setAccountId(accountId);
            activity.setSubmitTime(submitTime);
            activity.setKycLevel(userVerify.getKycLevel());
            activity.setAllPositionUsdt(allPosi);
            activity.setLotteryNo(lotteryNo + 1);
            activity.setCreated(time);
            activity.setUpdated(time);
            marginDailyOpenActivityMapper.insertSelective(activity);

            List<MarginDailyOpenActivity> activityList = marginDailyOpenActivityMapper.queryMarginOpenActivityByAid(header.getOrgId(), morningBeginTime, endTime, accountId);

            String lotteryNoStr = "";
            if (!activityList.isEmpty()) {
                if (activityList.size() == 2) {
                    lotteryNoStr = activityList.stream().map(a -> a.getLotteryNo().toString()).collect(Collectors.joining(";"));
                } else if (activityList.get(0).getSubmitTime() == morningBeginTime) {
                    lotteryNoStr = activityList.get(0).getLotteryNo() + ";" + 0;
                } else {
                    lotteryNoStr = 0 + ";" + activityList.get(0).getLotteryNo();
                }
            }
            OpenMarginActivity activityResult = openMarginActivityResult(activityList.get(0));
            activityResult = activityResult.toBuilder().setLotteryNo(lotteryNoStr).build();
            return SubmitOpenMarginActivityResponse.newBuilder().setData(activityResult).setRet(0).build();
        } finally {
            if (lock) {
                RedisLockUtils.releaseLock(redisTemplate, key);
            }
        }

    }

    public GetOpenMarginActivityResponse getOpenMarginActivity(Header header) {
        if (!BrokerService.checkModule(header, FunctionModule.MARGIN)) {
            return GetOpenMarginActivityResponse.getDefaultInstance();
        }
        Long accountId = accountService.checkMarginAccountId(header, 0L);
        long time = System.currentTimeMillis();
        long morningBeginTime = DateUtil.startOfDay(time);
        long endTime = morningBeginTime + 86400 * 10000L;
        List<MarginDailyOpenActivity> activityList = marginDailyOpenActivityMapper.queryMarginOpenActivityByAid(header.getOrgId(), morningBeginTime, endTime, accountId);
        if (activityList.isEmpty()) {
            return GetOpenMarginActivityResponse.getDefaultInstance();
        }
        OpenMarginActivity activity = openMarginActivityResult(activityList.get(0));
        String lotteryNoStr = "";
        if (!activityList.isEmpty()) {
            if (activityList.size() == 2) {
                lotteryNoStr = activityList.stream().map(a -> a.getLotteryNo().toString()).collect(Collectors.joining(";"));
            } else if (activityList.get(0).getSubmitTime() == morningBeginTime) {
                lotteryNoStr = activityList.get(0).getLotteryNo() + ";" + 0;
            } else {
                lotteryNoStr = 0 + ";" + activityList.get(0).getLotteryNo();
            }
        }
        activity = activity.toBuilder().setLotteryNo(lotteryNoStr).build();
        return GetOpenMarginActivityResponse.newBuilder()
                .setData(activity)
                .build();
    }

    public AdminQueryOpenMarginActivityResponse adminQueryOpenMarginActivity(Header header, Long accountId, Long
            startTime, Long endTime, Long fromId, Integer joinStatus, Integer limit) {
        if (!BrokerService.checkModule(header, FunctionModule.MARGIN)) {
            return AdminQueryOpenMarginActivityResponse.getDefaultInstance();
        }
        if (header.getUserId() > 0 || accountId > 0) {
            accountId = accountService.checkMarginAccountIdNoThrow(header, accountId);
        }
        if(accountId == null){
            return AdminQueryOpenMarginActivityResponse.getDefaultInstance();
        }
        Example example = new Example(MarginOpenActivity.class);
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("orgId", header.getOrgId());
        if (accountId != null && accountId > 0) {
            criteria.andEqualTo("accountId", accountId);
        }
        if (startTime > 0) {
            criteria.andGreaterThanOrEqualTo("submitTime", startTime);
        }
        if (endTime > 0) {
            criteria.andLessThanOrEqualTo("submitTime", endTime);
        }
        if (fromId > 0) {
            criteria.andLessThan("id", fromId);
        }
        example.setOrderByClause("id DESC");
        List<MarginDailyOpenActivity> activities = marginDailyOpenActivityMapper.selectByExampleAndRowBounds(example, new RowBounds(0, limit));
        return AdminQueryOpenMarginActivityResponse.newBuilder()
                .addAllData(activities.stream().map(this::openMarginActivityResult).collect(Collectors.toList()))
                .build();
    }

    public OpenMarginActivity openMarginActivityResult(MarginDailyOpenActivity activity) {
        return OpenMarginActivity.newBuilder()
                .setId(activity.getId())
                .setOrgId(activity.getOrgId())
                .setUserId(activity.getUserId())
                .setAccountId(activity.getAccountId())
                .setSubmitTime(activity.getSubmitTime())
                .setKycLevel(activity.getKycLevel())
                .setAllPositionUsdt(activity.getAllPositionUsdt().stripTrailingZeros().toPlainString())
                .setLotteryNo(activity.getLotteryNo().toString())
                .setCreated(activity.getCreated())
                .setUpdated(activity.getUpdated())
                .build();
    }

    /**
     * 设置特殊借币限额 -- 新系统可暂不支持
     *
     * @param header
     * @param loanLimit
     * @param isOpen
     * @return
     */
    public AdminSetSpecialLoanLimitResponse adminSetSpecialLoanLimit(Header header, String loanLimit, Integer isOpen) {
        if (!BrokerService.checkModule(header, FunctionModule.MARGIN)) {
            return AdminSetSpecialLoanLimitResponse.getDefaultInstance();
        }
        Long accountId = accountService.checkMarginAccountId(header, 0L);
        SetSpecialLoanLimitRequest request = SetSpecialLoanLimitRequest.newBuilder()
                .setBaseRequest(BaseRequest.newBuilder().setOrganizationId(header.getOrgId()).build())
                .setUserId(header.getUserId())
                .setAccountId(accountId)
                .setLoanLimit(DecimalUtil.fromBigDecimal(new BigDecimal(loanLimit)))
                .setIsOpen(isOpen)
                .build();
        grpcMarginService.setSpecialLoanLimit(request);
        return AdminSetSpecialLoanLimitResponse.getDefaultInstance();
    }

    /**
     * 查询特殊借币限额 -- 新系统可暂不支持
     *
     * @param header
     * @return
     */
    public AdminQuerySpecialLoanLimitResponse adminQuerySpecialLoanLimit(Header header) {
        if (!BrokerService.checkModule(header, FunctionModule.MARGIN)) {
            return AdminQuerySpecialLoanLimitResponse.getDefaultInstance();
        }
        Long accountId = 0L;
        if (header.getUserId() > 0) {
            accountId = accountService.checkMarginAccountIdNoThrow(header, 0L);
            if (accountId == null) {
                return AdminQuerySpecialLoanLimitResponse.getDefaultInstance();
            }
        }
        QuerySpecialLoanLimitRequest request = QuerySpecialLoanLimitRequest.newBuilder()
                .setBaseRequest(BaseRequest.newBuilder().setOrganizationId(header.getOrgId()).build())
                .setAccountId(accountId)
                .build();
        QuerySpecialLoanLimitReply reply = grpcMarginService.querySpecialLoanLimit(request);
        return AdminQuerySpecialLoanLimitResponse.newBuilder()
                .addAllData(reply.getDataList().stream().map(limit -> SpecialLoanLimit.newBuilder()
                        .setId(limit.getId())
                        .setOrgId(limit.getOrgId())
                        .setUserId(limit.getUserId())
                        .setAccountId(limit.getAccountId())
                        .setLoanLimit(DecimalUtil.toBigDecimal(limit.getLoanLimit()).stripTrailingZeros().toPlainString())
                        .setIsOpen(limit.getIsOpen())
                        .setCreated(limit.getCreated())
                        .setUpdated(limit.getUpdated()).build())
                        .collect(Collectors.toList()))
                .build();
    }

    /**
     * 删除特殊借币限额 -- 新系统可暂不支持
     *
     * @param header
     * @return
     */
    public AdminDelSpecialLoanLimitResponse adminDelSpecialLoanLimit(Header header) {
        if (!BrokerService.checkModule(header, FunctionModule.MARGIN)) {
            return AdminDelSpecialLoanLimitResponse.getDefaultInstance();
        }
        Long accountId = accountService.checkMarginAccountId(header, 0L);
        DelSpecialLoanLimitRequest request = DelSpecialLoanLimitRequest.newBuilder()
                .setBaseRequest(BaseRequest.newBuilder().setOrganizationId(header.getOrgId()).build())
                .setAccountId(accountId)
                .build();

        grpcMarginService.delSpecialLoanLimit(request);
        return AdminDelSpecialLoanLimitResponse.getDefaultInstance();

    }

    /**
     * 管理端设置杠杆币种借贷数量上限 -- 新系统可暂不支持
     */
    public AdminSetMarginLoanLimitResponse adminSetMarginLoanLimit(Header header, String tokenId, String limitAmount, Integer status) {
        if (!BrokerService.checkModule(header, FunctionModule.MARGIN)) {
            throw new BrokerException(BrokerErrorCode.OPERATION_HAS_NO_PERMISSION);
        }
        io.bhex.base.margin.TokenConfig tokenConfig = basicService.getOrgMarginToken(header.getOrgId(), tokenId);

        if (tokenConfig == null) {
            throw new BrokerException(BrokerErrorCode.PARAM_INVALID);
        }

        MarginLoanLimit marginLoanLimit = marginLoanLimitMapper.getByOrgIdAndTokenId(header.getOrgId(), tokenId);

        if (marginLoanLimit == null) {
            marginLoanLimit = new MarginLoanLimit();
            marginLoanLimit.setOrgId(header.getOrgId());
            marginLoanLimit.setTokenId(tokenId);
            marginLoanLimit.setLimitAmount(new BigDecimal(limitAmount));
            marginLoanLimit.setStatus(status == 1 ? 1 : 2);
            marginLoanLimit.setCreated(System.currentTimeMillis());
            marginLoanLimit.setUpdated(System.currentTimeMillis());
            marginLoanLimitMapper.insertSelective(marginLoanLimit);
        } else {
            marginLoanLimitMapper.updateByPrimaryKeySelective(MarginLoanLimit.builder().id(marginLoanLimit.getId()).limitAmount(new BigDecimal(limitAmount)).status(status == 1 ? 1 : 2).updated(System.currentTimeMillis()).build());
        }

        return AdminSetMarginLoanLimitResponse.newBuilder().build();
    }

    /**
     * 管理端查询杠杆币种借贷数量上限 -- 新系统可暂不支持
     */
    public AdminQueryMarginLoanLimitResponse adminQueryMarginLoanLimit(Header header, String tokenId) {

        Example example = new Example(MarginLoanLimit.class);
        Example.Criteria criteria = example.createCriteria().andEqualTo("orgId", header.getOrgId());
        if (!tokenId.isBlank()) {
            criteria.andEqualTo("tokenId", tokenId);
        }

        example.orderBy("tokenId");

        List<io.bhex.broker.grpc.margin.MarginLoanLimit> limitList = marginLoanLimitMapper.selectByExample(example).stream().map(marginLoanLimit -> io.bhex.broker.grpc.margin.MarginLoanLimit.newBuilder()
                .setId(marginLoanLimit.getId())
                .setOrgId(marginLoanLimit.getOrgId())
                .setTokenId(marginLoanLimit.getTokenId())
                .setLimitAmount(marginLoanLimit.getLimitAmount().stripTrailingZeros().toPlainString())
                .setStatus(marginLoanLimit.getStatus())
                .setCreated(marginLoanLimit.getCreated())
                .setUpdated(marginLoanLimit.getUpdated()).build()
        ).collect(Collectors.toList());

        return AdminQueryMarginLoanLimitResponse.newBuilder().addAllLimits(limitList).build();
    }

    /**
     * 管理端设置杠杆用户币种借贷数量上限 -- 新系统可暂不支持
     */
    public AdminSetMarginUserLoanLimitResponse adminSetMarginUserLoanLimit(Header header, Long userId, String tokenId, String limitAmount, Integer status) {
        if (!BrokerService.checkModule(header, FunctionModule.MARGIN)) {
            throw new BrokerException(BrokerErrorCode.OPERATION_HAS_NO_PERMISSION);
        }

        io.bhex.base.margin.TokenConfig tokenConfig = basicService.getOrgMarginToken(header.getOrgId(), tokenId);

        if (tokenConfig == null) {
            throw new BrokerException(BrokerErrorCode.PARAM_INVALID);
        }

        Long accountId = accountService.getAccountId(header.getOrgId(), userId, AccountType.MARGIN, 0);

        MarginUserLoanLimit marginUserLoanLimit = marginUserLoanLimitMapper.getByAccountIdAndTokenId(header.getOrgId(), userId, accountId, tokenId);

        if (marginUserLoanLimit == null) {
            marginUserLoanLimit = new MarginUserLoanLimit();
            marginUserLoanLimit.setOrgId(header.getOrgId());
            marginUserLoanLimit.setUserId(userId);
            marginUserLoanLimit.setAccountId(accountId);
            marginUserLoanLimit.setTokenId(tokenId);
            marginUserLoanLimit.setLimitAmount(new BigDecimal(limitAmount));
            marginUserLoanLimit.setStatus(status == 1 ? 1 : 2);
            marginUserLoanLimit.setCreated(System.currentTimeMillis());
            marginUserLoanLimit.setUpdated(System.currentTimeMillis());
            marginUserLoanLimitMapper.insertSelective(marginUserLoanLimit);
        } else {
            marginUserLoanLimitMapper.updateByPrimaryKeySelective(MarginUserLoanLimit.builder().id(marginUserLoanLimit.getId()).limitAmount(new BigDecimal(limitAmount)).status(status == 1 ? 1 : 2).updated(System.currentTimeMillis()).build());
        }

        return AdminSetMarginUserLoanLimitResponse.newBuilder().build();
    }

    /**
     * 管理端查询杠杆用户币种借贷数量上限 -- 新系统可暂不支持
     */
    public AdminQueryMarginUserLoanLimitResponse adminQueryMarginUserLoanLimit(Header header, Long userId, Long accountId, String tokenId) {
        Example example = new Example(MarginUserLoanLimit.class);
        Example.Criteria criteria = example.createCriteria().andEqualTo("orgId", header.getOrgId());

        if (userId != null && userId > 0) {
            criteria.andEqualTo("userId", userId);
        }

        if (accountId != null && accountId > 0) {
            criteria.andEqualTo("accountId", accountId);
        }

        if (!tokenId.isBlank()) {
            criteria.andEqualTo("tokenId", tokenId);
        }

        example.setOrderByClause("user_id, token_id");

        List<io.bhex.broker.grpc.margin.MarginUserLoanLimit> limitList = marginUserLoanLimitMapper.selectByExample(example).stream().map(marginUserLoanLimit -> io.bhex.broker.grpc.margin.MarginUserLoanLimit.newBuilder()
                .setId(marginUserLoanLimit.getId())
                .setOrgId(marginUserLoanLimit.getOrgId())
                .setUserId(marginUserLoanLimit.getUserId())
                .setAccountId(marginUserLoanLimit.getAccountId())
                .setTokenId(marginUserLoanLimit.getTokenId())
                .setLimitAmount(marginUserLoanLimit.getLimitAmount().stripTrailingZeros().toPlainString())
                .setStatus(marginUserLoanLimit.getStatus())
                .setCreated(marginUserLoanLimit.getCreated())
                .setUpdated(marginUserLoanLimit.getUpdated()).build()
        ).collect(Collectors.toList());

        return AdminQueryMarginUserLoanLimitResponse.newBuilder().addAllLimits(limitList).build();
    }

    /**
     * 获取币种剩余的可借币种上限 -- 新系统可暂不支持
     */
    public BigDecimal getRemainTokenLoanLimit(Long orgId, Long userId, Long accountId, String tokenId, BigDecimal inputAmount) {

        CrossLoanPositionRequest loanRequest = CrossLoanPositionRequest.newBuilder()
                .setBaseRequest(BaseRequest.newBuilder().setOrganizationId(orgId))
                .setAccountId(accountId)
                .setTokenId(tokenId)
                .build();
        CrossLoanPositionReply reply = grpcMarginService.getCrossLoanPosition(loanRequest);

        BigDecimal loanAmount = reply.getCrossLoanPositionList().isEmpty() ? BigDecimal.ZERO : DecimalUtil.toBigDecimal(reply.getCrossLoanPosition(0).getLoanTotal());

        MarginUserLoanLimit marginUserLoanLimit = marginUserLoanLimitMapper.getValidByAccountIdAndTokenId(orgId, userId, accountId, tokenId);

        if (marginUserLoanLimit != null) {
            BigDecimal subtract = marginUserLoanLimit.getLimitAmount().subtract(loanAmount);
            return subtract.compareTo(BigDecimal.ZERO) == -1 ? BigDecimal.ZERO : subtract;
        }

        MarginLoanLimit marginLoanLimit = marginLoanLimitMapper.getValidByOrgIdAndTokenId(orgId, tokenId);
        if (marginLoanLimit != null) {
            BigDecimal subtract = marginLoanLimit.getLimitAmount().subtract(loanAmount);
            return subtract.compareTo(BigDecimal.ZERO) == -1 ? BigDecimal.ZERO : subtract;
        }

        return inputAmount;
    }

}

