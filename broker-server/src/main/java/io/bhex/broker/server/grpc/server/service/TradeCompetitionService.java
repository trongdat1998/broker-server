package io.bhex.broker.server.grpc.server.service;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

import com.alibaba.fastjson.JSON;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.joda.time.DateTime;
import org.springframework.aop.framework.AopContext;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.task.TaskExecutor;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.security.SecureRandom;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

import io.bhex.base.account.AccountType;
import io.bhex.base.account.BatchSyncTransferRequest;
import io.bhex.base.account.BusinessSubject;
import io.bhex.base.account.SyncTransferRequest;
import io.bhex.base.account.SyncTransferResponse;
import io.bhex.base.account.UnlockBalanceResponse;
import io.bhex.base.constants.ProtoConstants;
import io.bhex.base.idgen.api.ISequenceGenerator;
import io.bhex.base.token.SymbolDetail;
import io.bhex.base.token.TokenCategory;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.common.util.JsonUtil;
import io.bhex.broker.grpc.activity.lockInterest.QueryMyPerformanceResponse;
import io.bhex.broker.grpc.activity.lockInterest.QueryTradeCompetitionInfoReply;
import io.bhex.broker.grpc.activity.lockInterest.Rank;
import io.bhex.broker.grpc.activity.lockInterest.SignUpTradeCompetitionRequest;
import io.bhex.broker.grpc.activity.lockInterest.SignUpTradeCompetitionResponse;
import io.bhex.broker.server.domain.TradeCompetitionInfo;
import io.bhex.broker.server.grpc.client.service.GrpcBatchTransferService;
import io.bhex.broker.server.grpc.server.service.statistics.StatisticsBalanceService;
import io.bhex.broker.server.grpc.server.service.statistics.StatisticsContractService;
import io.bhex.broker.server.grpc.server.service.statistics.StatisticsTradeService;
import io.bhex.broker.server.model.Account;
import io.bhex.broker.server.model.BalanceFlowSnapshot;
import io.bhex.broker.server.model.ContractSnapshot;
import io.bhex.broker.server.model.ContractTemporaryInfo;
import io.bhex.broker.server.model.StatisticsTradeAllDetail;
import io.bhex.broker.server.model.Symbol;
import io.bhex.broker.server.model.TradeCompetition;
import io.bhex.broker.server.model.TradeCompetition.CompetitionStatus;
import io.bhex.broker.server.model.TradeCompetitionExt;
import io.bhex.broker.server.model.TradeCompetitionLimit;
import io.bhex.broker.server.model.TradeCompetitionParticipant;
import io.bhex.broker.server.model.TradeCompetitionResult;
import io.bhex.broker.server.model.TradeCompetitionResultDaily;
import io.bhex.broker.server.model.User;
import io.bhex.broker.server.model.UserVerify;
import io.bhex.broker.server.primary.mapper.AccountMapper;
import io.bhex.broker.server.primary.mapper.ContractTemporaryInfoMapper;
import io.bhex.broker.server.primary.mapper.SymbolMapper;
import io.bhex.broker.server.primary.mapper.TradeCompetitionExtMapper;
import io.bhex.broker.server.primary.mapper.TradeCompetitionLimitMapper;
import io.bhex.broker.server.primary.mapper.TradeCompetitionMapper;
import io.bhex.broker.server.primary.mapper.TradeCompetitionParticipantMapper;
import io.bhex.broker.server.primary.mapper.TradeCompetitionResultDailyMapper;
import io.bhex.broker.server.primary.mapper.TradeCompetitionResultMapper;
import io.bhex.broker.server.primary.mapper.UserMapper;
import io.bhex.broker.server.primary.mapper.UserVerifyMapper;
import io.bhex.broker.server.util.BaseReqUtil;
import io.bhex.broker.server.util.CommonUtil;
import io.bhex.broker.server.util.PageUtil;
import io.bhex.broker.server.util.RedisLockUtils;
import lombok.extern.slf4j.Slf4j;
import tk.mybatis.mapper.entity.Example;

@Slf4j
@Service
public class TradeCompetitionService {


    //init 启动初始化local cache缓存 活动信息 缓存local cache 5分钟失效 只缓存进行中的比赛 ||| 未命中DB获取即可 |||

    //定时刷新比赛配置缓存数据函数

    //获取比赛详情数据接口 对API暴露

    //定时刷新快照数据函数

    //internal 配置白名单 配置参赛名单 配置limit限制

    //internal 配置交易大赛 更新交易大赛数据

    //活动派发奖励

    @Resource
    private TradeCompetitionMapper tradeCompetitionMapper;

    @Resource
    private TradeCompetitionExtMapper tradeCompetitionExtMapper;

    @Resource
    private TradeCompetitionLimitMapper tradeCompetitionLimitMapper;

    @Resource
    private TradeCompetitionResultMapper tradeCompetitionResultMapper;

    @Resource
    private StatisticsContractService statisticsContractService;

    @Resource
    private StatisticsBalanceService statisticsBalanceService;

    @Resource
    private StatisticsTradeService statisticsTradeService;

    @Resource
    private AccountService accountService;

    @Resource
    private BasicService basicService;

    @Autowired
    private GrpcBatchTransferService grpcBatchTransferService;

    @Resource
    private ISequenceGenerator sequenceGenerator;

    @Resource
    private SymbolMapper symbolMapper;

    @Resource(name = "stringRedisTemplate")
    protected StringRedisTemplate redisTemplate;

    @Resource
    private TradeCompetitionResultDailyMapper tradeCompetitionResultDailyMapper;

    @Resource
    private TradeCompetitionParticipantMapper tradeCompetitionParticipantMapper;

    @Resource(name = "asyncTaskExecutor")
    private TaskExecutor taskExecutor;

    @Resource
    private ContractTemporaryInfoMapper contractTemporaryInfoMapper;

    @Resource
    private BalanceService balanceService;

    @Resource
    private UserMapper userMapper;

    @Resource
    private AccountMapper accountMapper;

    @Resource
    private UserVerifyMapper userVerifyMapper;

    private final static String MASK = "****";

    private static final String TRADE_COMPETITION_CACHE_KEY = "org_competition_cache:%s_%s";

    public static final String FUTURES_POSITION_QUANTITY = "futures_position_quantity";

    public static final String DAY_TRADE_NUMBER = "DAY_TRADE_NUMBER";

    public static final String ALL_TRADE_NUMBER = "ALL_TRADE_NUMBER";

    private static final String REFRESH_TRADE_RANK_LIST_SNAPSHOT_TASK_V2 = "REFRESH::TRADE::RANK::LIST::SNAPSHOT::TASK::V2";

    private static final String REFRESH_TRADE_RANK_DAILY_LIST_SNAPSHOT_TASK = "REFRESH::TRADE::RANK::DAILY::LIST::SNAPSHOT::TASK";

    private static final String CHANGE_ACTIVITY_STATUS = "TRADE::COMPETITION::CHANGE::ACTIVITY::STATUS";

    private static final String CONTRACT_TEMPORARY_TASK = "CONTRACT::TEMPORARY::TASK";


    @PostConstruct
    public void initCompetitionConfig() throws Exception {
        try {
            //把所有进行中的比赛活动配置信息初始到local cache
            List<TradeCompetition> tradeCompetitionMappers = this.tradeCompetitionMapper.queryAll();
            if (CollectionUtils.isEmpty(tradeCompetitionMappers)) {
                return;
            }

            tradeCompetitionMappers.forEach(trade -> {
                //String key = String.format(TRADE_COMPETITION_CACHE_KEY, trade.getOrgId(), trade.getCompetitionCode());
                //competitionConfigCache.put(key, buildTradeCompetitionInfo(trade.getOrgId(), trade.getCompetitionCode()));
                refreshTradeCompetitionInfoCache(trade.getOrgId(), trade.getCompetitionCode());
            });
        } catch (Exception e) {
            log.error("initCompetitionConfig error {}", e);
            throw new Exception(e);
        }
    }

    @Scheduled(cron = "0/5 * * * * ? ")
    public void changeActivityStatus() {
        Boolean locked = RedisLockUtils.tryLock(redisTemplate, CHANGE_ACTIVITY_STATUS, 4 * 1000);
        if (!locked) {
            log.warn(" **** changeActivityStatus cannot get lock");
            return;
        }

        log.info(" **** changeActivityStatus work start");

        List<Integer> statuses = Lists.newArrayList();
        statuses.add(CompetitionStatus.INIT.getValue());
        statuses.add(CompetitionStatus.PROCESSING.getValue());

        Example exp = new Example(TradeCompetition.class);
        exp.createCriteria().andIn("status", statuses);
        exp.selectProperties("id", "status", "startTime", "endTime");

        List<TradeCompetition> competitions = this.tradeCompetitionMapper.selectByExample(exp);
        if (CollectionUtils.isEmpty(competitions)) {
            return;
        }

        long now = System.currentTimeMillis();

        competitions.forEach(trade -> {

            CompetitionStatus status = CompetitionStatus.UNKNOWN;
            boolean changed = false;

            if (trade.getEndTime().getTime() < now &&
                    CompetitionStatus.PROCESSING ==
                            CompetitionStatus.getEnum(trade.getStatus())) {
                status = CompetitionStatus.FINISH;
                changed = true;

            } else if (trade.getStartTime().getTime() <= now &&
                    CompetitionStatus.INIT ==
                            CompetitionStatus.getEnum(trade.getStatus())) {
                status = CompetitionStatus.PROCESSING;
                changed = true;
            }

            if (changed) {
                trade.setUpdateTime(new Date(now));
                trade.setStatus(status.getValue());
                this.tradeCompetitionMapper.updateByPrimaryKeySelective(trade);
            }

        });
    }

    @Scheduled(cron = "0 1,11,21,31,41,51 * * * ?")
    public void refreshTradeRankListSnapshotTask() throws Exception {
        Boolean locked = RedisLockUtils.tryLock(redisTemplate, REFRESH_TRADE_RANK_LIST_SNAPSHOT_TASK_V2, 9 * 60 * 1000);
        if (!locked) {
            return;
        }
        log.info(" **** TradeCompetition **** refreshTradeRankListSnapshotTask work start");
        List<TradeCompetition> tradeCompetitions = this.tradeCompetitionMapper.queryAll();
        if (CollectionUtils.isEmpty(tradeCompetitions)) {
            return;
        }

        tradeCompetitions.forEach(trade -> {
            log.info("refreshTradeRankListSnapshotTask trade {} ", new Gson().toJson(trade));
            //判断时间是否到期 如果到期则更新状态 不再更新快照
            if (trade.getEndTime().getTime() < System.currentTimeMillis()) {
                this.tradeCompetitionMapper.updateEndStatusByCode(trade.getOrgId(), trade.getCompetitionCode());
            } else {
                //判断是不是正向合约
                if (trade.getIsReverse().equals(0)) {
                    //普通单个合约
                    ((TradeCompetitionService) AopContext.currentProxy()).refreshTradeRankListSnapshot(trade);
                } else if (trade.getIsReverse().equals(1)) {
                    //正向合约
                    ((TradeCompetitionService) AopContext.currentProxy()).refreshForwardTradeRankListSnapshot(trade);
                } else if (trade.getIsReverse().equals(2)) {
                    //TODO 反向合约
                }
            }
        });
    }

    @Scheduled(cron = "0 1,11,21,31,41,51 * * * ?")
    public void refreshTradeRankListSnapshotDailyTask() throws Exception {
        Boolean locked = RedisLockUtils.tryLock(redisTemplate, REFRESH_TRADE_RANK_DAILY_LIST_SNAPSHOT_TASK, 9 * 60 * 1000);
        if (!locked) {
            return;
        }
        log.info(" **** TradeCompetition **** refreshTradeRankListSnapshotDailyTask work start");
        List<TradeCompetition> tradeCompetitions = this.tradeCompetitionMapper.queryAll();
        if (CollectionUtils.isEmpty(tradeCompetitions)) {
            return;
        }
        tradeCompetitions.forEach(trade -> {
            log.info("refreshTradeRankListSnapshotDailyTask trade {} ", new Gson().toJson(trade));
            //判断时间是否到期 如果到期则更新状态 不再更新快照
            if (trade.getEndTime().getTime() < System.currentTimeMillis()) {
                this.tradeCompetitionMapper.updateEndStatusByCode(trade.getOrgId(), trade.getCompetitionCode());
            } else {
                //每日统计
                if (trade.getIsReverse().equals(0)) {
                    //普通单个合约
                    ((TradeCompetitionService) AopContext.currentProxy()).refreshTradeRankListDailySnapshot(trade);
                } else if (trade.getIsReverse().equals(1)) {
                    //正向合约
                    ((TradeCompetitionService) AopContext.currentProxy()).refreshForwardTradeRankListDailySnapshot(trade);
                } else if (trade.getIsReverse().equals(2)) {
                    //TODO 反向合约
                }
            }
        });
    }

    @Transactional(rollbackFor = Throwable.class)
    public void refreshTradeRankListSnapshot(TradeCompetition tradeCompetition) {
        long startTime = System.currentTimeMillis();
        if (tradeCompetition == null || (tradeCompetition.getStartTime().getTime() > System.currentTimeMillis())) {
            return;
        }

        //获取交易大赛数据
        TradeCompetitionInfo info
                = getTradeCompetitionInfoByCache(tradeCompetition.getOrgId(), tradeCompetition.getCompetitionCode());
        if (info == null) {
            log.info("No find current trade competition orgId {} competitionCode {}", tradeCompetition.getOrgId(), tradeCompetition.getCompetitionCode());
            return;
        }

        //参赛名单 白名单数据
        Set<Long> whiteList = new HashSet<>(info.getEnterIdWhiteList());
        Set<Long> allList = queryAllUserList(info);
        if (org.apache.commons.collections4.CollectionUtils.isEmpty(allList)) {
            return;
        }

        //获取快照最新的批次号
        String newBatch = statisticsContractService.queryMaxTime(tradeCompetition.getOrgId(), tradeCompetition.getSymbolId());
        String batch = this.tradeCompetitionResultMapper.queryMaxBatchByCode(tradeCompetition.getOrgId(), tradeCompetition.getCompetitionCode());
        //如果批次号一致 则不需要重新计算更新
        if (StringUtils.isNotEmpty(batch)) {
            if (batch.equals(newBatch)) {
                log.info("batch is no difference local batch {} static batch {}", batch, newBatch);
                return;
            }
        }

        //获取最新参赛快照数据
        //快照列表
        List<ContractSnapshot> localList = statisticsContractService.queryNewContractSnapshot(tradeCompetition.getOrgId(), new LinkedList<>(allList), tradeCompetition.getSymbolId(), newBatch, null);
        log.info("contractSnapshot list {}", JSON.toJSONString(localList));
        if (org.apache.commons.collections4.CollectionUtils.isEmpty(localList)) {
            log.info("contractSnapshot list is null  orgId {} contractId {} batch {}", tradeCompetition.getOrgId(), tradeCompetition.getSymbolId(), newBatch);
            return;
        }

        //获取到所有的期货accountId 用户获取入账信息
        List<Long> accounts = localList.stream().map(ContractSnapshot::getAccountId).collect(Collectors.toList());
        if (org.apache.commons.collections4.CollectionUtils.isEmpty(accounts)) {
            log.info("futures accounts list is null {}", JSON.toJSONString(accounts));
            return;
        }
        //获取到期货的计价tokenId
        Symbol symbol = this.symbolMapper.getOrgSymbol(tradeCompetition.getOrgId(), tradeCompetition.getSymbolId());
        if (symbol == null || StringUtils.isEmpty(symbol.getQuoteTokenId())) {
            log.info("symbol is null orgId {} symbolId {}", tradeCompetition.getOrgId(), tradeCompetition.getSymbolId());
            throw new BrokerException(BrokerErrorCode.PARAM_INVALID);
        }

        Map<Long, BigDecimal> comeInbalanceMap = queryComeInbalanceMap(tradeCompetition.getOrgId(), accounts,
                new DateTime(tradeCompetition.getStartTime().getTime()).toString("yyyy-MM-dd HH:mm:ss"),
                new DateTime(tradeCompetition.getEndTime().getTime()).toString("yyyy-MM-dd HH:mm:ss"),
                symbol.getQuoteTokenId());

        Map<Long, BigDecimal> outgobalanceMap = queryOutgobalanceMap(tradeCompetition.getOrgId(), accounts,
                new DateTime(tradeCompetition.getStartTime().getTime()).toString("yyyy-MM-dd HH:mm:ss"),
                new DateTime(tradeCompetition.getEndTime().getTime()).toString("yyyy-MM-dd HH:mm:ss"),
                symbol.getQuoteTokenId());

        Map<String, String> listMap = queryTradeCompetitionLimitMap(info);
        List<TradeCompetitionResult> tradeCompetitionResultList = createTradeCompetitionResultList(localList, whiteList, allList, listMap, comeInbalanceMap, outgobalanceMap, tradeCompetition);
        if (org.apache.commons.collections4.CollectionUtils.isEmpty(tradeCompetitionResultList)) {
            log.info("last result list is null orgId {} competitionCode {}", tradeCompetition.getOrgId(), tradeCompetition.getCompetitionCode());
            return;
        }

        if (org.apache.commons.collections4.CollectionUtils.isNotEmpty(tradeCompetitionResultList)) {
            resultListHandel(tradeCompetitionResultList);
        }

        long endTime = System.currentTimeMillis();
        log.info("refreshTradeRankListSnapshot time {} userSize {} orgId {} competitionCode {}", (endTime - startTime), allList.size(), tradeCompetition.getOrgId(), tradeCompetition.getCompetitionCode());
    }

    private Set<Long> queryAllUserList(TradeCompetitionInfo info) {
        if (info != null) {
            Set<Long> enterList = new HashSet<>(info.getEnterIdList());
            Set<Long> whiteList = new HashSet<>(info.getEnterIdWhiteList());
            Set<Long> allList = new HashSet<>();
            allList.addAll(enterList);
            allList.addAll(whiteList);
            return allList;
        } else {
            return new HashSet<>();
        }
    }

    @Transactional(rollbackFor = Throwable.class)
    public void refreshForwardTradeRankListSnapshot(TradeCompetition tradeCompetition) {
        long startTime = System.currentTimeMillis();
        if (tradeCompetition == null || (tradeCompetition.getStartTime().getTime() > System.currentTimeMillis())) {
            log.info("No find current trade competition is null");
            return;
        }
        //获取交易大赛数据
        TradeCompetitionInfo info = getTradeCompetitionInfoByCache(tradeCompetition.getOrgId(), tradeCompetition.getCompetitionCode());
        if (info == null) {
            log.info("No find current trade competition orgId {} competitionCode {}", tradeCompetition.getOrgId(), tradeCompetition.getCompetitionCode());
            return;
        }

        //参赛名单 白名单数据
        Set<Long> whiteList = new HashSet<>(info.getEnterIdWhiteList());
        Set<Long> allList = queryAllUserList(info);
        if (org.apache.commons.collections4.CollectionUtils.isEmpty(allList)) {
            log.info("project code {} allList is null", tradeCompetition.getCompetitionCode());
            return;
        }

        //获取快照最新的批次号
        String newBatch = statisticsContractService.queryAllSymbolMaxTime(tradeCompetition.getOrgId());
        String batch = this.tradeCompetitionResultMapper.queryMaxBatchByCode(tradeCompetition.getOrgId(), tradeCompetition.getCompetitionCode());
        //如果批次号一致 则不需要重新计算更新
        if (StringUtils.isNotEmpty(batch)) {
            if (batch.equals(newBatch)) {
                log.info("batch is no difference local batch {} static batch {}", batch, newBatch);
                return;
            }
        }

        Map<String, BigDecimal> symbolIdList = queryForwardSymbolIdList(tradeCompetition.getOrgId());
        log.info("forward symbolIdList {}", new Gson().toJson(symbolIdList));
        //获取券商所有的正向合约最新参赛快照数据
        //快照列表
        List<ContractSnapshot> localList = statisticsContractService.queryALLForwardNewContractSnapshot(tradeCompetition.getOrgId(),
                new LinkedList<>(allList), new LinkedList<>(symbolIdList.keySet()), newBatch);

        if (org.apache.commons.collections4.CollectionUtils.isEmpty(localList)) {
            log.info("contractSnapshot list is null  orgId {} contractId {} batch {}", tradeCompetition.getOrgId(), symbolIdList, newBatch);
            return;
        }

        //获取到所有的期货accountId用户获取入账信息
        List<Long> accounts = localList.stream().map(ContractSnapshot::getAccountId).collect(Collectors.toList());
        if (org.apache.commons.collections4.CollectionUtils.isEmpty(accounts)) {
            log.info("futures accounts list is null {}", JSON.toJSONString(accounts));
            return;
        }

        Map<Long, BigDecimal> comeInbalanceMap = queryComeInbalanceMap(tradeCompetition.getOrgId(), accounts,
                new DateTime(tradeCompetition.getStartTime().getTime()).toString("yyyy-MM-dd HH:mm:ss"),
                new DateTime(tradeCompetition.getEndTime().getTime()).toString("yyyy-MM-dd HH:mm:ss"),
                "USDT");

        Map<Long, BigDecimal> outgobalanceMap = queryOutgobalanceMap(tradeCompetition.getOrgId(), accounts,
                new DateTime(tradeCompetition.getStartTime().getTime()).toString("yyyy-MM-dd HH:mm:ss"),
                new DateTime(tradeCompetition.getEndTime().getTime()).toString("yyyy-MM-dd HH:mm:ss"),
                "USDT");

        //获取交易大赛
        Map<String, String> listMap = queryTradeCompetitionLimitMap(info);
        //key accountId value 最新批次的快照列表
        Map<Long, List<ContractSnapshot>> contractSnapshotMap = localList.stream().collect(Collectors.groupingBy(ContractSnapshot::getAccountId, Collectors.toList()));
        createForwardTradeCompetitionResultList(
                contractSnapshotMap,
                whiteList,
                listMap,
                comeInbalanceMap,
                outgobalanceMap,
                tradeCompetition, symbolIdList, "", "", batch);
        long endTime = System.currentTimeMillis();
        log.info("refreshTradeRankListSnapshot time {} userSize {} orgId {} competitionCode {}", (endTime - startTime), allList.size(), tradeCompetition.getOrgId(), tradeCompetition.getCompetitionCode());
    }

    private Map<String, String> queryTradeCompetitionLimitMap(TradeCompetitionInfo info) {
        Map<String, String> limitMap = new HashMap<>();
        //门槛数据初始化
        if (StringUtils.isNotEmpty(info.getLimitStr())) {
            try {
                limitMap = JsonUtil.defaultGson().fromJson(info.getLimitStr(), Map.class);
            } catch (JsonSyntaxException e) {
                log.error("refreshTradeRankListSnapshot error: competitionCode {}, limit json {}", info.getCompetitionCode(), info.getLimitStr());
            }
        }
        return limitMap;
    }

    /**
     * 组装快照结果
     */
    private List<TradeCompetitionResult> createTradeCompetitionResultList(List<ContractSnapshot> localList,
                                                                          Set<Long> whiteList,
                                                                          Set<Long> allList,
                                                                          Map<String, String> listMap,
                                                                          Map<Long, BigDecimal> comeInbalanceMap,
                                                                          Map<Long, BigDecimal> outgobalanceMap,
                                                                          TradeCompetition tradeCompetition) {


        BigDecimal limit = queryFuturesPositionQuantity(listMap);

        //活动开始时间最近的一批数据
        String earliestBatch = this.statisticsContractService.queryMaxTimeByTime(tradeCompetition.getOrgId(),
                tradeCompetition.getSymbolId(), new DateTime(tradeCompetition.getStartTime().getTime()).toString("yyyy-MM-dd HH:mm:ss"));

        //快照列表
        List<ContractSnapshot> earliestList = statisticsContractService.queryNewContractSnapshot(tradeCompetition.getOrgId(), new LinkedList<>(allList), tradeCompetition.getSymbolId(), earliestBatch, limit.toPlainString());

        Map<Long, ContractSnapshot> snapshotMap = earliestList.stream().collect(Collectors.toMap(ContractSnapshot::getUserId, Function.identity()));

        List<TradeCompetitionResult> tradeCompetitionResultList = new ArrayList<>();
        try {
            for (int i = 0; i < localList.size(); i++) {
                ContractSnapshot s = localList.get(i);
                if (s == null) {
                    log.warn("createTradeCompetitionResultList,Hasn't the current snapshot,userId={}", s.getUserId());
                    continue;
                }

                //获取距离活动开始时间最近的快照
                ContractSnapshot contractSnapshot = snapshotMap.get(s.getUserId());
                boolean afterStart = false;
                //如果期初没有快照，则从离开始后最近时间取快照
                if (Objects.isNull(contractSnapshot)) {

                    LocalDateTime ldt = tradeCompetition.getStartTime().toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
                    String startTime = ldt.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

                    List<ContractSnapshot> tmp = statisticsContractService.queryListSnapshotByTime(tradeCompetition.getOrgId(), s.getAccountId(), tradeCompetition.getSymbolId(), startTime, limit);
                    if (org.apache.commons.collections4.CollectionUtils.isEmpty(tmp) || tmp.size() < 2) {
                        log.warn("createTradeCompetitionResultList,Hasn't the base snapshot,userId={}", s.getUserId());
                        continue;
                    }

                    contractSnapshot = tmp.get(0);
                    afterStart = true;
                }

                //是否白名单标志
                Boolean isWhite = whiteList.contains(s.getUserId());
                //并且持仓小于配置 不在白名单 则不存储计算结果
                if (listMap.size() > 0 && StringUtils.isNotEmpty(listMap.get(FUTURES_POSITION_QUANTITY))) {
                    if (s.getContractBalance().compareTo(new BigDecimal(listMap.get(FUTURES_POSITION_QUANTITY))) < 0 && !isWhite) {
                        log.info("Current user does not meet the restrictions userId {} balance {} limitBalance {}", s.getUserId(), s.getContractBalance(), listMap.get(FUTURES_POSITION_QUANTITY));
                        continue;
                    }
                }

                TradeCompetitionResult tradeCompetitionResult = buildTradeCompetitionResultBaseInfo(tradeCompetition, s);

                /** 计算成交率 start **/
                BigDecimal rate = BigDecimal.ZERO;
                //正值 累计转入量
                BigDecimal changedTotal;
                //负值 累计转出量
                BigDecimal changedTotalNegative = outgobalanceMap.get(s.getAccountId()) != null ? outgobalanceMap.get(s.getAccountId()) : BigDecimal.ZERO;

                changedTotal = comeInbalanceMap.get(s.getAccountId()) != null ? comeInbalanceMap.get(s.getAccountId()) : BigDecimal.ZERO;

                log.info("createTradeCompetitionResultList,code={},userId={}," +
                                "baseSnapshot={},currentSnapshot={},transferIn={},transOut={}",
                        tradeCompetition.getCompetitionCode(), s.getUserId(),
                        contractSnapshot, s, CommonUtil.bigDecimalToString(changedTotal), CommonUtil.bigDecimalToString(changedTotalNegative));


                //TODO 期货账户余额 只加一次 （累计转入量 - 累计转出量）
                //分子:(当前最新快照) = 期货账户余额 + 未实现盈亏 - （累计转入量 - 累计转出量）- （初始快照账户余额 + 初始快照未实现盈亏)
                BigDecimal numerator = null;
                if (afterStart) {

                    BigDecimal tmp = BigDecimal.ZERO;
                    if (changedTotal.compareTo(contractSnapshot.getContractBalance()) > 0) {
                        tmp = changedTotal.subtract(changedTotalNegative.abs());
                    } else {
                        tmp = contractSnapshot.getContractBalance().add(contractSnapshot.getUnrealizedPnl());
                    }

                    numerator = s.getContractBalance()
                            .add(s.getUnrealizedPnl())
                            .subtract(tmp)
                            .setScale(18, RoundingMode.DOWN);
                } else {
                    numerator = s.getContractBalance()
                            .add(s.getUnrealizedPnl())
                            .subtract(changedTotal.subtract(changedTotalNegative.abs()))
                            .subtract(contractSnapshot.getContractBalance().add(contractSnapshot.getUnrealizedPnl()))
                            .setScale(18, RoundingMode.DOWN);
                }

                //TODO 期货账户余额 累计转入量 只加1次
                //分母计算:活动开始时间最近的快照 = (期货账户余额 + 未实现盈亏 + 累计转入量)
                BigDecimal denominator = BigDecimal.ZERO;
                if (afterStart) {
                    BigDecimal tmp = contractSnapshot.getContractBalance();
                    if (changedTotal.compareTo(tmp) > 0) {
                        tmp = changedTotal;
                    }

                    denominator = tmp.add(contractSnapshot.getUnrealizedPnl())
                            .setScale(18, RoundingMode.DOWN);
                } else {
                    denominator = contractSnapshot.getContractBalance()
                            .add(changedTotal)
                            .add(contractSnapshot.getUnrealizedPnl())
                            .setScale(18, RoundingMode.DOWN);
                }

                if (denominator != null && denominator.compareTo(BigDecimal.ZERO) != 0) {
                    rate = numerator.divide(denominator, 18, RoundingMode.DOWN);
                }
                /** 计算成交率 end **/

                log.info("createTradeCompetitionResultList,code={},userId={}," +
                                "baseSnapshot={},currentSnapshot={},transferIn={},transOut={}" +
                                "afterStart={}",
                        tradeCompetition.getCompetitionCode(), s.getUserId(),
                        contractSnapshot, s, CommonUtil.bigDecimalToString(changedTotal),
                        CommonUtil.bigDecimalToString(changedTotalNegative),
                        afterStart
                );

                //冗余原始数据
                //收益金额
                tradeCompetitionResult.setIncomeAmount(numerator.setScale(18, RoundingMode.DOWN));
                tradeCompetitionResult.setRate(rate.setScale(18, RoundingMode.DOWN));
                tradeCompetitionResult.setChangedTotal(changedTotal);
                tradeCompetitionResult.setCurrentContractBalance(s.getContractBalance() != null ? s.getContractBalance().setScale(18, RoundingMode.DOWN) : BigDecimal.ZERO);
                tradeCompetitionResult.setCurrentUnrealizedPnl(s.getUnrealizedPnl() != null ? s.getUnrealizedPnl().setScale(18, RoundingMode.DOWN) : BigDecimal.ZERO);
                tradeCompetitionResult.setStartBatch(contractSnapshot.getSnapshotTime());
                tradeCompetitionResult.setStartContractBalance(contractSnapshot.getContractBalance() != null ? contractSnapshot.getContractBalance().setScale(18, RoundingMode.DOWN) : BigDecimal.ZERO);
                tradeCompetitionResult.setStartUnrealizedPnl(contractSnapshot.getUnrealizedPnl() != null ? contractSnapshot.getUnrealizedPnl().setScale(18, RoundingMode.DOWN) : BigDecimal.ZERO);
                tradeCompetitionResult.setChangedTotalNegative(changedTotalNegative);
                tradeCompetitionResult.setCurrentTradeAmount(BigDecimal.ZERO);
                tradeCompetitionResult.setCurrentTradeAmountTokenId("");
                tradeCompetitionResultList.add(tradeCompetitionResult);
            }
        } catch (Exception ex) {
            log.error("refreshTradeRankListSnapshot error orgId {},code {} error {}", tradeCompetition.getOrgId(), tradeCompetition.getCompetitionCode(), ex);
        }
        return tradeCompetitionResultList;
    }


    private BigDecimal queryFuturesPositionQuantity(Map<String, String> listMap) {
        BigDecimal limit = BigDecimal.ZERO;
        if (listMap.size() > 0 && StringUtils.isNotEmpty(listMap.get(FUTURES_POSITION_QUANTITY))) {
            try {
                limit = new BigDecimal(listMap.get(FUTURES_POSITION_QUANTITY));
            } catch (Exception e) {
                log.warn(e.getMessage(), e);
            }
        }
        return limit;
    }

    /**
     * 计算正向合约大赛
     */
    private void createForwardTradeCompetitionResultList(Map<Long, List<ContractSnapshot>> contractSnapshotMap,
                                                         Set<Long> whiteList,
                                                         Map<String, String> listMap,
                                                         Map<Long, BigDecimal> comeInbalanceMap,
                                                         Map<Long, BigDecimal> outgobalanceMap,
                                                         TradeCompetition tradeCompetition,
                                                         Map<String, BigDecimal> symbolIdList,
                                                         String dayTime,
                                                         String startDate,
                                                         String batch) {
        try {
            List<TradeCompetitionResult> tradeCompetitionResultList = new ArrayList<>();
            List<TradeCompetitionResultDaily> tradeCompetitionResultDailyList = new ArrayList<>();
            //key accountId ,value 快照list
            contractSnapshotMap.keySet().forEach(k -> {
                long calculateStartTime = System.currentTimeMillis();
                List<ContractSnapshot> contractSnapshotList = contractSnapshotMap.get(k);
                if (CollectionUtils.isEmpty(contractSnapshotList)) {
                    log.info("not find current snapshot accountId {}", k);
                    return;
                }
                ContractSnapshot currentAnySnapshot = contractSnapshotList.get(0);
                log.info("createForwardTradeCompetitionResultList accountId {} currentAnySnapshot {}", k, new Gson().toJson(currentAnySnapshot));
                Boolean isInvalid = Boolean.TRUE;
                //结果都计算出来 方便后续排查问题 查询的地方改造 按门槛条件来过滤合格数据
                //是否白名单标志
                Boolean isWhite = whiteList.contains(currentAnySnapshot.getUserId());
                //并且持仓小于配置 不在白名单 则不存储计算结果
                if (listMap.size() > 0 && StringUtils.isNotEmpty(listMap.get(FUTURES_POSITION_QUANTITY))) {
                    if (currentAnySnapshot.getContractBalance().compareTo(new BigDecimal(listMap.get(FUTURES_POSITION_QUANTITY))) < 0 && !isWhite) {
                        isInvalid = Boolean.FALSE;
                    }
                }

                //当前的最新快照的合约账户余额
                BigDecimal currentBalance = currentAnySnapshot.getContractBalance();
                //当前未实现盈亏累加
                BigDecimal currentUnrealizedPnl = new BigDecimal(contractSnapshotList.stream().mapToDouble(s -> s.getUnrealizedPnl().doubleValue()).sum());
                //负值 累计转出量
                BigDecimal changedTotalNegative = outgobalanceMap.get(k) != null ? outgobalanceMap.get(k) : BigDecimal.ZERO;
                //正值 累计转入量
                BigDecimal changedTotal = comeInbalanceMap.get(k) != null ? comeInbalanceMap.get(k) : BigDecimal.ZERO;

                //初始快照账户余额
                BigDecimal beforeBalance = BigDecimal.ZERO;
                BigDecimal beforeUnrealizedPnl = BigDecimal.ZERO;
                String beforeBatch = "";

                String startTime;
                if (StringUtils.isNotEmpty(startDate)) {
                    startTime = startDate;
                } else {
                    startTime = tradeCompetition.getStartTime().toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                }

                //获取距离最近的一个批次 然后获取当前用户这个批次的正向币对的数据
                List<ContractSnapshot> timeList = statisticsContractService.queryNearestContractSnapshotByTime(tradeCompetition.getOrgId(), k, new LinkedList<>(symbolIdList.keySet()), startTime, true);
                if (org.apache.commons.collections4.CollectionUtils.isEmpty(timeList)) {
                    log.info("createForwardTradeCompetitionResultList no latest snapshot data found");
                    return;
                }

                String snapshotTime = timeList.get(0).getSnapshotTime();
                List<ContractSnapshot> nearestList = statisticsContractService.queryNearestContractSnapshotByTime(tradeCompetition.getOrgId(), k, new LinkedList<>(symbolIdList.keySet()), startTime, true);
                if (org.apache.commons.collections4.CollectionUtils.isEmpty(nearestList)) {
                    log.info("accountId {} snapshotTime {} not find nearestList !!!", k, snapshotTime);
                    return;
                }
                beforeBatch = snapshotTime;
                beforeBalance = nearestList.get(0).getContractBalance();
                log.info("createForwardTradeCompetitionResultList beforeBalance accountId {} info {}", k, new Gson().toJson(beforeBalance));
                beforeUnrealizedPnl = new BigDecimal(nearestList.stream().mapToDouble(s -> s.getUnrealizedPnl().doubleValue()).sum());
                Boolean midway = Boolean.FALSE;
                //获取距离最近的一个批次 然后获取当前用户这个批次的正向币对的数据
                List<ContractSnapshot> nearestTimeList = statisticsContractService.queryNearestContractLessTime(k, new LinkedList<>(symbolIdList.keySet()), startTime, false);
                if (org.apache.commons.collections4.CollectionUtils.isNotEmpty(nearestTimeList)) {
                    ContractSnapshot contractSnapshot = nearestTimeList.get(0);
                    if (contractSnapshot != null && contractSnapshot.getContractBalance().compareTo(BigDecimal.ONE) >= 0) {
                        midway = Boolean.TRUE;
                    }
                }
                log.info("createForwardTradeCompetitionResultList beforeBalance accountId {} midway {} info {}", k, midway, new Gson().toJson(beforeBalance));
                /** 计算成交率 start **/
                BigDecimal numerator;
                //非中途参赛收益率计算
                if (midway) {
                    numerator = currentBalance
                            .add(currentUnrealizedPnl)
                            .subtract(changedTotal.subtract(changedTotalNegative.abs()))
                            .subtract(beforeBalance.add(beforeUnrealizedPnl))
                            .setScale(18, RoundingMode.DOWN);
                } else {
                    //中途参赛收益率计算
                    BigDecimal tmp;
                    if (changedTotal.compareTo(beforeBalance) > 0) {
                        tmp = changedTotal.subtract(changedTotalNegative.abs());
                    } else {
                        tmp = beforeBalance.add(beforeUnrealizedPnl);
                    }
                    numerator = currentBalance
                            .add(currentUnrealizedPnl)
                            .subtract(tmp)
                            .setScale(18, RoundingMode.DOWN);
                }

                //分母计算:活动开始时间最近的快照 = (期货账户余额 + 未实现盈亏 + 累计转入量)
                BigDecimal denominator;
                if (midway) {
                    denominator = beforeBalance
                            .add(changedTotal)
                            .add(beforeUnrealizedPnl)
                            .setScale(18, RoundingMode.DOWN);
                } else {
                    BigDecimal tmp = beforeBalance;
                    if (changedTotal.compareTo(tmp) > 0) {
                        tmp = changedTotal;
                    }
                    denominator = tmp.add(beforeUnrealizedPnl).setScale(18, RoundingMode.DOWN);
                }

                BigDecimal rate = BigDecimal.ZERO;
                if (denominator != null && denominator.compareTo(BigDecimal.ZERO) != 0) {
                    rate = numerator.divide(denominator, 18, RoundingMode.DOWN);
                }
                long calculateEndTime = System.currentTimeMillis();
                log.info("Trading contest result accountId {} currentBalance {} changedTotal {} beforeBalance {} changedTotalNegative {} currentUnrealizedPnl {} beforeUnrealizedPnl {} numerator {} denominator {} midway {} rate {} time {} ",
                        k, currentBalance, changedTotal, beforeBalance, changedTotalNegative, currentUnrealizedPnl, beforeUnrealizedPnl, numerator, denominator, midway, rate, (calculateEndTime - calculateStartTime));

                /** 计算成交率 end **/
                TradeCompetitionResult tradeCompetitionResult = buildTradeCompetitionResultBaseInfo(tradeCompetition, currentAnySnapshot);
                tradeCompetitionResult.setIncomeAmount(numerator.setScale(18, RoundingMode.DOWN));
                tradeCompetitionResult.setRate(rate.setScale(18, RoundingMode.DOWN));
                tradeCompetitionResult.setChangedTotal(changedTotal);
                tradeCompetitionResult.setChangedTotalNegative(changedTotalNegative);
                tradeCompetitionResult.setCurrentContractBalance(currentBalance != null ? currentBalance.setScale(18, RoundingMode.DOWN) : BigDecimal.ZERO);
                tradeCompetitionResult.setCurrentUnrealizedPnl(currentUnrealizedPnl != null ? currentUnrealizedPnl.setScale(18, RoundingMode.DOWN) : BigDecimal.ZERO);
                tradeCompetitionResult.setStartBatch(beforeBatch);
                tradeCompetitionResult.setStartContractBalance(beforeBalance != null ? beforeBalance.setScale(18, RoundingMode.DOWN) : BigDecimal.ZERO);
                tradeCompetitionResult.setStartUnrealizedPnl(beforeUnrealizedPnl != null ? beforeUnrealizedPnl.setScale(18, RoundingMode.DOWN) : BigDecimal.ZERO);

                //获取交易量
                List<StatisticsTradeAllDetail> tradeAllDetailList = statisticsTradeService.queryTradeAllDetailByStartTime(tradeCompetition.getOrgId(),
                        currentAnySnapshot.getAccountId(), new LinkedList<>(symbolIdList.keySet()), startTime);

                BigDecimal tradeAmount = BigDecimal.ZERO;
                if (org.apache.commons.collections4.CollectionUtils.isNotEmpty(tradeAllDetailList)) {
                    tradeAmount = new BigDecimal(tradeAllDetailList.stream().mapToDouble(s -> s.getAmount().multiply(symbolIdList.get(s.getSymbolId())).doubleValue()).sum());
                }
                tradeCompetitionResult.setCurrentTradeAmount(tradeAmount);
                tradeCompetitionResult.setCurrentTradeAmountTokenId("USDT");

                TradeCompetitionResultDaily competitionResultDaily = new TradeCompetitionResultDaily();
                if (StringUtils.isNotEmpty(startDate)) {
                    BeanUtils.copyProperties(tradeCompetitionResult, competitionResultDaily);
                    //天排行榜判断成交量是否符合标准
                    if (listMap.size() > 0 && StringUtils.isNotEmpty(listMap.get(DAY_TRADE_NUMBER))) {
                        if (tradeCompetitionResult.getCurrentTradeAmount().compareTo(new BigDecimal(listMap.get(DAY_TRADE_NUMBER))) < 0 && !isWhite) {
                            isInvalid = Boolean.FALSE;
                        }
                    }
                    if (!isInvalid) {
                        competitionResultDaily.setStatus(0);
                    }
                    competitionResultDaily.setDay(dayTime);
                    tradeCompetitionResultDailyList.add(competitionResultDaily);
                } else {
                    //排行榜判断成交量是否符合标准
                    if (listMap.size() > 0 && StringUtils.isNotEmpty(listMap.get(ALL_TRADE_NUMBER))) {
                        if (tradeCompetitionResult.getCurrentTradeAmount().compareTo(new BigDecimal(listMap.get(ALL_TRADE_NUMBER))) < 0 && !isWhite) {
                            isInvalid = Boolean.FALSE;
                        }
                    }
                    if (!isInvalid) {
                        tradeCompetitionResult.setStatus(0);
                    }
                    tradeCompetitionResultList.add(tradeCompetitionResult);
                }

                if (org.apache.commons.collections4.CollectionUtils.isEmpty(tradeCompetitionResultList) && org.apache.commons.collections4.CollectionUtils.isEmpty(tradeCompetitionResultDailyList)) {
                    log.info("last result list is null orgId {} competitionCode {}", tradeCompetition.getOrgId(), tradeCompetition.getCompetitionCode());
                    return;
                }

                if (org.apache.commons.collections4.CollectionUtils.isNotEmpty(tradeCompetitionResultList)) {
                    resultListHandel(tradeCompetitionResultList);
                }
                if (org.apache.commons.collections4.CollectionUtils.isNotEmpty(tradeCompetitionResultDailyList)) {
                    dailyListResultHandel(tradeCompetition, tradeCompetitionResultDailyList, batch, dayTime);
                }
            });
        } catch (Exception ex) {
            log.info("createForwardTradeCompetitionResultList fail {}", ex);
        }
    }

    private TradeCompetitionResult buildTradeCompetitionResultBaseInfo(TradeCompetition tradeCompetition, ContractSnapshot contractSnapshot) {
        TradeCompetitionResult tradeCompetitionResult = new TradeCompetitionResult();
        tradeCompetitionResult.setClientTransferId(sequenceGenerator.getLong());
        tradeCompetitionResult.setAccountId(contractSnapshot.getAccountId());
        tradeCompetitionResult.setBatch(contractSnapshot.getSnapshotTime());
        tradeCompetitionResult.setCompetitionCode(tradeCompetition.getCompetitionCode());
        tradeCompetitionResult.setCompetitionId(tradeCompetition.getId());
        tradeCompetitionResult.setOrgId(tradeCompetition.getOrgId());
        tradeCompetitionResult.setUserId(contractSnapshot.getUserId());
        tradeCompetitionResult.setReceiveTokenId(tradeCompetition.getReceiveTokenId());
        tradeCompetitionResult.setReceiveQuantity(BigDecimal.ZERO);
        tradeCompetitionResult.setTransferStatus(0);
        tradeCompetitionResult.setStatus(1);
        return tradeCompetitionResult;
    }

    private void resultListHandel(List<TradeCompetitionResult> resultList) {
        long saveStartTime = System.currentTimeMillis();
        if (org.apache.commons.collections4.CollectionUtils.isNotEmpty(resultList)) {
            //清理历史数据
            resultList.forEach(result -> {
                log.info("resultListHandel userId {} accountId {} result {}", result.getUserId(), result.getAccountId(), new Gson().toJson(result));
                TradeCompetitionResult competitionResult
                        = tradeCompetitionResultMapper.queryByUserIdAndBatch(result.getOrgId(), result.getCompetitionCode(), result.getUserId(), result.getBatch());
                if (competitionResult != null) {
                    result.setId(competitionResult.getId());
                    result.setUpdateTime(new Date());
                    tradeCompetitionResultMapper.updateByPrimaryKeySelective(result);
                } else {
                    result.setCreateTime(new Date());
                    result.setUpdateTime(new Date());
                    try {
                        tradeCompetitionResultMapper.insertSelective(result);
                    } catch (DuplicateKeyException e) {
                        log.warn("Insert result repeated userId:{} code:{} batch",
                                result.getUserId(), result.getCompetitionCode(), result.getBatch());
                    }
                }
            });
        }
        long saveEndTime = System.currentTimeMillis();
        log.info("save competition result use time {}", (saveEndTime - saveStartTime));
    }

    private void dailyListResultHandel(TradeCompetition tradeCompetition, List<TradeCompetitionResultDaily> dailyList, String batch, String dayTime) {
        if (org.apache.commons.collections4.CollectionUtils.isEmpty(dailyList)) {
            log.info("last result list is null orgId {} competitionCode {}", tradeCompetition.getOrgId(), tradeCompetition.getCompetitionCode());
            return;
        }
        long saveStartTime = System.currentTimeMillis();
        //清理历史数据
        dailyList.forEach(result -> {
            TradeCompetitionResultDaily competitionResult
                    = tradeCompetitionResultDailyMapper.queryByUserIdAndBatch(result.getOrgId(), result.getCompetitionCode(), result.getUserId(), result.getBatch(), dayTime);
            if (competitionResult != null) {
                result.setId(competitionResult.getId());
                result.setUpdateTime(new Date());
                tradeCompetitionResultDailyMapper.updateByPrimaryKeySelective(result);
            } else {
                result.setCreateTime(new Date());
                result.setUpdateTime(new Date());
                try {
                    tradeCompetitionResultDailyMapper.insertSelective(result);
                } catch (DuplicateKeyException e) {
                    log.warn("Insert result repeated userId:{} code:{} batch {} day {} ",
                            result.getUserId(), result.getCompetitionCode(), result.getBatch(), result.getDay());
                }
            }
        });
        long saveEndTime = System.currentTimeMillis();
        log.info("save daily competition result use time {}", (saveEndTime - saveStartTime));
    }

    public void releaseTradeRankListSnapshotKey() {
        RedisLockUtils.releaseLock(redisTemplate, REFRESH_TRADE_RANK_DAILY_LIST_SNAPSHOT_TASK);
        RedisLockUtils.releaseLock(redisTemplate, REFRESH_TRADE_RANK_LIST_SNAPSHOT_TASK_V2);
    }


    @Transactional(rollbackFor = Throwable.class)
    public void refreshTradeRankListDailySnapshot(TradeCompetition tradeCompetition) {

        if (tradeCompetition.getStartTime().getTime() > System.currentTimeMillis()) {
            return;
        }

        long startTime = System.currentTimeMillis();
        if (tradeCompetition == null) {
            log.info("No find current trade competition is null");
            return;
        }

        Pair<LocalDateTime, LocalDateTime> startEnd = getStatisticsDay(tradeCompetition.getStartTime());
        //day格式 2019-11-13~2019-11-14
        String day = getStatisticsDuration(startEnd.getKey(), startEnd.getValue());
        log.info("day {},code={}", day, tradeCompetition.getCompetitionCode());

        //清理多余的冗余数据
        String batch
                = this.tradeCompetitionResultDailyMapper.queryMaxBatchByCode(tradeCompetition.getOrgId(), tradeCompetition.getCompetitionCode(), day);

        //获取交易大赛数据
        TradeCompetitionInfo info
                = getTradeCompetitionInfoByCache(tradeCompetition.getOrgId(), tradeCompetition.getCompetitionCode());
        if (info == null) {
            log.info("No find current trade competition orgId {} competitionCode {}", tradeCompetition.getOrgId(), tradeCompetition.getCompetitionCode());
            return;
        }

        //参赛名单 白名单数据
        Set<Long> enterList = Sets.newHashSet(info.getEnterIdList());
        Set<Long> whiteList = Sets.newHashSet(info.getEnterIdWhiteList());
        Set<Long> allList = Sets.newHashSet();
        allList.addAll(enterList);
        allList.addAll(whiteList);
        log.info("all list {}", JSON.toJSONString(allList));

        //获取快照最新的批次号
        String newBatch = statisticsContractService.queryMaxTime(tradeCompetition.getOrgId(), tradeCompetition.getSymbolId());
        log.info("newBatch {},code={}", newBatch, tradeCompetition.getCompetitionCode());

        //如果批次号一致 则不需要重新计算更新
        if (StringUtils.isNotEmpty(batch)) {
            if (batch.equals(newBatch)) {
                log.info("batch is no difference local batch {} static batch {}", batch, newBatch);
                return;
            }
        }

        if (org.apache.commons.collections4.CollectionUtils.isEmpty(allList)) {
            return;
        }

        //获取最新参赛快照数据
        //快照列表
        List<ContractSnapshot> localList = statisticsContractService.queryNewContractSnapshot(tradeCompetition.getOrgId(), new LinkedList<>(allList),
                tradeCompetition.getSymbolId(), newBatch, null);

        log.info("contractSnapshot list {}", JSON.toJSONString(localList));
        if (org.apache.commons.collections4.CollectionUtils.isEmpty(localList)) {
            log.info("contractSnapshot list is null  orgId {} contractId {} batch {}", tradeCompetition.getOrgId(), tradeCompetition.getSymbolId(), newBatch);
            return;
        }

        //获取到所有的期货accountId 用户获取入账信息
        List<Long> accounts = localList.stream().map(ContractSnapshot::getAccountId).collect(Collectors.toList());
        if (org.apache.commons.collections4.CollectionUtils.isEmpty(accounts)) {
            log.info("futures accounts list is null {}", JSON.toJSONString(accounts));
            return;
        }
        //获取到期货的计价tokenId
        Symbol symbol = this.symbolMapper.getOrgSymbol(tradeCompetition.getOrgId(), tradeCompetition.getSymbolId());
        if (symbol == null || StringUtils.isEmpty(symbol.getQuoteTokenId())) {
            log.info("symbol is null orgId {} symbolId {}", tradeCompetition.getOrgId(), tradeCompetition.getSymbolId());
            throw new BrokerException(BrokerErrorCode.PARAM_INVALID);
        }

        String start = startEnd.getLeft().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        String end = startEnd.getRight().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

        //查询每天的资金流水
        List<BalanceFlowSnapshot> comeInbalanceList = this.statisticsBalanceService
                .queryComeInBalanceFlow(tradeCompetition.getOrgId(), symbol.getQuoteTokenId(),
                        accounts,
                        start, end,
                        BusinessSubject.USER_ACCOUNT_TRANSFER_VALUE);

        log.info("comein balance list {}", JSON.toJSONString(comeInbalanceList));

        List<BalanceFlowSnapshot> outgobalanceList = this.statisticsBalanceService
                .queryOutgoBalanceFlow(tradeCompetition.getOrgId(), symbol.getQuoteTokenId(),
                        accounts,
                        start, end,
                        BusinessSubject.USER_ACCOUNT_TRANSFER_VALUE);

        log.info("outgo balance list {}", JSON.toJSONString(comeInbalanceList));

        Map<Long, BigDecimal> comeInbalanceMap = comeInbalanceList
                .stream().collect(Collectors.toMap(BalanceFlowSnapshot::getAccountId, BalanceFlowSnapshot::getChanged));

        Map<Long, BigDecimal> outgobalanceMap = outgobalanceList
                .stream().collect(Collectors.toMap(BalanceFlowSnapshot::getAccountId, BalanceFlowSnapshot::getChanged));

        Map<String, String> listMap = new HashMap<>();
        //门槛数据初始化
        if (StringUtils.isNotEmpty(info.getLimitStr())) {
            try {
                listMap = JsonUtil.defaultGson().fromJson(info.getLimitStr(), Map.class);
            } catch (JsonSyntaxException e) {
                log.error("refreshTradeRankListSnapshot error: competitionCode {}, limit json {}", info.getCompetitionCode(), info.getLimitStr());
            }
        }

        List<TradeCompetitionResultDaily> tradeCompetitionResultList = buildListOfTradeCompetitionResultDaily(localList, whiteList, allList, listMap,
                comeInbalanceMap, outgobalanceMap, tradeCompetition, day, start, end);

        if (org.apache.commons.collections4.CollectionUtils.isNotEmpty(tradeCompetitionResultList)) {
            //存储结果
            dailyListResultHandel(tradeCompetition, tradeCompetitionResultList, batch, day);
        }
        long endTime = System.currentTimeMillis();

        log.info("refreshTradeRankListDailySnapshot consume mills {}, userSize {}, orgId {}, competitionCode {}", (endTime - startTime), allList.size(), tradeCompetition.getOrgId(), tradeCompetition.getCompetitionCode());
    }

    private Map<String, BigDecimal> queryForwardSymbolIdList(Long orgId) {
        List<Integer> categories = Collections.singletonList(TokenCategory.FUTURE_CATEGORY.getNumber());
        List<Symbol> symbolList = this.symbolMapper.queryOrgSymbols(orgId, categories);
        if (org.apache.commons.collections4.CollectionUtils.isEmpty(symbolList)) {
            return new HashMap<>();
        }

        //获取最新参赛快照数据
        List<SymbolDetail> symbolDetailList = new ArrayList<>();
        symbolList.forEach(symbol -> {
            SymbolDetail symbolDetail = basicService.getSymbolDetailFutures(symbol.getExchangeId(), symbol.getSymbolId());
            if (symbolDetail != null && !symbolDetail.getSymbolId().equalsIgnoreCase("BTC-PERP-BUSDT")) {
                symbolDetailList.add(symbolDetail);
            }
        });

        if (org.apache.commons.collections4.CollectionUtils.isEmpty(symbolDetailList)) {
            return new HashMap<>();
        }

        List<SymbolDetail> newSymbolIdList = symbolDetailList.stream().filter(s -> s.getIsReverse() == Boolean.FALSE).collect(Collectors.toList());
        Map<String, BigDecimal> symbolIdMap = newSymbolIdList.stream().collect(Collectors.toMap(i -> i.getSymbolId(), i -> new BigDecimal(i.getFuturesMultiplier().getStr())));
        if (symbolIdMap.size() == 0) {
            return new HashMap<>();
        }
        return symbolIdMap;
    }

    @Transactional(rollbackFor = Throwable.class)
    public void refreshForwardTradeRankListDailySnapshot(TradeCompetition tradeCompetition) {
        if (tradeCompetition == null || tradeCompetition.getStartTime().getTime() > System.currentTimeMillis()) {
            return;
        }

        long startTime = System.currentTimeMillis();
        Pair<LocalDateTime, LocalDateTime> startEnd = getStatisticsDay(tradeCompetition.getStartTime());
        //day格式 2019-11-13~2019-11-14
        String day = getStatisticsDuration(startEnd.getKey(), startEnd.getValue());
        log.info("day {},code={}", day, tradeCompetition.getCompetitionCode());

        //获取交易大赛数据
        TradeCompetitionInfo info
                = getTradeCompetitionInfoByCache(tradeCompetition.getOrgId(), tradeCompetition.getCompetitionCode());
        if (info == null) {
            log.info("No find current trade competition orgId {} competitionCode {}", tradeCompetition.getOrgId(), tradeCompetition.getCompetitionCode());
            return;
        }

        //参赛名单 白名单数据
        Set<Long> whiteList = Sets.newHashSet(info.getEnterIdWhiteList());
        Set<Long> allList = queryAllUserList(info);
        if (org.apache.commons.collections4.CollectionUtils.isEmpty(allList)) {
            log.info("daily project code {} All list is null ", tradeCompetition.getCompetitionCode());
            return;
        }

        //获取快照最新的批次号
        String newBatch = statisticsContractService.queryAllSymbolMaxTime(tradeCompetition.getOrgId());
        String batch = this.tradeCompetitionResultDailyMapper.queryMaxBatchByCode(tradeCompetition.getOrgId(), tradeCompetition.getCompetitionCode(), day);
        //如果批次号一致 则不需要重新计算更新
        if (StringUtils.isNotEmpty(batch)) {
            if (batch.equals(newBatch)) {
                log.info("batch is no difference local batch {} static batch {}", batch, newBatch);
                return;
            }
        }

        Map<String, BigDecimal> symbolIdList = queryForwardSymbolIdList(tradeCompetition.getOrgId());
        log.info("forward symbolIdList {}", new Gson().toJson(symbolIdList));
        if (symbolIdList.size() == 0) {
            log.info("forward symbolIdList is null");
            return;
        }
        //获取最新参赛快照数据
        Example multiLangExp = Example.builder(ContractSnapshot.class).build();
        multiLangExp.createCriteria()
                .andIn("userId", allList)
                .andIn("contractId", symbolIdList.keySet())//所有的正向
                .andEqualTo("snapshotTime", newBatch);
        //快照列表
        List<ContractSnapshot> localList = statisticsContractService.queryALLForwardNewContractSnapshot(tradeCompetition.getOrgId(),
                new LinkedList<>(allList), new LinkedList<>(symbolIdList.keySet()), newBatch);
        if (org.apache.commons.collections4.CollectionUtils.isEmpty(localList)) {
            log.info("contractSnapshot list is null  orgId {} contractId {} batch {}", tradeCompetition.getOrgId(), tradeCompetition.getSymbolId(), newBatch);
            return;
        }

        //获取到所有的期货accountId 用户获取入账信息
        List<Long> accounts = localList.stream().map(ContractSnapshot::getAccountId).collect(Collectors.toList());
        if (org.apache.commons.collections4.CollectionUtils.isEmpty(accounts)) {
            log.info("futures accounts list is null {}", JSON.toJSONString(accounts));
            return;
        }

        String start = startEnd.getLeft().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        String end = startEnd.getRight().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        Map<Long, BigDecimal> comeInbalanceMap = queryComeInbalanceMap(tradeCompetition.getOrgId(), accounts, start, end, "USDT");
        Map<Long, BigDecimal> outgobalanceMap = queryOutgobalanceMap(tradeCompetition.getOrgId(), accounts, start, end, "USDT");

        Map<String, String> listMap = queryTradeCompetitionLimitMap(info);
        //key accountId value 最新批次的快照列表
        Map<Long, List<ContractSnapshot>> contractSnapshotMap = localList.stream().collect(Collectors.groupingBy(ContractSnapshot::getAccountId, Collectors.toList()));
        createForwardTradeCompetitionResultList(contractSnapshotMap, whiteList, listMap,
                comeInbalanceMap, outgobalanceMap, tradeCompetition, symbolIdList, day, start, batch);

        long endTime = System.currentTimeMillis();
        log.info("refreshTradeRankListDailySnapshot consume mills {}, userSize {}, orgId {}, competitionCode {}", (endTime - startTime), allList.size(), tradeCompetition.getOrgId(), tradeCompetition.getCompetitionCode());
    }

    /**
     * 获取转入流水
     */
    private Map<Long, BigDecimal> queryComeInbalanceMap(Long orgId, List<Long> accounts, String startTime, String endTime, String tokenId) {
        //查询每天的资金流水
        List<BalanceFlowSnapshot> comeInbalanceList = this.statisticsBalanceService
                .queryComeInBalanceFlow(orgId, tokenId,
                        accounts,
                        startTime, endTime,
                        BusinessSubject.USER_ACCOUNT_TRANSFER_VALUE);

        return comeInbalanceList
                .stream().collect(Collectors.toMap(BalanceFlowSnapshot::getAccountId, BalanceFlowSnapshot::getChanged));
    }

    /**
     * 获取转出流水
     */
    private Map<Long, BigDecimal> queryOutgobalanceMap(Long orgId, List<Long> accounts, String startTime, String endTime, String tokenId) {
        List<BalanceFlowSnapshot> outgobalanceList = this.statisticsBalanceService
                .queryOutgoBalanceFlow(orgId, tokenId,
                        accounts,
                        startTime, endTime,
                        BusinessSubject.USER_ACCOUNT_TRANSFER_VALUE);

        return outgobalanceList
                .stream().collect(Collectors.toMap(BalanceFlowSnapshot::getAccountId, BalanceFlowSnapshot::getChanged));
    }


    public Pair<LocalDateTime, LocalDateTime> getStatisticsDay(Date startTime) {
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime ldt = startTime.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
        LocalTime lt = ldt.toLocalTime();

        LocalDateTime start, end;
        //当前时间部分小于开始时间部分，则属于上一天统计范围，否则属于下一天统计范围
        if (now.toLocalTime().compareTo(lt) > 0) {
            start = LocalDateTime.of(now.toLocalDate(), lt);
            end = LocalDateTime.of(now.toLocalDate().plusDays(1), lt);
        } else {

            start = LocalDateTime.of(now.toLocalDate().minusDays(1), lt);
            end = LocalDateTime.of(now.toLocalDate(), lt);
        }

        return Pair.of(start, end);
    }

    public String getStatisticsDuration(LocalDateTime start, LocalDateTime end) {
        return start.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")) + "~" +
                end.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
    }

    public String getStatisticsDuration(Date startTime) {
        Pair<LocalDateTime, LocalDateTime> startEnd = getStatisticsDay(startTime);
        return getStatisticsDuration(startEnd.getKey(), startEnd.getValue());
    }

    /**
     * 组装每日快照结果
     */
    private List<TradeCompetitionResultDaily> buildListOfTradeCompetitionResultDaily(List<ContractSnapshot> localList,
                                                                                     Set<Long> whiteList,
                                                                                     Set<Long> allList,
                                                                                     Map<String, String> listMap,
                                                                                     Map<Long, BigDecimal> comeInbalanceMap,
                                                                                     Map<Long, BigDecimal> outgobalanceMap,
                                                                                     TradeCompetition tradeCompetition,
                                                                                     String daily, String start, String end) {


        BigDecimal limit = BigDecimal.ZERO;
        if (listMap.size() > 0 && StringUtils.isNotEmpty(listMap.get(FUTURES_POSITION_QUANTITY))) {
            try {
                limit = new BigDecimal(listMap.get(FUTURES_POSITION_QUANTITY));
            } catch (Exception e) {
                log.warn(e.getMessage(), e);
            }
        }

        //活动开始时间最近的一批数据
        String earliestBatch = this.statisticsContractService.queryMaxTimeByTime(
                tradeCompetition.getOrgId(),
                tradeCompetition.getSymbolId(), start);
        //快照列表
        List<ContractSnapshot> earliestList = statisticsContractService.queryNewContractSnapshot(tradeCompetition.getOrgId(),
                new LinkedList<>(allList), tradeCompetition.getSymbolId(), earliestBatch, null);

        Map<Long, ContractSnapshot> snapshotMap = earliestList.stream().collect(Collectors.toMap(ContractSnapshot::getUserId, Function.identity()));

        List<TradeCompetitionResultDaily> tradeCompetitionResultList = new ArrayList<>();
        try {
            for (int i = 0; i < localList.size(); i++) {
                ContractSnapshot currentSnapshot = localList.get(i);
                long userId = currentSnapshot.getUserId();
                if (currentSnapshot == null) {
                    log.warn("buildListOfTradeCompetitionResultDaily,Hasn't the current snapshot,userId={}", userId);
                    continue;
                }

                //获取距离活动开始时间最近的快照
                ContractSnapshot contractSnapshot = snapshotMap.get(userId);
                boolean afterStart = false;
                //如果不存在活动开始前的快照，则获取活动开始后最早的快照
                if (Objects.isNull(contractSnapshot)) {

                    LocalDateTime ldt = tradeCompetition.getStartTime().toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
                    String startTime = ldt.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

                    List<ContractSnapshot> tmp = statisticsContractService.queryListSnapshotByTime(tradeCompetition.getOrgId(),
                            currentSnapshot.getAccountId(), tradeCompetition.getSymbolId(), startTime, limit);
                    //只有一条数据表示无法进行计算
                    if (org.apache.commons.collections4.CollectionUtils.isEmpty(tmp) || tmp.size() < 2) {
                        log.warn("buildListOfTradeCompetitionResultDaily,Hasn't the base snapshot,userId={}", userId);
                        continue;
                    }

                    contractSnapshot = tmp.get(0);
                    afterStart = true;
                }

                //是否白名单标志
                Boolean isWhite = whiteList.contains(currentSnapshot.getUserId());
                //并且持仓小于配置 不在白名单 则不存储计算结果
                if (listMap.size() > 0 && StringUtils.isNotEmpty(listMap.get(FUTURES_POSITION_QUANTITY))) {
                    if (currentSnapshot.getContractBalance().compareTo(new BigDecimal(listMap.get(FUTURES_POSITION_QUANTITY))) < 0 && !isWhite) {
                        log.info("Current user does not meet the restrictions userId {} balance {} limitBalance {}",
                                userId, currentSnapshot.getContractBalance(), listMap.get(FUTURES_POSITION_QUANTITY));
                        continue;
                    }
                }

                //TODO 批量获取accountId效率更好
                TradeCompetitionResultDaily tradeCompetitionResult = new TradeCompetitionResultDaily();
                //获取币币账户Id
                Long coinAccountId = this.accountService.getAccountId(tradeCompetition.getOrgId(), userId);
                if (coinAccountId == null) {
                    log.info("not find coinAccountId orgId {} userId {}", tradeCompetition.getOrgId(), userId);
                    continue;
                }
                tradeCompetitionResult.setDay(daily);
                tradeCompetitionResult.setClientTransferId(sequenceGenerator.getLong());
                tradeCompetitionResult.setAccountId(currentSnapshot.getAccountId());
                tradeCompetitionResult.setBatch(currentSnapshot.getSnapshotTime());
                tradeCompetitionResult.setCompetitionCode(tradeCompetition.getCompetitionCode());
                tradeCompetitionResult.setCompetitionId(tradeCompetition.getId());
                tradeCompetitionResult.setOrgId(tradeCompetition.getOrgId());
                tradeCompetitionResult.setUserId(currentSnapshot.getUserId());
                tradeCompetitionResult.setReceiveTokenId(tradeCompetition.getReceiveTokenId());
                tradeCompetitionResult.setReceiveQuantity(BigDecimal.ZERO);
                tradeCompetitionResult.setTransferStatus(0);
                tradeCompetitionResult.setStatus(1);

                /** 计算成交率 start **/
                BigDecimal rate = BigDecimal.ZERO;
                //正值 累计转入量
                BigDecimal changedTotal;
                //负值 累计转出量
                BigDecimal changedTotalNegative = outgobalanceMap.get(currentSnapshot.getAccountId()) != null ?
                        outgobalanceMap.get(currentSnapshot.getAccountId()) : BigDecimal.ZERO;

                changedTotal = comeInbalanceMap.get(currentSnapshot.getAccountId()) != null ?
                        comeInbalanceMap.get(currentSnapshot.getAccountId()) : BigDecimal.ZERO;

                log.info("buildListOfTradeCompetitionResultDaily,code={},userId={}," +
                                "baseSnapshot={},currentSnapshot={},transferIn={},transOut={},start={},end={}",
                        tradeCompetition.getCompetitionCode(), userId,
                        contractSnapshot, currentSnapshot, CommonUtil.bigDecimalToString(changedTotal), CommonUtil.bigDecimalToString(changedTotalNegative),
                        start, end);

                //分子:(当前最新快照) = 期货账户余额 + 未实现盈亏 - （累计转入量 - 累计转出量）- （初始快照账户余额 + 初始快照未实现盈亏)
                BigDecimal numerator = null;
                if (afterStart) {

                    BigDecimal tmp = BigDecimal.ZERO;
                    if (changedTotal.compareTo(contractSnapshot.getContractBalance()) > 0) {
                        tmp = changedTotal.subtract(changedTotalNegative.abs());
                    } else {
                        tmp = contractSnapshot.getContractBalance().add(contractSnapshot.getUnrealizedPnl());
                    }

                    numerator = currentSnapshot.getContractBalance()
                            .add(currentSnapshot.getUnrealizedPnl())
                            .subtract(tmp)
                            .setScale(18, RoundingMode.DOWN);
                } else {
                    numerator = currentSnapshot.getContractBalance()
                            .add(currentSnapshot.getUnrealizedPnl())
                            .subtract(changedTotal.subtract(changedTotalNegative.abs()))
                            .subtract(contractSnapshot.getContractBalance().add(contractSnapshot.getUnrealizedPnl()))
                            .setScale(18, RoundingMode.DOWN);
                }

                //分母计算:活动开始时间最近的快照 = (期货账户余额 + 未实现盈亏 + 累计转入量)
                BigDecimal denominator = BigDecimal.ZERO;
                if (afterStart) {

                    BigDecimal tmp = contractSnapshot.getContractBalance();
                    if (changedTotal.compareTo(tmp) > 0) {
                        tmp = changedTotal;
                    }

                    denominator = tmp.add(contractSnapshot.getUnrealizedPnl())
                            .setScale(18, RoundingMode.DOWN);
                } else {
                    denominator = contractSnapshot.getContractBalance()
                            .add(changedTotal)
                            .add(contractSnapshot.getUnrealizedPnl())
                            .setScale(18, RoundingMode.DOWN);
                }

                if (denominator != null && denominator.compareTo(BigDecimal.ZERO) != 0) {
                    rate = numerator.divide(denominator, 18, RoundingMode.DOWN);
                }
                /** 计算成交率 end **/
                log.info("buildListOfTradeCompetitionResultDaily,code={},userId {},rate {},numerator {},denominator {}," +
                                "changedTotal {},changedTotalNegative={},earliestBatch {}" +
                                ",batch {},start={},end={},afterStart={} ",
                        tradeCompetition.getCompetitionCode(), userId, rate,
                        numerator, denominator, changedTotal, changedTotalNegative,
                        earliestBatch, currentSnapshot.getSnapshotTime(),
                        start, end, afterStart);

                //每日收益
                tradeCompetitionResult.setIncomeAmount(numerator);
                //冗余原始数据
                tradeCompetitionResult.setRate(rate.setScale(18, RoundingMode.DOWN));
                tradeCompetitionResult.setChangedTotal(changedTotal);
                tradeCompetitionResult.setCurrentContractBalance(currentSnapshot.getContractBalance() != null ? currentSnapshot.getContractBalance().setScale(18, RoundingMode.DOWN) : BigDecimal.ZERO);
                tradeCompetitionResult.setCurrentUnrealizedPnl(currentSnapshot.getUnrealizedPnl() != null ? currentSnapshot.getUnrealizedPnl().setScale(18, RoundingMode.DOWN) : BigDecimal.ZERO);
                tradeCompetitionResult.setStartBatch(contractSnapshot.getSnapshotTime());
                tradeCompetitionResult.setStartContractBalance(contractSnapshot.getContractBalance() != null ? contractSnapshot.getContractBalance().setScale(18, RoundingMode.DOWN) : BigDecimal.ZERO);
                tradeCompetitionResult.setStartUnrealizedPnl(contractSnapshot.getUnrealizedPnl() != null ? contractSnapshot.getUnrealizedPnl().setScale(18, RoundingMode.DOWN) : BigDecimal.ZERO);
                tradeCompetitionResult.setChangedTotalNegative(changedTotalNegative);
                tradeCompetitionResult.setCurrentTradeAmount(BigDecimal.ZERO);
                tradeCompetitionResult.setCurrentTradeAmountTokenId("");
                tradeCompetitionResultList.add(tradeCompetitionResult);
            }
        } catch (Exception ex) {
            log.error("refreshTradeRankListSnapshot error orgId {},code {} error {}", tradeCompetition.getOrgId(), tradeCompetition.getCompetitionCode(), ex);
        }
        return tradeCompetitionResultList;
    }

    public TradeCompetitionInfo getTradeCompetitionInfoByCache(Long orgId, String competitionCode) {
        try {
            String key = String.format(TRADE_COMPETITION_CACHE_KEY, orgId, competitionCode);
            String str = redisTemplate.opsForValue().get(key);
            if (StringUtils.isNoneBlank(str)) {
                return JSON.parseObject(str, TradeCompetitionInfo.class);
            }

            TradeCompetitionInfo tci = buildTradeCompetitionInfo(orgId, competitionCode);
            redisTemplate.opsForValue().set(key, JSON.toJSONString(tci), 10, TimeUnit.SECONDS);
            return tci;

            //return competitionConfigCache.get(key, () -> buildTradeCompetitionInfo(orgId, competitionCode));
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return buildTradeCompetitionInfo(orgId, competitionCode);
        }
    }

    private void refreshTradeCompetitionInfoCache(Long orgId, String competitionCode) {
        String key = String.format(TRADE_COMPETITION_CACHE_KEY, orgId, competitionCode);
        TradeCompetitionInfo tci = buildTradeCompetitionInfo(orgId, competitionCode);
        if (Objects.isNull(tci)) {
            return;
        }
        redisTemplate.opsForValue().set(key, JSON.toJSONString(tci), 10, TimeUnit.SECONDS);
    }

    private TradeCompetitionInfo buildTradeCompetitionInfo(Long orgId, String competitionCode) {
        TradeCompetition tradeCompetition = this.tradeCompetitionMapper.queryByCode(orgId, competitionCode);
        if (tradeCompetition == null) {
            log.info("tradeCompetition not find orgId {} competitionCode {}", orgId, competitionCode);
            throw new BrokerException(BrokerErrorCode.PARAM_INVALID);
        }

        List<TradeCompetitionExt> extList
                = this.tradeCompetitionExtMapper.queryTradeCompetitionExtByCode(tradeCompetition.getOrgId(), tradeCompetition.getCompetitionCode());
        if (CollectionUtils.isEmpty(extList)) {
            log.info("tradeCompetition tradeCompetitionExt not find orgId {} competitionCode {}", orgId, competitionCode);
            throw new BrokerException(BrokerErrorCode.PARAM_INVALID);
        }

        Map<String, TradeCompetitionExt> tradeCompetitionExtMap = new HashMap<>();
        extList.forEach(ext -> {
            tradeCompetitionExtMap.put(ext.getLanguage(), ext);
        });

        TradeCompetitionLimit limit
                = this.tradeCompetitionLimitMapper.queryByCode(tradeCompetition.getOrgId(), tradeCompetition.getCompetitionCode());

        return TradeCompetitionInfo
                .builder()
                .competitionCode(tradeCompetition.getCompetitionCode())
                .symbolId(tradeCompetition.getSymbolId())
                .effectiveQuantity(tradeCompetition.getEffectiveQuantity())
                .orgId(tradeCompetition.getOrgId())
                .startTime(tradeCompetition.getStartTime())
                .endTime(tradeCompetition.getEndTime())
                .receiveTokenAmount(tradeCompetition.getReceiveTokenAmount())
                .receiveTokenId(tradeCompetition.getReceiveTokenId())
                .receiveTokenName(tradeCompetition.getReceiveTokenName())
                .enterIdStr((limit != null && StringUtils.isNotEmpty(limit.getEnterIdStr())) ? limit.getEnterIdStr() : "")
                .enterStatus((limit != null && null != limit.getEnterStatus()) ? limit.getEnterStatus() : 0)
                .enterWhiteStr((limit != null && StringUtils.isNotEmpty(limit.getEnterWhiteStr()) ? limit.getEnterWhiteStr() : ""))
                .enterWhiteStatus((limit != null && null != limit.getEnterWhiteStatus()) ? limit.getEnterWhiteStatus() : 0)
                .limitStr((limit != null && StringUtils.isNotEmpty(limit.getLimitStr())) ? limit.getLimitStr() : "")
                .limitStatus((limit != null && null != limit.getEnterStatus()) ? limit.getLimitStatus() : 0)
                .status(tradeCompetition.getStatus())
                .type(tradeCompetition.getType())
                .extMap(tradeCompetitionExtMap)
                .rankTypes(tradeCompetition.getRankTypes())
                .isReverse(tradeCompetition.getIsReverse())
                .build();
    }

    public TradeCompetitionResult queryUserPerformance(Long orgId, String competitionCode, Long userId) {
        String batch = tradeCompetitionResultMapper.queryMaxBatchByCode(orgId, competitionCode);
        if (StringUtils.isEmpty(batch)) {
            return null;
        }
        return this.tradeCompetitionResultMapper.queryUserPerformance(orgId, competitionCode, batch, userId);
    }

    //queryUserPerformanceToday
    public TradeCompetitionResultDaily queryUserPerformanceByDay(Long orgId, String competitionCode, Long userId, String day) {
        String batch = tradeCompetitionResultDailyMapper.queryMaxBatchByCode(orgId, competitionCode, day);
        if (StringUtils.isEmpty(batch)) {
            return null;
        }
        return this.tradeCompetitionResultDailyMapper.queryUserPerformanceByDay(orgId, competitionCode, batch, userId, day);

    }

    public List<TradeCompetitionResult> queryRateListByCode(Long orgId, String competitionCode, Integer limit) {
        String batch = tradeCompetitionResultMapper.queryMaxBatchByCode(orgId, competitionCode);
        if (StringUtils.isEmpty(batch)) {
            return new ArrayList<>();
        }
        return this.tradeCompetitionResultMapper.queryRateListByCode(orgId, competitionCode, batch, limit);
    }


    public List<TradeCompetitionResult> listIncomeAmountByCode(Long orgId, String competitionCode, Integer limit) {
        String batch = tradeCompetitionResultMapper.queryMaxBatchByCode(orgId, competitionCode);
        if (StringUtils.isEmpty(batch)) {
            return new ArrayList<>();
        }
        return this.tradeCompetitionResultMapper.listIncomeAmountByCode(orgId, competitionCode, batch, limit);
    }

    public List<TradeCompetitionResult> listRateDailyListByCode(Long orgId, String competitionCode, Integer limit, String daily) {
        String batch = tradeCompetitionResultDailyMapper.queryMaxBatchByCode(orgId, competitionCode, daily);
        if (StringUtils.isEmpty(batch)) {
            return new ArrayList<>();
        }
        List<TradeCompetitionResultDaily> list = this.tradeCompetitionResultDailyMapper.queryRateListByCode(orgId, competitionCode, batch, limit);
        return list.stream().map(i -> {
            TradeCompetitionResult rt = new TradeCompetitionResult();
            rt.setUserId(i.getUserId());
            rt.setRate(i.getRate());
            rt.setIncomeAmount(i.getIncomeAmount());
            return rt;
        }).collect(Collectors.toList());
    }


    public List<TradeCompetitionResult> listIncomeDailyAmountByCode(Long orgId, String competitionCode, Integer limit, String daily) {
        String batch = tradeCompetitionResultDailyMapper.queryMaxBatchByCode(orgId, competitionCode, daily);
        if (StringUtils.isEmpty(batch)) {
            return new ArrayList<>();
        }
        List<TradeCompetitionResultDaily> list = this.tradeCompetitionResultDailyMapper.listIncomeAmountByCode(orgId, competitionCode, batch, limit);
        return list.stream().map(i -> {
            TradeCompetitionResult rt = new TradeCompetitionResult();
            rt.setUserId(i.getUserId());
            rt.setIncomeAmount(i.getIncomeAmount());
            rt.setIncomeAmount(i.getIncomeAmount());
            return rt;
        }).collect(Collectors.toList());
    }


    /**
     * { "competitionId":1000,//比赛ID "competitionCode":"BTC-BHT-6001-20190929144105", //比赛Code
     * "rankList":[ { "rate":20, "userId":1570786173932 }, { "rate":10, "userId":1570786173932 } ],
     * "bannerUrl":"", //banner "description":"<p>111</p>", //比赛描述 "startTime":1570871385710, //开始时间
     * "receiveTokenName":"BHT", //奖励tokenId "endTime":1570871385710, //结束时间 "type":1, //1：期货大赛
     * "receiveTokenId":"BHT", //奖励tokenName "status":1 //比赛状态  0初始化 1进行中 2结束 3 无效 }
     **/
    public QueryTradeCompetitionInfoReply queryTradeCompetitionDetail(Long orgId, long userId, String language, String competitionCode, int rankType) {
        TradeCompetitionInfo competitionInfo = getTradeCompetitionInfoByCache(orgId, competitionCode);
        if (StringUtils.isEmpty(competitionCode)) {
            throw new BrokerException(BrokerErrorCode.PARAM_INVALID);
        }

        if (competitionInfo == null) {
            QueryTradeCompetitionInfoReply.newBuilder().build();
        }

        //未指定排行榜类型，则选一个默认排行榜
        if (rankType == 0) {
            rankType = CollectionUtils.isEmpty(competitionInfo.getRankTypeList()) ? 1 : competitionInfo.getRankTypeList().get(0);
        }

        String today = getStatisticsDuration(competitionInfo.getStartTime());
        List<TradeCompetitionResult> rankList = Lists.newArrayList();
        log.info("queryTradeCompetitionDetail,orgId={},code={},quantity={},date={},type={}",
                orgId, competitionCode, competitionInfo.getEffectiveQuantity(), today, rankType);

        //1=总收益率,2=总收益额,3=当日收益率,4=当日收益额
        if (rankType == 1) {
            rankList = queryRateListByCode(orgId, competitionCode, competitionInfo.getEffectiveQuantity());
        }
        if (rankType == 2) {
            rankList = listIncomeAmountByCode(orgId, competitionCode, competitionInfo.getEffectiveQuantity());
        }

        if (rankType == 3) {
            rankList = listRateDailyListByCode(orgId, competitionCode, competitionInfo.getEffectiveQuantity(), today);
        }
        if (rankType == 4) {
            rankList = listIncomeDailyAmountByCode(orgId, competitionCode, competitionInfo.getEffectiveQuantity(), today);
        }

        log.info("queryTradeCompetitionDetail,rankList size={}", rankList.size());

        Set<Long> userIds = rankList.stream().map(i -> i.getUserId()).collect(Collectors.toSet());

        Map<Long, String> nicknameMap = Maps.newHashMap();

        if (org.apache.commons.collections4.CollectionUtils.isNotEmpty(userIds)) {
            //查询昵称
            Example tcpExp = new Example(TradeCompetitionParticipant.class);
            tcpExp.createCriteria()
                    .andEqualTo("orgId", orgId)
                    .andEqualTo("competitionCode", competitionCode)
                    .andIn("userId", userIds);
            List<TradeCompetitionParticipant> participants = tradeCompetitionParticipantMapper.selectByExample(tcpExp);
            nicknameMap = participants.stream().collect(Collectors.toMap(i -> i.getUserId(), i -> i.getNickname()));

            log.info("queryTradeCompetitionDetail,nickname size={}", nicknameMap.size());

        }

        List<Rank> ranks = new ArrayList<>();
        for (TradeCompetitionResult rank : rankList) {
            log.info("queryTradeCompetitionDetail,build rank,userId={}", rank.getUserId());
            ranks.add(Rank
                    .newBuilder()
                    .setRate(rank.getRateSafeWithScale(8, RoundingMode.DOWN))
                    .setUserId(rank.getUserId())
                    .setIncomeAmount(rank.getIncomeAmountSafeWithScale(8, RoundingMode.DOWN))
                    .setNickName(nicknameMap.getOrDefault(rank.getUserId(), ""))
                    .build());
        }

        TradeCompetitionExt tradeCompetitionExt = competitionInfo.getExtMap().get(language);
        log.info("getExtMap  {}", JSON.toJSONString(competitionInfo.getExtMap().get(language)));
        if (tradeCompetitionExt == null) {
            tradeCompetitionExt = competitionInfo.getExtMap().get(Locale.US.toString());
        }

        //是否白名单
        int isWhite = 0;
        if (userId > 0) {
            Example tcpExp = new Example(TradeCompetitionParticipant.class);
            tcpExp.createCriteria()
                    .andEqualTo("orgId", orgId)
                    .andEqualTo("competitionCode", competitionCode)
                    .andEqualTo("userId", userId);
            TradeCompetitionParticipant participants = tradeCompetitionParticipantMapper.selectOneByExample(tcpExp);
            if (participants != null) {
                isWhite = 1;
            }
        }

        String token;
        if (competitionInfo.getIsReverse().equals(1)) {
            token = "USDT";
        } else {
            token = getContractToken(orgId, competitionInfo.getSymbolId());
        }

        return QueryTradeCompetitionInfoReply
                .newBuilder()
                .setCompetitionCode(competitionInfo.getCompetitionCode())
                .setMobileBannerUrl(tradeCompetitionExt != null && StringUtils.isNotEmpty(tradeCompetitionExt.getMobileBannerUrl()) ? tradeCompetitionExt.getMobileBannerUrl() : "")
                .setBannerUrl(tradeCompetitionExt != null && StringUtils.isNotEmpty(tradeCompetitionExt.getBannerUrl()) ? tradeCompetitionExt.getBannerUrl() : "")
                .setDescription(tradeCompetitionExt != null && StringUtils.isNotEmpty(tradeCompetitionExt.getDescription()) ? tradeCompetitionExt.getDescription() : "")
                .setEndTime(competitionInfo.getEndTime().getTime())
                .setStartTime(competitionInfo.getStartTime().getTime())
                .setType(competitionInfo.getType())
                .setStatus(competitionInfo.getStatus())
                .addAllRank(ranks)
                .setIsWhite(isWhite)
                .addAllRankTypes(competitionInfo.getRankTypeList())
                .setRankType(rankType)
                .setToken(token)
                .build();
    }

    public static String maskKey(String key) {
        if (StringUtils.isBlank(key)) {

            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 6; i++) {
                SecureRandom rnd = new SecureRandom();
                int d = rnd.nextInt(26) + 97;
                sb.append((char) d);
            }
            return sb.insert(3, MASK).toString();
        }

        if (key.length() <= 3) {
            StringBuilder sb = new StringBuilder(key.charAt(0) + MASK);
            for (int i = 0; i < 3; i++) {
                SecureRandom rnd = new SecureRandom();
                int d = rnd.nextInt(26) + 97;
                sb.append((char) d);
            }
            return sb.toString();
        }

        return key.substring(0, 3) + MASK + key.substring(key.length() - 1);
    }


    public void saveTradeCompetition(TradeCompetition trade) {
        TradeCompetition tradeCompetition = this.tradeCompetitionMapper.queryByCode(trade.getOrgId(), trade.getCompetitionCode());
        TradeCompetition newTradeCompetition = new TradeCompetition();
        newTradeCompetition.setOrgId(trade.getOrgId());
        if (trade.getStartTime() != null) {
            newTradeCompetition.setStartTime(trade.getStartTime());
        }

        if (trade.getEndTime() != null) {
            newTradeCompetition.setEndTime(trade.getEndTime());
        }

        if (StringUtils.isNotEmpty(trade.getReceiveTokenId())) {
            newTradeCompetition.setReceiveTokenId(trade.getReceiveTokenId());
        }

        if (StringUtils.isNotEmpty(trade.getReceiveTokenName())) {
            newTradeCompetition.setReceiveTokenName(trade.getReceiveTokenName());
        }

        if (trade.getReceiveTokenAmount() != null) {
            newTradeCompetition.setReceiveTokenAmount(trade.getReceiveTokenAmount());
        }
        if (trade.getType() != null) {
            newTradeCompetition.setType(trade.getType());
        }

        if (StringUtils.isNotEmpty(trade.getSymbolId())) {
            newTradeCompetition.setSymbolId(trade.getSymbolId());
        }

        if (trade.getStatus() != null) {
            newTradeCompetition.setStatus(0);
        }

        if (trade.getEffectiveQuantity() != null) {
            newTradeCompetition.setEffectiveQuantity(trade.getEffectiveQuantity());
        }

        if (StringUtils.isNotEmpty(trade.getRankTypes())) {
            newTradeCompetition.setRankTypes(trade.getRankTypes());
        }

        if (tradeCompetition != null) {
            newTradeCompetition.setId(tradeCompetition.getId());
            newTradeCompetition.setUpdateTime(new Date());
            this.tradeCompetitionMapper.updateByPrimaryKeySelective(newTradeCompetition);
        } else {
            String competitionCode = Joiner.on("-").join(trade.getSymbolId(), trade.getOrgId(), new DateTime().toString("yyyyMMddHHmmss"));
            newTradeCompetition.setCreateTime(new Date());
            newTradeCompetition.setUpdateTime(new Date());
            newTradeCompetition.setCompetitionCode(competitionCode);
            this.tradeCompetitionMapper.insertSelective(newTradeCompetition);
        }
    }

    //添加赛事国际化信息
    public void saveTradeCompetitionExt(TradeCompetitionExt ext) {
        if (StringUtils.isEmpty(ext.getCompetitionCode())) {
            throw new BrokerException(BrokerErrorCode.PARAM_INVALID);
        }

        TradeCompetition competition = this.tradeCompetitionMapper.queryByCode(ext.getOrgId(), ext.getCompetitionCode());
        if (Objects.isNull(competition)) {
            throw new BrokerException(BrokerErrorCode.PARAM_ERROR);
        }

        TradeCompetitionExt competitionExt
                = this.tradeCompetitionExtMapper.queryTradeCompetitionExtByLanguage(ext.getOrgId(), ext.getCompetitionCode(), ext.getLanguage());
        TradeCompetitionExt tradeCompetitionExt = new TradeCompetitionExt();
        tradeCompetitionExt.setOrgId(ext.getOrgId());
        if (StringUtils.isNotEmpty(ext.getName())) {
            tradeCompetitionExt.setName(ext.getName());
        }

        if (StringUtils.isNotEmpty(ext.getBannerUrl())) {
            tradeCompetitionExt.setBannerUrl(ext.getBannerUrl());
        }

        if (StringUtils.isNotEmpty(ext.getDescription())) {
            tradeCompetitionExt.setDescription(ext.getDescription());
        }

        if (StringUtils.isNotEmpty(ext.getLanguage())) {
            tradeCompetitionExt.setLanguage(ext.getLanguage());
        }

        if (StringUtils.isNotEmpty(ext.getMobileBannerUrl())) {

            tradeCompetitionExt.setMobileBannerUrl(ext.getMobileBannerUrl());
        }
        if (competitionExt != null) {
            tradeCompetitionExt.setId(competitionExt.getId());
            this.tradeCompetitionExtMapper.updateByPrimaryKeySelective(tradeCompetitionExt);
        } else {
            tradeCompetitionExt.setCompetitionCode(ext.getCompetitionCode());
            tradeCompetitionExt.setCompetitionId(competition.getId());
            this.tradeCompetitionExtMapper.insertSelective(tradeCompetitionExt);
        }
    }

    //添加名单限制条件接口
    @Transactional
    public void saveTradeCompetitionLimit(TradeCompetitionLimit tradeCompetitionLimit, String enterIdStr, String enterWhiteStr) {
        TradeCompetitionLimit competitionLimit
                = this.tradeCompetitionLimitMapper.queryByCode(tradeCompetitionLimit.getOrgId(), tradeCompetitionLimit.getCompetitionCode());
        TradeCompetitionLimit limit = new TradeCompetitionLimit();

        TradeCompetition competition = this.tradeCompetitionMapper.queryByCode(tradeCompetitionLimit.getOrgId(), tradeCompetitionLimit.getCompetitionCode());
        if (Objects.isNull(competition)) {
            throw new BrokerException(BrokerErrorCode.PARAM_ERROR);
        }


        String userIds = fetchUserIds(enterIdStr);
        String whiteListIds = fetchUserIds(enterWhiteStr);

        limit.setEnterIdStr(userIds);
        limit.setEnterWhiteStr(whiteListIds);

        if (tradeCompetitionLimit.getEnterStatus() != null && tradeCompetitionLimit.getEnterStatus() > 0) {
            limit.setEnterStatus(tradeCompetitionLimit.getEnterStatus());
        }

        if (tradeCompetitionLimit.getEnterWhiteStatus() != null && tradeCompetitionLimit.getEnterWhiteStatus() > 0) {
            limit.setEnterWhiteStatus(tradeCompetitionLimit.getEnterWhiteStatus());
        }

        if (StringUtils.isNotEmpty(tradeCompetitionLimit.getLimitStr())) {
            limit.setLimitStr(tradeCompetitionLimit.getLimitStr());
        }

        if (tradeCompetitionLimit.getLimitStatus() != null && tradeCompetitionLimit.getLimitStatus() > 0) {
            limit.setLimitStatus(tradeCompetitionLimit.getLimitStatus());
        }

        if (tradeCompetitionLimit.getOrgId() != null) {
            limit.setOrgId(tradeCompetitionLimit.getOrgId());
        }

        if (competitionLimit != null) {
            limit.setId(competitionLimit.getId());
            this.tradeCompetitionLimitMapper.updateByPrimaryKeySelective(limit);
        } else {
            limit.setCompetitionId(competition.getId());
            limit.setCompetitionCode(competition.getCompetitionCode());
            this.tradeCompetitionLimitMapper.insertSelective(limit);
        }

        saveParticipant(competition, enterIdStr + "," + enterWhiteStr);
    }

    private String fetchUserIds(String enterIdStr) {
        if (StringUtils.isBlank(enterIdStr)) {
            return "";
        }

        if (!enterIdStr.contains("=")) {
            return enterIdStr;
        }

        Map<String, String> map = Splitter.on(",").omitEmptyStrings().trimResults().withKeyValueSeparator("=").split(enterIdStr);
        return Joiner.on(",").join(map.keySet());
    }

    private void saveParticipant(TradeCompetition competition, String string) {

        if (Objects.isNull(competition) || StringUtils.isBlank(string)) {
            return;
        }

        Map<String, String> map = Maps.newHashMap();

        if (string.contains("=")) {
            Map<String, String> tmp = Splitter.on(",").omitEmptyStrings().trimResults().withKeyValueSeparator("=").split(string);
            map.putAll(tmp);
        } else {
            List<String> ids = Splitter.on(",").omitEmptyStrings().trimResults().splitToList(string);
            Map<String, String> tmp = ids.stream().collect(Collectors.toMap(i -> i, i -> ""));
            map.putAll(tmp);
        }

        if (MapUtils.isEmpty(map)) {
            return;
        }

        Set<Long> ids = map.keySet().stream().map(i -> Long.parseLong(i)).collect(Collectors.toSet());

        Example existExp = new Example(TradeCompetitionParticipant.class);
        existExp.selectProperties("userId");
        existExp.createCriteria()
                .andEqualTo("orgId", competition.getOrgId())
                .andEqualTo("competitionCode", competition.getCompetitionCode())
                .andIn("userId", ids);

        List<TradeCompetitionParticipant> exists = tradeCompetitionParticipantMapper.selectByExample(existExp);
        Set<Long> existIds = exists.stream().map(i -> i.getUserId()).collect(Collectors.toSet());
        Set<Long> differIds = Sets.difference(ids, existIds);

        differIds.forEach(id -> {

            String nickname = map.getOrDefault(id.toString(), "");
            if (StringUtils.isBlank(nickname)) {
                return;
            }

            TradeCompetitionParticipant tcp = new TradeCompetitionParticipant();
            tcp.setCompetitionId(competition.getId());
            tcp.setCompetitionCode(competition.getCompetitionCode());
            tcp.setUserId(id);
            tcp.setNickname(nickname);
            tcp.setOrgId(competition.getOrgId());

            tradeCompetitionParticipantMapper.insertSelective(tcp);

        });
    }

    /**
     * 奖励派发
     */
    public void handelTradeCompetitionResult(Long orgId, String competitionCode) {
        if (getTradeCompetitionInfoByCache(orgId, competitionCode) == null) {
            return;
        }

        TradeCompetition tradeCompetition = this.tradeCompetitionMapper.queryByCode(orgId, competitionCode);

        if (tradeCompetition == null) {
            log.info("handelTradeCompetitionResult error tradeCompetition is null");
            throw new BrokerException(BrokerErrorCode.PARAM_INVALID);
        }

        if (!tradeCompetition.getStatus().equals(2)) {
            log.info("handelTradeCompetitionResult error status {}", tradeCompetition.getStatus());
            throw new BrokerException(BrokerErrorCode.PARAM_INVALID);
        }
        List<TradeCompetitionResult> competitionResultList
                = this.tradeCompetitionResultMapper.queryWaitingTradeCompetitionList(orgId, competitionCode);

        if (org.apache.commons.collections4.CollectionUtils.isEmpty(competitionResultList)) {
            return;
        }

        competitionResultList.forEach(result -> {
            List<SyncTransferRequest> syncTransferRequests = new ArrayList<>();
            if (StringUtils.isNotEmpty(result.getReceiveTokenId()) && result.getReceiveQuantity().compareTo(BigDecimal.ZERO) > 0) {
                syncTransferRequests.add(SyncTransferRequest.newBuilder()
                        .setClientTransferId(result.getClientTransferId())
                        .setSourceOrgId(orgId)
                        .setSourceFlowSubject(BusinessSubject.TRANSFER)
                        .setSourceAccountType(AccountType.OPERATION_ACCOUNT)
                        .setFromPosition(false)
                        .setTokenId(result.getReceiveTokenId())
                        .setAmount(result.getReceiveQuantity().setScale(ProtoConstants.PRECISION, RoundingMode.DOWN).toPlainString())
                        .setTargetAccountId(result.getAccountId()) //用户账户
                        .setTargetOrgId(orgId)
                        .setTargetFlowSubject(BusinessSubject.TRANSFER)
                        .build());
            }

            if (syncTransferRequests.size() == 0) {
                log.info("Sync transfer requests is null");
            }

            BatchSyncTransferRequest batchSyncTransferRequest = BatchSyncTransferRequest
                    .newBuilder()
                    .setBaseRequest(BaseReqUtil.getBaseRequest(orgId))
                    .addAllSyncTransferRequestList(syncTransferRequests)
                    .build();
            SyncTransferResponse response = grpcBatchTransferService.batchSyncTransfer(batchSyncTransferRequest);
            if (response == null || response.getCode() != SyncTransferResponse.ResponseCode.SUCCESS) {
                return;
            }
            //转账成功 修改订单状态
            if (tradeCompetitionResultMapper.updateTradeCompetitionStatus(result.getId()) == 1) {
                log.info("trade competition userId {} accountId {} tokenId {} backAmount {} transfer success",
                        result.getUserId(), result.getAccountId(), result.getReceiveTokenId(), result.getReceiveQuantity());
            } else {
                log.info("trade competition userId {} accountId {} tokenId {} backAmount {} transfer fail resultMsg {} resultCode {}",
                        result.getUserId(), result.getAccountId(), result.getReceiveTokenId(), result.getReceiveQuantity(), response.getMsg(), response.getCodeValue());
            }
        });
    }

    public void addParticipant(TradeCompetitionLimit input, String entryIds, String whiteListIds) {

        String ids = fetchUserIds(entryIds);
        String wids = fetchUserIds(whiteListIds);
        input.setEnterWhiteStr(wids);
        input.setEnterIdStr(ids);

        if (StringUtils.isBlank(input.getEnterIdStr()) &&
                StringUtils.isBlank(input.getEnterWhiteStr())) {
            return;
        }

        TradeCompetitionLimit entity
                = this.tradeCompetitionLimitMapper.queryByCode(input.getOrgId(), input.getCompetitionCode());

        if (Objects.isNull(entity)) {
            log.warn("limit not exist,org={},code={}", input.getOrgId(), input.getCompetitionCode());
            return;
        }

        TradeCompetition exp = new TradeCompetition();
        exp.setOrgId(input.getOrgId());
        exp.setCompetitionCode(input.getCompetitionCode());

        TradeCompetition competition = this.tradeCompetitionMapper.selectOne(exp);
        if (!competition.getStatus().equals(0) && !competition.getStatus().equals(1)) {
            log.warn("illegal status,org={},code={}", input.getOrgId(), input.getCompetitionCode());
            return;
        }

        if (competition.getEndTime().compareTo(new Date()) <= 0) {
            log.warn("competition is finish,org={},code={}", input.getOrgId(), input.getCompetitionCode());
            return;
        }

        input.setId(entity.getId());

        Set<Long> entryIdSet = Sets.newHashSet(input.listEnterId());
        Set<Long> whiteIdSet = Sets.newTreeSet(input.listWhiteEnterId());

        Set<Long> existEntryIdSet = Sets.newHashSet(entity.listEnterId());
        Set<Long> existWhiteListIdSet = Sets.newHashSet(entity.listWhiteEnterId());

        Set<Long> diffEntryIds = Sets.difference(entryIdSet, existEntryIdSet);
        Set<Long> diffWhiteEntryIds = Sets.difference(whiteIdSet, existEntryIdSet);

        if (org.apache.commons.collections4.CollectionUtils.isEmpty(diffEntryIds) &&
                org.apache.commons.collections4.CollectionUtils.isEmpty(diffWhiteEntryIds)) {
            return;
        }

        existEntryIdSet.addAll(diffEntryIds);
        existWhiteListIdSet.addAll(diffWhiteEntryIds);

        input.putEntryIds(existEntryIdSet);
        input.putWhiteEntryIds(existWhiteListIdSet);

        this.tradeCompetitionLimitMapper.updateByPrimaryKeySelective(input);
        saveParticipant(competition, entryIds + "," + whiteListIds);
        //更新缓存
        refreshTradeCompetitionInfoCache(input.getOrgId(), input.getCompetitionCode());

        //todo test cache
        //TradeCompetitionInfo result=this.getTradeCompetitionInfoByCache(input.getOrgId(),input.getCompetitionCode());
    }

    public QueryMyPerformanceResponse queryMyPerformance(long orgId, long userId, String competitionCode) {

        if (StringUtils.isEmpty(competitionCode)) {
            throw new BrokerException(BrokerErrorCode.PARAM_INVALID);
        }

        TradeCompetitionInfo competitionInfo = getTradeCompetitionInfoByCache(orgId, competitionCode);

        if (competitionInfo == null) {
            QueryMyPerformanceResponse.newBuilder().build();
        }

        String token = getContractToken(orgId, competitionInfo.getSymbolId());

        String today = getStatisticsDuration(competitionInfo.getStartTime());
        List<TradeCompetitionResult> rankList = Lists.newArrayList();
        //1=总收益率,2=总收益额,3=当日收益率,4=当日收益额
        TradeCompetitionResult totalResult = queryUserPerformance(orgId, competitionCode, userId);
        TradeCompetitionResultDaily todayResult = queryUserPerformanceByDay(orgId, competitionCode, userId, today);

        String earningsAmount = "0";
        String earningsAmountToday = "0";
        String earningsRate = "0";
        String earningsRateToday = "0";

        if (Objects.nonNull(todayResult)) {
            earningsAmountToday = todayResult.getIncomeAmountSafe();
            earningsRateToday = todayResult.getRateSafe();
        }

        if (Objects.nonNull(totalResult)) {
            earningsAmount = totalResult.getIncomeAmountSafe();
            earningsRate = totalResult.getRateSafe();
        }

        return QueryMyPerformanceResponse.newBuilder()
                .setEarningsAmountToday(earningsAmountToday)
                .setEarningsAmountTotal(earningsAmount)
                .setEarningsRateToday(earningsRateToday)
                .setEarningsRateTotal(earningsRate)
                .setToken(token)
                .build();
    }

    public String getContractToken(Long orgId, String symbolId) {
        //获取到期货的计价tokenId
        Symbol symbol = this.symbolMapper.getOrgSymbol(orgId, symbolId);
        if (Objects.isNull(symbol)) {
            return "";
        }

        return symbol.getQuoteTokenId();
    }


    public void getUserSnapshot(Long orgId, Long competitionId, String startStr, String endStr, Long userId) {

        log.info("getTradeRankListDailySnapshot work start...");

        TradeCompetition exp = new TradeCompetition();
        exp.setOrgId(orgId);
        exp.setId(competitionId);

        TradeCompetition tradeCompetition = tradeCompetitionMapper.selectOne(exp);

        if (tradeCompetition == null) {
            log.info("No find current trade competition is null");
            return;
        }


        if (tradeCompetition.getStartTime().getTime() > System.currentTimeMillis()) {
            return;
        }

        long startTime = System.currentTimeMillis();
        DateTimeFormatter df = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime start = LocalDateTime.parse(startStr, df);
        LocalDateTime end = LocalDateTime.parse(endStr, df);

        //day格式 2019-11-13~2019-11-14
        String day = getStatisticsDuration(start, end);
        log.info("day {},code={}", day, tradeCompetition.getCompetitionCode());

        //获取交易大赛数据
        TradeCompetitionInfo info
                = getTradeCompetitionInfoByCache(tradeCompetition.getOrgId(), tradeCompetition.getCompetitionCode());
        if (info == null) {
            log.info("No find current trade competition orgId {} competitionCode {}", tradeCompetition.getOrgId(), tradeCompetition.getCompetitionCode());
            return;
        }

        //参赛名单
        Set<Long> allList = Sets.newHashSet(userId);
        log.info("all list {}", JSON.toJSONString(allList));

        //获取快照最新的批次号
        String newBatch = statisticsContractService.queryMaxTimeByTime(tradeCompetition.getOrgId(), tradeCompetition.getSymbolId(), endStr);
        log.info("newBatch {},code={}", newBatch, tradeCompetition.getCompetitionCode());

        //获取最新参赛快照数据
        //快照列表
        List<ContractSnapshot> localList = statisticsContractService.queryNewContractSnapshot(tradeCompetition.getOrgId(), new LinkedList<>(allList),
                tradeCompetition.getSymbolId(), newBatch, null);

        log.info("contractSnapshot list {}", JSON.toJSONString(localList));
        if (org.apache.commons.collections4.CollectionUtils.isEmpty(localList)) {
            log.info("contractSnapshot list is null  orgId {} contractId {} batch {}", tradeCompetition.getOrgId(), tradeCompetition.getSymbolId(), newBatch);
            return;
        }

        //获取到所有的期货accountId 用户获取入账信息
        List<Long> accounts = localList.stream().map(ContractSnapshot::getAccountId).collect(Collectors.toList());
        if (org.apache.commons.collections4.CollectionUtils.isEmpty(accounts)) {
            log.info("futures accounts list is null {}", JSON.toJSONString(accounts));
            return;
        }
        //获取到期货的计价tokenId
        Symbol symbol = this.symbolMapper.getOrgSymbol(tradeCompetition.getOrgId(), tradeCompetition.getSymbolId());
        if (symbol == null || StringUtils.isEmpty(symbol.getQuoteTokenId())) {
            log.info("symbol is null orgId {} symbolId {}", tradeCompetition.getOrgId(), tradeCompetition.getSymbolId());
            throw new BrokerException(BrokerErrorCode.PARAM_INVALID);
        }

        //查询每天的资金流水
        List<BalanceFlowSnapshot> comeInbalanceList = this.statisticsBalanceService
                .queryComeInBalanceFlow(tradeCompetition.getOrgId(), symbol.getQuoteTokenId(),
                        accounts,
                        startStr, endStr,
                        BusinessSubject.USER_ACCOUNT_TRANSFER_VALUE);

        log.info("comein balance list {}", JSON.toJSONString(comeInbalanceList));

        List<BalanceFlowSnapshot> outgobalanceList = this.statisticsBalanceService
                .queryOutgoBalanceFlow(tradeCompetition.getOrgId(), symbol.getQuoteTokenId(),
                        accounts,
                        startStr, endStr,
                        BusinessSubject.USER_ACCOUNT_TRANSFER_VALUE);

        log.info("outgo balance list {}", JSON.toJSONString(comeInbalanceList));

        Map<Long, BigDecimal> comeInbalanceMap = comeInbalanceList
                .stream().collect(Collectors.toMap(BalanceFlowSnapshot::getAccountId, BalanceFlowSnapshot::getChanged));

        Map<Long, BigDecimal> outgobalanceMap = outgobalanceList
                .stream().collect(Collectors.toMap(BalanceFlowSnapshot::getAccountId, BalanceFlowSnapshot::getChanged));

        Map<String, String> listMap = new HashMap<>();
        //门槛数据初始化
        if (StringUtils.isNotEmpty(info.getLimitStr())) {
            try {
                listMap = JsonUtil.defaultGson().fromJson(info.getLimitStr(), Map.class);
            } catch (JsonSyntaxException e) {
                log.error("refreshTradeRankListSnapshot error: competitionCode {}, limit json {}", info.getCompetitionCode(), info.getLimitStr());
            }
        }

        List<TradeCompetitionResultDaily> tradeCompetitionResultList = buildListOfTradeCompetitionResultDaily(localList, Sets.newHashSet(), allList, listMap,
                comeInbalanceMap, outgobalanceMap, tradeCompetition, day, startStr, endStr);
        if (org.apache.commons.collections4.CollectionUtils.isEmpty(tradeCompetitionResultList)) {
            log.info("last result list is null orgId {} competitionCode {}", tradeCompetition.getOrgId(), tradeCompetition.getCompetitionCode());
            return;
        }

        long endTime = System.currentTimeMillis();
        log.info("getTradeRankListDailySnapshot consume mills {}, result={}", (endTime - startTime), JSON.toJSON(tradeCompetitionResultList.get(0)));
    }

    //合约帝自动锁仓解锁TASK
    @Scheduled(cron = "0 1,11,21,31,41,51 * * * ?")
    public void contractTemporaryTask() throws Exception {
        Boolean locked = RedisLockUtils.tryLock(redisTemplate, CONTRACT_TEMPORARY_TASK, 9 * 60 * 1000);
        if (!locked) {
            log.warn(" **** TradeCompetition **** contractTemporaryTask cannot get lock");
            return;
        }

        List<ContractTemporaryInfo> contractTemporaryInfoList = this.contractTemporaryInfoMapper.queryAllContractTemporaryInfo();
        if (org.apache.commons.collections4.CollectionUtils.isEmpty(contractTemporaryInfoList)) {
            return;
        }
        contractTemporaryInfoList.forEach(info -> {
            User user = this.userMapper.getByMobileAndOrgId(info.getOrgId(), info.getMobile());
            if (info.getStatus().equals(0)) {
                //是否注册 注册就空投到锁仓
                if (user != null && info.getLockOrderId() > 0) {
                    Account account = this.accountMapper.getMainAccount(user.getOrgId(), user.getUserId());
                    List<SyncTransferRequest> syncTransferRequests = new ArrayList<>();
                    syncTransferRequests.add(SyncTransferRequest.newBuilder()
                            .setClientTransferId(info.getLockOrderId())
                            .setSourceOrgId(info.getOrgId())
                            .setSourceFlowSubject(BusinessSubject.AIRDROP)
                            .setSourceAccountType(AccountType.OPERATION_ACCOUNT)
                            .setFromPosition(false)
                            .setTokenId(info.getTokenId())
                            .setAmount(info.getAmount().setScale(ProtoConstants.PRECISION, RoundingMode.DOWN).toPlainString())
                            .setTargetAccountId(account.getAccountId())
                            .setToPosition(true)
                            .setTargetOrgId(info.getOrgId())
                            .setTargetFlowSubject(BusinessSubject.AIRDROP)
                            .build());
                    BatchSyncTransferRequest batchSyncTransferRequest = BatchSyncTransferRequest
                            .newBuilder()
                            .setBaseRequest(BaseReqUtil.getBaseRequest(info.getOrgId()))
                            .addAllSyncTransferRequestList(syncTransferRequests)
                            .build();
                    SyncTransferResponse response = grpcBatchTransferService.batchSyncTransfer(batchSyncTransferRequest);
                    if (response == null || !SyncTransferResponse.ResponseCode.SUCCESS.equals(response.getCode())) {
                        log.info("contractTemporaryTask lock fail userId {} tokenId {} amount {} msg {}", user.getUserId(), info.getTokenId(), info.getAmount(), response.getMsg());
                    } else {
                        //更新状态
                        this.contractTemporaryInfoMapper.updateStatusById(1, info.getId());
                    }
                }
            } else if (info.getStatus().equals(1) && user != null && info.getUnlockOrderId() > 0) {
                UserVerify userVerify = UserVerify.decrypt(userVerifyMapper.getByUserId(user.getUserId()));
                if (userVerify == null) {
                    return;
                }
                //是否高级KYC 高级KYC就解锁到可用
                if (!userVerify.isSeniorVerifyPassed()) {
                    return;
                }

                UnlockBalanceResponse response = balanceService.unLockBalance(user.getUserId(),
                        info.getOrgId(),
                        info.getAmount().stripTrailingZeros().toPlainString(),
                        info.getTokenId(),
                        "合约帝活动",
                        info.getUnlockOrderId());

                if (response == null || response.getCode() != UnlockBalanceResponse.ReplyCode.SUCCESS) {
                    log.info("contractTemporaryTask unlock fail resultMsg {} resultCode {}", response.getMsg(), response.getCodeValue());
                    return;
                }

                this.contractTemporaryInfoMapper.updateStatusById(2, info.getId());
            }
        });
    }

    public void initContractTemporaryOrderId() {
        List<ContractTemporaryInfo> contractTemporaryInfoList = this.contractTemporaryInfoMapper.queryAllInitContractTemporaryInfo();
        contractTemporaryInfoList.forEach(info -> {
            this.contractTemporaryInfoMapper.updateLockOrderIdById(sequenceGenerator.getLong(), sequenceGenerator.getLong(), info.getId());
        });
    }

    @Transactional(isolation = Isolation.READ_COMMITTED, propagation = Propagation.REQUIRED, rollbackFor = Throwable.class)
    public SignUpTradeCompetitionResponse signUpTradeCompetition(SignUpTradeCompetitionRequest request) {
        //校验大赛是否存在 过期
        TradeCompetition tradeCompetition = this.tradeCompetitionMapper.queryByCode(request.getHeader().getOrgId(), request.getCompetitionCode());
        if (tradeCompetition == null) {
            log.info("signUpTradeCompetition error tradeCompetition is null");
            throw new BrokerException(BrokerErrorCode.PARAM_INVALID);
        }

        if (tradeCompetition.getEndTime().getTime() < System.currentTimeMillis()) {
            log.info("signUpTradeCompetition error tradeCompetition is null");
            throw new BrokerException(BrokerErrorCode.PARAM_INVALID);
        }

        //交易当前用户是否存在大赛中
        TradeCompetitionInfo info = getTradeCompetitionInfoByCache(tradeCompetition.getOrgId(), tradeCompetition.getCompetitionCode());
        if (info == null) {
            return SignUpTradeCompetitionResponse.getDefaultInstance();
        }
//        //参赛名单 白名单数据
//        Set<Long> allList = queryAllUserList(info);
//        if (allList.contains(request.getHeader().getUserId())) {
//            log.info("SignUpTradeCompetition error user already exists userId {} ", request.getHeader().getUserId());
//            return SignUpTradeCompetitionResponse.getDefaultInstance();
//        }
        Example example = new Example(TradeCompetitionParticipant.class);
        example.createCriteria()
                .andEqualTo("orgId", request.getHeader().getOrgId())
                .andEqualTo("userId", request.getHeader().getUserId())
                .andEqualTo("competitionCode", request.getCompetitionCode());
        TradeCompetitionParticipant participantList = tradeCompetitionParticipantMapper.selectOneByExample(example);
        if (participantList == null) {
            TradeCompetitionParticipant tradeCompetitionParticipant = new TradeCompetitionParticipant();
            tradeCompetitionParticipant.setOrgId(request.getHeader().getOrgId());
            tradeCompetitionParticipant.setCompetitionId(tradeCompetition.getId());
            tradeCompetitionParticipant.setCompetitionCode(request.getCompetitionCode());
            tradeCompetitionParticipant.setUserId(request.getHeader().getUserId());
            tradeCompetitionParticipant.setNickname(request.getNickName());
            tradeCompetitionParticipant.setWechat(request.getWechat());
            this.tradeCompetitionParticipantMapper.insert(tradeCompetitionParticipant);
        }
        TradeCompetitionLimit competitionLimit
                = tradeCompetitionLimitMapper.queryByCode(request.getHeader().getOrgId(), request.getCompetitionCode());
        if (competitionLimit != null && StringUtils.isNotEmpty(competitionLimit.getEnterIdStr())) {
            //lock 增加参赛人员
            TradeCompetitionLimit limit = tradeCompetitionLimitMapper.lock(competitionLimit.getId());
            Set<Long> entryIdSet = Sets.newHashSet(limit.listEnterId());
            if (entryIdSet.contains(String.valueOf(request.getHeader().getUserId()))) {
                return SignUpTradeCompetitionResponse.getDefaultInstance();
            }
            String enterId = limit.getEnterIdStr() + "," + request.getHeader().getUserId();
            this.tradeCompetitionLimitMapper.updateEnterUser(enterId, limit.getId());
        } else {
            TradeCompetitionLimit limit = tradeCompetitionLimitMapper.lock(competitionLimit.getId());
            String enterId = String.valueOf(request.getHeader().getUserId());
            this.tradeCompetitionLimitMapper.updateEnterUser(enterId, limit.getId());
        }
        return SignUpTradeCompetitionResponse.getDefaultInstance();
    }


    public static void main(String[] args) {
//        TradeCompetitionRank tradeCompetitionRank = new TradeCompetitionRank();
//        tradeCompetitionRank.setRate(new BigDecimal("10"));
//        tradeCompetitionRank.setUserId(1570786173932L);
//
//        TradeCompetitionRank tradeCompetitionRank2 = new TradeCompetitionRank();
//        tradeCompetitionRank2.setRate(new BigDecimal("20"));
//        tradeCompetitionRank2.setUserId(1570786173932L);
//
//        List<TradeCompetitionRank> ranks = new TreeList<>();
//        ranks.add(tradeCompetitionRank2);
//        ranks.add(tradeCompetitionRank);
//
//        Map<String, Object> map = new HashMap<>();
//        map.put("competitionId", 1000);
//        map.put("competitionCode", "BTC-BHT-6001-20190929144105");
//        map.put("startTime", new Date().getTime());
//        map.put("endTime", new Date().getTime());
//        map.put("description", "<p>111</p>");
//        map.put("bannerUrl", "");
//        map.put("type", 1);
//        map.put("status", 1);
//        map.put("receiveTokenId", "BHT");
//        map.put("receiveTokenName", "BHT");
//        map.put("rankList", ranks);
//        System.out.println(JSON.toJSONString(map));

//        BigDecimal rate = BigDecimal.ZERO;
//        BigDecimal denominator = new BigDecimal("-1").setScale(18, RoundingMode.DOWN);
//        BigDecimal value = new BigDecimal("-100");

        //分母必须大于0 否则收益率置为0
//        if (denominator != null && denominator.compareTo(BigDecimal.ZERO) != 0) {
//            rate = value.divide(denominator, 18, RoundingMode.DOWN);
//        }

//        System.out.println(rate.toPlainString());

        BigDecimal bigDecimal = new BigDecimal("-1");


        System.out.println(Locale.CHINA);

        System.out.println(Locale.US);

        int startIndex = PageUtil.getStartIndex(1, 1);

        System.out.println(startIndex);

        DateTimeFormatter timeDtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        LocalDateTime ldt = LocalDateTime.parse("2020-02-24 04:00:00", timeDtf).atZone(ZoneId.systemDefault()).toLocalDateTime();

        System.out.println(ldt.toString());

        //TODO 计算分子
        BigDecimal currentBalance = new BigDecimal("144.492302218071270000");
        BigDecimal currentUnrealizedPnl = new BigDecimal("4.640600000000006275");
        BigDecimal changedTotal = new BigDecimal("101.000000000000000000");
        BigDecimal changedTotalNegative = new BigDecimal("0.0");
        BigDecimal beforeBalance = new BigDecimal("100.614134000000000000");
        BigDecimal beforeUnrealizedPnl = new BigDecimal("-9.750000000000000000");

        //TODO 非中途参赛算法
        BigDecimal numerator = currentBalance
                .add(currentUnrealizedPnl)
                .subtract(changedTotal.subtract(changedTotalNegative.abs()))
                .subtract(beforeBalance.add(beforeUnrealizedPnl))
                .setScale(18, RoundingMode.DOWN);
        System.out.println("非中途加入 分子为:" + numerator.toPlainString());

        //TODO 中途参赛算法
        BigDecimal tmp;
        if (changedTotal.compareTo(beforeBalance) > 0) {
            tmp = changedTotal.subtract(changedTotalNegative.abs());
        } else {
            tmp = beforeBalance.add(beforeUnrealizedPnl);
        }
        BigDecimal newNumerator = currentBalance
                .add(currentUnrealizedPnl)
                .subtract(tmp)
                .setScale(18, RoundingMode.DOWN);

        System.out.println("中途加入 分子为:" + newNumerator.toPlainString());

        BigDecimal denominator = beforeBalance
                .add(changedTotal)
                .add(beforeUnrealizedPnl)
                .setScale(18, RoundingMode.DOWN);
        System.out.println("非中途加入 分母为:" + denominator.toPlainString());

        BigDecimal tmp1 = beforeBalance;
        if (changedTotal.compareTo(tmp1) > 0) {
            tmp1 = changedTotal;
        }
        BigDecimal newDenominator = tmp1.add(beforeUnrealizedPnl)
                .setScale(18, RoundingMode.DOWN);

        System.out.println("中途加入 分母为:" + newDenominator.toPlainString());

        BigDecimal rate = BigDecimal.ZERO;
        if (denominator != null && denominator.compareTo(BigDecimal.ZERO) != 0) {
            rate = numerator.divide(denominator, 18, RoundingMode.DOWN);
        }

        BigDecimal newRate = BigDecimal.ZERO;
        if (newDenominator != null && newDenominator.compareTo(BigDecimal.ZERO) != 0) {
            newRate = newNumerator.divide(newDenominator, 18, RoundingMode.DOWN);
        }

        System.out.println("非中途加入 收益率为:" + rate.toPlainString() + " || 收益额为:" + numerator);
        System.out.println("中途加入 收益率为:" + newRate.toPlainString() + " || 收益额为:" + newNumerator);


        Map<String, BigDecimal> map = new HashMap<>();
        map.put("BUSDT", BigDecimal.ZERO);
        System.out.println(new Gson().toJson(map.keySet()));
//        String string = "217324520720302080,569394061023389184,562991557192832768,235208370586648576,570476570226215168,565478114256487936,559161864106642176,557983170889611520,243981498217398272,509269718205785600,447883561123785728,562449982645718528,411621691538882048,394893364673425152,569025261425927936,569025253918123008,566182278825513472,243906253485768704,559661135960578048,564358192877136128,241090761150693376,415956082440366848,569379893176182016,344586742457958400,564367170717739520,564592591086415360,347646878235757312,527113013791305728,569458823820226816,475337804910720000,567314971793560576,569491296700611584,569654050132604928,558383741224188928,565239599279112192,565366014276274432,351150345159970048,396143320776357120,569374809780397568,215870172853174272,402326901961843712,570292691242600192,569019417216951552,255682410698768384,509267192387528704,566570225521397504,328268321256636416,339630294816784384,475156521974786560,559198474147827200,559075323166427648,558237408551074048,424562861852180992,567267246637059840,415661673756845056,487648348774667776,559215597872057088,558200486906657792,496374794573703168,241214160359391232,559227669599062784,216464534410625024,479055861537617920,419988392596839424,565172308466204672,427259334503023616,569495415591025152,566501568254578176,553516005511743488,564344839135947520,345268346192920576,502860016127351040,489664985094763520,327586793035923456,564353123196725760,570487175330941184,471535392131401984";
//        List<String> stringList = Splitter.on(",").trimResults().splitToList(string);
//        System.out.println(stringList.size());
    }
}
