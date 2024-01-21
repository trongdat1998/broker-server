package io.bhex.broker.server.grpc.server.service;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import io.bhex.base.account.BusinessSubject;
import io.bhex.base.account.QueryBalanceReply;
import io.bhex.base.account.QueryBalanceRequest;
import io.bhex.broker.grpc.common.Header;
import io.bhex.broker.grpc.statistics.QueryColdWalletBalanceResponse;
import io.bhex.broker.server.elasticsearch.entity.BalanceFlow;
import io.bhex.broker.server.elasticsearch.entity.TradeDetail;
import io.bhex.broker.server.elasticsearch.service.IBalanceFlowHistoryService;
import io.bhex.broker.server.elasticsearch.service.ITradeDetailHistoryService;
import io.bhex.broker.server.grpc.client.service.GrpcBalanceService;
import io.bhex.broker.server.grpc.server.service.statistics.StatisticsOrgService;
import io.bhex.broker.server.model.*;
import io.bhex.broker.server.primary.mapper.SymbolMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

@Slf4j
@Service
public class OrgStatisticsService {

    private static final int MAX_LIMIT = 500;

    private static final int MAX_TOP = 500;

    @Resource
    private GrpcBalanceService grpcBalanceService;

    @Resource
    private AccountService accountService;

    @Resource
    private ITradeDetailHistoryService tradeDetailHistoryService;

    @Resource
    private IBalanceFlowHistoryService balanceFlowHistoryService;

    @Resource
    private StatisticsOrgService statisticsOrgService;


    @Resource
    private SymbolMapper symbolMapper;

    public List<OrgBalanceSummary> orgBalanceSummary(Long orgId) {
        return statisticsOrgService.orgBalanceSummary(orgId);
    }

    /**
     * 查询资产持仓信息(支持查询用户)
     *
     * @param orgId   券商id
     * @param userId  用户id，可以为空或者0
     * @param tokenId 资产token
     * @param fromId  记录fetch from Id: trade_detail_id < fromId
     * @param endId   记录fetch end id: trade_detail_id > endId
     * @param limit   limit
     * @return balance info
     */
    public List<StatisticsBalance> queryTokenHoldInfo(Long orgId, Long userId, String tokenId,
                                                      Long fromId, Long endId, Integer limit) {
        return statisticsOrgService.queryTokenHoldInfo(orgId, userId, tokenId,
                fromId, endId, limit);
    }

    public QueryColdWalletBalanceResponse queryColdWalletBalanceInfo(Long orgId) {
        QueryBalanceReply reply = grpcBalanceService.queryColdWalletInfo(QueryBalanceRequest.newBuilder().setOrgId(orgId).build());
        Map<String, QueryBalanceReply.ColdWalletBalance> coldWalletBalanceMap = reply.getBalancesMap();
        List<QueryColdWalletBalanceResponse.ColdWalletBalance> coldWalletBalanceList = Lists.newArrayList();
        coldWalletBalanceMap.forEach((key, value) -> {
            QueryColdWalletBalanceResponse.ColdWalletBalance coldWalletBalance = QueryColdWalletBalanceResponse.ColdWalletBalance.newBuilder()
                    .setTokenId(key)
                    .setTotal(value.getTotal())
                    .setColdHold(value.getColdWalletHold())
                    .setHotRemain(value.getHotWalletRemain())
                    .build();
            coldWalletBalanceList.add(coldWalletBalance);
        });
        return QueryColdWalletBalanceResponse.newBuilder().addAllColdWalletBalance(coldWalletBalanceList).build();
    }

    public List<StatisticsBalance> queryTokenHoldTopInfo(Long orgId, String tokenId, Integer top) {
        return statisticsOrgService.queryTokenHoldTopInfo(orgId, tokenId, top);
    }

    //后台查询资产排行信息
    public List<StatisticsBalance> adminQueryTokenHoldTopInfo(Long orgId, String tokenId, Long userId, Integer top) {
        return statisticsOrgService.adminQueryTokenHoldTopInfo(orgId, tokenId, userId, top);
    }

    /**
     * 查询充币订单(支持查询用户)
     *
     * @param orgId     券商id
     * @param userId    用户id，可以为空或者0
     * @param tokenId   交易token
     * @param startTime 开始时间 match_time >= startTime
     * @param endTime   结束时间 match_time <= endTime
     * @param fromId    记录fetch from Id: trade_detail_id < fromId
     * @param endId     记录fetch end id: trade_detail_id > endId
     * @param limit     limit
     * @return 充币订单列表
     */
    public List<StatisticsDepositOrder> queryOrgDepositOrder(Long orgId, Long userId, String tokenId, Long startTime, Long endTime,
                                                             Long fromId, Long endId, Integer limit, String address, String txId) {
        return statisticsOrgService.queryOrgDepositOrder(orgId, userId, tokenId, startTime, endTime, fromId, endId, limit, address, txId);
    }

    /**
     * 查询提币订单(支持查询用户)
     *
     * @param orgId     券商id
     * @param userId    用户id，可以为空或者0
     * @param tokenId   交易token
     * @param startTime 开始时间 match_time >= startTime
     * @param endTime   结束时间 match_time <= endTime
     * @param fromId    记录fetch from Id: trade_detail_id < fromId
     * @param endId     记录fetch end id: trade_detail_id > endId
     * @param limit     limit
     * @return 提币订单列表
     */
    public List<StatisticsWithdrawOrder> queryOrgWithdrawOrder(Long orgId, Long userId, String tokenId, Long startTime, Long endTime,
                                                               Long fromId, Long endId, Integer limit, String address, String txId) {
        return statisticsOrgService.queryOrgWithdrawOrder(orgId, userId, tokenId, startTime, endTime, fromId, endId, limit, address, txId);
    }

    /**
     * 查询otc订单(支持查询用户)
     *
     * @param orgId     券商id
     * @param userId    用户id，可以为空或者0
     * @param tokenId   交易token
     * @param startTime 开始时间 match_time >= startTime
     * @param endTime   结束时间 match_time <= endTime
     * @param fromId    记录fetch from Id: trade_detail_id < fromId
     * @param endId     记录fetch end id: trade_detail_id > endId
     * @param limit     limit
     * @return OTC订单数据
     */
    public List<StatisticsOTCOrder> queryOrgOTCOrder(Long orgId, Long userId, String tokenId, Long startTime, Long endTime,
                                                     Long fromId, Long endId, Integer limit) {
        return statisticsOrgService.queryOrgOTCOrder(orgId, userId, tokenId, startTime, endTime, fromId, endId, limit);
    }

    /**
     * 币对交易手续费统计(可支持查询用户)
     *
     * @param orgId     券商id
     * @param userId    用户id，可以为空或者0
     * @param symbolId  交易币对，可以为空
     * @param startTime 开始时间 match_time >= startTime
     * @param endTime   结束时间 match_time <= endTime
     * @return 币对交易手续费信息
     */
    public List<StatisticsSymbolTradeFee> statisticsSymbolTradeFee(Long orgId, Long userId, String symbolId, Long startTime, Long endTime) {
        return statisticsOrgService.statisticsSymbolTradeFee(orgId, userId != null && userId > 0 ? accountService.getAccountId(orgId, userId) : 0, symbolId,
                startTime, endTime);
    }

    /**
     * 成交手续费token top统计
     *
     * @param orgId      券商id
     * @param symbolId   交易币对
     * @param feeTokenId 手续费token
     * @param startTime  开始时间 match_time >= startTime
     * @param endTime    结束时间 match_time <= endTime
     * @param top        top值
     * @return top用户列表Id
     */
    public List<StatisticsTradeFeeTop> statisticsTradeFeeTop(Long orgId, String symbolId, String feeTokenId, Long startTime, Long endTime, Integer top) {
        return statisticsOrgService.statisticsTradeFeeTop(orgId, symbolId, feeTokenId,
                startTime, endTime, top);
    }

    public List<TradeDetail> queryEsTradeDetail(Header header, Long accountId, int queryDataType, Long orderId,
                                                String symbolId, Long startTime, Long endTime, Long fromId, Long endId, Integer limit) {
        boolean withFuturesInfo = false;
        if (!Strings.isNullOrEmpty(symbolId)) {
            queryDataType = 0;
            Symbol symbol = symbolMapper.getOrgSymbol(header.getOrgId(), symbolId);
            if (symbol != null && symbol.getCategory() == 4) {
                withFuturesInfo = true;
            }
        } else if (queryDataType == 3) {
            withFuturesInfo = true;
        }
        log.info("queryEsTradeDetail -> queryWithCondition:condition:[orgId:{}, userId:{}, accountId:{}, queryDateType:{}, orderId:{} symbolId:{}, startTime:{}, endTIme:{}. fromId:{}, endId:{}, limit:{}, withFuturesInfo:{}]",
                header.getOrgId(), header.getUserId(), accountId, queryDataType, orderId, symbolId, startTime, endTime, fromId, endId, limit, withFuturesInfo);
        List<TradeDetail> tradeDetailList = tradeDetailHistoryService.queryWithCondition(header, accountId, queryDataType, orderId, symbolId,
                "", null, startTime, endTime, fromId, endId, limit, withFuturesInfo);
        if (fromId == 0 && endId > 0 && !CollectionUtils.isEmpty(tradeDetailList)) {
            Collections.reverse(tradeDetailList);
        }
        return tradeDetailList;
    }

    public List<BalanceFlow> queryEsBalanceFlow(Header header, Long accountId, String tokenId, List<Integer> businessSubjectValueList,
                                                long startTime, long endTime, Long fromId, Long endId, int limit) {
        List<String> tokenIds = Lists.newArrayList();
        if (!Strings.isNullOrEmpty(tokenId)) {
            tokenIds.add(tokenId);
        }
        List<BusinessSubject> businessSubjectList = Lists.newArrayList();
        if (!CollectionUtils.isEmpty(businessSubjectValueList)) {
            businessSubjectList = businessSubjectValueList.stream().map(BusinessSubject::forNumber).filter(Objects::nonNull).collect(Collectors.toList());
        }
        List<BalanceFlow> balanceFlowList = balanceFlowHistoryService.queryWithCondition(header, accountId, tokenIds, businessSubjectList,
                startTime, endTime, fromId, endId, limit);
        if (fromId == 0 && endId > 0 && !CollectionUtils.isEmpty(balanceFlowList)) {
            Collections.reverse(balanceFlowList);
        }
        return balanceFlowList;
    }

    /**
     * 查询成交明细列表(可以支持查询用户的成交明细)
     *
     * @param orgId     券商id
     * @param symbolId  交易币对，可以为空
     * @param startTime 开始时间 match_time >= startTime
     * @param endTime   结束时间 match_time <= endTime
     * @param fromId    记录fetch from Id: trade_detail_id < fromId
     * @param endId     记录fetch end id: trade_detail_id > endId
     * @param limit     limit
     * @return 成交记录
     */
    public List<StatisticsTradeDetail> queryOrgTradeDetail(Long orgId, String symbolId, Long startTime, Long endTime,
                                                           Long fromId, Long endId, Integer limit) {
        return statisticsOrgService.queryOrgTradeDetail(orgId, symbolId, startTime, endTime,
                fromId, endId, limit);
    }

    public List<StatisticsBalanceLockRecord> queryBalanceLockRecordList(Long orgId, Long userId, String tokenId, Integer businessSubject, Integer secondBusinessSubject,
                                                                        Integer status, Long fromId, Long endId, Integer limit) {
        Long accountId = accountService.getAccountId(orgId, userId);
        return statisticsOrgService.queryBalanceLockRecordList(orgId, accountId, tokenId, businessSubject, secondBusinessSubject,
                status, fromId, endId, limit);
    }

    public List<StatisticsBalanceUnlockRecord> queryBalanceUnlockRecordList(Long orgId, Long userId, String tokenId, Integer businessSubject, Integer secondBusinessSubject,
                                                                            Long fromId, Long endId, Integer limit) {
        Long accountId = accountService.getAccountId(orgId, userId);
        return statisticsOrgService.queryBalanceUnlockRecordList(orgId, accountId, tokenId, businessSubject, secondBusinessSubject, fromId, endId, limit);
    }
}
