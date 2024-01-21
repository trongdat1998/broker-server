package io.bhex.broker.server.grpc.server.service.statistics;

import io.bhex.broker.server.model.*;
import io.bhex.broker.server.statistics.clear.mapper.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import tk.mybatis.mapper.entity.Example;

import javax.annotation.Resource;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author zhangcb
 * @description
 */
@Service
@Slf4j
public class StatisticsOrgService {
    private static final int MAX_LIMIT = 500;

    private static final int MAX_TOP = 500;

    @Resource
    private StatisticsBalanceMapper statisticsBalanceMapper;

    @Resource
    private StatisticsDepositOrderMapper statisticsDepositOrderMapper;

    @Resource
    private StatisticsWithdrawOrderMapper statisticsWithdrawOrderMapper;

    @Resource
    private StatisticsOTCOrderMapper statisticsOTCOrderMapper;

    @Resource
    private StatisticsTradeDetailMapper statisticsTradeDetailMapper;

    @Resource
    private StatisticsSymbolMapper statisticsSymbolMapper;

    @Resource
    private StatisticsBalanceLockRecordMapper statisticsBalanceLockRecordMapper;

    @Resource
    private StatisticsBalanceUnlockRecordMapper statisticsBalanceUnlockRecordMapper;

    public List<OrgBalanceSummary> orgBalanceSummary(Long orgId) {
        return statisticsBalanceMapper.orgBalanceSummary(orgId);
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
        boolean orderDesc = true;
        if (fromId == 0 && endId > 0) {
            orderDesc = false;
        }
        List<StatisticsBalance> balanceList = statisticsBalanceMapper
                .queryOrgTokenBalance(orgId, userId, tokenId,
                        fromId, endId, limit > MAX_LIMIT ? MAX_LIMIT : limit, orderDesc);
        if (!orderDesc) {
            Collections.reverse(balanceList);
        }
        return balanceList;
    }

    public List<StatisticsBalance> queryTokenHoldTopInfo(Long orgId, String tokenId, Integer top) {
        return statisticsBalanceMapper
                .queryOrgTokenBalanceTop(orgId, tokenId, top > MAX_TOP ? MAX_TOP : top);
    }

    //后台查询资产排行信息
    public List<StatisticsBalance> adminQueryTokenHoldTopInfo(Long orgId, String tokenId,
                                                              Long userId, Integer top) {
        return statisticsBalanceMapper
                .queryAdminOrgTokenBalanceTop(orgId, tokenId, userId, top > MAX_TOP ? MAX_TOP : top);
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
    public List<StatisticsDepositOrder> queryOrgDepositOrder(Long orgId, Long userId,
                                                             String tokenId, Long startTime, Long endTime,
                                                             Long fromId, Long endId, Integer limit, String address, String txId) {
        boolean orderDesc = true;
        if (fromId == 0 && endId > 0) {
            orderDesc = false;
        }
        List<StatisticsDepositOrder> depositOrderList = statisticsDepositOrderMapper
                .queryOrgDepositOrder(orgId, userId, tokenId,
                        startTime > 0 ? new Timestamp(startTime) : null,
                        endTime > 0 ? new Timestamp(endTime) : null,
                        fromId, endId, limit > MAX_LIMIT ? MAX_LIMIT : limit, orderDesc, address, txId);
        if (!orderDesc) {
            Collections.reverse(depositOrderList);
        }
        return depositOrderList;
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
    public List<StatisticsWithdrawOrder> queryOrgWithdrawOrder(Long orgId, Long userId,
                                                               String tokenId, Long startTime, Long endTime,
                                                               Long fromId, Long endId, Integer limit, String address, String txId) {
        boolean orderDesc = true;
        if (fromId == 0 && endId > 0) {
            orderDesc = false;
        }
        List<StatisticsWithdrawOrder> withdrawOrderList = statisticsWithdrawOrderMapper
                .queryOrgWithdrawOrder(orgId, userId, tokenId,
                        startTime > 0 ? new Timestamp(startTime) : null,
                        endTime > 0 ? new Timestamp(endTime) : null,
                        fromId, endId, limit > MAX_LIMIT ? MAX_LIMIT : limit, orderDesc, address, txId);
        if (!orderDesc) {
            Collections.reverse(withdrawOrderList);
        }
        return withdrawOrderList;
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
    public List<StatisticsOTCOrder> queryOrgOTCOrder(Long orgId, Long userId, String tokenId,
                                                     Long startTime, Long endTime,
                                                     Long fromId, Long endId, Integer limit) {
        boolean orderDesc = true;
        if (fromId == 0 && endId > 0) {
            orderDesc = false;
        }
        List<StatisticsOTCOrder> otcOrderList = statisticsOTCOrderMapper
                .queryOrgOTCOrder(orgId, userId, tokenId,
                        startTime > 0 ? new Timestamp(startTime) : null,
                        endTime > 0 ? new Timestamp(endTime) : null,
                        fromId, endId, limit > MAX_LIMIT ? MAX_LIMIT : limit, orderDesc);
        if (!orderDesc) {
            Collections.reverse(otcOrderList);
        }
        return otcOrderList;
    }

    /**
     * 币对交易手续费统计(可支持查询用户)
     *
     * @param orgId     券商id
     * @param accountId 用户id，可以为空或者0
     * @param symbolId  交易币对，可以为空
     * @param startTime 开始时间 match_time >= startTime
     * @param endTime   结束时间 match_time <= endTime
     * @return 币对交易手续费信息
     */
    public List<StatisticsSymbolTradeFee> statisticsSymbolTradeFee(Long orgId, Long accountId,
                                                                   String symbolId, Long startTime, Long endTime) {
        return statisticsTradeDetailMapper.statisticsSymbolTradeFee(orgId,
                accountId, symbolId,
                startTime > 0 ? new Timestamp(startTime) : null,
                endTime > 0 ? new Timestamp(endTime) : null);
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
    public List<StatisticsTradeFeeTop> statisticsTradeFeeTop(Long orgId, String symbolId,
                                                             String feeTokenId, Long startTime, Long endTime, Integer top) {
        return statisticsTradeDetailMapper.statisticsTradeFeeTop(orgId, symbolId, feeTokenId,
                startTime > 0 ? new Timestamp(startTime) : null,
                endTime > 0 ? new Timestamp(endTime) : null, top > MAX_TOP ? MAX_TOP : top);
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
    public List<StatisticsTradeDetail> queryOrgTradeDetail(Long orgId, String symbolId,
                                                           Long startTime, Long endTime,
                                                           Long fromId, Long endId, Integer limit) {
        boolean orderDesc = true;
        if (fromId == 0 && endId > 0) {
            orderDesc = false;
        }
        long sevenDaysBefore = System.currentTimeMillis() - 7 * 24 * 3600 * 1000;
        if (orgId == 7070 && (startTime == null || startTime == 0 || startTime < sevenDaysBefore)) {
            startTime = sevenDaysBefore;
        }
        List<StatisticsTradeDetail> tradeDetailList = statisticsTradeDetailMapper.queryOnlyOrgTradeDetail(orgId, symbolId,
                startTime > 0 ? new Timestamp(startTime) : null, endTime > 0 ? new Timestamp(endTime) : null,
                fromId, endId, limit > MAX_LIMIT ? MAX_LIMIT : limit, orderDesc);
        if (!CollectionUtils.isEmpty(tradeDetailList)) {
            List<String> symbolIds = tradeDetailList.stream().map(StatisticsTradeDetail::getSymbolId).distinct().collect(Collectors.toList());
            List<Long> ticketIds = tradeDetailList.stream().map(StatisticsTradeDetail::getTicketId).distinct().collect(Collectors.toList());
            List<StatisticsTradeDetail> matchTradeDetailList = statisticsTradeDetailMapper.queryOrgTradeDetailByTicketId(ticketIds);
            Map<Long, List<StatisticsTradeDetail>> matchTradeMap =
                    matchTradeDetailList.stream().collect(Collectors.groupingBy(StatisticsTradeDetail::getTicketId));
            Example example = new Example(StatisticsSymbol.class);
            Example.Criteria criteria = example.createCriteria();
            criteria.andIn("symbolId", symbolIds);
            List<StatisticsSymbol> symbolList = statisticsSymbolMapper.selectByExample(example);
            Map<String, StatisticsSymbol> symbolMap = symbolList.stream().collect(Collectors.toMap(StatisticsSymbol::getSymbolId, tradeDetail -> tradeDetail, (p, q) -> q));
            tradeDetailList = tradeDetailList.stream().map(tradeDetail -> {
                String feeToken = null;
                if (tradeDetail.getIsActuallyUsedSysToken() == 1) {
                    feeToken = tradeDetail.getSysTokenId();
                } else {
                    StatisticsSymbol symbol = symbolMap.get(tradeDetail.getSymbolId());
                    if (tradeDetail.getSide() == 1 || (symbol.getSecurityType() == 2 || symbol.getSecurityType() == 3)) {
                        feeToken = symbol.getQuoteTokenId();
                    } else {
                        feeToken = symbol.getBaseTokenId();
                    }
                }
                List<StatisticsTradeDetail> matchTradeList = matchTradeMap.get(tradeDetail.getTicketId());
                StatisticsTradeDetail matchTrade = null;
                for (StatisticsTradeDetail statisticsTradeDetail : matchTradeList) {
                    if (!statisticsTradeDetail.getTradeId().equals(tradeDetail.getTradeId())) {
                        matchTrade = statisticsTradeDetail;
                    }
                }
                if (matchTrade == null) {
                    log.warn("cannot find matchInfo with tradeId:{} in statistics trade_detail", tradeDetail.getTradeId());
                    return tradeDetail.toBuilder()
                            .feeToken(feeToken)
                            .matchOrgId(0L)
                            .matchUserId(0L)
                            .matchAccountId(0L)
                            .build();
                } else {
                    return tradeDetail.toBuilder()
                            .feeToken(feeToken)
                            .matchOrgId(tradeDetail.getOrgId().equals(matchTrade.getOrgId()) ? matchTrade.getOrgId() : 0L)
                            .matchUserId(tradeDetail.getOrgId().equals(matchTrade.getOrgId()) ? matchTrade.getUserId() : 0L)
                            .matchAccountId(matchTrade.getAccountId())
                            .build();
                }
            }).collect(Collectors.toList());
            if (!orderDesc) {
                Collections.reverse(tradeDetailList);
            }
        }
        return tradeDetailList;
    }

    public List<StatisticsBalanceLockRecord> queryBalanceLockRecordList(Long orgId, Long accountId,
                                                                        String tokenId, Integer businessSubject, Integer secondBusinessSubject,
                                                                        Integer status, Long fromId, Long endId, Integer limit) {
        boolean orderDesc = true;
        if (fromId == 0 && endId > 0) {
            orderDesc = false;
        }

        List<StatisticsBalanceLockRecord> statisticsBalanceLockRecords = statisticsBalanceLockRecordMapper
                .queryBalanceLockRecordList(orgId, accountId, StringUtils
                                .isEmpty(tokenId) ? null : tokenId,
                        businessSubject, secondBusinessSubject, status, fromId, endId,
                        limit > MAX_LIMIT ? MAX_LIMIT : limit, orderDesc);
        if (!orderDesc) {
            Collections.reverse(statisticsBalanceLockRecords);
        }
        return statisticsBalanceLockRecords;
    }

    public List<StatisticsBalanceUnlockRecord> queryBalanceUnlockRecordList(Long orgId, Long accountId,
                                                                            String tokenId, Integer businessSubject, Integer secondBusinessSubject,
                                                                            Long fromId, Long endId, Integer limit) {
        boolean orderDesc = true;
        if (fromId == 0 && endId > 0) {
            orderDesc = false;
        }
        List<StatisticsBalanceUnlockRecord> statisticsBalanceLockRecords = statisticsBalanceUnlockRecordMapper
                .queryBalanceUnlockRecordList(orgId, accountId,
                        StringUtils.isEmpty(tokenId) ? null : tokenId,
                        businessSubject, secondBusinessSubject, fromId, endId,
                        limit > MAX_LIMIT ? MAX_LIMIT : limit, orderDesc);
        if (!orderDesc) {
            Collections.reverse(statisticsBalanceLockRecords);
        }
        return statisticsBalanceLockRecords;
    }

    public Integer countByUserId(Long orgId, String userId) {
        return statisticsWithdrawOrderMapper.countByUserId(orgId, userId);
    }

    public List<StatisticsWithdrawOrder> getByUserIdWithGiveTime(Long orgId, Long userId, Long startTime, Long endTime) {
        return statisticsWithdrawOrderMapper.getByUserIdWithGiveTime(orgId, userId,
                new Timestamp(startTime), new Timestamp(endTime));
    }
}
