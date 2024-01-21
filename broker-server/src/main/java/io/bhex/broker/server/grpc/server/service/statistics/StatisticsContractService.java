package io.bhex.broker.server.grpc.server.service.statistics;

import io.bhex.broker.server.model.ContractSnapshot;
import io.bhex.broker.server.statistics.statistics.mapper.StatisticsContractSnapshotMapper;
import org.apache.ibatis.session.RowBounds;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;
import tk.mybatis.mapper.entity.Example;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.util.List;

/**
 * @author wangsc
 * @description 统计合同相关
 * @date 2020-04-17 16:38
 */
@Service
public class StatisticsContractService {

    @Resource
    private StatisticsContractSnapshotMapper statisticsContractSnapshotMapper;

    public String queryAllSymbolMaxTime(Long orgId) {
        return statisticsContractSnapshotMapper.queryAllMaxTime();
    }

    public String queryMaxTime(Long orgId, String symbolId) {
        return statisticsContractSnapshotMapper.queryMaxTime(symbolId);
    }

    public String queryMaxTimeByTime(Long orgId, String symbolId, String endTime) {
        return statisticsContractSnapshotMapper.queryEarliestTimeByTime(endTime, symbolId);
    }

    public List<ContractSnapshot> queryALLForwardNewContractSnapshot(Long orgId, List<Long> userIdList, List<String> symbolIdList, String snapshotTime) {
        //获取券商所有的正向合约最新参赛快照数据
        Example multiLangExp = Example.builder(ContractSnapshot.class).build();
        multiLangExp.createCriteria()
                .andIn("userId", userIdList)
                .andIn("contractId", symbolIdList)
                .andEqualTo("snapshotTime", snapshotTime);
        //快照列表
        return statisticsContractSnapshotMapper.selectByExample(multiLangExp);
    }

    public List<ContractSnapshot> queryNearestContractSnapshotByTime(Long orgId, Long accountId, List<String> symbolIdList, String startTime, Boolean isAsc) {
        Example contractSnapshotExample;
        if (isAsc) {
            contractSnapshotExample = Example.builder(ContractSnapshot.class).orderByAsc("createdAt").build();
        } else {
            contractSnapshotExample = Example.builder(ContractSnapshot.class).orderByDesc("createdAt").build();
        }
        contractSnapshotExample.createCriteria()
                .andEqualTo("accountId", accountId)
                .andIn("contractId", symbolIdList)
                .andGreaterThanOrEqualTo("createdAt", startTime);
        return statisticsContractSnapshotMapper.selectByExampleAndRowBounds(contractSnapshotExample, new RowBounds(0, 1));
    }

    public List<ContractSnapshot> queryNearestContractLessTime(Long accountId, List<String> symbolIdList, String startTime, Boolean isAsc) {
        Example contractSnapshotExample;
        if (isAsc) {
            contractSnapshotExample = Example.builder(ContractSnapshot.class).orderByAsc("createdAt").build();
        } else {
            contractSnapshotExample = Example.builder(ContractSnapshot.class).orderByDesc("createdAt").build();
        }
        contractSnapshotExample.createCriteria()
                .andEqualTo("accountId", accountId)
                .andIn("contractId", symbolIdList)
                .andLessThan("createdAt", startTime);
        return statisticsContractSnapshotMapper.selectByExampleAndRowBounds(contractSnapshotExample, new RowBounds(0, 1));
    }

    public List<ContractSnapshot> queryNearestContractSnapshotBySnapshotTime(Long orgId, Long accountId, List<String> symbolIdList, String snapshotTime) {
        Example beforeMultiLangExp = new Example(ContractSnapshot.class);
        beforeMultiLangExp.createCriteria()
                .andEqualTo("accountId", accountId)
                .andIn("contractId", symbolIdList)
                .andEqualTo("snapshotTime", snapshotTime);
        return statisticsContractSnapshotMapper.selectByExample(beforeMultiLangExp);
    }

    public List<ContractSnapshot> queryNewContractSnapshot(Long orgId, List<Long> userIdList, String symbolId, String snapshotTime,
                                                           String contractBalance) {
        Example multiLangExp = new Example(ContractSnapshot.class);
        if (contractBalance != null && new BigDecimal(contractBalance).compareTo(BigDecimal.ZERO) > 0) {
            multiLangExp.createCriteria()
                    .andIn("userId", userIdList)
                    .andEqualTo("contractId", symbolId)
                    .andEqualTo("snapshotTime", snapshotTime)
                    .andGreaterThanOrEqualTo("contractBalance", contractBalance);
        } else {
            multiLangExp.createCriteria()
                    .andIn("userId", userIdList)
                    .andEqualTo("contractId", symbolId)
                    .andEqualTo("snapshotTime", snapshotTime);
        }
        //快照列表
        return statisticsContractSnapshotMapper.selectByExample(multiLangExp);
    }

    public List<ContractSnapshot> queryListSnapshotByTime(Long orgId, Long accountId, String symbolId, String startTime, BigDecimal contractBalance) {
        return statisticsContractSnapshotMapper.listSnapshot(symbolId, startTime, accountId, contractBalance);
    }
}
