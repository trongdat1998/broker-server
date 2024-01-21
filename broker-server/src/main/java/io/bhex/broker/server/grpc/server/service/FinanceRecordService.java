package io.bhex.broker.server.grpc.server.service;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.server.primary.mapper.FinanceRecordMapper;
import io.bhex.broker.server.model.FinanceRecord;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import javax.annotation.Resource;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Service
public class FinanceRecordService {

    private static final Long MAX_LONG_NUM_ID = 99999999999L;

    @Resource
    private FinanceRecordMapper financeRecordMapper;

    public List<FinanceRecord> getFinanceRecordList(Long orgId, Long userId, Long startRecordId, Integer limit) {
        if (startRecordId == null || startRecordId == 0) {
            startRecordId = MAX_LONG_NUM_ID;
        }
        return financeRecordMapper.getFinanceRecordList(orgId, userId, startRecordId, limit);
    }

    public FinanceRecord getSingleFinanceRecord(Long orgId, Long userId, Long recordId) {
        FinanceRecord record = financeRecordMapper.getFinanceRecord(recordId, orgId, userId);
        if (record == null) {
            throw new BrokerException(BrokerErrorCode.FINANCE_BALANCE_FLOW_NOT_FOUND);
        }
        return record;
    }

    public List<io.bhex.broker.grpc.finance.FinanceRecord> convertRecordList(List<FinanceRecord> recordList) {
        if (CollectionUtils.isEmpty(recordList)) {
            return Lists.newArrayList();
        }
        return recordList.stream().map(this::convertRecord).collect(Collectors.toList());
    }

    public io.bhex.broker.grpc.finance.FinanceRecord convertRecord(FinanceRecord record) {
        return io.bhex.broker.grpc.finance.FinanceRecord.newBuilder()
                .setId(record.getId())
                .setOrgId(record.getOrgId())
                .setUserId(record.getUserId())
                .setAccountId(record.getAccountId())
                .setProductId(record.getProductId())
                .setType(record.getType())
                .setToken(record.getToken())
                .setAmount(record.getAmount().stripTrailingZeros().toPlainString())
                .setStatisticsTime(Strings.nullToEmpty(record.getStatisticsTime()))
                .setStatus(record.getStatus())
                .setCreatedAt(record.getCreatedAt())
                .setUpdatedAt(record.getUpdatedAt())
                .build();
    }

}
