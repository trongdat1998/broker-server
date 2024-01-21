package io.bhex.broker.server.grpc.server.service;

import io.bhex.base.account.UnlockBalanceResponse;
import io.bhex.base.constants.ProtoConstants;
import io.bhex.base.idgen.api.ISequenceGenerator;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.server.model.ActivityLockInterest;
import io.bhex.broker.server.model.ActivityLockInterestCommon;
import io.bhex.broker.server.model.ActivityLockInterestOrder;
import io.bhex.broker.server.model.ActivityUnlockInBatchesRecord;
import io.bhex.broker.server.primary.mapper.ActivityLockInterestCommonMapper;
import io.bhex.broker.server.primary.mapper.ActivityLockInterestMapper;
import io.bhex.broker.server.primary.mapper.ActivityLockInterestOrderMapper;
import io.bhex.broker.server.primary.mapper.ActivityUnlockInBatchesRecordMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;
import tk.mybatis.mapper.entity.Example;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.grpc.server.service
 * @Author: ming.xu
 * @CreateDate: 2019/12/30 2:31 PM
 * @Copyright（C）: 2019 BHEX Inc. All rights reserved.
 */
@Slf4j
@Service
public class ActivityUnlockInBatchesService {

    @Autowired
    private ActivityUnlockInBatchesRecordMapper unlockInBatchesRecordMapper;

    @Autowired
    private ActivityLockInterestMapper activityLockInterestMapper;

    @Autowired
    private ActivityLockInterestOrderMapper activityLockInterestOrderMapper;

    @Autowired
    private ISequenceGenerator sequenceGenerator;

    @Autowired
    private BalanceService balanceService;

    /**
     * 分批解锁 - 记录生成
     * Unlock in batches
     */
    @Transactional(rollbackFor = Throwable.class)
    public String unlockInBatchesResult(Long projectId, Long orgId, BigDecimal unlockRate) {
        //获取活动信息
        ActivityLockInterest activityLamb
                = activityLockInterestMapper.getActivityLockInterestById(projectId);
        if (activityLamb == null) {
            log.info("Activity lock interest not find");
            throw new BrokerException(BrokerErrorCode.DB_RECORD_NOT_EXISTS);
        }

        //获取购买成功活动订单信息
        List<ActivityLockInterestOrder> activityLockInterestOrders = this.activityLockInterestOrderMapper.queryOrderIdListByStatus(projectId, 1);

        if (CollectionUtils.isEmpty(activityLockInterestOrders)) {
            log.info("Activity lock interest orders is null");
            throw new BrokerException(BrokerErrorCode.DB_RECORD_NOT_EXISTS);
        }

        //获取兑换比例
        BigDecimal ratio = activityLamb.getReceiveTokenQuantity().divide(activityLamb.getValuationTokenQuantity(), 18, RoundingMode.DOWN);

        activityLockInterestOrders.forEach(order -> {
            //按百分比解锁 （总购买量 * 兑换比例）= 目标币种获得量 再 乘上 解锁百分比 = 本次解锁量
            BigDecimal unlockAmount
                    = order.getAmount().multiply(ratio).multiply(unlockRate).setScale(ProtoConstants.PRECISION, RoundingMode.DOWN);

            ActivityUnlockInBatchesRecord record = new ActivityUnlockInBatchesRecord();
            record.setBrokerId(order.getBrokerId());
            record.setAccountId(order.getAccountId());
            record.setUserId(order.getUserId());
            record.setProjectId(order.getProjectId());
            record.setProjectCode(order.getProjectCode());
            record.setAmount(unlockAmount);
            record.setTokenId(activityLamb.getReceiveTokenId());
            record.setTokenName(activityLamb.getReceiveTokenName());
            // init status
            record.setUnlockedStatus(ActivityUnlockInBatchesRecord.UNLOCK_INIT);
            record.setCreatedTime(System.currentTimeMillis());
            record.setUpdatedTime(System.currentTimeMillis());
            record.setUnlockClientOrderId(sequenceGenerator.getLong());
            if (unlockInBatchesRecordMapper.insert(record) != 1) {
                throw new BrokerException(BrokerErrorCode.SYSTEM_ERROR);
            }
        });
        return "success";
    }

    /**
     * 分批解锁 - 按记录解锁
     * Unlock in batches
     */
    public String unlockInBatchesUnlock(Long projectId, Long orgId) {
        List<ActivityUnlockInBatchesRecord> batchesRecords
                = queryInitRecord(projectId, orgId);
        if (CollectionUtils.isEmpty(batchesRecords)) {
            log.info("Activity unlock record info not find");
        }

        //拼装解锁的信息
        batchesRecords.forEach(record -> {
            // Long userId, Long orgId, String unLockAmount, String tokenId, String reason, Long clientOrderId
            UnlockBalanceResponse response = balanceService.unLockBalance(record.getUserId(), record.getBrokerId(), record.getAmount().toPlainString(), record.getTokenId(), "Activity Unlock In Batches", record.getUnlockClientOrderId());

            if (response == null || response.getCode() != UnlockBalanceResponse.ReplyCode.SUCCESS) {
                log.warn("unlock fail accountId {} unlockClientOrderId {} msg {}", record.getAccountId(), record.getUnlockClientOrderId(), response.getMsg());
                unlockInBatchesRecordMapper.updateUnlockedStatusById(record.getId(), ActivityUnlockInBatchesRecord.UNLOCK_FAILED);
            } else {
                unlockInBatchesRecordMapper.updateUnlockedStatusById(record.getId(), ActivityUnlockInBatchesRecord.UNLOCK_SUCCESS);
            }
        });

        return "success";
    }

    private List<ActivityUnlockInBatchesRecord> queryInitRecord(Long projectId, Long orgId) {
        Example example = Example.builder(ActivityUnlockInBatchesRecord.class).build();
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("brokerId", orgId);
        criteria.andEqualTo("projectId", projectId);
        // 查询状态为初始化的待解锁订单
        criteria.andEqualTo("unlockedStatus", ActivityUnlockInBatchesRecord.UNLOCK_INIT);

        return unlockInBatchesRecordMapper.selectByExample(example);
    }
}
