package io.bhex.broker.server.grpc.server.service;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Date;
import java.util.List;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

import io.bhex.base.account.BusinessSubject;
import io.bhex.base.account.SyncTransferRequest;
import io.bhex.base.account.SyncTransferResponse;
import io.bhex.base.account.UnlockBalanceResponse;
import io.bhex.base.idgen.api.ISequenceGenerator;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.server.domain.ActivityMappingStatusType;
import io.bhex.broker.server.grpc.client.service.GrpcBatchTransferService;
import io.bhex.broker.server.grpc.server.service.po.ActivityLockInterestUserInfo;
import io.bhex.broker.server.model.ActivityLockInterest;
import io.bhex.broker.server.model.ActivityLockInterestCommon;
import io.bhex.broker.server.model.ActivityLockInterestLockRecord;
import io.bhex.broker.server.model.ActivityLockInterestMapping;
import io.bhex.broker.server.model.ActivityLockInterestOrder;
import io.bhex.broker.server.primary.mapper.ActivityLockInterestCommonMapper;
import io.bhex.broker.server.primary.mapper.ActivityLockInterestLockRecordMapper;
import io.bhex.broker.server.primary.mapper.ActivityLockInterestMapper;
import io.bhex.broker.server.primary.mapper.ActivityLockInterestMappingMapper;
import io.bhex.broker.server.primary.mapper.ActivityLockInterestOrderMapper;
import io.bhex.broker.server.util.BaseReqUtil;
import io.bhex.broker.server.util.RedisLockUtils;
import lombok.extern.slf4j.Slf4j;
import tk.mybatis.mapper.entity.Example;


/**
 * 活动算奖 发奖
 */
@Slf4j
@Service
public class ActivityLockInterestMappingService {

    private static Integer LUCK_ORDER_STATUS = 11;

    @Autowired
    private ActivityLockInterestMapper activityLockInterestMapper;
    @Autowired
    private ActivityLockInterestOrderMapper activityLockInterestOrderMapper;

    @Autowired
    private ActivityLockInterestMappingMapper activityLockInterestMappingMapper;

    @Autowired
    private GrpcBatchTransferService grpcBatchTransferService;

    @Resource
    private ISequenceGenerator sequenceGenerator;

    @Resource
    private BalanceService balanceService;
    @Resource
    private ActivityLockInterestCommonMapper activityLockInterestCommonMapper;
    @Resource
    private AccountService accountService;

    @Resource
    private ActivityLockInterestLockRecordMapper lockInterestLockRecordMapper;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    /**
     * IEO 均摊 失败转账解冻补偿 10分钟刷一次
     */
    @PostConstruct
    @Scheduled(cron = "0 0/10 * * * ?")
    public void activityFailTask() {
        String lockKey = "ACTIVITY::FAIL::TASK::LOCK::KEY";
        Boolean locked = RedisLockUtils.tryLock(stringRedisTemplate, lockKey, 10 * 60 * 1000);
        if (!locked) {
            return;
        }
        log.info("activityFailTask start");
        //取出来所有的失败的 orgId projectId list 逐条发起重试
        List<ActivityLockInterestMapping> activityLockInterestMappings
                = this.activityLockInterestMappingMapper.queryNotFinishedActivity();

        activityLockInterestMappings.forEach(s -> {
            handelLambActivityTransfer(s.getBrokerId(), s.getProjectId(), ActivityMappingStatusType.FAIL);
            handelActivityUnLockBalance(s.getBrokerId(), s.getProjectId(), ActivityMappingStatusType.FAIL);
        });
    }

    @Transactional(rollbackFor = Throwable.class)
    public void saveActivityResult(Long orgId, Long projectId) {
        //获取活动信息
        ActivityLockInterest activityLamb = this.activityLockInterestMapper.getActivityLockInterestById(projectId);
        if (activityLamb == null) {
            log.info("Activity lock interest not find");
            throw new BrokerException(BrokerErrorCode.DB_RECORD_NOT_EXISTS);
        }

        ActivityLockInterestCommon activityLockInterestCommon = activityLockInterestCommonMapper.getCommonInfoByProjectCode(activityLamb.getProjectCode(), orgId);
        if (activityLockInterestCommon == null || activityLockInterestCommon.getAssetUserId() == null) {
            log.info("Activity lock interest common info not find");
            throw new BrokerException(BrokerErrorCode.DB_RECORD_NOT_EXISTS);
        }

        //通过用户ID 拿到accountId IEO资产账户
        Long accountId = accountService.getAccountId(orgId, activityLockInterestCommon.getAssetUserId());
        log.info("sourceAccountId :{}", accountId);
        if (accountId == null || accountId == 0L) {
            log.info("Activity source account not find");
            throw new BrokerException(BrokerErrorCode.DB_RECORD_NOT_EXISTS);
        }

        //获取购买成功活动订单信息
        List<ActivityLockInterestOrder> activityLockInterestOrders = this.activityLockInterestOrderMapper.queryOrderIdListByStatus(projectId, 1);
        if (CollectionUtils.isEmpty(activityLockInterestOrders)) {
            log.info("Activity lock interest orders is null");
            throw new BrokerException(BrokerErrorCode.DB_RECORD_NOT_EXISTS);
        }

        //计算系数 申购系数 = AT总额/卖出AT总额
        BigDecimal modulus;
        // 如果发售总额大于卖出的数额，就1:1兑换
        if (activityLamb.getTotalCirculation().compareTo(activityLamb.getSoldAmount()) >= 0) {
            modulus = BigDecimal.ONE;
        } else {
            // 如果发售总额小于卖出的数额，就按比例兑换
            modulus = activityLamb.getTotalCirculation().divide(activityLamb.getSoldAmount(), 8, RoundingMode.DOWN);
        }
        //获取兑换比例
        BigDecimal ratio = activityLamb.getReceiveTokenQuantity().divide(activityLamb.getValuationTokenQuantity(), 8, RoundingMode.DOWN);
        log.info("ratio :{}", ratio.toPlainString());
        activityLockInterestOrders.forEach(lamb -> {
            //消耗多少token
            BigDecimal useAmount = lamb.getAmount().multiply(modulus).setScale(8, RoundingMode.UP);
            //能获得多少目标token
            BigDecimal luckyAmount = useAmount.multiply(ratio).setScale(8, RoundingMode.DOWN);
            //需要返还多少token
            BigDecimal backAmount = lamb.getAmount().subtract(useAmount).setScale(8, RoundingMode.DOWN);
            ActivityLockInterestMapping activityLockInterestMapping = new ActivityLockInterestMapping();
            activityLockInterestMapping.setBrokerId(orgId);
            activityLockInterestMapping.setAmount(lamb.getAmount());
            activityLockInterestMapping.setUseAmount(useAmount);
            activityLockInterestMapping.setBackAmount(backAmount);
            activityLockInterestMapping.setLuckyAmount(luckyAmount);
            activityLockInterestMapping.setModulus(modulus);
            activityLockInterestMapping.setProjectId(projectId);
            activityLockInterestMapping.setUserId(lamb.getUserId());
            activityLockInterestMapping.setAccountId(lamb.getAccountId());
            activityLockInterestMapping.setSourceAccountId(accountId); //IEO资产账户
            activityLockInterestMapping.setSourceTokenId(activityLamb.getReceiveTokenId()); // IEO 目标tokenId
            activityLockInterestMapping.setTokenId(activityLamb.getPurchaseTokenId()); // IEO tokenId
            activityLockInterestMapping.setCreateTime(new Date());
            activityLockInterestMapping.setIsLock(0);
            activityLockInterestMapping.setTransferStatus(0);
            activityLockInterestMapping.setUnlockStatus(0);
            activityLockInterestMapping.setOrderId(lamb.getId());
            activityLockInterestMapping.setUnlockClientOrderId(sequenceGenerator.getLong());
            activityLockInterestMapping.setUserClientOrderId(sequenceGenerator.getLong());
            activityLockInterestMapping.setIeoAccountClientOrderId(sequenceGenerator.getLong());
            if (this.activityLockInterestMappingMapper.insert(activityLockInterestMapping) != 1) {
                throw new BrokerException(BrokerErrorCode.SYSTEM_ERROR);
            }
        });
    }

    /**
     * 锦鲤中奖名单生成
     */
    @Transactional(rollbackFor = Throwable.class)
    public void saveActivityLuckResult(Long orgId, Long projectId) {
        //获取活动信息
        ActivityLockInterest activityLamb
                = this.activityLockInterestMapper.getActivityLockInterestById(projectId);
        if (activityLamb == null) {
            log.info("Activity lock interest not find");
            throw new BrokerException(BrokerErrorCode.DB_RECORD_NOT_EXISTS);
        }

        ActivityLockInterestCommon activityLockInterestCommon
                = activityLockInterestCommonMapper.getCommonInfo(activityLamb.getProjectCode(), "zh_CN", orgId);
        if (activityLockInterestCommon == null || activityLockInterestCommon.getAssetUserId() == null) {
            log.info("Activity lock interest common info not find");
            throw new BrokerException(BrokerErrorCode.DB_RECORD_NOT_EXISTS);
        }

        //通过用户ID 拿到accountId IEO资产账户
        Long accountId = accountService.getAccountId(orgId, activityLockInterestCommon.getAssetUserId());
        log.info("sourceAccountId :{}", accountId);
        if (accountId == null || accountId == 0L) {
            log.info("Activity source account not find");
            throw new BrokerException(BrokerErrorCode.DB_RECORD_NOT_EXISTS);
        }

        //获取购买成功活动订单信息
        List<ActivityLockInterestOrder> activityLockInterestOrders = this.activityLockInterestOrderMapper.queryOrderIdListByStatus(projectId, LUCK_ORDER_STATUS);
        if (CollectionUtils.isEmpty(activityLockInterestOrders)) {
            log.info("Activity lock interest orders is null");
            throw new BrokerException(BrokerErrorCode.DB_RECORD_NOT_EXISTS);
        }

        //计算锦鲤中奖系数 申购系数 = 锦鲤全额兑奖 所以申购系数是 1
        BigDecimal modulus = BigDecimal.ONE;
        log.info("modulus :{}", modulus.toPlainString());
        BigDecimal useAmountCount = BigDecimal.ZERO;
        BigDecimal luckyAmountCount = BigDecimal.ZERO;

        //获取兑换比例
        BigDecimal ratio = activityLamb.getReceiveTokenQuantity().divide(activityLamb.getValuationTokenQuantity(), 8, RoundingMode.DOWN);
        log.info("ratio :{}", ratio.toPlainString());
        for (ActivityLockInterestOrder lamb : activityLockInterestOrders) {
            //消耗多少token
            BigDecimal useAmount = lamb.getAmount().multiply(modulus).setScale(8, RoundingMode.UP);
            //能获得多少目标token
            BigDecimal luckyAmount = useAmount.multiply(ratio).setScale(8, RoundingMode.DOWN);
            //需要返还多少token
            BigDecimal backAmount = lamb.getAmount().subtract(useAmount).setScale(8, RoundingMode.DOWN);
            ActivityLockInterestMapping activityLockInterestMapping = new ActivityLockInterestMapping();
            activityLockInterestMapping.setBrokerId(orgId);
            activityLockInterestMapping.setAmount(lamb.getAmount());
            activityLockInterestMapping.setUseAmount(useAmount);
            activityLockInterestMapping.setBackAmount(backAmount);
            activityLockInterestMapping.setLuckyAmount(luckyAmount);
            activityLockInterestMapping.setModulus(modulus);
            activityLockInterestMapping.setProjectId(projectId);
            activityLockInterestMapping.setUserId(lamb.getUserId());
            activityLockInterestMapping.setAccountId(lamb.getAccountId());
            activityLockInterestMapping.setSourceAccountId(accountId); //IEO资产账户
            activityLockInterestMapping.setSourceTokenId(activityLamb.getReceiveTokenId()); // IEO 目标tokenId
            activityLockInterestMapping.setTokenId(activityLamb.getPurchaseTokenId()); // IEO tokenId
            activityLockInterestMapping.setCreateTime(new Date());
            activityLockInterestMapping.setIsLock(0);
            activityLockInterestMapping.setTransferStatus(0);
            activityLockInterestMapping.setUnlockStatus(0);
            activityLockInterestMapping.setOrderId(lamb.getId());
            if (this.activityLockInterestMappingMapper.insert(activityLockInterestMapping) != 1) {
                throw new BrokerException(BrokerErrorCode.SYSTEM_ERROR);
            }

            // 消耗token计数
            useAmountCount = useAmountCount.add(useAmount);
            // 获得token计数
            luckyAmountCount = luckyAmountCount.add(luckyAmount);
        }
        BigDecimal totalReceiveCirculation = activityLamb.getTotalReceiveCirculation().subtract(luckyAmountCount);
        BigDecimal totalCirculation = totalReceiveCirculation.divide(activityLamb.getReceiveTokenQuantity(), 8, RoundingMode.HALF_UP).multiply(activityLamb.getValuationTokenQuantity()).setScale(2, RoundingMode.DOWN);
        activityLamb.setTotalCirculation(totalCirculation);
        activityLamb.setSoldAmount(activityLamb.getSoldAmount().subtract(useAmountCount));
        activityLamb.setRealSoldAmount(activityLamb.getRealSoldAmount().subtract(useAmountCount));
        activityLamb.setTotalReceiveCirculation(totalReceiveCirculation);
        if (activityLockInterestMapper.updateByPrimaryKey(activityLamb) != 1) {
            throw new BrokerException(BrokerErrorCode.SYSTEM_ERROR);
        }
    }

    // 处理活动结果
    public void handelLambActivityTransfer(Long orgId, Long projectId, ActivityMappingStatusType statusType) {
        List<ActivityLockInterestMapping> lambMappings
                = this.activityLockInterestMappingMapper.queryMappingListTransferStatus(orgId, projectId, statusType.value());
        if (CollectionUtils.isEmpty(lambMappings)) {
            log.info("Activity lamb mapping info not find");
        }

        //拼装转账的信息 如果失败 则不更新记录 成功则更新记录
        lambMappings.forEach(lamb -> {
            SyncTransferResponse orgTransferResponse;
            SyncTransferResponse userTransferResponse;
            try {
                //运营账户获得AT 从用户手里转出
                SyncTransferRequest transferRequest = SyncTransferRequest.newBuilder()
                        .setBaseRequest(BaseReqUtil.getBaseRequest(orgId))
                        .setClientTransferId(lamb.getIeoAccountClientOrderId())
                        .setSourceOrgId(orgId)
                        .setSourceFlowSubject(BusinessSubject.TRANSFER)
                        .setSourceAccountId(lamb.getAccountId())
                        .setFromPosition(true)
                        .setTokenId(lamb.getTokenId())
                        .setAmount(lamb.getUseAmount().setScale(8, RoundingMode.HALF_UP).toPlainString())
                        .setTargetAccountId(lamb.getSourceAccountId()) //IEO 活动账户
                        .setTargetOrgId(orgId)
                        .setTargetFlowSubject(BusinessSubject.TRANSFER)
                        .build();
                orgTransferResponse
                        = grpcBatchTransferService.syncTransfer(transferRequest);

                //用户获得目标的token 从指定账户转出
                SyncTransferRequest userTransferRequest = SyncTransferRequest.newBuilder()
                        .setBaseRequest(BaseReqUtil.getBaseRequest(orgId))
                        .setClientTransferId(lamb.getUserClientOrderId())
                        .setSourceOrgId(orgId)
                        .setSourceFlowSubject(BusinessSubject.AIRDROP)
                        .setSourceAccountId(lamb.getSourceAccountId()) //IEO 活动账户
                        .setFromPosition(false)
                        .setTokenId(lamb.getSourceTokenId())
                        .setAmount(lamb.getLuckyAmount().setScale(8, RoundingMode.HALF_UP).toPlainString())
                        .setTargetAccountId(lamb.getAccountId())
                        .setTargetOrgId(orgId)
                        .setTargetFlowSubject(BusinessSubject.AIRDROP)
                        .build();
                userTransferResponse
                        = grpcBatchTransferService.syncTransfer(userTransferRequest);
            } catch (Exception ex) {
                //如果有异常 或者超时 则置为失败 补偿重试
                log.warn("IEO Transfer fail id {} ex {}", lamb.getId(), ex);
                activityLockInterestMappingMapper.updateTransferStatusById(lamb.getId(), 2);
                return;
            }

            if ((orgTransferResponse.getCode() != SyncTransferResponse.ResponseCode.SUCCESS) || (userTransferResponse.getCode() != SyncTransferResponse.ResponseCode.SUCCESS)) {
                activityLockInterestMappingMapper.updateTransferStatusById(lamb.getId(), 2);
                return;
            }

            //转账成功 修改订单状态
            if (activityLockInterestMappingMapper.updateTransferStatusById(lamb.getId(), 1) == 1) {
                log.info("lamb userId {} accountId {} tokenId {} backAmount {} transfer success",
                        lamb.getUserId(), lamb.getAccountId(), lamb.getTokenId(), lamb.getBackAmount());
            } else {
                log.info("lamb userId {} accountId {} tokenId {} backAmount {} transfer fail resultMsg {} resultCode {}",
                        lamb.getUserId(), lamb.getAccountId(), lamb.getTokenId(), lamb.getBackAmount());
            }
        });
    }

    public void handelActivityUnLockBalance(Long orgId, Long projectId, ActivityMappingStatusType statusType) {
        List<ActivityLockInterestMapping> activityLockInterestMappings
                = this.activityLockInterestMappingMapper.queryMappingListByUnlockStatus(orgId, projectId, statusType.value());
        if (CollectionUtils.isEmpty(activityLockInterestMappings)) {
            log.info("Activity lamb mapping is null");
        }

        activityLockInterestMappings.forEach(lamb -> {
            if (lamb.getBackAmount().compareTo(BigDecimal.ZERO) == 0) {
                activityLockInterestMappingMapper.updateLockStatusById(lamb.getId(), 1);
                return;
            }
            UnlockBalanceResponse response;
            try {
                response = balanceService.unLockBalance(lamb.getUserId(), lamb.getBrokerId(), lamb.getBackAmount().toPlainString(), lamb.getTokenId(), "", lamb.getUnlockClientOrderId());
            } catch (Exception ex) {
                //如果超时了或者其他异常 或者是其他原因 则直接配置成失败 自动补偿重试
                log.warn("IEO UnLock Balance fail id {} ex {}", lamb.getId(), ex);
                activityLockInterestMappingMapper.updateLockStatusById(lamb.getId(), 2);
                return;
            }
            if (response == null || response.getCode() != UnlockBalanceResponse.ReplyCode.SUCCESS) {
                activityLockInterestMappingMapper.updateLockStatusById(lamb.getId(), 2);
                log.warn("Transfer fail resultMsg {} resultCode {}", response.getMsg(), response.getCodeValue());
                return;
            }

            if (activityLockInterestMappingMapper.updateLockStatusById(lamb.getId(), 1) == 1) {
                log.info("Lamb userId {} accountId {} tokenId {} backAmount {} unlock success",
                        lamb.getUserId(), lamb.getAccountId(), lamb.getTokenId(), lamb.getBackAmount());
            } else {
                log.info("Lamb userId {} accountId {} tokenId {} backAmount {} unlock fail resultMsg {} resultCode {}",
                        lamb.getUserId(), lamb.getAccountId(), lamb.getTokenId(), lamb.getBackAmount(), response.getMsg(), response.getCodeValue());
            }
        });
    }

    public ActivityLockInterestUserInfo queryUserProjectInfo(Long orgId, Long projectId, Long userId) {
        List<ActivityLockInterestMapping> activityLockInterestMappings
                = this.activityLockInterestMappingMapper.queryMappingInfoListByUserId(orgId, projectId, userId);

        if (CollectionUtils.isEmpty(activityLockInterestMappings)) {
            return ActivityLockInterestUserInfo
                    .builder()
                    .backAmount(BigDecimal.ZERO)
                    .luckyAmount(BigDecimal.ZERO)
                    .userAmount(BigDecimal.ZERO)
                    .userId(userId)
                    .build();
        }

        BigDecimal backAmount = BigDecimal.ZERO;
        BigDecimal luckyAmount = BigDecimal.ZERO;
        BigDecimal userAmount = BigDecimal.ZERO;

        for (ActivityLockInterestMapping lamb : activityLockInterestMappings) {
            backAmount = backAmount.add(lamb.getBackAmount());
            luckyAmount = luckyAmount.add(lamb.getLuckyAmount());
            userAmount = userAmount.add(lamb.getUseAmount());
        }

        return ActivityLockInterestUserInfo
                .builder()
                .backAmount(backAmount)
                .luckyAmount(luckyAmount)
                .userAmount(userAmount)
                .userId(userId)
                .build();
    }

    public void createActivityUserLockRecord(Long orgId, String activityIds, String project) {
        List<ActivityLockInterestOrder> activityLockInterestOrders
                = this.activityLockInterestOrderMapper.queryOrderGiftAmountByProjectId(orgId, activityIds);
        if (CollectionUtils.isEmpty(activityLockInterestOrders)) {
            log.info("queryOrderGiftAmountByProjectId list is null");
            return;
        }

        lockInterestLockRecordMapper.delete(new ActivityLockInterestLockRecord());
        activityLockInterestOrders.forEach(lock -> {
            ActivityLockInterestLockRecord lockRecord
                    = this.lockInterestLockRecordMapper.queryActivityLockInterestLockRecordByUserId(orgId, project, lock.getUserId());
            if (lockRecord == null) {
                ActivityLockInterestLockRecord activityLockInterestLockRecord = new ActivityLockInterestLockRecord();
                activityLockInterestLockRecord.setOrgId(orgId);
                activityLockInterestLockRecord.setUserId(lock.getUserId());
                activityLockInterestLockRecord.setAccountId(lock.getAccountId());
                activityLockInterestLockRecord.setTokenId(lock.getGiftTokenId());
                activityLockInterestLockRecord.setLockAmount(lock.getGiftAmount());
                activityLockInterestLockRecord.setProject(project);
                activityLockInterestLockRecord.setActivitys(activityIds);
                activityLockInterestLockRecord.setReleaseAmount(BigDecimal.ZERO);
                activityLockInterestLockRecord.setLastAmount(lock.getGiftAmount());
                activityLockInterestLockRecord.setCreatedTime(new Date());
                activityLockInterestLockRecord.setUpdatedTime(new Date());
                this.lockInterestLockRecordMapper.insertSelective(activityLockInterestLockRecord);
            }
        });
    }

    public boolean deleteMappingRecordByProjectId(Long projectId) {

        long count = activityLockInterestMappingMapper.countInvalidStatusRecord(projectId);
        if (count > 0) {
            log.info("Could not delete record,projectId={}", projectId);
            return false;
        }

        Example exp = new Example(ActivityLockInterestMapping.class);
        exp.createCriteria()
                .andEqualTo("projectId", projectId)
                .andEqualTo("transferStatus", 0)
                .andEqualTo("unlockStatus", 0)
        ;

        int rows = activityLockInterestMappingMapper.deleteByExample(exp);
        log.info("delete success,project={},rows={}", projectId, rows);
        return true;
    }
}
