package io.bhex.broker.server.grpc.server.service;

import com.github.pagehelper.PageHelper;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.protobuf.TextFormat;
import io.bhex.base.account.*;
import io.bhex.base.idgen.api.ISequenceGenerator;
import io.bhex.base.proto.BaseRequest;
import io.bhex.base.proto.DecimalUtil;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.grpc.activity.experiencefund.*;
import io.bhex.broker.server.grpc.client.service.GrpcAccountService;
import io.bhex.broker.server.grpc.client.service.GrpcBalanceService;
import io.bhex.broker.server.grpc.client.service.GrpcBatchTransferService;
import io.bhex.broker.server.grpc.client.service.GrpcRiskBalanceService;
import io.bhex.broker.server.model.ActivityExperienceFund;
import io.bhex.broker.server.model.ActivityExperienceFundRedeemDetail;
import io.bhex.broker.server.model.ActivityExperienceFundTransferRecord;
import io.bhex.broker.server.primary.mapper.ActivityExperienceFundMapper;
import io.bhex.broker.server.primary.mapper.ActivityExperienceFundRedeemDetailMapper;
import io.bhex.broker.server.primary.mapper.ActivityExperienceFundTransferRecordMapper;
import io.bhex.broker.server.util.BaseReqUtil;
import io.bhex.broker.server.util.BeanCopyUtils;
import io.bhex.broker.server.util.RedisLockUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;
import tk.mybatis.mapper.entity.Example;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Service
public class ActivityExperienceFundService {
    @Resource
    private ActivityExperienceFundMapper experienceFundMapper;
    @Resource
    private ActivityExperienceFundTransferRecordMapper recordMapper;
    @Autowired
    private ActivityExperienceFundRedeemDetailMapper redeemDetailMapper;

    @Resource
    private GrpcRiskBalanceService grpcRiskBalanceService;

    @Resource
    private GrpcAccountService grpcAccountService;
    @Resource
    private GrpcBalanceService grpcBalanceService;
    @Resource
    private GrpcBatchTransferService grpcBatchTransferService;
    @Resource
    private ISequenceGenerator sequenceGenerator;
    @Resource(name = "stringRedisTemplate")
    private StringRedisTemplate redisTemplate;

    private static final String LOCK_KEY = "experience.fund.%s";

    @Transactional
    public void saveExperienceFundInfo(SaveExperienceFundInfoRequest request) {
        ActivityExperienceFund experienceFund = new ActivityExperienceFund();
        BeanCopyUtils.copyPropertiesIgnoreNull(request, experienceFund);
        experienceFund.setId(sequenceGenerator.getLong());
        experienceFund.setCreatedAt(System.currentTimeMillis());
        experienceFund.setUpdatedAt(System.currentTimeMillis());
        experienceFund.setStatus(ActivityExperienceFund.STATUS_INIT);
        experienceFund.setTokenAmount(new BigDecimal(request.getTokenAmount()));
        Map<Long, Long> userMap = request.getUserInfoMap();
        experienceFund.setUserCount(userMap.size());
        experienceFund.setDeadline(request.getDeadline());
        experienceFund.setTransferAssetAmount(BigDecimal.ZERO);
        experienceFund.setRedeemAmount(BigDecimal.ZERO);
        experienceFundMapper.insertSelective(experienceFund);

        for (Long userId : userMap.keySet()) {
            ActivityExperienceFundTransferRecord record = new ActivityExperienceFundTransferRecord();
            record.setId(sequenceGenerator.getLong());
            record.setUserId(userId);
            record.setAccountId(userMap.get(userId));
            record.setTokenId(request.getTokenId());
            record.setTokenAmount(new BigDecimal(request.getTokenAmount()));
            record.setActivityId(experienceFund.getId());
            record.setCreatedAt(System.currentTimeMillis());
            record.setUpdatedAt(System.currentTimeMillis());
            record.setBrokerId(request.getBrokerId());
            record.setExecRedeemTimes(0);
            record.setStatus(ActivityExperienceFundTransferRecord.INIT_STATUS);
            record.setRedeemAmount(BigDecimal.ZERO);
            recordMapper.insertSelective(record);
        }


        try {
            batchTransferAndRiskBalance(experienceFund);
        } catch (Exception e) {
            log.error("transfer error", e);
        }
    }

    public List<ExperienceFundInfo> listExperienceFundInfos(QueryExperienceFundsRequest request) {
        Example example = Example.builder(ActivityExperienceFund.class).orderByDesc("id").build();
        PageHelper.startPage(0, request.getPageSize());
        Example.Criteria criteria = example.createCriteria();
        if (StringUtils.isNotEmpty(request.getTitle())) {
            criteria.andLike("title", "%" + request.getTitle() + "%");
        }
        if (request.getFromId() > 0) {
            criteria.andLessThan("id", request.getFromId());
        }
        if (request.getType() > 0) {
            criteria.andEqualTo("type", request.getType());
        }
        if (request.getStatus() > 0) {
            criteria.andEqualTo("status", request.getStatus());
        }
        criteria.andEqualTo("brokerId", request.getBrokerId());
        example.setOrderByClause("id desc");
        List<ActivityExperienceFund> list = experienceFundMapper.selectByExample(example);
        if (CollectionUtils.isEmpty(list)) {
            return new ArrayList<>();
        }
        List<ExperienceFundInfo> result = list.stream().map(f -> {
            ExperienceFundInfo.Builder builder = ExperienceFundInfo.newBuilder();
            BeanCopyUtils.copyPropertiesIgnoreNull(f, builder);
            builder.setTokenAmount(f.getTokenAmount().stripTrailingZeros().toPlainString());
            builder.setRedeemAmount(f.getRedeemAmount().stripTrailingZeros().toPlainString());
            builder.setUserCount(f.getUserCount());
            return builder.build();
        }).collect(Collectors.toList());
        return result;
    }

    public ExperienceFundInfo detail(long brokerId, long activityId) {
        Example example = new Example(ActivityExperienceFund.class);

        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("brokerId", brokerId);
        criteria.andEqualTo("id", activityId);
        ActivityExperienceFund experienceFund = experienceFundMapper.selectOneByExample(example);
        if (experienceFund == null) {
            return ExperienceFundInfo.getDefaultInstance();
        }
        ExperienceFundInfo.Builder builder = ExperienceFundInfo.newBuilder();
        BeanCopyUtils.copyPropertiesIgnoreNull(experienceFund, builder);
        builder.setTokenAmount(experienceFund.getTokenAmount().stripTrailingZeros().toPlainString());
        builder.setRedeemAmount(experienceFund.getRedeemAmount().stripTrailingZeros().toPlainString());
        builder.setUserCount(experienceFund.getUserCount());
        List<String> userIds = recordMapper.getUserIds(brokerId, activityId).stream().map(s -> s + "").collect(Collectors.toList());
        builder.setUserIds(String.join(",", userIds));
        return builder.build();
    }

    public List<ExperienceFundTransferRecord> queryTransferList(QueryTransferRecordsRequest request) {
        Example example = Example.builder(ActivityExperienceFundTransferRecord.class).orderByDesc("id").build();
        PageHelper.startPage(0, request.getPageSize());
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("brokerId", request.getBrokerId());
        criteria.andEqualTo("activityId", request.getActivityId());

        if (request.getFromId() > 0) {
            criteria.andLessThan("id", request.getFromId());
        }
//        if (request.getStatus() > 0) {
//            criteria.andEqualTo("status", request.getStatus());
//        }
        List<ActivityExperienceFundTransferRecord> list = recordMapper.selectByExample(example);
        if (CollectionUtils.isEmpty(list)) {
            return new ArrayList<>();
        }
        ActivityExperienceFund experienceFund = experienceFundMapper.selectByPrimaryKey(request.getActivityId());
        List<ExperienceFundTransferRecord> result = list.stream().map(r -> {
            ExperienceFundTransferRecord.Builder builder = ExperienceFundTransferRecord.newBuilder();
            BeanCopyUtils.copyPropertiesIgnoreNull(r, builder);

            builder.setRedeemAmount(r.getRedeemAmount().stripTrailingZeros().toPlainString());
            builder.setTokenAmount(r.getTokenAmount().stripTrailingZeros().toPlainString());
            ExperienceFundTransferRecord.Status status = ExperienceFundTransferRecord.Status.INIT_STATUS;
            if (r.getStatus() == ActivityExperienceFundTransferRecord.RELEASE_RISK_BALANCE_STATUS) {
                if (r.getRedeemAmount().compareTo(r.getTokenAmount()) == 0) { //回收完成
                    status = ExperienceFundTransferRecord.Status.REDEEM_ENDED; //回收完成
                } else {
                    status = ExperienceFundTransferRecord.Status.REDEEM_DEADLINE_STATUS; //回收结束但没收全
                }
                builder.setRedeemTime(r.getUpdatedAt());
            } else if (System.currentTimeMillis() > experienceFund.getRedeemTime()) {
                status = ExperienceFundTransferRecord.Status.REDEEMING_STATUS; //回收中
                if (r.getRedeemAmount().compareTo(BigDecimal.ZERO) > 0) {
                    builder.setRedeemTime(r.getUpdatedAt());
                }
            } else if (System.currentTimeMillis() < experienceFund.getRedeemTime()) {
                status = ExperienceFundTransferRecord.Status.TRANSFER_IN_STATUS; //还没开始回收
            }
            builder.setStatus(status);
            return builder.build();
        }).collect(Collectors.toList());

        return result;
    }


    public Map<Long, Boolean> checkJoinedActivity(CheckAccountJoinedActivityRequest request) {
        Example example = new Example(ActivityExperienceFundTransferRecord.class);
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("brokerId", request.getBrokerId());
        criteria.andIn("accountId", request.getAccountIdList());

        List<ActivityExperienceFundTransferRecord> records = recordMapper.selectByExample(example);
        List<Long> exitedAccountIds = CollectionUtils.isEmpty(records) ? new ArrayList<>()
                : records.stream().map(r -> r.getAccountId()).collect(Collectors.toList());

        Map<Long, Boolean> result = new HashMap<>();
        for (Long accountId : request.getAccountIdList()) {
            result.put(accountId, exitedAccountIds.contains(accountId));
        }
        return result;
    }


    public Pair<Long, Long> getUserIdByAccountType(Long orgId, int accountType) {
        GetAccountByTypeRequest request = GetAccountByTypeRequest.newBuilder()
                .setBaseRequest(BaseReqUtil.getBaseRequest(orgId))
                .setOrgId(orgId)
                .setType(io.bhex.base.account.AccountType.valueOf(accountType))
                .build();
        GetAccountByTypeReply response = grpcAccountService.getUserIdByAccountType(request);
        if (Strings.isNullOrEmpty(response.getBrokerUserId())) {
            throw new BrokerException(BrokerErrorCode.USER_NOT_EXIST);
        }
        return Pair.of(Long.valueOf(response.getBrokerUserId()), response.getAccountId());
    }

    private void batchTransferAndRiskBalance(ActivityExperienceFund experienceFund) {
        long orgId = experienceFund.getBrokerId();
        Pair<Long, Long> orgOperationUser = getUserIdByAccountType(orgId, AccountType.OPERATION_ACCOUNT_VALUE);
        long orgOperationUserId = orgOperationUser.getLeft();
        long orgOperationAccount = orgOperationUser.getRight();

        //lock org balance
        if (experienceFund.getStatus() == ActivityExperienceFund.STATUS_INIT) {
            LockBalanceRequest lockBalanceRequest = LockBalanceRequest.newBuilder()
                    .setBaseRequest(BaseRequest.newBuilder().setOrganizationId(orgId).setBrokerUserId(orgOperationUserId).build())
                    .setAccountId(orgOperationAccount)
                    .setTokenId(experienceFund.getTokenId())
                    .setLockAmount(experienceFund.getTokenAmount().multiply(new BigDecimal(experienceFund.getUserCount())).toPlainString())
                    .setClientReqId(experienceFund.getId())
                    .build();
            LockBalanceReply lockBalanceReply = grpcBalanceService.lockBalance(lockBalanceRequest);
            if (lockBalanceReply.getCode() == LockBalanceReply.ReplyCode.SUCCESS) {
                experienceFund.setStatus(ActivityExperienceFund.STATUS_LOCK_ORG_BALANCE);
                experienceFundMapper.updateByPrimaryKeySelective(experienceFund);
                log.info("lock org:{} operate user {} {}", orgId, experienceFund.getTokenAmount(), experienceFund.getTokenId());
            } else {
                log.error("lock org operation account failed, {} , {}", TextFormat.shortDebugString(lockBalanceRequest), TextFormat.shortDebugString(lockBalanceReply));
                return;
            }
        }

        if (experienceFund.getStatus() == ActivityExperienceFund.STATUS_LOCK_ORG_BALANCE) {
            Example example = new Example(ActivityExperienceFundTransferRecord.class);
            example.createCriteria().andEqualTo("brokerId", orgId)
                    .andEqualTo("activityId", experienceFund.getId())
                    .andIn("status", Arrays.asList(ActivityExperienceFundTransferRecord.INIT_STATUS, ActivityExperienceFundTransferRecord.TRANSFER_IN_STATUS));

            List<ActivityExperienceFundTransferRecord> records = recordMapper.selectByExample(example);
            if (!CollectionUtils.isEmpty(records)) {
                for (ActivityExperienceFundTransferRecord record : records) {
                    try {
                        if (record.getStatus() == ActivityExperienceFundTransferRecord.INIT_STATUS) {
                            SyncTransferRequest transferRequest = SyncTransferRequest.newBuilder()
                                    .setBaseRequest(BaseReqUtil.getBaseRequest(orgId))
                                    .setClientTransferId(record.getId())
                                    .setSourceOrgId(orgId)
                                    .setSourceFlowSubject(BusinessSubject.CONTRACT_PRESENT)
                                    .setSourceFlowSecondSubject(0)
                                    .setSourceAccountType(AccountType.OPERATION_ACCOUNT)
                                    .setSourceAccountId(orgOperationAccount)
                                    .setTokenId(experienceFund.getTokenId())
                                    .setAmount(experienceFund.getTokenAmount().stripTrailingZeros().toPlainString())
                                    .setTargetOrgId(orgId)
                                    .setTargetAccountId(record.getAccountId())
                                    .setTargetAccountType(AccountType.GENERAL_ACCOUNT)
                                    .setTargetFlowSubject(BusinessSubject.CONTRACT_PRESENT)
                                    .setTargetFlowSecondSubject(0)
                                    .setFromPosition(true)
                                    .setToPosition(false)
                                    .build();
                            SyncTransferResponse response = grpcBatchTransferService.syncTransfer(transferRequest);
                            if (response.getCode() == SyncTransferResponse.ResponseCode.SUCCESS) {
                                experienceFund.setTransferAssetAmount(experienceFund.getTransferAssetAmount().add(experienceFund.getTokenAmount()));
                                record.setStatus(ActivityExperienceFundTransferRecord.TRANSFER_IN_STATUS);
                                recordMapper.updateByPrimaryKeySelective(record);
                                log.info("transfer to {} {} {}", record.getAccountId(), record.getTokenId(), record.getTokenAmount());
                                addRiskBalance(record);
                            } else {
                                log.error("transfer to {} {} {} {}", record.getAccountId(), record.getTokenId(),
                                        record.getTokenAmount(), TextFormat.shortDebugString(response));
                            }
                        } else if (record.getStatus() == ActivityExperienceFundTransferRecord.TRANSFER_IN_STATUS) {
                            addRiskBalance(record);
                        }
                    } catch (Exception e) {
                        log.error("exfu error ", e);
                    }
                }
            }

            if (CollectionUtils.isEmpty(recordMapper.selectByExample(example))) {
                experienceFund.setStatus(ActivityExperienceFund.STATUS_ORG_TRANSFER_OUT_SUCCESS);
                experienceFundMapper.updateByPrimaryKeySelective(experienceFund);
            }
        }
    }


    private void addRiskBalance(ActivityExperienceFundTransferRecord record) {
        AddRiskInTransferRequest request = AddRiskInTransferRequest.newBuilder()
                .setOrgId(record.getBrokerId())
                .setAccountId(record.getAccountId())
                .setBizId(record.getId())
                .setRiskAmount(record.getTokenAmount().toPlainString())
                .setSubject(RiskBalanceSubject.RISK_BALANCE_SUBJECT_PRESENT)
                .setTokenId(record.getTokenId())
                .build();
        AddRiskInTransferReply reply = grpcRiskBalanceService.addRiskInTransfer(request);
        log.info("req:{} reply:{}", request, reply);

        record.setStatus(ActivityExperienceFundTransferRecord.ADD_RISK_BALANCE_STATUS);
        recordMapper.updateByPrimaryKeySelective(record);
        log.info("addRiskBalance to {} {} {}", record.getAccountId(), record.getTokenId(), record.getTokenAmount());
    }



    @Scheduled(cron = "8 1/5 * * * ?")
    public void transferReloaded() { //创建活动记录时出错 此定时任务进行重试，达到最终转账成功的状态
        String lockKey = String.format(LOCK_KEY, "transferReloaded");
        boolean lock = RedisLockUtils.tryLock(redisTemplate, lockKey, 300_000);
        if (!lock) {
            return;
        }
        try {
            Example example = new Example(ActivityExperienceFund.class);
            example.createCriteria()
                    .andIn("status", Arrays.asList(ActivityExperienceFund.STATUS_INIT, ActivityExperienceFund.STATUS_LOCK_ORG_BALANCE));
            List<ActivityExperienceFund> list = experienceFundMapper.selectByExample(example);
            for (ActivityExperienceFund activityExperienceFund : list) {
                batchTransferAndRiskBalance(activityExperienceFund);
            }
        } catch (Exception e) {
            log.info("transferReloaded", e);
        } finally {
            RedisLockUtils.releaseLock(redisTemplate, lockKey);
        }
    }

    @Scheduled(cron = "37 1/1 * * * ?")
    public void redeemNewFund() { //每分钟回收一次 只回收最近1分钟到期的
        List<ActivityExperienceFund> list = experienceFundMapper.getNewUnredeemTask(System.currentTimeMillis() - 60_000, System.currentTimeMillis());
        if (CollectionUtils.isEmpty(list)) {
            return;
        }
        log.info("redeemNewFund");
        redeemFundList(list);
    }

    @Scheduled(cron = "37 16 * * * ?")
    public void redeemFund() { //每小时回收一次 不包括刚到期的任务
        List<ActivityExperienceFund> list = experienceFundMapper.getUnredeemTask(System.currentTimeMillis() - 60_000);
        if (CollectionUtils.isEmpty(list)) {
            return;
        }
        log.info("redeemMoreTimesFund");
        redeemFundList(list);
    }

    public void redeemFundList(List<ActivityExperienceFund> list) {

        Collections.shuffle(list);
        for (ActivityExperienceFund experienceFund : list) {
            String lockKey = String.format(LOCK_KEY, "redeemFund." + experienceFund.getId());
            boolean lock = RedisLockUtils.tryLock(redisTemplate, lockKey, 300_000);
            if (!lock) {
                continue;
            }
            try {
                long orgId = experienceFund.getBrokerId();
                Pair<Long, Long> orgOperationUser = getUserIdByAccountType(orgId, AccountType.OPERATION_ACCOUNT_VALUE);
                long orgOperationAccountId = orgOperationUser.getRight();

                Example example = new Example(ActivityExperienceFundTransferRecord.class);
                example.createCriteria().andEqualTo("brokerId", orgId)
                        .andEqualTo("activityId", experienceFund.getId())
                        .andIn("status", Arrays.asList(ActivityExperienceFundTransferRecord.ADD_RISK_BALANCE_STATUS,
                                ActivityExperienceFundTransferRecord.TRANSFER_OUT_STATUS));

                List<ActivityExperienceFundTransferRecord> records = recordMapper.selectByExample(example);

                for (ActivityExperienceFundTransferRecord record : records) {
                    //add redeem detail
                    if (experienceFund.getRedeemType() == 1) {//1-到期赎回
                        List<ActivityExperienceFundRedeemDetail> redeemDetails = redeemDetailMapper.getUnDoneDetails(record.getId());
                        if (CollectionUtils.isEmpty(redeemDetails)) {
                            ActivityExperienceFundRedeemDetail redeemDetail = addRedeemDetail(record);
                            if (redeemDetail != null) {
                                redeemDetails = Lists.newArrayList(redeemDetail);
                            }
                        }
                        if (!CollectionUtils.isEmpty(redeemDetails)) {
                            for (ActivityExperienceFundRedeemDetail redeemDetail : redeemDetails) {
                                if (redeemDetail.getStatus() == ActivityExperienceFundRedeemDetail.INIT_STATUS) {
                                    redeemTransferToOrg(experienceFund, record, orgOperationAccountId, redeemDetail);
                                }
                                if (redeemDetail.getStatus() == ActivityExperienceFundRedeemDetail.TRANSFER_SUC_STATUS) {
                                    releaseRiskBalance(record, redeemDetail);
                                }
                            }
                        }
                    }

                    if (experienceFund.getDeadline() < System.currentTimeMillis()) { //到截止日期后，风险金全部释放
                        ReleaseRiskInTransferRequest request = ReleaseRiskInTransferRequest.newBuilder()
                                .setOrgId(record.getBrokerId())
                                .setAccountId(record.getAccountId())
                                .setBizId(record.getId())
                                .setReleaseAmount(record.getTokenAmount().subtract(record.getRedeemAmount()).stripTrailingZeros().toPlainString())
                                .setClientReleaseId(record.getId())
                                .build();
                        ReleaseRiskInTransferReply reply = grpcRiskBalanceService.releaseRiskInTransfer(request);
                        log.info("req:{} reply:{}", request, reply);
                        record.setStatus(ActivityExperienceFundTransferRecord.RELEASE_RISK_BALANCE_STATUS);
                        record.setUpdatedAt(System.currentTimeMillis());
                        recordMapper.updateByPrimaryKeySelective(record);
                    }
                }

                if (CollectionUtils.isEmpty(recordMapper.selectByExample(example))) {
                    experienceFund.setStatus(ActivityExperienceFund.STATUS_ENDED);
                    experienceFund.setUpdatedAt(System.currentTimeMillis());
                    experienceFundMapper.updateByPrimaryKey(experienceFund);
                }
            } catch (Exception e) {
                log.info("transferReloaded", e);
            } finally {
                RedisLockUtils.releaseLock(redisTemplate, lockKey);
            }
        }
    }

    //创建赎回记录，可以多次赎回 每次都最大赎回
    private ActivityExperienceFundRedeemDetail addRedeemDetail(ActivityExperienceFundTransferRecord record) {
        record.setExecRedeemTimes(record.getExecRedeemTimes() + 1);
        record.setUpdatedAt(System.currentTimeMillis());
        BigDecimal leftAmount = record.getTokenAmount().subtract(record.getRedeemAmount());

        GetBalanceDetailRequest balanceDetailRequest = GetBalanceDetailRequest.newBuilder()
                .setBaseRequest(BaseReqUtil.getBaseRequest(record.getBrokerId()))
                .setAccountId(record.getAccountId())
                .addTokenId(record.getTokenId()).build();
        List<BalanceDetail> balanceDetails = grpcBalanceService.getBalanceDetail(balanceDetailRequest).getBalanceDetailsList();
        BigDecimal available = DecimalUtil.toBigDecimal(balanceDetails.get(0).getAvailable());
        if (available.compareTo(BigDecimal.ZERO) == 0) {
            return null;
        }
        BigDecimal transferAmount = available.compareTo(leftAmount) > 0 ? leftAmount : available;

        long detailId = sequenceGenerator.getLong();
        ActivityExperienceFundRedeemDetail redeemDetail = new ActivityExperienceFundRedeemDetail();
        redeemDetail.setId(detailId);
        redeemDetail.setCreatedAt(System.currentTimeMillis());
        redeemDetail.setStatus(0);
        redeemDetail.setRedeemAmount(transferAmount);
        redeemDetail.setTransferRecordId(record.getId());
        redeemDetail.setUpdatedAt(System.currentTimeMillis());
        redeemDetailMapper.insertSelective(redeemDetail);
        return redeemDetail;
    }




    //从账户转账到运营账户
    private boolean redeemTransferToOrg(ActivityExperienceFund experienceFund, ActivityExperienceFundTransferRecord record, long orgAccountId, ActivityExperienceFundRedeemDetail redeemDetail) {
        try {
            BigDecimal redeemAmount = redeemDetail.getRedeemAmount();
            SyncTransferRequest transferRequest = SyncTransferRequest.newBuilder()
                    .setBaseRequest(BaseReqUtil.getBaseRequest(record.getBrokerId()))
                    .setClientTransferId(redeemDetail.getId())

                    .setSourceOrgId(record.getBrokerId())
                    .setSourceFlowSubject(BusinessSubject.CONTRACT_PRESENT)
                    .setSourceFlowSecondSubject(0)
                    .setSourceAccountId(record.getAccountId())
                    .setSourceAccountType(AccountType.GENERAL_ACCOUNT)
                    .setFromPosition(false)

                    .setTokenId(record.getTokenId())
                    .setAmount(redeemAmount.stripTrailingZeros().toPlainString())

                    .setTargetOrgId(record.getBrokerId())
                    .setTargetAccountType(AccountType.OPERATION_ACCOUNT)
                    .setTargetAccountId(orgAccountId)
                    .setTargetFlowSubject(BusinessSubject.CONTRACT_PRESENT)
                    .setTargetFlowSecondSubject(0)
                    .setToPosition(false)
                    .build();
            SyncTransferResponse response = grpcBatchTransferService.syncTransfer(transferRequest);
            log.info("accountTransferToOrg req:{} res:{}", transferRequest, response);
            boolean suc = response.getCode() == SyncTransferResponse.ResponseCode.SUCCESS;
            if (suc) {
                record.setUpdatedAt(System.currentTimeMillis());
                record.setRedeemAmount(record.getRedeemAmount().add(redeemAmount));
                if (record.getRedeemAmount().compareTo(record.getTokenAmount()) == 0) {
                    record.setStatus(ActivityExperienceFundTransferRecord.TRANSFER_OUT_STATUS);
                }
                redeemDetail.setUpdatedAt(System.currentTimeMillis());
                redeemDetail.setStatus(ActivityExperienceFundRedeemDetail.TRANSFER_SUC_STATUS);
                redeemDetailMapper.updateByPrimaryKeySelective(redeemDetail);

                experienceFund.setUpdatedAt(System.currentTimeMillis());
                experienceFund.setRedeemAmount(experienceFund.getRedeemAmount().add(redeemAmount));
                experienceFundMapper.updateByPrimaryKeySelective(experienceFund);
                log.info("redeemTransferToOrg activity:{} from:{} to:{} {}:{} suc", record.getActivityId(), record.getAccountId(), orgAccountId, record.getTokenId(), redeemAmount);
            }
            recordMapper.updateByPrimaryKeySelective(record);
            return suc;
        } catch (Exception e) { //直接抛出异常，程序会定时再次执行
            log.info("redeemTransferToOrg error", e);
        }
        return false;
    }

    //释放风险金 可以多次释放
    private void releaseRiskBalance(ActivityExperienceFundTransferRecord record, ActivityExperienceFundRedeemDetail redeemDetail) {
        try {
            ReleaseRiskInTransferRequest request = ReleaseRiskInTransferRequest.newBuilder()
                    .setOrgId(record.getBrokerId())
                    .setAccountId(record.getAccountId())
                    .setBizId(record.getId())
                    .setReleaseAmount(redeemDetail.getRedeemAmount().stripTrailingZeros().toPlainString())
                    .setClientReleaseId(redeemDetail.getId())
                    .build();
            ReleaseRiskInTransferReply reply = grpcRiskBalanceService.releaseRiskInTransfer(request);
            log.info("req:{} reply:{}", request, reply);

            if (record.getRedeemAmount().compareTo(record.getTokenAmount()) == 0) {
                record.setUpdatedAt(System.currentTimeMillis());
                record.setStatus(ActivityExperienceFundTransferRecord.RELEASE_RISK_BALANCE_STATUS);
                recordMapper.updateByPrimaryKeySelective(record);
            }

            redeemDetail.setUpdatedAt(System.currentTimeMillis());
            redeemDetail.setStatus(ActivityExperienceFundRedeemDetail.RELEASE_RISK_BALANCE_SUC_STATUS);
            redeemDetailMapper.updateByPrimaryKeySelective(redeemDetail);
            log.info("release risk balance");
        } catch (Exception e) { //直接抛出异常，程序会定时再次执行
            log.info("releaseRiskBalance error", e);
        }
    }

}
