package io.bhex.broker.server.grpc.server.service;


import com.github.pagehelper.PageHelper;
import com.google.gson.JsonObject;
import com.google.protobuf.TextFormat;
import io.bhex.base.account.*;
import io.bhex.base.constants.ProtoConstants;
import io.bhex.base.idgen.api.ISequenceGenerator;
import io.bhex.base.proto.Decimal;
import io.bhex.base.proto.DecimalUtil;
import io.bhex.base.token.GetTokenRequest;
import io.bhex.broker.common.util.JsonUtil;
import io.bhex.broker.grpc.account.*;
import io.bhex.broker.grpc.admin.*;
import io.bhex.broker.grpc.common.Header;
import io.bhex.broker.server.domain.AirdropStatus;
import io.bhex.broker.server.domain.BrokerLockKeys;
import io.bhex.broker.server.domain.NoticeBusinessType;
import io.bhex.broker.server.grpc.client.service.GrpcAccountService;
import io.bhex.broker.server.grpc.client.service.GrpcBalanceService;
import io.bhex.broker.server.grpc.client.service.GrpcBatchTransferService;
import io.bhex.broker.server.grpc.client.service.GrpcTokenService;
import io.bhex.broker.server.grpc.server.service.auditflow.FlowAuditEndEvent;
import io.bhex.broker.server.grpc.server.service.auditflow.FlowAuditEvent;
import io.bhex.broker.server.grpc.server.service.po.AirdropPO;
import io.bhex.broker.server.grpc.server.service.statistics.StatisticsBalanceService;
import io.bhex.broker.server.model.AirdropTmplRecord;
import io.bhex.broker.server.model.AirdropTransferGroupInfo;
import io.bhex.broker.server.model.AirdropTransferRecord;
import io.bhex.broker.server.model.AssetSnapshotRecord;
import io.bhex.broker.server.model.StatisticsBalanceSnapshot;
import io.bhex.broker.server.primary.mapper.*;
import io.bhex.broker.server.util.BaseReqUtil;
import io.bhex.broker.server.util.BeanCopyUtils;
import io.bhex.broker.server.util.RedisLockUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.event.EventListener;
import org.springframework.core.task.TaskExecutor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;
import tk.mybatis.mapper.entity.Example;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.grpc.server.service
 * @Author: ming.xu
 * @CreateDate: 10/11/2018 7:09 PM
 * @Copyright（C）: 2018 BHEX Inc. All rights reserved.
 */
@Slf4j
@Service
public class AdminAirdropInfoService {

    @Autowired
    private AirdropInfoMapper airdropInfoMapper;

    @Autowired
    private AirdropTransferRecordMapper transferRecordMapper;

    @Autowired
    private AirdropTransferGroupInfoMapper transferGroupInfoMapper;

    @Autowired
    private AssetSnapshotRecordMapper assetSnapshotRecordMapper;

    @Autowired
    private AirdropTmplRecordMapper airdropTmplRecordMapper;

    @Resource
    private ApplicationContext applicationContext;

    @Resource
    private ISequenceGenerator sequenceGenerator;
    @Resource
    UserMapper userMapper;
    @Resource
    AccountService accountService;
    @Resource
    LoginLogMapper loginLogMapper;
    @Resource
    NoticeTemplateService noticeTemplateService;
    @Resource
    GrpcAccountService grpcAccountService;
    @Resource
    GrpcTokenService grpcTokenService;
    @Resource
    GrpcBalanceService grpcBalanceService;
    @Resource
    GrpcBatchTransferService grpcBatchTransferService;

    @Resource
    AdminAirdropInfoService adminAirdropService;

    @Resource
    BalanceService balanceService;

    @Resource
    StatisticsBalanceService statisticsBalanceService;

    @Resource(name = "stringRedisTemplate")
    private StringRedisTemplate redisTemplate;

    @Resource(name = "airDropRequestHandleTaskExecutor")
    private TaskExecutor airDropExecutor;

    private static final Integer BATCH_LIMIT = 500;

    private static final String SPLIT_REGEX = ",";

    @Scheduled(cron = "30 * * * * ?")
    private void scheduleAirdrop() {
        QueryAirdropInfoReply reply = listScheduleAirdrop();
        List<AirdropInfo> infos = reply.getAirdropInfoList();
        if (!CollectionUtils.isEmpty(infos)) {
            for (AirdropInfo info : infos) {
                CompletableFuture.runAsync(() -> {
                    boolean lock = false;
                    String lockKey = String.format(BrokerLockKeys.AIRDROP_TASK_KEY, info.getBrokerId(), info.getId());
                    try {
                        lock = RedisLockUtils.tryLock(redisTemplate, lockKey, BrokerLockKeys.AIRDROP_TASK_KEY_EXPIRE);
                        log.info("scheduleAirdrop locked: {}, {}", lockKey, lock);
                        if (!lock) {
                            return;
                        }
                        // 执行生成空投记录
                        adminAirdropService.airdropRecordProcess(info);
                    } catch (Exception e) {
                        log.error("scheduleAirdrop error", e);
                    } finally {
                        if (lock) {
                            RedisLockUtils.releaseLock(redisTemplate, lockKey);
                        }
                    }
                }, airDropExecutor);
            }
        }
    }

    @Transactional(rollbackFor = Exception.class)
    public Boolean airdropRecordProcess(AirdropInfo airdropInfo) {
        //1.获取用户信息 （分页）
        //2.取得总用户数，计算总共空投钱数，验证空投账户余额是否够
        //3.拉取快照 快照数据入库
        //4.根据用户资产计算空投数量
        //4.分组生成转账记录入库

        //判断空投状态，如果为初始化，则锁定并变更空投状态为 空投中
        io.bhex.broker.server.model.AirdropInfo airdrop = airdropInfoMapper.getAirdropInfAndLock(airdropInfo.getId(), airdropInfo.getBrokerId());
        if (airdrop == null || !airdrop.getStatus().equals(AirdropStatus.STATUS_AUDIT_PASSED)) {
            return false;
        }
        int ret = airdropInfoMapper.updateStatus(airdrop.getId(), airdrop.getBrokerId(), AirdropStatus.STATUS_AIRDROP, airdrop.getStatus());
        if (ret <= 0) {
            return false;
        }

        boolean tmplModel = StringUtils.isNotBlank(airdropInfo.getTmplUrl());

        int index = 0;
        long fromId = 0L;
        while (true) {
            if (!tmplModel) {
                List<Long> accountIds = new ArrayList<>();
                if (AirdropPO.USER_TYPE_ALL == airdropInfo.getUserType()) {
                    GetBrokerAccountListResponse reply = getAllUserPageable(airdropInfo.getAirdropTime(), fromId, airdropInfo.getBrokerId(), BATCH_LIMIT);
                    List<SimpleAccount> simpleAccounts = reply.getAccountsList();
                    if (!CollectionUtils.isEmpty(simpleAccounts)) {
                        for (SimpleAccount sa : simpleAccounts) {
                            fromId = Math.max(sa.getId(), fromId);
                            accountIds.add(sa.getAccountId());
                        }
                    }
                } else if (AirdropPO.USER_TYPE_SPECIAL == airdropInfo.getUserType()) {
                    accountIds = getSpecialUserPageable(airdropInfo, index, BATCH_LIMIT);
                    index += BATCH_LIMIT;
                }
                // 如果待发送用户为空，则退出发送
                if (CollectionUtils.isEmpty(accountIds)) {
                    break;
                }
                // 校验account id是否为本交易所用户
                accountIds = verifyAccountIds(airdropInfo.getBrokerId(), accountIds);
                // 是否已经转过钱了，查询转账记录并过滤account_id
                accountIds = transferRecordFilter(accountIds, airdropInfo.getId());

                List<TransferRecord> transferItems = getUnTmplModelTransferItems(accountIds, airdropInfo, index, BATCH_LIMIT);
                // 存储转账记录，后续按此记录进行转账
                saveTransferRecord(airdropInfo, transferItems);
            } else {
                log.info("tmpl model airdrop:{}", airdropInfo.getId());
                List<TransferRecord> transferItems = getTmplModelTransferItems(airdropInfo, index, BATCH_LIMIT);
                if (CollectionUtils.isEmpty(transferItems)) {
                    break;
                }
                // 存储转账记录，后续按此记录进行转账
                saveTransferRecord(airdropInfo, transferItems);
                index += BATCH_LIMIT;
            }
        }
        ret = airdropInfoMapper.updateStatus(airdrop.getId(), airdrop.getBrokerId(), AirdropStatus.STATUS_AIRDROP_TRANSFER_WAITING, AirdropStatus.STATUS_AIRDROP);
        return ret > 0;
    }

    @Scheduled(cron = "50 * * * * ?")
    private void scheduleAirdropExecute() {
        Example example = new Example(io.bhex.broker.server.model.AirdropInfo.class);
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("status", AirdropStatus.STATUS_AIRDROP_TRANSFER_WAITING);
        List<io.bhex.broker.server.model.AirdropInfo> airdropInfos = airdropInfoMapper.selectByExample(example);
        List<AirdropInfo> infos = processAirdropInfo(airdropInfos, false);
        if (!CollectionUtils.isEmpty(infos)) {
            for (AirdropInfo info : infos) {
                CompletableFuture.runAsync(() -> {
                    boolean lock = false;
                    String lockKey = String.format(BrokerLockKeys.AIRDROP_EXECUTE_TASK_KEY, info.getBrokerId(), info.getId());
                    try {
                        lock = RedisLockUtils.tryLock(redisTemplate, lockKey, BrokerLockKeys.AIRDROP_EXECUTE_TASK_KEY_EXPIRE);
                        log.info("scheduleAirdropExecute locked: {}, {}", lockKey, lock);
                        if (!lock) {
                            return;
                        }
                        // 执行空投
                        adminAirdropService.airdropExecute(info);
                    } catch (Exception e) {
                        log.error("scheduleAirdropExecute error: {},", info.getId(), e);
                    } finally {
                        if (lock) {
                            RedisLockUtils.releaseLock(redisTemplate, lockKey);
                        }
                    }
                }, airDropExecutor);
            }
        }
    }

    /**
     *  必须为public,否则airdropInfoMapper将为null
      * @param airdropInfo
     * @return
     */
   public Boolean airdropExecute(AirdropInfo airdropInfo) {
       //5.根据转账记录进行转账
       //判断空投状态，则锁定并变更空投状态为 空投执行中
       io.bhex.broker.server.model.AirdropInfo airdrop = airdropInfoMapper.getAirdropInfAndLock(airdropInfo.getId(), airdropInfo.getBrokerId());
       if (airdrop == null || !airdrop.getStatus().equals(AirdropStatus.STATUS_AIRDROP_TRANSFER_WAITING)) {
           return false;
       }
       int ret = airdropInfoMapper.updateStatus(airdrop.getId(), airdrop.getBrokerId(), AirdropStatus.STATUS_AIRDROP_TRANSFER_EXECUTING, airdrop.getStatus());
       if (ret <= 0) {
           return false;
       }

       Integer status = batchTransferProcess(airdropInfo);
       updateAirdropStatus(airdropInfo.getId(), airdropInfo.getBrokerId(), status);
       return true;
   }

    /**
     * 判断机构用户余额是否够本次空投
     *
     * @param airdropInfo
     * @param transferGroupInfos
     * @return
     */
    private Boolean isOrgBalanceEnough(AirdropInfo airdropInfo, List<TransferGroupInfo> transferGroupInfos) {
        Map<String, BigDecimal> transferMap = new HashMap<>();

        for (TransferGroupInfo info : transferGroupInfos) {
            if (AirdropTransferGroupInfo.STATUS_INIT == info.getStatus()) {
                String token = info.getTokenId();
                if (transferMap.containsKey(token)) {
                    BigDecimal transferAmount = transferMap.get(token).add(new BigDecimal(info.getTransferAssetAmount()));
                    transferMap.put(token, transferAmount);
                } else {
                    transferMap.put(token, new BigDecimal(info.getTransferAssetAmount()));
                }
            }
        }

        for (String token : transferMap.keySet()) {
            BigDecimal orgBalance = getOrgBalance(airdropInfo.getAccountId(), token, airdropInfo.getBrokerId());
            if (orgBalance.compareTo(transferMap.get(token)) < 0) {
                // 机构账户余额小于待发送总钱数，不予发送
                updateAirdropStatus(airdropInfo.getId(), airdropInfo.getBrokerId(), AirdropStatus.STATUS_FAILED);
                log.warn("Airdrop ERROR: Insufficient account balance. token:{} orgBalance: {},transferAmount: {}, info: {}",
                        token, orgBalance, transferMap.get(token), TextFormat.shortDebugString(airdropInfo));
                return false;
            }
        }
        return true;
    }

    /**
     * 获取机构账户余额
     *
     * @param orgAccountId
     * @param tokenId
     */
    private BigDecimal getOrgBalance(Long orgAccountId, String tokenId, Long orgId) {
        GetBalanceDetailRequest request = GetBalanceDetailRequest.newBuilder()
                .setBaseRequest(BaseReqUtil.getBaseRequest(orgId))
                .setAccountId(orgAccountId)
                .addAllTokenId(Collections.singletonList(tokenId))
                .build();

        BalanceDetailList replay = grpcBalanceService.getBalanceDetail(request);
        List<BalanceDetail> balanceDetailsList = replay.getBalanceDetailsList();
        if (!CollectionUtils.isEmpty(balanceDetailsList)) {
            BalanceDetail balanceDetail = balanceDetailsList.get(0);
            Decimal decimal = balanceDetail.getAvailable();
            BigDecimal available = DecimalUtil.toBigDecimal(decimal);
            return available;
        }
        return new BigDecimal(0);
    }


    /**
     * 按照分组进行空投转账
     *
     * @param airdropInfo
     * @return
     */
    private Integer batchTransferProcess(AirdropInfo airdropInfo) {
        //1.计算发送账户资金是否够空投
        //2.获取全部分组信息
        //3.按分组信息进行空投
        //4.空投成功更新对应分组状态
        //5.更新空投状态

        ListAllTransferGroupRequest request = ListAllTransferGroupRequest.newBuilder()
                .setAirdropId(airdropInfo.getId())
                .setBrokerId(airdropInfo.getBrokerId())
                .build();
        ListAllTransferGroupReply reply = listAllTransferGroup(request);
        List<TransferGroupInfo> transferGroupInfos = reply.getTransferGroupInfosList();
        if (!isOrgBalanceEnough(airdropInfo, transferGroupInfos)) {
            return AirdropStatus.STATUS_FAILED_INSUFFICIENT;
        }
        Integer status = AirdropStatus.STATUS_SUCCESS;
        boolean haveError = false;
        boolean haveSuccess = false;
        if (!CollectionUtils.isEmpty(transferGroupInfos)) {
            status = AirdropStatus.STATUS_FAILED;
            for (TransferGroupInfo groupInfo : transferGroupInfos) {
                if (AirdropTransferGroupInfo.STATUS_INIT == groupInfo.getStatus()) {
                    // 开始发送，更新状态为发送中
                    UpdateTransferGroupStatusRequest updateTransferGroupStatusRequest = UpdateTransferGroupStatusRequest.newBuilder()
                            .setAirdropId(airdropInfo.getId())
                            .setBrokerId(airdropInfo.getBrokerId())
                            .setStatus(AirdropTransferGroupInfo.STATUS_AIRDOP)
                            .setGroupId(groupInfo.getId())
                            .build();
                    updateTransferGroupStatus(updateTransferGroupStatusRequest);

                    // 查询对应的记录
                    ListTransferRecordRequest listTransferRecordRequest = ListTransferRecordRequest.newBuilder()
                            .setAirdropId(airdropInfo.getId())
                            .setBrokerId(airdropInfo.getBrokerId())
                            .setGroupId(groupInfo.getId())
                            .build();
                    List<AirdropTransferRecord> transferRecords = listTransferRecordByGroup(listTransferRecordRequest);
                    Integer transferStatus = batchTransfer(airdropInfo, transferRecords, groupInfo.getId());
                    if (transferStatus.equals(AirdropTransferGroupInfo.STATUS_FAILED)) {
                        haveError = true;
                    } else if (transferStatus.equals(AirdropTransferGroupInfo.STATUS_SUCCESS)) {
                        haveSuccess = true;
                    } else if (transferStatus.equals(AirdropTransferGroupInfo.STATUS_PART_SUCCESS)) {
                        haveSuccess = true;
                        haveError = true;
                    }
                } else {
                    haveSuccess = true;
                }
            }
            // 更新空投状态 发送完毕
            if (haveError && haveSuccess) {
                //有异常就是部分成功
                status = AirdropStatus.STATUS_PART_SUCCESS;
            } else if (haveError) {
                //有错误，没有成功 失败
                status = AirdropStatus.STATUS_FAILED;
            } else if (haveSuccess) {
                //有成功，没有失败 成功
                status = AirdropStatus.STATUS_SUCCESS;
            }
        }
        return status;
    }

    private boolean syncTransfer(AirdropInfo airdropInfo, AirdropTransferRecord transfer, Integer index) {
        try {
            SyncTransferRequest request = SyncTransferRequest.newBuilder()
                    .setBaseRequest(BaseReqUtil.getBaseRequest(airdropInfo.getBrokerId()))
                    // transfer id 、token 、amount
                    .setClientTransferId(transfer.getClientTransferId())
                    .setTokenId(transfer.getTokenId())
                    .setAmount(transfer.getTokenAmount().toPlainString())
                    // set source account
                    .setSourceOrgId(airdropInfo.getBrokerId())
                    .setSourceAccountId(airdropInfo.getAccountId())
                    .setSourceFlowSubject(BusinessSubject.AIRDROP)
                    // set target account
                    .setTargetOrgId(transfer.getBrokerId())
                    .setTargetAccountId(transfer.getAccountId())
                    .setTargetFlowSubject(BusinessSubject.AIRDROP)
                    //.setBalanceToLocked(airdropInfo.getLockModel() == 1)
                    .build();
            SyncTransferResponse response = grpcBatchTransferService.syncTransfer(request);
            log.info("syncTransfer request:{}, response:{}", TextFormat.shortDebugString(request), TextFormat.shortDebugString(response));
            if (response.getCode() == SyncTransferResponse.ResponseCode.SUCCESS) {
                log.info("syncTransfer success. request:{}", TextFormat.shortDebugString(request));
                if (airdropInfo.getLockModel() == 1) {
                    log.info("空投锁仓测试日志 org:{} aid:{} {}", transfer.getBrokerId(), transfer.getAccountId(), airdropInfo.getLockModel());
                    Long userId = accountService.getUserIdByAccountId(transfer.getBrokerId(), transfer.getAccountId());
                    log.info("空投锁仓测试日志 org:{} uid:{}", transfer.getBrokerId(), userId);
                    //添加锁仓记录并锁仓
                    String ids = balanceService.userLockBalanceForAdmin(transfer.getBrokerId(), userId + "", transfer.getTokenAmount().toPlainString(), 4, transfer.getTokenId(), "", 0L);
                    log.info("空投锁仓测试日志 结果 org:{} ids:{}", transfer.getBrokerId(), ids);
                }
                return true;
            } else {
                log.warn("syncTransfer failed. error code: {},request:{}", response.getCodeValue(), TextFormat.shortDebugString(request));
                return false;
            }
        } catch (Exception e) {
            log.error("syncTransfer error. {} => {}", airdropInfo, e);
            if (index == 0) {
                // 转账第一次执行遇到异常则重试一次，如果还是异常则返回失败
                return syncTransfer(airdropInfo, transfer, ++index);
            } else {
                return false;
            }
        }
    }

    /**
     * 批量转账
     *
     * @param airdropInfo
     * @param userTransferList
     * @param groupId
     */
    private Integer batchTransfer(AirdropInfo airdropInfo, List<AirdropTransferRecord> userTransferList, Long groupId) {
        List<Long> failRecordIds = new ArrayList<>();
        List<Integer> failLineIds = new ArrayList<>();
        List<Long> successRecordIds = new ArrayList<>();
        List<Integer> successLineIds = new ArrayList<>();
        for (AirdropTransferRecord transfer : userTransferList) {
            boolean isOK = syncTransfer(airdropInfo, transfer, 0);
            if (isOK) {
                successRecordIds.add(transfer.getId());
                successLineIds.add(transfer.getTmplLineId());
            } else {
                failRecordIds.add(transfer.getId());
                failLineIds.add(transfer.getTmplLineId());
            }
        }
        Integer transferStatus;
        if (failRecordIds.size() == 0 && successRecordIds.size() > 0) {
            // 全部成功
            transferStatus = AirdropTransferGroupInfo.STATUS_SUCCESS;
        } else if (failRecordIds.size() > 0 && successRecordIds.size() == 0) {
            // 全部失败
            transferStatus = AirdropTransferGroupInfo.STATUS_FAILED;
        } else if (failRecordIds.size() > 0) {
            // 部分成功，部分失败
            transferStatus = AirdropTransferGroupInfo.STATUS_PART_SUCCESS;
        } else {
            // 成功数量为0，失败数量也为0，默认为成功
            transferStatus = AirdropTransferGroupInfo.STATUS_SUCCESS;
        }
        UpdateTransferGroupStatusRequest updateRequest = UpdateTransferGroupStatusRequest.newBuilder()
                .setAirdropId(airdropInfo.getId())
                .setBrokerId(airdropInfo.getBrokerId())
                .setStatus(transferStatus)
                .setGroupId(groupId)
                .build();
        updateTransferGroupStatus(updateRequest, successRecordIds, successLineIds, failRecordIds, failLineIds);
        return transferStatus;
    }

    /**
     * 更新空投状态
     *
     * @param airdropId
     * @param brokerId
     * @param status
     * @return
     */
    private Boolean updateAirdropStatus(Long airdropId, Long brokerId, Integer status) {
        UpdateAirdropStatusRequest request = UpdateAirdropStatusRequest.newBuilder()
                .setAirdropId(airdropId)
                .setBrokerId(brokerId)
                .setStatus(status)
                .build();
        return updateAirdropStatus(request).getResult();
    }

    private List<TransferRecord> getTmplModelTransferItems(AirdropInfo airdropInfo, Integer index, Integer limit) {
        ListTmplRecordsRequest.Builder builder = ListTmplRecordsRequest.newBuilder()
                .setAirdropId(airdropInfo.getId())
                .setBrokerId(airdropInfo.getBrokerId())
                .setGroupId(0)
                .setLimit(limit);

        if (airdropInfo.getType() == AirdropPO.AIRDROP_TYPE_CANDY) {
            builder.setOrderByColumn("tokenId");
        } else if (airdropInfo.getType() == AirdropPO.AIRDROP_TYPE_FORK) {
            builder.setOrderByColumn("haveTokenId");
        }
        ListTmplRecordsReply replay = listTmplRecords(builder.build());
        List<TmplRecord> tmplRecords = replay.getTmplRecordList();
        if (CollectionUtils.isEmpty(tmplRecords)) {
            return new ArrayList<>();
        }

        List<TmplRecord> usedRecords = new ArrayList<>();
        usedRecords.add(tmplRecords.get(0));
        for (int i = 1; i < tmplRecords.size(); i++) {
            TmplRecord record = tmplRecords.get(i);
            if (airdropInfo.getType() == AirdropPO.AIRDROP_TYPE_CANDY) {
                if (record.getTokenId().equals(tmplRecords.get(i - 1).getTokenId())) {
                    usedRecords.add(record);
                } else {
                    break;
                }
            } else if (airdropInfo.getType() == AirdropPO.AIRDROP_TYPE_FORK) {
                if (record.getTokenId().equals(tmplRecords.get(i - 1).getTokenId())
                        && record.getHaveTokenId().equals(tmplRecords.get(i - 1).getHaveTokenId())) {
                    usedRecords.add(record);
                } else {
                    break;
                }
            }
        }


//        // 免费糖果 按照设定比例直接空投
        if (airdropInfo.getType() == AirdropPO.AIRDROP_TYPE_CANDY) {
            List<TransferRecord> transferItems = usedRecords.stream().map(d -> {
                return TransferRecord.newBuilder()
                        .setAccountId(d.getAccountId())
                        .setBrokerId(d.getBrokerId())
                        .setSnapshotTime(airdropInfo.getSnapshotTime())
                        .setTokenId(d.getTokenId())
                        .setTokenAmount(d.getTokenAmount())
                        .setTmplLineId(d.getTmplLineId())
                        .build();
            }).collect(Collectors.toList());
            return transferItems;
            // 分叉空投 需要根据持有币数量进行空投
        } else if (airdropInfo.getType() == AirdropPO.AIRDROP_TYPE_FORK) {
            List<Long> accountIds = usedRecords.stream().map(u -> u.getAccountId()).collect(Collectors.toList());
            Map<Long, BigDecimal> userAssetMap = getUserAsset(airdropInfo, accountIds, usedRecords.get(0).getHaveTokenId(), airdropInfo.getSnapshotTime());


            List<TransferRecord> transferItems = usedRecords.stream().map(d -> {
                return TransferRecord.newBuilder()
                        .setAccountId(d.getAccountId())
                        .setBrokerId(d.getBrokerId())
                        .setSnapshotTime(airdropInfo.getSnapshotTime())
                        .setTokenId(d.getTokenId())

                        .setTokenAmount(countTmplModelAirdropTokenNum(d, userAssetMap.get(d.getAccountId()), d.getBrokerId()).toPlainString())

                        .setTmplLineId(d.getTmplLineId())
                        .build();
            }).collect(Collectors.toList());

            return transferItems;
        }
        return new ArrayList<>();
    }

    private BigDecimal countTmplModelAirdropTokenNum(TmplRecord dto, BigDecimal balance, Long brokerId) {
        if (BigDecimal.ZERO.compareTo(new BigDecimal(dto.getHaveTokenAmount())) == 0
                || balance == null || BigDecimal.ZERO.compareTo(balance) == 0) {
            return new BigDecimal(0);
        }
        BigDecimal airdropNum = new BigDecimal(dto.getTokenAmount())
                .multiply(balance.divide(new BigDecimal(dto.getHaveTokenAmount()), RoundingMode.DOWN));
        // 获取投放token的最小精度
        Integer tokenScale = getTokenScale(dto.getTokenId(), brokerId);
        airdropNum = airdropNum.setScale(tokenScale, BigDecimal.ROUND_DOWN);
        return airdropNum;
    }


    /**
     * 转账记录入库
     *
     * @param airdropInfo
     * @param transferRecords
     */
    private void saveTransferRecord(AirdropInfo airdropInfo, List<TransferRecord> transferRecords) {
        if (CollectionUtils.isEmpty(transferRecords)) {
            return;
        }
        AddTransferRecordRequest request = AddTransferRecordRequest.newBuilder()
                .setAirdropId(airdropInfo.getId())
                .setBrokerId(airdropInfo.getBrokerId())
                .setTokenId(transferRecords.get(0).getTokenId())
                .addAllTransferRecordList(transferRecords)
                .build();
        Boolean isOk = addTransferRecord(request).getResult();
        if (!isOk) {
            log.error("Save Transfer Record Error. {}", airdropInfo);
        }
    }

    //非模板转账条目
    private List<TransferRecord> getUnTmplModelTransferItems(List<Long> accountIds, AirdropInfo airdropInfo, Integer index, Integer limit) {
        // 免费糖果 按照设定比例直接空投
        if (airdropInfo.getType() == AirdropPO.AIRDROP_TYPE_CANDY) {
            List<TransferRecord> transferItems = accountIds.stream().map(accountId -> {
                return TransferRecord.newBuilder()
                        .setAccountId(accountId)
                        .setBrokerId(airdropInfo.getBrokerId())
                        .setSnapshotTime(airdropInfo.getSnapshotTime())
                        .setTokenId(airdropInfo.getAirdropTokenId())
                        .setTokenAmount(airdropInfo.getAirdropTokenNum())
                        .build();
            }).collect(Collectors.toList());
            return transferItems;
            // 分叉空投 需要根据持有币数量进行空投
        } else if (airdropInfo.getType() == AirdropPO.AIRDROP_TYPE_FORK) {
            Map<Long, BigDecimal> userAssetMap = getUserAsset(airdropInfo, accountIds, airdropInfo.getHaveTokenId(), airdropInfo.getSnapshotTime());
            List<TransferRecord> transferItems = new ArrayList<>();
            if (null != userAssetMap) {
                userAssetMap.forEach((accountId, asset) -> {
                    transferItems.add(TransferRecord.newBuilder()
                            .setAccountId(accountId)
                            .setBrokerId(airdropInfo.getBrokerId())
                            .setSnapshotTime(airdropInfo.getSnapshotTime())
                            .setTokenId(airdropInfo.getAirdropTokenId())
                            .setTokenAmount(countAirdropTokenNum(airdropInfo, asset).toString())
                            .build());
                });
            }
            return transferItems;
        }
        return new ArrayList<>();
    }

    /**
     * 计算空投数量
     *
     * @param airdropInfo
     * @param balance
     * @return
     */
    private BigDecimal countAirdropTokenNum(AirdropInfo airdropInfo, BigDecimal balance) {
        if (BigDecimal.ZERO.compareTo(new BigDecimal(airdropInfo.getHaveTokenNum())) == 0 || BigDecimal.ZERO.compareTo(balance) == 0) {
            log.info("Airdrop countAirdropTokenNum is 0: airdrop id => {}, balance => {}.", airdropInfo.getId(), balance.toPlainString());
            return new BigDecimal(0);
        }
        BigDecimal airdropNum = new BigDecimal(airdropInfo.getAirdropTokenNum()).multiply(balance.divide(new BigDecimal(airdropInfo.getHaveTokenNum())));
        // 获取投放token的最小精度
        Integer tokenScale = getTokenScale(airdropInfo.getAirdropTokenId(), airdropInfo.getBrokerId());
        airdropNum = airdropNum.setScale(tokenScale, BigDecimal.ROUND_DOWN);
        return airdropNum;
    }

    /**
     * 获取token最小精度，默认为18位
     *
     * @param tokenId
     * @return
     */
    private Integer getTokenScale(String tokenId, Long orgId) {
        GetTokenRequest request = GetTokenRequest.newBuilder()
                .setBaseRequest(BaseReqUtil.getBaseRequest(orgId))
                .setTokenId(tokenId)
                .build();
        io.bhex.base.token.TokenDetail token = grpcTokenService.getToken(request);
        if (null != token) {
            log.info("Airdrop getTokenScale: tokenId => {}, MinPrecision => {}", tokenId, token.getMinPrecision());
            return token.getMinPrecision();
        }
        log.info("Airdrop getTokenScale: tokenId => {}, MinPrecision => {}", tokenId, 18);
        return 18;
    }

    /**
     * 获取资产快照
     *
     * @param accountIds
     * @param tokenId
     * @param snapshotTime
     */
    private Map<Long, BigDecimal> getUserAsset(AirdropInfo airdropInfo, List<Long> accountIds, String tokenId, Long snapshotTime) {
        Map<Long, BigDecimal> result = new HashMap<>();
        try {
            log.info("Get Asset Snapshot From Clear Begin. Sanpshot Time:{}, Account Ids {}, Token Id {}", snapshotTime, accountIds, tokenId);
            List<StatisticsBalanceSnapshot> balanceList = statisticsBalanceService.queryAssetSnapByAccountIds(accountIds,tokenId,snapshotTime);
            if (!CollectionUtils.isEmpty(balanceList)) {
                log.info("Get Asset Snapshot From Clear: Success. Sanpshot Time:{}, Token Id {}, Asset List Size {}", snapshotTime, tokenId, balanceList.size());
                for (StatisticsBalanceSnapshot balance : balanceList) {
                    if (balance.getTotal().compareTo(BigDecimal.ZERO) > 0) {
                        result.put(balance.getAccountId(), balance.getTotal());
                        log.info("Asset Log: {} {}", balance.getAccountId(), balance.getTotal());
                    }
                }
                //资产快照入库
                saveUserAssetRecord(airdropInfo.getBrokerId(), airdropInfo.getHaveTokenId(), airdropInfo.getSnapshotTime(), result, airdropInfo);
            }
        } catch (Exception e) {
            log.error("Get Asset Snapshot From Clear: Error. Sanpshot Time:{}, Account Ids {}, Token Id {}", snapshotTime, accountIds, tokenId, e);
        }
        return result;
    }

    /**
     * 用户资产快照入库
     *
     * @param userAssetMap
     */
    private void saveUserAssetRecord(long brokerId, String haveTokenId, long snapshotTime, Map<Long, BigDecimal> userAssetMap, AirdropInfo airdropInfo) {
        List<AssetSnapshot> assetSnapshots = new ArrayList<>();
        userAssetMap.forEach((accountId, asset) -> {
            if (asset.compareTo(BigDecimal.ZERO) > 0) {
                assetSnapshots.add(AssetSnapshot.newBuilder()
                        .setAccountId(accountId)
                        .setAssetAmount(asset.toString())
//                    .setAirdropId(airdropInfo.)
                        .setBrokerId(brokerId)
                        .setSnapshotTime(snapshotTime)
                        .setTokenId(haveTokenId)
                        .build());
            }
        });
        AddAssetSnapshotRequest.Builder builder = AddAssetSnapshotRequest.newBuilder();
        builder.addAllAssetSnapshotList(assetSnapshots);
        Boolean isOk = addAssetSnapshot(builder.build()).getResult();
        if (!isOk) {
            log.error("Save User Asset Record Error. {}", airdropInfo);
        }
    }

    /**
     * 根据转账记录过滤已经转过钱的account id
     *
     * @return
     */
    private List<Long> transferRecordFilter(List<Long> accountIds, Long airdropId) {
        if (CollectionUtils.isEmpty(accountIds)) {
            return new ArrayList();
        }
        TransferRecordFilterRequest request = TransferRecordFilterRequest.newBuilder()
                .setAirdropId(airdropId)
                .addAllAccountIds(accountIds)
                .build();
        TransferRecordFilterReply reply = transferRecordFilter(request);
        return reply.getAccountIdsList();
    }

    public List<Long> verifyAccountIds(Long brokerId, List<Long> accountIds) {
        List<Long> collect = new ArrayList<>();
        Header header = Header.newBuilder()
                .setOrgId(brokerId)
                .build();
        VerifyBrokerAccountRequest request = VerifyBrokerAccountRequest.newBuilder()
                .addAllAccountIds(accountIds)
                .setHeader(header)
                .build();
        VerifyBrokerAccountResponse response = accountService.verifyBrokerAccount(request.getHeader().getOrgId(),
                request.getAccountIdsList());
        List<SimpleAccount> accountsList = response.getAccountsList();
        if (!CollectionUtils.isEmpty(accountIds)) {
            collect = accountsList.stream().map(sa -> {
                return sa.getAccountId();
            }).collect(Collectors.toList());
        }
        return collect;
    }

    public GetBrokerAccountListResponse getAllUserPageable(Long registerTime, Long fromId, Long brokerId, Integer limit) {
        Header header = Header.newBuilder()
                .setOrgId(brokerId)
                .build();
        GetBrokerAccountListRequest request = GetBrokerAccountListRequest.newBuilder()
                .setHeader(header)
                .setBeginTime(0)
                .setEndTime(registerTime)
                .setFromId(fromId)
                .setLimit(limit)
                .build();
        GetBrokerAccountListResponse reply = accountService.getBrokerAccountList(request.getHeader().getOrgId(), request.getFromId(),
                request.getBeginTime(), request.getEndTime(), request.getLimit());
        return reply;
    }

    /**
     * 分页获取指定用户的account id
     *
     * @param airdropInfo
     * @param index
     * @param limit
     * @return
     */
    private static List<Long> getSpecialUserPageable(AirdropInfo airdropInfo, Integer index, Integer limit) {
        List<Long> accountIds = new ArrayList<>();
        String userAccountIdStr = airdropInfo.getUserAccountIds();
        userAccountIdStr = userAccountIdStr.replaceAll("\\s*", "");
        if (org.springframework.util.StringUtils.isEmpty(userAccountIdStr)) {
            return accountIds;
        }
        String[] accountIdArray = userAccountIdStr.split(SPLIT_REGEX);
        Integer length;
        if (index >= accountIdArray.length || accountIdArray.length < 1) {
            return accountIds;
        } else if (index + limit > accountIdArray.length) {
            length = accountIdArray.length;
        } else {
            length = index + limit;
        }
        for (Integer i = index; i < length; i++) {
            if (!org.springframework.util.StringUtils.isEmpty(accountIdArray[i])) {
                accountIds.add(new Long(accountIdArray[i]));
            }
        }
        return accountIds;
    }


    public QueryAirdropInfoReply queryAirdropInfo(QueryAirdropInfoRequest request) {
        Example example = new Example(io.bhex.broker.server.model.AirdropInfo.class).excludeProperties("userIds", "userAccountIds");
        PageHelper.startPage(0, 200); //只取最新200条
        Example.Criteria criteria = example.createCriteria();
        if (StringUtils.isNotEmpty(request.getTitle())) {
            criteria.andLike("title", "%" + request.getTitle() + "%");
        }
        if (request.getBeginTime() != 0) {
            criteria.andGreaterThanOrEqualTo("airdropTime", request.getBeginTime());
        }
        if (request.getEndTime() != 0) {
            criteria.andLessThanOrEqualTo("airdropTime", request.getEndTime());
        }
        criteria.andEqualTo("brokerId", request.getBrokerId());
        example.setOrderByClause("created_at desc");
        List<io.bhex.broker.server.model.AirdropInfo> airdropInfos = airdropInfoMapper.selectByExample(example);
        List<AirdropInfo> infos = processAirdropInfo(airdropInfos, true);
        return QueryAirdropInfoReply.newBuilder()
                .addAllAirdropInfo(infos)
                .build();
    }

    public QueryAirdropInfoReply listScheduleAirdrop() {
        Example example = new Example(io.bhex.broker.server.model.AirdropInfo.class);
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("status", AirdropStatus.STATUS_AUDIT_PASSED);
        // 调整为读取1分钟前的空投记录，防止模版空投时，模版空投明细未插入到数据库，最后空投状态显示成功实际未空投给用户
        criteria.andLessThanOrEqualTo("airdropTime", System.currentTimeMillis() - 60000);
        List<io.bhex.broker.server.model.AirdropInfo> airdropInfos = airdropInfoMapper.selectByExample(example);
        List<AirdropInfo> infos = processAirdropInfo(airdropInfos, false);
        QueryAirdropInfoReply reply = QueryAirdropInfoReply.newBuilder()
                .addAllAirdropInfo(infos)
                .build();
        return reply;
    }

    private List<AirdropInfo> processAirdropInfo(List<io.bhex.broker.server.model.AirdropInfo> airdropInfos, Boolean changeStatus) {
        List<AirdropInfo> infos = new ArrayList<>();
        if (!CollectionUtils.isEmpty(airdropInfos)) { //列表里不传用户id列表了，数据量太大卡死了，详情里有此信息
            infos = airdropInfos.stream().map(info -> {
                if (StringUtils.isEmpty(info.getTmplUrl())) {
                    info.setTmplUrl("");
                }
                AirdropInfo.Builder builder = AirdropInfo.newBuilder();
                BeanCopyUtils.copyPropertiesIgnoreNull(info, builder);
                //builder.setUserAccountIds(info.getUserAccountIds());
                //builder.setUserIds(info.getUserIds());
                builder.setAirdropTokenNum(info.getAirdropTokenNum().toString());
                builder.setHaveTokenNum(info.getHaveTokenNum().toString());
                //处理新增的中间状态
                if (changeStatus && Objects.equals(AirdropStatus.STATUS_AIRDROP_TRANSFER_WAITING, info.getStatus())) {
                    builder.setStatus(AirdropStatus.STATUS_AIRDROP);
                } else if (changeStatus && Objects.equals(AirdropStatus.STATUS_AIRDROP_TRANSFER_EXECUTING, info.getStatus())) {
                    builder.setStatus(AirdropStatus.STATUS_AIRDROP);
                }
                return builder.build();
            }).collect(Collectors.toList());
        }
        return infos;
    }


    public CreateAirdropInfoReply createAirdropInfo(CreateAirdropInfoRequest request) {
        boolean tmplModel = StringUtils.isNotEmpty(request.getTmplUrl());
        io.bhex.broker.server.model.AirdropInfo airdropInfo = new io.bhex.broker.server.model.AirdropInfo();
        BeanUtils.copyProperties(request, airdropInfo);
        airdropInfo.setStatus(AirdropStatus.STATUS_INIT);

        if (!tmplModel) {
            //最小精度判断
            BigDecimal airdropTokenNum = new BigDecimal(request.getAirdropTokenNum());
            BigDecimal haveTokenNum = new BigDecimal(request.getHaveTokenNum());
            if (airdropTokenNum.scale() > ProtoConstants.PRECISION || haveTokenNum.scale() > ProtoConstants.PRECISION) {
                return CreateAirdropInfoReply.newBuilder()
                        .setResult(false)
                        .setMessage("min.scale.error")
                        .build();
            }
            airdropInfo.setAirdropTokenNum(airdropTokenNum);
            airdropInfo.setHaveTokenNum(haveTokenNum);
        } else {
            airdropInfo.setAirdropTokenNum(BigDecimal.ZERO);
            airdropInfo.setHaveTokenNum(BigDecimal.ZERO);
        }
        BigDecimal transferAssetAmount = StringUtils.isNotEmpty(request.getTransferAssetAmount()) ? new BigDecimal(request.getTransferAssetAmount()) : new BigDecimal(0);
        if (transferAssetAmount.scale() > ProtoConstants.PRECISION) {
            return CreateAirdropInfoReply.newBuilder()
                    .setResult(false)
                    .setMessage("min.scale.error")
                    .build();
        }
        airdropInfo.setTransferAssetAmount(transferAssetAmount);
        airdropInfo.setUpdatedAt(System.currentTimeMillis());
        airdropInfo.setCreatedAt(System.currentTimeMillis());
        boolean isOk = airdropInfoMapper.insert(airdropInfo) > 0;
        CreateAirdropInfoReply reply = CreateAirdropInfoReply.newBuilder()
                .setResult(isOk)
                .setAirdropId(airdropInfo.getId())
                .build();
        if (isOk) {
            FlowAuditEvent event = FlowAuditEvent.builder()
                    .bizId(airdropInfo.getId())
                    .bizType(airdropInfo.getType()) //免费空投 持币空投
                    .title(airdropInfo.getTitle())
                    .orgId(airdropInfo.getBrokerId())
                    .applicant(request.getAdminId())
                    .applicantName(request.getAdminRealName())
                    .applicantEmail(request.getAdminUserName())
                    .applyDate(System.currentTimeMillis())
                    .build();
            applicationContext.publishEvent(event);
        }
        return reply;
    }

    private void createAirdropTmpl() {

    }

    public AirdropInfo getAirdropInfo(GetAirdropInfoRequest request) {
        io.bhex.broker.server.model.AirdropInfo info = getAirdropInfo(request.getAirdropId(), request.getBrokerId());
        AirdropInfo.Builder builder = AirdropInfo.newBuilder();
        if (null != info) {
            if (StringUtils.isEmpty(info.getTmplUrl())) {
                info.setTmplUrl("");
            }
            BeanUtils.copyProperties(info, builder);
            builder.setAirdropTokenNum(info.getAirdropTokenNum().toString());
            builder.setHaveTokenNum(info.getHaveTokenNum().toString());
        }
        return builder.build();
    }

    @Transactional(rollbackFor = Exception.class)
    public LockAndAirdropReply lockAndAirdrop(LockAndAirdropRequest request) {
        LockAndAirdropReply.Builder builder = LockAndAirdropReply.newBuilder();
        io.bhex.broker.server.model.AirdropInfo airdropInfo = airdropInfoMapper.getAirdropInfAndLock(request.getAirdropId(), request.getBrokerId());
        if (airdropInfo.getStatus() == AirdropStatus.STATUS_AUDIT_PASSED) {
            airdropInfo.setStatus(AirdropStatus.STATUS_AIRDROP);
            airdropInfoMapper.updateByPrimaryKeySelective(airdropInfo);
            builder.setIsLocked(true);
        } else {
            builder.setIsLocked(false);
        }
        return builder.build();
    }

    private io.bhex.broker.server.model.AirdropInfo getAirdropInfo(Long airdropId, Long brokerId) {
        Example example = new Example(io.bhex.broker.server.model.AirdropInfo.class);
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("id", airdropId);
        criteria.andEqualTo("brokerId", brokerId);
        return airdropInfoMapper.selectOneByExample(example);
    }

    public AddTransferRecordReply addTransferRecord(AddTransferRecordRequest request) {
        List<TransferRecord> transferRecordList = request.getTransferRecordListList();
        if (!CollectionUtils.isEmpty(transferRecordList)) {
            // 生成分组信息
            AirdropTransferGroupInfo groupInfo = new AirdropTransferGroupInfo();
            groupInfo.setId(sequenceGenerator.getLong());
            groupInfo.setAirdropId(request.getAirdropId());
            groupInfo.setBrokerId(request.getBrokerId());
            groupInfo.setStatus(AirdropTransferGroupInfo.STATUS_INIT);
            groupInfo.setTransferCount(new Long(transferRecordList.size()));
            groupInfo.setTransferAssetAmount(StringUtils.EMPTY);
            groupInfo.setTransferAssetAmount(new BigDecimal(0).toString());
            groupInfo.setTokenId(request.getTokenId());
            groupInfo.setCreatedAt(System.currentTimeMillis());
            transferGroupInfoMapper.insert(groupInfo);
            // 记录每笔转账信息
            Long groupId = groupInfo.getId();
            BigDecimal assetAmount = new BigDecimal(0);

            List<Integer> tmplLineIds = new ArrayList<>();
            List<AirdropTransferRecord> records = new ArrayList<>();
            for (TransferRecord record : transferRecordList) {
                AirdropTransferRecord r = new AirdropTransferRecord();
                BeanUtils.copyProperties(record, r);
                r.setGroupId(groupId);
                r.setAirdropId(request.getAirdropId());
                r.setTokenAmount(new BigDecimal(record.getTokenAmount()));
                r.setCreatedAt(System.currentTimeMillis());
                r.setMsgNoticed(0);
                r.setLockedStatus(0);
                r.setClientTransferId(sequenceGenerator.getLong());
                assetAmount = assetAmount.add(r.getTokenAmount());
                records.add(r);
                tmplLineIds.add(record.getTmplLineId());
            }
            transferRecordMapper.insertList(records);
            airdropTmplRecordMapper.updageGroups(request.getBrokerId(), request.getAirdropId(), tmplLineIds, groupId);

            // 更新总钱数
            groupInfo = new AirdropTransferGroupInfo();
            groupInfo.setId(groupId);
            groupInfo.setTransferAssetAmount(assetAmount.toString());
            transferGroupInfoMapper.updateByPrimaryKeySelective(groupInfo);
        }
        AddTransferRecordReply reply = AddTransferRecordReply.newBuilder()
                .setResult(true)
                .build();
        return reply;
    }

    @Transactional
    public AddTmplRecordReply addTmplRecord(AddTmplRecordRequest request) {
        List<TmplRecord> records = request.getTmplRecordListList();
        if (CollectionUtils.isEmpty(records)) {
            return AddTmplRecordReply.newBuilder().build();
        }

        List<AirdropTmplRecord> list = new ArrayList<>();
        for (TmplRecord record : records) {
            AirdropTmplRecord r = new AirdropTmplRecord();
            BeanUtils.copyProperties(record, r);
            r.setAirdropId(request.getAirdropId());
            r.setBrokerId(request.getBrokerId());
            r.setTokenAmount(new BigDecimal(record.getTokenAmount()));
            r.setHaveTokenId(record.getHaveTokenId());
            r.setHaveTokenAmount(new BigDecimal(record.getHaveTokenAmount()));
            r.setCreatedAt(System.currentTimeMillis());
            r.setTransferTokenAmount(BigDecimal.ZERO);
            list.add(r);

        }
        airdropTmplRecordMapper.insertList(list);
        return AddTmplRecordReply.newBuilder().build();

    }


    //int32 limit = 4;
    //string order_by_column = 5;


    public ListTmplRecordsReply listTmplRecords(ListTmplRecordsRequest request) {
        Example example;

        if (request.getOrderByColumn().equals("tokenId")) {
            example = Example.builder(AirdropTmplRecord.class).orderByAsc("tokenId").build();
        } else if (request.getOrderByColumn().equals("haveTokenId")) {
            example = Example.builder(AirdropTmplRecord.class).orderByAsc("tokenId").orderByAsc("haveTokenId").build();
        } else {
            example = Example.builder(AirdropTmplRecord.class).build();
        }
        PageHelper.startPage(0, request.getLimit());


        //Example example = new Example(io.bhex.broker.server.model.AirdropTmplRecord.class).orderByDesc("id");
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("brokerId", request.getBrokerId());
        criteria.andEqualTo("airdropId", request.getAirdropId());
        if (request.getGroupId() >= 0) {
            criteria.andEqualTo("groupId", request.getGroupId());
        }
        List<AirdropTmplRecord> list = airdropTmplRecordMapper.selectByExample(example);
        if (CollectionUtils.isEmpty(list)) {
            return ListTmplRecordsReply.newBuilder().build();
        }

        List<TmplRecord> resultList = list.stream().map(l -> {
            TmplRecord.Builder builder = TmplRecord.newBuilder();
            BeanCopyUtils.copyPropertiesIgnoreNull(l, builder);
            builder.setTokenAmount(l.getTokenAmount().toPlainString());
            builder.setHaveTokenAmount(l.getHaveTokenAmount().toPlainString());
            return builder.build();
        }).collect(Collectors.toList());
        return ListTmplRecordsReply.newBuilder().addAllTmplRecord(resultList).build();

    }

    public AddAssetSnapshotReply addAssetSnapshot(AddAssetSnapshotRequest request) {
        List<AssetSnapshot> assetSnapshotList = request.getAssetSnapshotListList();
        if (!CollectionUtils.isEmpty(assetSnapshotList)) {
            List<AssetSnapshotRecord> records = assetSnapshotList.stream().map(record -> {
                AssetSnapshotRecord r = new AssetSnapshotRecord();
                BeanUtils.copyProperties(record, r);
                r.setAssetAmount(new BigDecimal(record.getAssetAmount()));
                r.setCreatedAt(System.currentTimeMillis());
                return r;
            }).collect(Collectors.toList());
            for (AssetSnapshotRecord r : records) {
                assetSnapshotRecordMapper.insertSelective(r);
            }
        }
        AddAssetSnapshotReply reply = AddAssetSnapshotReply.newBuilder()
                .setResult(true)
                .build();
        return reply;
    }

    public TransferGroupInfo getTransferGroupInfo(GetTransferGroupInfoRequest request) {
        Example example = new Example(io.bhex.broker.server.model.AirdropInfo.class);
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("id", request.getGroupId());
        criteria.andEqualTo("brokerId", request.getBrokerId());
        criteria.andEqualTo("airdropId", request.getBrokerId());

        AirdropTransferGroupInfo groupInfo = transferGroupInfoMapper.selectOneByExample(example);
        TransferGroupInfo.Builder builder = TransferGroupInfo.newBuilder();
        BeanUtils.copyProperties(groupInfo, builder);

        return builder.build();
    }

    public UpdateAirdropStatusReply updateAirdropStatus(UpdateAirdropStatusRequest request) {
        UpdateAirdropStatusReply.Builder builder = UpdateAirdropStatusReply.newBuilder();
        builder.setResult(false);
        Example example = new Example(io.bhex.broker.server.model.AirdropInfo.class);
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("id", request.getAirdropId());
        criteria.andEqualTo("brokerId", request.getBrokerId());
        //增加关闭空投时状态保护
        if (request.getStatus() == AirdropStatus.STATUS_CLOSED) {
            criteria.andEqualTo("status", AirdropStatus.STATUS_AUDIT_PASSED);
        }
        io.bhex.broker.server.model.AirdropInfo airdropInfo = airdropInfoMapper.selectOneByExample(example);
        if (null != airdropInfo) {
            airdropInfo.setStatus(request.getStatus());
            Boolean isOk = airdropInfoMapper.updateByExample(airdropInfo, example) > 0 ? true : false;
            builder.setResult(isOk);
        }
        return builder.build();
    }

    public TransferRecordFilterReply transferRecordFilter(TransferRecordFilterRequest request) {
        TransferRecordFilterReply.Builder builder = TransferRecordFilterReply.newBuilder();
        List<Long> accountIdsList = request.getAccountIdsList();
        List<Long> result = new ArrayList<>();
        if (!CollectionUtils.isEmpty(accountIdsList)) {
            Example example = new Example(AirdropTransferRecord.class);
            Example.Criteria criteria = example.createCriteria();
            criteria.andEqualTo("airdropId", request.getAirdropId());
            criteria.andIn("accountId", accountIdsList);
            List<AirdropTransferRecord> transferRecords = transferRecordMapper.selectByExample(example);
            List<Long> sentAccountIds = transferRecords.stream().map(AirdropTransferRecord::getAccountId).collect(Collectors.toList());
            for (Long id : accountIdsList) {
                if (!sentAccountIds.contains(id)) {
                    result.add(id);
                }
            }
        }
        builder.addAllAccountIds(result);
        return builder.build();
    }

    public ListAllTransferGroupReply listAllTransferGroup(ListAllTransferGroupRequest request) {
        Example example = new Example(AirdropTransferGroupInfo.class);
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("airdropId", request.getAirdropId());
        criteria.andEqualTo("brokerId", request.getBrokerId());
        List<AirdropTransferGroupInfo> transferGroupInfos = transferGroupInfoMapper.selectByExample(example);
        List<TransferGroupInfo> groupInfos = transferGroupInfos.stream().map(info -> {
            TransferGroupInfo.Builder builder = TransferGroupInfo.newBuilder();
            BeanUtils.copyProperties(info, builder);
            return builder.build();
        }).collect(Collectors.toList());
        return ListAllTransferGroupReply.newBuilder()
                .addAllTransferGroupInfos(groupInfos)
                .build();
    }

    public UpdateTransferGroupStatusReply updateTransferGroupStatus(UpdateTransferGroupStatusRequest request) {
        UpdateTransferGroupStatusReply.Builder builder = UpdateTransferGroupStatusReply.newBuilder();
        builder.setResult(false);
        Example example = new Example(AirdropTransferGroupInfo.class);
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("id", request.getGroupId());
        criteria.andEqualTo("airdropId", request.getAirdropId());
        criteria.andEqualTo("brokerId", request.getBrokerId());
        AirdropTransferGroupInfo transferGroupInfo = transferGroupInfoMapper.selectOneByExample(example);
        if (null != transferGroupInfo) {
            transferGroupInfo.setStatus(request.getStatus());
            boolean isOk = transferGroupInfoMapper.updateByExample(transferGroupInfo, example) > 0;
            builder.setResult(isOk);
            // 如果本分组发送成功，要更新空投信息中的投放人数、投放钱数
            updateAirdropUserCount(transferGroupInfo);

            if (request.getStatus() == AirdropTransferGroupInfo.STATUS_SUCCESS || request.getStatus() == AirdropTransferGroupInfo.STATUS_FAILED) {
                //如果空投成功或失败同时更新转账记录表
                int row1 = transferRecordMapper.updateStatus(request.getBrokerId(), request.getAirdropId(), request.getGroupId(), request.getStatus());

                int row2 = airdropTmplRecordMapper.updateStatus(request.getBrokerId(), request.getAirdropId(), request.getGroupId(), request.getStatus());

                log.info("updateRecordRows:{} tmplRow:{}", row1, row2);
            }

            if (request.getStatus() == AirdropTransferGroupInfo.STATUS_SUCCESS) {
                io.bhex.broker.server.model.AirdropInfo airdropInfo = airdropInfoMapper.selectByPrimaryKey(transferGroupInfo.getAirdropId());
                if (airdropInfo.getLockModel() == 1) {
                    transferRecordMapper.updateLockedStatus(request.getBrokerId(), request.getAirdropId(), request.getGroupId(), AirdropTransferRecord.NEED_LOCK_STATUS);
                } else {
                    transferRecordMapper.updateLockedStatus(request.getBrokerId(), request.getAirdropId(), request.getGroupId(), AirdropTransferRecord.UNLOCKED_STATUS);
                }
            }
        }
        return builder.build();
    }

    public void updateTransferGroupStatus(UpdateTransferGroupStatusRequest request, List<Long> successRecordIds, List<Integer> successLineIds, List<Long> failRecordIds, List<Integer> failLineIds) {
        Example example = new Example(AirdropTransferGroupInfo.class);
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("id", request.getGroupId());
        criteria.andEqualTo("airdropId", request.getAirdropId());
        criteria.andEqualTo("brokerId", request.getBrokerId());
        AirdropTransferGroupInfo transferGroupInfo = transferGroupInfoMapper.selectOneByExample(example);
        if (null != transferGroupInfo) {
            transferGroupInfo.setStatus(request.getStatus());
            transferGroupInfoMapper.updateByExample(transferGroupInfo, example);
            // 如果本分组发送成功，要更新空投信息中的投放人数、投放钱数
            updateAirdropUserCount(transferGroupInfo);

            if (request.getStatus() == AirdropTransferGroupInfo.STATUS_SUCCESS || request.getStatus() == AirdropTransferGroupInfo.STATUS_FAILED) {
                //如果空投成功或失败同时更新转账记录表
                int row1 = transferRecordMapper.updateStatus(request.getBrokerId(), request.getAirdropId(), request.getGroupId(), request.getStatus());
                int row2 = airdropTmplRecordMapper.updateStatus(request.getBrokerId(), request.getAirdropId(), request.getGroupId(), request.getStatus());
                log.info("updateRecordRows:{} tmplRow:{}", row1, row2);
            } else if (request.getStatus() == AirdropTransferGroupInfo.STATUS_PART_SUCCESS) {
                //部分成功时更新成功部分
                int row1 = transferRecordMapper.updateStatusByGroup(request.getBrokerId(), request.getAirdropId(), request.getGroupId(), successRecordIds, AirdropTransferGroupInfo.STATUS_SUCCESS);
                int row2 = airdropTmplRecordMapper.updateStatusByGroup(request.getBrokerId(), request.getAirdropId(), request.getGroupId(), successLineIds, AirdropTransferGroupInfo.STATUS_SUCCESS);
                log.info("updateRecordRows:{} tmplRow:{}", row1, row2);

                //部分成功时更新失败部分
                int row3 = transferRecordMapper.updateStatusByGroup(request.getBrokerId(), request.getAirdropId(), request.getGroupId(), failRecordIds, AirdropTransferGroupInfo.STATUS_FAILED);
                int row4 = airdropTmplRecordMapper.updateStatusByGroup(request.getBrokerId(), request.getAirdropId(), request.getGroupId(), failLineIds, AirdropTransferGroupInfo.STATUS_FAILED);
                log.info("update fail RecordRows:{} tmplRow:{}", row3, row4);
            }

            if (request.getStatus() == AirdropTransferGroupInfo.STATUS_SUCCESS) {
                io.bhex.broker.server.model.AirdropInfo airdropInfo = airdropInfoMapper.selectByPrimaryKey(transferGroupInfo.getAirdropId());
                if (airdropInfo.getLockModel() == 1) {
                    transferRecordMapper.updateLockedStatus(request.getBrokerId(), request.getAirdropId(), request.getGroupId(), AirdropTransferRecord.NEED_LOCK_STATUS);
                } else {
                    transferRecordMapper.updateLockedStatus(request.getBrokerId(), request.getAirdropId(), request.getGroupId(), AirdropTransferRecord.UNLOCKED_STATUS);
                }
            }
        }
    }

    /**
     * 更新空投信息中的投放人数、投放钱数
     */
    private void updateAirdropUserCount(AirdropTransferGroupInfo transferGroupInfo) {
        // 如果本分组发送成功，要更新空投信息中的投放人数、投放钱数
        if (AirdropTransferGroupInfo.STATUS_SUCCESS.equals(transferGroupInfo.getStatus()) ||
                AirdropTransferGroupInfo.STATUS_PART_SUCCESS.equals(transferGroupInfo.getStatus())) {
            Example airdropExample = new Example(io.bhex.broker.server.model.AirdropInfo.class);
            Example.Criteria airdropCriteria = airdropExample.createCriteria();
            airdropCriteria.andEqualTo("id", transferGroupInfo.getAirdropId());
            airdropCriteria.andEqualTo("brokerId", transferGroupInfo.getBrokerId());
            io.bhex.broker.server.model.AirdropInfo airdropInfo = airdropInfoMapper.selectOneByExample(airdropExample);
            if (null != airdropInfo) {
                airdropInfo.setUserCount(airdropInfo.getUserCount() + transferGroupInfo.getTransferCount());
                airdropInfo.setTransferAssetAmount(airdropInfo.getTransferAssetAmount().add(new BigDecimal(transferGroupInfo.getTransferAssetAmount())));
                airdropInfo.setUpdatedAt(System.currentTimeMillis());
                airdropInfoMapper.updateByExample(airdropInfo, airdropExample);
            }
        }
    }

    public List<AirdropTransferRecord> listTransferRecordByGroup(ListTransferRecordRequest request) {
        Example example = new Example(AirdropTransferRecord.class);
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("airdropId", request.getAirdropId());
        criteria.andEqualTo("brokerId", request.getBrokerId());
        criteria.andEqualTo("groupId", request.getGroupId());
        return transferRecordMapper.selectByExample(example);
    }

    public ListTransferRecordReply listTransferRecordByGroupId(ListTransferRecordRequest request) {
        Example example = new Example(AirdropTransferRecord.class);
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("airdropId", request.getAirdropId());
        criteria.andEqualTo("brokerId", request.getBrokerId());
        criteria.andEqualTo("groupId", request.getGroupId());
        List<AirdropTransferRecord> transferRecords = transferRecordMapper.selectByExample(example);
        List<TransferRecord> recordList = transferRecords.stream().map(tr -> {
            TransferRecord.Builder builder = TransferRecord.newBuilder();
            BeanUtils.copyProperties(tr, builder);
            builder.setTokenAmount(tr.getTokenAmount().toString());
            return builder.build();
        }).collect(Collectors.toList());
        return ListTransferRecordReply.newBuilder()
                .addAllTransferRecord(recordList)
                .build();
    }


    @Scheduled(cron = "15 */1  * * * ?")
    public void sendNotice() {
        String lockKey = BrokerLockKeys.AIRDROP_NOTICE_KEY;
        boolean lock = RedisLockUtils.tryLock(redisTemplate, lockKey, BrokerLockKeys.AIRDROP_NOTICE_KEY_EXPIRE);
        log.info("sendAirdropNotice locked:{}", lock);
        if (!lock) {
            return;
        }
        try {
            Map<Long, Boolean> userAllTypeMap = new HashMap<>();
            Example example = new Example(AirdropTransferRecord.class);
            Example.Criteria criteria = example.createCriteria();
            criteria.andEqualTo("status", 1);
            criteria.andEqualTo("msgNoticed", 0);
            criteria.andGreaterThan("tokenAmount", 0);
            PageHelper.startPage(0, 500);
            List<AirdropTransferRecord> transferRecords = transferRecordMapper.selectByExample(example);
            if (CollectionUtils.isEmpty(transferRecords)) {
                log.info("no AirdropNotice");
                return;
            }
            Map<Long, List<AirdropTransferRecord>> group = transferRecords.stream()
                    .collect(Collectors.groupingBy(AirdropTransferRecord::getBrokerId));

            for (Long brokerId : group.keySet()) {
                if (!noticeTemplateService.canSendPush(brokerId, NoticeBusinessType.AIRDROP_NOTICE)) {
                    transferRecordMapper.updateBrokerMsgNoticedStatus(brokerId);
                    continue;
                }
                List<AirdropTransferRecord> records = group.get(brokerId);
                for (AirdropTransferRecord record : records) {
                    if (!userAllTypeMap.containsKey(record.getAirdropId())) {
                        io.bhex.broker.server.model.AirdropInfo airdropInfo = airdropInfoMapper.selectByPrimaryKey(record.getAirdropId());
                        userAllTypeMap.put(airdropInfo.getId(), airdropInfo.getUserType() == 1);
                        if (userAllTypeMap.get(record.getAirdropId())) {
                            transferRecordMapper.updateMsgNoticedStatus(record.getBrokerId(), record.getAirdropId());
                        }
                    }
                    if (userAllTypeMap.get(record.getAirdropId())) { //发送给全部用户就不通知了
                        continue;
                    }

                    Header header = Header.newBuilder().setOrgId(record.getBrokerId()).setLanguage("").build();
                    JsonObject jsonObject = new JsonObject();
                    jsonObject.addProperty("quantity", record.getTokenAmount().stripTrailingZeros().toPlainString());
                    jsonObject.addProperty("token", record.getTokenId());
                    Long userId = accountService.getUserIdByAccountId(record.getBrokerId(), record.getAccountId());
                    noticeTemplateService.sendBizPushNotice(header, userId, NoticeBusinessType.AIRDROP_NOTICE,
                            "airdrop_transfer_record_" + record.getId(), jsonObject, null);

                    record.setMsgNoticed(1);
                    transferRecordMapper.updateByPrimaryKeySelective(record);
                }
            }

        } finally {
            RedisLockUtils.releaseLock(redisTemplate, lockKey);
        }

    }

    @Async
    @EventListener
    public void auditEndEvent(FlowAuditEndEvent event) {
        try {
            if (event.getBizType() == 1 || event.getBizType() == 2) {
                int status = event.getAuditStatus() == 1 ? AirdropStatus.STATUS_AUDIT_PASSED : AirdropStatus.STATUS_AUDIT_REJECTED;
                updateAirdropStatus(event.getBizId(), event.getOrgId(), status);
            }
        } catch (Exception e) {
            log.error("auditEndEvent exception:{}", JsonUtil.defaultGson().toJson(event), e);
        }
    }


    //锁仓功能 还没有，也没测试过
//    @Resource
//    BalanceService balanceService;
//
//    @Scheduled(cron = "15 */1  * * * ?")
//    public void lockModel() {
//        String lockKey = BrokerLockKeys.AIRDROP_NOTICE_KEY;
//        boolean lock = RedisLockUtils.tryLock(redisTemplate, lockKey, BrokerLockKeys.AIRDROP_NOTICE_KEY_EXPIRE);
//        if (!lock) {
//            return;
//        }
//        try {
//            Map<Long, Long> userAdminMap = new HashMap<>();
//
//            Example example = new Example(AirdropTransferRecord.class);
//            Example.Criteria criteria = example.createCriteria();
//            criteria.andEqualTo("status", 1);
//            criteria.andEqualTo("lockedStatus", AirdropTransferRecord.NEED_LOCK_STATUS);
//            criteria.andGreaterThan("tokenAmount", 0);
//
//            PageHelper.startPage(0, 60);
//            List<AirdropTransferRecord> transferRecords = transferRecordMapper.selectByExample(example);
//            for (AirdropTransferRecord record : transferRecords) {
//                if (!userAdminMap.containsKey(record.getAirdropId())) {
//                    io.bhex.broker.server.model.AirdropInfo airdropInfo = airdropInfoMapper.selectByPrimaryKey(record.getAirdropId());
//                    userAdminMap.put(airdropInfo.getId(), airdropInfo.getAdminId());
//                }
//
//                Account account = accountMapper.getAccountByAccountId(record.getAccountId());
//                User user = userMapper.getByOrgAndUserId(record.getBrokerId(), account.getUserId());
//
//                String errorUserId = balanceService.userLockBalanceForAdmin(record.getBrokerId(), user.getUserId().toString(), record.getTokenAmount().toPlainString(),
//                        4, record.getTokenId(), "");
//                if (StringUtils.isEmpty(errorUserId)) {
//                    record.setLockedStatus(AirdropTransferRecord.LOCKED_STATUS);
//                    transferRecordMapper.updateByPrimaryKeySelective(record);
//                } else {
//                    log.error("ERROR airdrop lock failed airdrop:{} user:{} token:{} amonut:{}", record.getAirdropId(),
//                            errorUserId, record.getTokenId(), record.getTokenAmount());
//                }
//
//
//            }
//        } finally {
//            RedisLockUtils.releaseLock(redisTemplate, lockKey);
//        }
//
//    }
}
