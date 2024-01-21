package io.bhex.broker.server.grpc.server.service;

import com.google.common.collect.Lists;
import io.bhex.base.account.*;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.server.grpc.client.service.GrpcBatchTransferService;
import io.bhex.broker.server.grpc.server.service.activity.ActivityRepositoryService;
import io.bhex.broker.server.model.Activity;
import io.bhex.broker.server.model.ActivityTransferAccountLog;
import io.bhex.broker.server.model.User;
import io.bhex.broker.server.util.BaseReqUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * @ProjectName: broker.server
 * @Package: io.bhex.broker.server.grpc.server.service
 * @Author: yuehao  <hao.yue@bhex.com>
 * @CreateDate: 2018/11/21 下午2:33
 * @Copyright（C）: 2018 BHEX Inc. All rights reserved.
 */

@Service
@Slf4j
public class TransferService {

    @Resource
    private ActivityRepositoryService activityRepositoryService;

    @Resource
    private UserService userService;

    @Resource
    private AccountService accountService;

    @Resource
    private GrpcBatchTransferService grpcBatchTransferService;

    public Boolean transferToUser(Long userId, String total, Long clientTransferId) {
        Activity activity = activityRepositoryService.findActivity(1L);
        if (activity == null) {
            throw new IllegalStateException("transfer to user fail... not find activity info activityId 1L");
        }

        Long opsAccountId = activity.getAccountId();
        Long opsOrgId = activity.getOrgId();

        User user = userService.getUser(userId);

        Long personAccountId;
        try {
            personAccountId = accountService.getAccountId(user.getOrgId(), userId);
        } catch (BrokerException e) {
            return Boolean.FALSE;
        }

        if (personAccountId == null || personAccountId.longValue() == 0) {
            return Boolean.FALSE;
        }

        BatchTransferItem item = BatchTransferItem.newBuilder()
                .setAmount(total.toString())
                .setTargetAccountId(personAccountId)
                .setTargetAccountType(AccountType.GENERAL_ACCOUNT)
                .setTargetOrgId(user.getOrgId())
                .setTokenId(activity.getToken())
                .setSubject(BusinessSubject.TRANSFER)
                .build();

        List<BatchTransferItem> items = Lists.newArrayList(item);
        BatchTransferRequest request = BatchTransferRequest.newBuilder()
                .setBaseRequest(BaseReqUtil.getBaseRequest(opsOrgId))
                .setClientTransferId(clientTransferId)
                .setSourceAccountType(AccountType.OPERATION_ACCOUNT)
                .setSourceOrgId(opsOrgId)
                .setSubject(BusinessSubject.TRANSFER)
                .setSourceAccountId(opsAccountId)
                .addAllTransferTo(items)
                .build();
        BatchTransferResponse response = grpcBatchTransferService.batchTransfer(request);
        if (response.getErrorCode() != 0) {
            log.error("Transfer fail...,userId={},total={},activityId={}", userId, total.toString(), 1L);
            throw new IllegalStateException("transfer to user fail...");
        }

        List<ActivityTransferAccountLog> logs = new ArrayList<>();
        //记录转账流水
        items.stream().forEach(it -> {
            logs.add(ActivityTransferAccountLog
                    .builder().transferId(request.getClientTransferId())
                    .sourceAccountId(request.getSourceAccountId())
                    .targetAccountId(it.getTargetAccountId())
                    .amount(new BigDecimal(it.getAmount()))
                    .token(it.getTokenId())
                    .createTime(System.currentTimeMillis())
                    .userId(userId)
                    .build());
        });

        if (logs.size() > 0) {
            activityRepositoryService.saveActivityTransferAccountLog(logs);
            log.info("转账流水记录成功 userId {}", userId);
        }
        return true;
    }

}
