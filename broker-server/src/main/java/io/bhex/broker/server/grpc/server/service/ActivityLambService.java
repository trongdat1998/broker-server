package io.bhex.broker.server.grpc.server.service;

import io.bhex.base.account.LockBalanceReply;
import io.bhex.base.account.LockBalanceRequest;
import io.bhex.base.account.UnlockBalanceRequest;
import io.bhex.base.account.UnlockBalanceResponse;
import io.bhex.base.proto.BaseRequest;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.grpc.activity.lamb.*;
import io.bhex.broker.server.grpc.client.service.GrpcBalanceService;
import io.bhex.broker.server.model.ActivityLockInterest;
import io.bhex.broker.server.model.ActivityLockInterestOrder;
import io.bhex.broker.server.primary.mapper.ActivityLambInterestMapper;
import io.bhex.broker.server.primary.mapper.ActivityLambMapper;
import io.bhex.broker.server.primary.mapper.ActivityLambOrderMapper;
import io.bhex.broker.server.primary.mapper.ActivityLambRateMapper;
import io.bhex.broker.server.model.ActivityLamb;
import io.bhex.broker.server.model.ActivityLambOrder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;
import tk.mybatis.mapper.entity.Example;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.grpc.server.service
 * @Author: ming.xu
 * @CreateDate: 2019/5/29 11:41 AM
 * @Copyright（C）: 2019 BHEX Inc. All rights reserved.
 */
@Slf4j
@Service
public class ActivityLambService {

    private final static String LANGUAGE_ZH = "zh_CN";

    private final ActivityLambRateMapper lambRateMapper;

    private final AccountService accountService;

    private final ActivityLambMapper lambMapper;

    private final ActivityLambOrderMapper lambOrderMapper;

    private final ActivityLambInterestMapper lambInterestMapper;

    private GrpcBalanceService balanceService;

    @Autowired
    public ActivityLambService(GrpcBalanceService balanceService, ActivityLambRateMapper lambRateMapper, AccountService accountService, ActivityLambMapper lambMapper, ActivityLambOrderMapper lambOrderMapper, ActivityLambInterestMapper lambInterestMapper) {
        this.balanceService = balanceService;
        this.lambRateMapper = lambRateMapper;
        this.accountService = accountService;
        this.lambMapper = lambMapper;
        this.lambOrderMapper = lambOrderMapper;
        this.lambInterestMapper = lambInterestMapper;
    }


    public ListProjectInfoReply listProjectInfo(ListProjectInfoRequest request) {
        List<ActivityLamb> activityLambs = listProjectInfo(request.getHeader().getOrgId());
        List<ProjectInfo> infoList = new ArrayList<>();
        activityLambs.forEach(info -> {
            String language = request.getHeader().getLanguage();
            ProjectInfo.Builder builder = ProjectInfo.newBuilder();
            builder.setId(info.getId());
            builder.setBrokerId(info.getBrokerId());
            if (LANGUAGE_ZH.equals(language)) {
                builder.setProjectName(info.getProjectNameZh());
                builder.setTitle(info.getTitleZh());
                builder.setDescript(info.getDescriptZh());
            } else {
                builder.setProjectName(info.getProjectName());
                builder.setTitle(info.getTitle());
                builder.setDescript(info.getDescript());
            }
            builder.setLockedPeriod(info.getLockedPeriod());
            builder.setPlatformLimit(info.getPlatformLimit().toPlainString());
            builder.setMinPurchaseLimit(info.getMinPurchaseLimit().toPlainString());
            builder.setPurchaseTokenId(info.getPurchaseTokenId());
            builder.setStartTime(info.getStartTime());
            builder.setEndTime(info.getEndTime());
            builder.setCreatedTime(info.getCreatedTime());
            builder.setUpdatedTime(info.getUpdatedTime());
            builder.setStatus(info.getStatus());
            builder.setPurchaseableQuantity(info.getPurchaseableQuantity().toPlainString());
            builder.setFixedInterestRate(info.getFixedInterestRate());
            builder.setFloatingRate(info.getFloatingRate());
            builder.setProjectType(info.getProjectType());
            builder.setSoldAmount(info.getSoldAmount().toPlainString());

            // 用户可购买数额
            BigDecimal userLimit = info.getUserLimit();
            if (request.getHeader().getUserId() != 0) {
                BigDecimal purchaseAmount = userPurchaseAmount(request.getHeader().getOrgId(), info.getId(), accountService.getMainAccountId(request.getHeader()));
                userLimit = userLimit.subtract(purchaseAmount);
                if (userLimit.compareTo(info.getPurchaseableQuantity()) >= 0) {
                    userLimit = info.getPurchaseableQuantity();
                }
                if (userLimit.compareTo(BigDecimal.ZERO) < 0) {
                    userLimit = BigDecimal.ZERO;
                }
            }
            builder.setUserLimit(userLimit.toPlainString());
            infoList.add(builder.build());
        });
        ListProjectInfoReply reply = ListProjectInfoReply.newBuilder()
                .addAllProjectInfo(infoList)
                .build();
        return reply;
    }

    private List<ActivityLamb> listProjectInfo(Long brokerId) {
        List<ActivityLamb> activityLambs = lambMapper.listProjectInfo(brokerId);
        return activityLambs;
    }

    @Transactional(rollbackFor = Exception.class)
    public CreateLambOrderReply createLambOrder(CreateLambOrderRequest request) {
        //必须购买整数份
        BigDecimal fractionalPart = new BigDecimal(request.getAmount()).remainder( BigDecimal.ONE );
        if (fractionalPart.compareTo(BigDecimal.ZERO) != 0) {
            throw new BrokerException(BrokerErrorCode.ORDER_QUANTITY_TOO_BIG);
        }
        // 购买产品
        // 1.检验产品剩余可购买总额，检验个人可购买限额，购买数量是否为最小下单的整数倍，检验余额是否充足
        // 有锁
        ActivityLamb activityLamb = lambMapper.getByIdWithLock(request.getProjectId());
        if (Objects.nonNull(activityLamb)) {
            // 项目结束 停止购买
            if (activityLamb.getStatus() != 1 || (activityLamb.getEndTime() !=0 && System.currentTimeMillis() > activityLamb.getEndTime())) {
                log.error("Activity LAMB new order failed: activity not start or already end. org id -> {}, error projectId -> {}, now time:{}, end time:{}.", request.getHeader().getOrgId(), request.getProjectId(), System.currentTimeMillis(), activityLamb.getEndTime());
                throw new BrokerException(BrokerErrorCode.ORDER_FAILED);
            }
            BigDecimal purchaseTokenAmount = activityLamb.getMinPurchaseLimit().multiply(new BigDecimal(request.getAmount()));
            Long mainAccountId = accountService.getMainAccountId(request.getHeader());
            BigDecimal alreadyPurchase = userPurchaseAmount(request.getHeader().getOrgId(), request.getProjectId(), mainAccountId);
            // 如果用户限额 小于 已经购买 + 本次要购买 提示超过限额
            if (activityLamb.getUserLimit().compareTo(alreadyPurchase.add(purchaseTokenAmount)) == -1) {
                log.info("Activity LAMB new order error: user limit. userId: {}, alreadyPurchase: {}, purchaseTokenAmount -> {}, projectId: {}.", request.getHeader().getUserId(), alreadyPurchase, purchaseTokenAmount, request.getProjectId());
                throw new BrokerException(BrokerErrorCode.ORDER_OVER_USER_LIMIT);
            // 平台剩余可购买
            } else if (activityLamb.getPurchaseableQuantity().compareTo(purchaseTokenAmount) == -1) {
                log.info("Activity LAMB new order error: platform limit. userId: {}, alreadyPurchase: {}, purchaseTokenAmount -> {}, projectId: {}.", request.getHeader().getUserId(), alreadyPurchase, purchaseTokenAmount, request.getProjectId());
                throw new BrokerException(BrokerErrorCode.ORDER_OVER_PLATFORM_LIMIT);
            }

            // 2.创建订单（待支付）
            ActivityLambOrder order = initOrder(request, mainAccountId, purchaseTokenAmount);
            // 3.锁仓，成功后变更用户支付订单状态为支付成功
            Boolean lockBalance = lockBalance(request.getHeader().getOrgId(), mainAccountId, request.getClientOrderId(), activityLamb.getPurchaseTokenId(), purchaseTokenAmount);
            if (lockBalance) {
                //变更订单状态
                log.info("Activity LAMB new order success: orderId -> {}, purchaseTokenAmount -> {}.", order.getId(), purchaseTokenAmount);
                order.setStatus(ActivityLambOrder.STATUS_PAY_SUCCESS);
                order.setStatus(ActivityLambOrder.LOCKED_STATUS_SUCCESS);
                order.setPurchaseTime(System.currentTimeMillis());
                Boolean isOk = lambOrderMapper.updateByPrimaryKey(order) > 0 ? true: false;
                if (isOk) {
                    log.info("Activity LAMB new order success: update order info success orderId -> {}, purchaseTokenAmount -> {}.", order.getId(), purchaseTokenAmount);
                } else {
                    log.error("Activity LAMB new order success: update order info orderId -> {}, purchaseTokenAmount -> {}.", order.getId(), purchaseTokenAmount);
                }
                // 4.购买成功后 减除产品可购买限额 增加已售出数额
                //   4.1 减除产品可购买限额
                activityLamb.setPurchaseableQuantity(activityLamb.getPurchaseableQuantity().subtract(purchaseTokenAmount));
                //   4.2 增加已售出数额
                activityLamb.setSoldAmount(activityLamb.getSoldAmount().add(purchaseTokenAmount));
                activityLamb.setRealSoldAmount(activityLamb.getRealSoldAmount().add(purchaseTokenAmount));
                //   4.3 更新
                lambMapper.updateByPrimaryKey(activityLamb);
            } else {
                order.setStatus(ActivityLambOrder.STATUS_PAY_FAILED);
                order.setStatus(ActivityLambOrder.LOCKED_STATUS_FAILED);
                Boolean isOk = lambOrderMapper.updateByPrimaryKey(order) > 0 ? true: false;
                if (isOk) {
                    log.info("Activity LAMB new order error: update order info success orderId -> {}, purchaseTokenAmount -> {}.", order.getId(), purchaseTokenAmount);
                } else {
                    log.error("Activity LAMB new order error: update order info orderId -> {}, purchaseTokenAmount -> {}.", order.getId(), purchaseTokenAmount);
                }
                log.error("Activity LAMB new order: lock balance error orderId -> {}, purchaseTokenAmount -> {}.", order.getId(), purchaseTokenAmount);
                throw new BrokerException(BrokerErrorCode.ORDER_FAILED);
            }
        } else {
            log.error("Activity LAMB new order error: project not exist. org id -> {}, error projectId -> {}.", request.getHeader().getOrgId(), request.getProjectId());
            throw new BrokerException(BrokerErrorCode.ORDER_FAILED);
        }
        CreateLambOrderReply reply = CreateLambOrderReply.newBuilder()
                .build();
        return reply;
    }

    private Boolean lockBalance(Long brokerId, Long accountId, Long clientOrderId, String tokenId, BigDecimal lockAmount) {
        BaseRequest baseRequest = BaseRequest.newBuilder()
                .setOrganizationId(brokerId)
                .build();

        LockBalanceRequest request = LockBalanceRequest.newBuilder()
                .setBaseRequest(baseRequest)
                .setClientReqId(clientOrderId)
                .setAccountId(accountId)
                .setTokenId(tokenId)
                .setLockAmount(lockAmount.toPlainString())
                .build();

        LockBalanceReply reply = balanceService.lockBalance(request);
        if (reply.getCode() == LockBalanceReply.ReplyCode.SUCCESS) {
            return true;
        } else {
            log.error("Activity LAMB new order error: lock balance error. error code: {}.", reply.getCodeValue());
            return false;
        }
    }


    private ActivityLambOrder initOrder(CreateLambOrderRequest request, Long accountId, BigDecimal tokenAmount) {
        ActivityLambOrder order = new ActivityLambOrder();
        order.setBrokerId(request.getHeader().getOrgId());
        order.setClientOrderId(request.getClientOrderId());
        order.setAccountId(accountId);
        order.setProjectId(request.getProjectId());
        order.setAmount(tokenAmount);
        order.setTokenId(request.getTokenId());
        order.setStatus(ActivityLambOrder.STATUS_INIT);
        order.setLockedStatus(ActivityLambOrder.LOCKED_STATUS_INIT);
        order.setPurchaseTime(0L);
        order.setCreatedTime(System.currentTimeMillis());
        order.setUpdatedTime(System.currentTimeMillis());

        Boolean isOk = lambOrderMapper.insert(order) > 0 ? true: false;
        if (isOk) {
            return order;
        } else {
            throw new BrokerException(BrokerErrorCode.ORDER_FAILED);
        }
    }

    private BigDecimal userPurchaseAmount(Long brokerId, Long projectId, Long accountId) {
        List<ActivityLambOrder> activityLambOrders = listOrderInfo(brokerId, projectId, accountId);
        BigDecimal amount = BigDecimal.ZERO;
        for (ActivityLambOrder order: activityLambOrders) {
            amount = amount.add(order.getAmount());
        }
        return amount;
    }

    public ListOrderInfoReply listOrderInfo(ListOrderInfoRequest request) {
        ListOrderInfoReply.Builder builder = ListOrderInfoReply.newBuilder();
        List<ActivityLambOrder> activityLambOrders = listOrderInfo(request.getHeader().getOrgId(), null, accountService.getMainAccountId(request.getHeader()));
        if (!CollectionUtils.isEmpty(activityLambOrders)) {
            List<ActivityLamb> activityLambs = listProjectInfo(request.getHeader().getOrgId());
            Map<Long, ActivityLamb> activityLambMap = activityLambs.stream().collect(Collectors.toMap(ActivityLamb::getId, Function.identity()));
            List<LambOrderInfo> infos = new ArrayList<>();
            for (ActivityLambOrder order : activityLambOrders) {
                LambOrderInfo.Builder b = LambOrderInfo.newBuilder();
                b.setId(order.getId());
                b.setAccountId(order.getAccountId());
                b.setTokenId(order.getTokenId());
                b.setProjectId(order.getProjectId());
                b.setPurchaseTime(order.getPurchaseTime());
                b.setAmount(order.getAmount().toPlainString());

                ActivityLamb activityLamb = activityLambMap.get(order.getProjectId());
                if (Objects.nonNull(activityLamb)) {
                    if (LANGUAGE_ZH.equals(request.getHeader().getLanguage())) {
                        b.setProjectName(activityLamb.getProjectNameZh());
                    } else {
                        b.setProjectName(activityLamb.getProjectName());
                    }
                }
                infos.add(b.build());
            }
            builder.addAllOrderInfo(infos);

        }
        return builder.build();
    }

    private List<ActivityLambOrder> listOrderInfo(Long brokerId, Long projectId, Long accountId) {
        Example example = Example.builder(ActivityLambOrder.class).build();
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("brokerId", brokerId);
        if (!StringUtils.isEmpty(projectId)) {
            criteria.andEqualTo("projectId", projectId);
        }
        criteria.andEqualTo("accountId", accountId);
        criteria.andEqualTo("status", ActivityLambOrder.STATUS_PAY_SUCCESS);
        List<ActivityLambOrder> activityLambOrders = lambOrderMapper.selectByExample(example);
        return activityLambOrders;
    }

    public SettlementInterestReply settlementInterest(SettlementInterestRequest request) {
        return null;
    }


    public String activityExpiredUnlock(Long brokerId, Long projectId) {
        ActivityLamb lockInterest = lambMapper.getActivityLockInterestById(projectId);
        if (Objects.nonNull(lockInterest) && brokerId.equals(lockInterest.getBrokerId())) {
            if (System.currentTimeMillis() < lockInterest.getEndTime()) {
                log.error("Activity not end.");
                return "Activity not end.";
            } else {
                //获取购买成功活动订单信息
                List<ActivityLambOrder> activityLockInterestOrders = lambOrderMapper.queryOrderIdListByStatus(projectId, 1);
                activityLockInterestOrders.forEach(order -> {
                    if (order.getLockedStatus() == ActivityLambOrder.LOCKED_STATUS_SUCCESS) {
                        Boolean unlockSuccess = unLockBalance(order.getBrokerId(), order.getAccountId(), order.getClientOrderId(), order.getTokenId(), order.getAmount());
                        log.info("Activity Unlock: status: {}, brokerId: {}, accountId: {}, clientOrderId: {}, tokenId: {}, lockAmount: {}", unlockSuccess, order.getBrokerId(), order.getAccountId(), order.getClientOrderId(), order.getTokenId(), order.getAmount());
                        if (unlockSuccess) {
                            order.setLockedStatus(ActivityLambOrder.LOCKED_STATUS_UNLOCKED);
                            order.setUpdatedTime(System.currentTimeMillis());
                            Boolean updateSuccess = lambOrderMapper.updateByPrimaryKey(order) > 0? true: false;
                            if (!updateSuccess) {
                                log.error("Activity Unlock Success, update order status error: brokerId: {}, accountId: {}, clientOrderId: {}, tokenId: {}, lockAmount: {}", order.getBrokerId(), order.getAccountId(), order.getClientOrderId(), order.getTokenId(), order.getAmount());
                            }
                        }
                    }
                });
                return "success";
            }
        }
        log.error("Activity not exist.");
        return "Activity not exist.";
    }

    private Boolean unLockBalance(Long brokerId, Long accountId, Long clientOrderId, String tokenId, BigDecimal lockAmount) {
        BaseRequest baseRequest = BaseRequest.newBuilder()
                .setOrganizationId(brokerId)
                .build();

        UnlockBalanceRequest request = UnlockBalanceRequest.newBuilder()
                .setBaseRequest(baseRequest)
                .setClientReqId(clientOrderId)
                .setAccountId(accountId)
                .setTokenId(tokenId)
                .setUnlockAmount(lockAmount.toPlainString())
                .build();

        UnlockBalanceResponse reply = balanceService.unLockBalance(request);
        if (reply.getCode() == UnlockBalanceResponse.ReplyCode.SUCCESS) {
            return true;
        } else {
            log.error("Activity LOCK INTEREST: unlock balance error. brokerId: {}, clientOrderId: {}, error code: {}.",brokerId, clientOrderId, reply.getCodeValue());
            return false;
        }
    }
}
