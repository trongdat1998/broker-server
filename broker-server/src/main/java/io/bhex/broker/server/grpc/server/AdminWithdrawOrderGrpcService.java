package io.bhex.broker.server.grpc.server;

import io.bhex.base.account.WithdrawalBrokerAuditEnum;
import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.broker.grpc.admin.*;
import io.bhex.broker.server.grpc.server.service.WithdrawOrderService;
import io.bhex.broker.server.model.WithdrawOrder;
import io.bhex.broker.server.util.BeanCopyUtils;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.CollectionUtils;

import javax.annotation.Resource;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @Description: 提现订单grpc
 * @Date: 2018/9/19 下午4:49
 * @Author: liwei
 * @Copyright（C）: 2018 BlueHelix Inc. All rights reserved.
 */

@Slf4j
@GrpcService
public class AdminWithdrawOrderGrpcService extends AdminWithdrawOrderServiceGrpc.AdminWithdrawOrderServiceImplBase {

    @Resource
    private WithdrawOrderService withdrawOrderService;

    @Override
    public void queryUnverfiedOrders(QueryUnverfiedOrdersRequest request, StreamObserver<QueryUnverfiedOrdersResponse> observer) {
        List<WithdrawOrder> orders = withdrawOrderService.queryOrders(request.getBrokerId(), request.getAccountId(), request.getFromId(), request.getEndId(),
                request.getTokenId(), request.getLimit(), WithdrawalBrokerAuditEnum.BROKER_AUDITING_VALUE);
        if (CollectionUtils.isEmpty(orders)) {
            observer.onNext(QueryUnverfiedOrdersResponse.newBuilder().build());
            observer.onCompleted();
            return;
        }

        List<UnverfiedWithdrawOrder> grpcOrders = orders.stream().map(order -> {
            UnverfiedWithdrawOrder.Builder builder = UnverfiedWithdrawOrder.newBuilder();
            BeanCopyUtils.copyPropertiesIgnoreNull(order, builder);
            builder.setQuantity(order.getQuantity() + "");
            builder.setArrivalQuantity(order.getArrivalQuantity() + "");
            return builder.build();
        }).collect(Collectors.toList());


        QueryUnverfiedOrdersResponse reply = QueryUnverfiedOrdersResponse.newBuilder()
                .setRet(0)
                .addAllOrders(grpcOrders)
                .build();

        observer.onNext(reply);
        observer.onCompleted();

    }

    @Override
    public void verifyOrder(VerifyOrderRequest request, StreamObserver<VerifyOrderResponse> observer) {
        WithdrawOrder withdrawOrder = withdrawOrderService.queryOrderByBhOrderId(request.getWithdrawOrderId());
        if (withdrawOrder == null || withdrawOrder.getUserId() != request.getUserId()) {
            log.warn("request parameter error,{} withdraw order not exitsed", request.getWithdrawOrderId());
            observer.onNext(VerifyOrderResponse.newBuilder().setRet(1).setMsg("not.found").build());
            observer.onCompleted();
            return;
        }

        if (withdrawOrder.getStatus() == WithdrawalBrokerAuditEnum.PASS_VALUE
                || withdrawOrder.getStatus() == WithdrawalBrokerAuditEnum.NO_PASS_VALUE) {
            observer.onNext(VerifyOrderResponse.newBuilder().setRet(1).setMsg("has.verified").build());
            observer.onCompleted();
            return;
        }

        withdrawOrderService.addVerifyHistory(request.getBrokerId(), withdrawOrder.getUserId(), withdrawOrder.getAccountId(), withdrawOrder.getId(),
                request.getAdminUserName(), request.getRemark(), request.getVerifyPassed(), request.getFailedReason(), request.getRefuseReason());
        withdrawOrder.setFailedReason(request.getFailedReason());
        withdrawOrderService.sendVerifyNotify(withdrawOrder);

        observer.onNext(VerifyOrderResponse.newBuilder().build());
        observer.onCompleted();

    }

    @Override
    public void queryVerfiedOrders(QueryVerfiedOrdersRequest request, StreamObserver<QueryVerfiedOrdersResponse> observer) {
        List<VerfiedWithdrawOrder> orders = withdrawOrderService.queryVerfiedOrders(request.getBrokerId(), request.getAccountId(), request.getFromId(), request.getEndId(),
                request.getTokenId(), request.getLimit(), request.getBhWithdrawId());

        observer.onNext(QueryVerfiedOrdersResponse.newBuilder().setRet(0).addAllOrders(orders).build());
        observer.onCompleted();
    }

    @Override
    public void queryWithdrawOrder(QueryWithdrawOrderRequest request, StreamObserver<QueryWithdrawOrderResponse> observer) {
        WithdrawOrder order = withdrawOrderService.queryOrderByBhOrderId(request.getWithdrawOrderId());
        if (order == null) {
            observer.onNext(QueryWithdrawOrderResponse.newBuilder().build());
            observer.onCompleted();
            return;
        }

        QueryWithdrawOrderResponse.Builder builder = QueryWithdrawOrderResponse.newBuilder();
        BeanCopyUtils.copyPropertiesIgnoreNull(order, builder);
        builder.setQuantity(order.getQuantity() + "");
        builder.setArrivalQuantity(order.getArrivalQuantity() + "");
        builder.setBhOrderId(order.getOrderId());
        builder.setVerifyStatus(order.getStatus());

        observer.onNext(builder.build());
        observer.onCompleted();

    }
}
