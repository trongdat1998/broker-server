package io.bhex.broker.server.grpc.server.service;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import io.bhex.base.account.*;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.common.util.JsonUtil;
import io.bhex.broker.core.domain.BaseResult;
import io.bhex.broker.grpc.common.Header;
import io.bhex.broker.grpc.payment.*;
import io.bhex.broker.server.domain.AccountType;
import io.bhex.broker.server.domain.AuthType;
import io.bhex.broker.server.domain.VerifyCodeType;
import io.bhex.broker.server.grpc.client.service.GrpcPayService;
import io.bhex.broker.server.primary.mapper.PaymentOrderMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Service
public class PaymentService {

    @Resource
    private AccountService accountService;

    @Resource
    private GrpcPayService grpcPayService;

    @Resource
    private UserSecurityService userSecurityService;

    @Resource
    private PaymentOrderMapper paymentOrderMapper;

    public CreatePaymentOrderResponse createOrder(Header header, String clientOrderId, Long payerUserId, Long payeeUserId,
                                                  List<PayInfo> payInfoList, List<PayInfo> productInfoList, Integer expiredSeconds, String desc, String extendInfo) {
        try {
            List<OrderItem> orderItemList = payInfoList.stream().map(this::getOrderItem).collect(Collectors.toList());
            if (!productInfoList.isEmpty()) {
                orderItemList.addAll(productInfoList.stream().map(this::getOrderItem)
                        .map(orderItem -> orderItem.toBuilder().setType(PayType.PAY_COMMODITY).build())
                        .collect(Collectors.toList()));
            }
            CreateOrderRequest request = CreateOrderRequest.newBuilder()
                    .setClientOrderId(clientOrderId)
                    .setOrgId(header.getOrgId())
                    .setPayAccountId(accountService.getAccountId(header.getOrgId(), payerUserId, AccountType.MAIN))
                    .setReceiptAccountId(accountService.getAccountId(header.getOrgId(), payeeUserId, AccountType.MAIN))
                    .addAllItems(orderItemList)
                    .setEffectiveDuration(expiredSeconds)
                    .setDescribe(desc)
                    .setExtraMsg(extendInfo)
                    .build();
            CreateOrderReply response = grpcPayService.createPaymentOrder(request);
            io.bhex.broker.server.model.PaymentOrder paymentOrder = io.bhex.broker.server.model.PaymentOrder.builder()
                    .orgId(header.getOrgId())
                    .clientOrderId(clientOrderId)
                    .orderId(response.getOrder().getOrderId())
                    .payerUserId(payerUserId)
                    .payerAccountId(request.getPayAccountId())
                    .payeeUserId(payeeUserId)
                    .payeeAccountId(request.getReceiptAccountId())
                    .isMapping(productInfoList.isEmpty() ? 0 : 1)
                    .payInfo(JsonUtil.defaultGson().toJson(payInfoList))
                    .productInfo(JsonUtil.defaultGson().toJson(productInfoList))
                    .expiredTime(response.getOrder().getExpireTime())
                    .orderStatus(0) // 现在没什么意义
                    .desc(desc)
                    .extendInfo(extendInfo)
                    .created(response.getOrder().getCreatedAt())
                    .updated(response.getOrder().getUpdatedAt())
                    .build();
            try {
                io.bhex.broker.server.model.PaymentOrder queryObj = new io.bhex.broker.server.model.PaymentOrder();
                queryObj.setOrgId(header.getOrgId());
                queryObj.setClientOrderId(clientOrderId);
                io.bhex.broker.server.model.PaymentOrder existOrder = paymentOrderMapper.selectOne(queryObj);
                if (existOrder == null) {
                    paymentOrderMapper.insertSelective(paymentOrder);
                }
            } catch (Exception e) {
                log.error("!!! Verify Import! please execute insert. Table: tb_payment_order, Info:{}", JsonUtil.defaultGson().toJson(paymentOrder));
            }
            return CreatePaymentOrderResponse.newBuilder().setOrder(getPaymentOrder(response.getOrder())).build();
        } catch (BrokerException e) {
            return CreatePaymentOrderResponse.newBuilder().setRet(e.getCode()).build();
        }
    }

    public QueryPaymentOrderResponse queryOrders(Header header, Long payerUserId, Long payeeUserId, List<PaymentOrderStatus> orderStatusList,
                                                 Long fromId, Long endId, Long startTime, Long endTime, Integer limit) {
        List<PayOrderStatus> payOrderStatusList = Lists.newArrayList();
        if (orderStatusList.size() > 0) {
            PayOrderStatus payOrderStatus = getPayOrderStatus(orderStatusList.get(0));
            if (payOrderStatus != null) {
                payOrderStatusList.add(payOrderStatus);
            }
        }
        GetPayOrdersRequest request = GetPayOrdersRequest.newBuilder()
                .setOrgId(header.getOrgId())
                .setPayAccountId(payerUserId != 0 ? accountService.getAccountId(header.getOrgId(), payerUserId, AccountType.MAIN) : payerUserId)
                .setReceiptAccountId(payeeUserId != 0 ? accountService.getAccountId(header.getOrgId(), payeeUserId, AccountType.MAIN) : payeeUserId)
                .addAllStatus(payOrderStatusList)
                .setFromOrderId(fromId)
                .setEndOrderId(endId)
                .setStartTime(startTime)
                .setEndTime(endTime)
                .setLimit(limit)
                .build();
        GetPayOrdersReply response = grpcPayService.queryPaymentOrders(request);
        return QueryPaymentOrderResponse.newBuilder()
                .addAllOrders(response.getOrdersList().stream().map(this::getPaymentOrder).collect(Collectors.toList()))
                .build();
    }

    public GetPaymentOrderDetailResponse getPaymentOrder(Header header, Long orderId, String clientOrderId, Long payerUserId, Long payeeUserId) {
        try {
            if (payerUserId == 0L && payeeUserId == 0L) {
                io.bhex.broker.server.model.PaymentOrder paymentOrder = getLocalPaymentOrder(header, orderId, clientOrderId);
                payerUserId = paymentOrder.getPayerUserId();
                payeeUserId = paymentOrder.getPayeeUserId();
            }
            GetPayOrderRequest request = GetPayOrderRequest.newBuilder()
                    .setOrgId(header.getOrgId())
                    .setOrderId(orderId)
                    .setClientOrderId(clientOrderId)
                    .setPayAccountId(payerUserId != 0 ? accountService.getAccountId(header.getOrgId(), payerUserId, AccountType.MAIN) : payerUserId)
                    .setReceiptAccountId(payeeUserId != 0 ? accountService.getAccountId(header.getOrgId(), payeeUserId, AccountType.MAIN) : payeeUserId)
                    .build();
            GetPayOrderReply response = grpcPayService.getPaymentOrder(request);
            return GetPaymentOrderDetailResponse.newBuilder().setOrder(getPaymentOrder(response.getOrder())).build();
        } catch (BrokerException e) {
            return GetPaymentOrderDetailResponse.newBuilder().setRet(e.getCode()).build();
        }
    }

    public CheckOrderPayResponse checkOrderPay(Header header, Long orderId) {
        CheckQuickpassRequest request = CheckQuickpassRequest.newBuilder()
                .setOrderId(orderId)
                .setPayAccountId(accountService.getMainAccountId(header))
                .build();
        CheckQuickpassReply response = grpcPayService.checkOrderPay(request);
        return CheckOrderPayResponse.newBuilder().setNeed2Fa(!response.getCanQuickpass()).build();
    }

    public OrderPayResponse orderPay(Header header, Long orderId, Long payeeUserId, boolean check2FA,
                                     Integer authTypeValue, Long verifyCodeOrderId, String verifyCode) {
        try {
            if (header.getUserId() == 842145604923681280L) { //1.禁止这个子账户交易 7070的子账号
                return OrderPayResponse.newBuilder().setRet(BrokerErrorCode.FEATURE_SUSPENDED.code()).build();
            }
            if (check2FA) {
                AuthType authType = AuthType.fromValue(authTypeValue);
                if (authType == AuthType.GA) {
                    userSecurityService.validGACode(header, header.getUserId(), verifyCode);
                } else {
                    userSecurityService.validVerifyCode(header, authType, verifyCodeOrderId, verifyCode, VerifyCodeType.ORDER_PAY);
                }
            }
            // 这里可以查询本地订单，也可以查询bh订单数据，来获取receiptAccountId参数值
            io.bhex.broker.server.model.PaymentOrder paymentOrder = getLocalPaymentOrder(header, orderId, null);
            ConfirmPayRequest request = ConfirmPayRequest.newBuilder()
                    .setOrgId(header.getOrgId())
                    .setOrderId(orderId)
                    .setReceiptAccountId(paymentOrder.getPayeeAccountId())
                    .setIsQuickpass(!check2FA)
                    .build();
            ConfirmPayReply response = grpcPayService.orderPay(request);
            return OrderPayResponse.newBuilder().setOrder(getPaymentOrder(response.getOrder())).build();
        } catch (BrokerException e) {
            if (e.getCode() == BrokerErrorCode.PAYMENT_CANNOT_SKIP_2FA.code()) {
                return OrderPayResponse.newBuilder().setNeed2Fa(true).build();
            }
            return OrderPayResponse.newBuilder().setRet(e.getCode()).build();
        }
    }

    public OrderSettleResponse orderSettle(Header header, Long orderId, String clientOrderId, Long payeeUserId,
                                           String tokenId, String amount, String settleClientOrderId) {
        try {
            io.bhex.broker.server.model.PaymentOrder localPaymentOrder = getLocalPaymentOrder(header, orderId, clientOrderId);
            LockedSettleRequest request = LockedSettleRequest.newBuilder()
                    .setOrgId(header.getOrgId())
                    .setOrderId(localPaymentOrder.getOrderId())
                    .setReceiptAccountId(localPaymentOrder.getPayeeAccountId())
                    .setTokenId(tokenId)
                    .setAmount(amount)
                    .setClientReqId(settleClientOrderId)
                    .build();
            LockedSettleReply response = grpcPayService.orderSettle(request);
            return OrderSettleResponse.newBuilder().setOrder(getPaymentOrder(response.getOrder())).build();
        } catch (BrokerException e) {
            return OrderSettleResponse.newBuilder().setRet(e.getCode()).build();
        }
    }

    public OrderRefundResponse orderRefund(Header header, Long orderId, String clientOrderId, Long payeeUserId,
                                           Long itemId, String tokenId, String amount, String refundClientOrderId) {
        try {
            io.bhex.broker.server.model.PaymentOrder localPaymentOrder = getLocalPaymentOrder(header, orderId, clientOrderId);
            RefundRequest request = RefundRequest.newBuilder()
                    .setOrgId(header.getOrgId())
                    .setOrderId(localPaymentOrder.getOrderId())
                    .setReceiptAccountId(localPaymentOrder.getPayeeAccountId())
                    .setTokenId(tokenId)
                    .setAmount(amount)
                    .setClientReqId(refundClientOrderId)
                    .setUniqueItemId(itemId)
                    .build();
            RefundReply response = grpcPayService.orderRefund(request);
            return OrderRefundResponse.newBuilder().setOrder(getPaymentOrder(response.getOrder())).build();
        } catch (BrokerException e) {
            return OrderRefundResponse.newBuilder().setRet(e.getCode()).build();
        }
    }

    public OrderCancelResponse orderCancel(Header header, Long orderId, String clientOrderId, Long payeeUserId) {
        try {
            io.bhex.broker.server.model.PaymentOrder localPaymentOrder = getLocalPaymentOrder(header, orderId, clientOrderId);
            CancelPayOrderRequest request = CancelPayOrderRequest.newBuilder()
                    .setOrgId(header.getOrgId())
                    .setOrderId(localPaymentOrder.getOrderId())
                    .setReceiptAccountId(localPaymentOrder.getPayeeAccountId())
                    .build();
            CancelPayOrderReply response = grpcPayService.orderCancel(request);
            return OrderCancelResponse.newBuilder().setOrder(getPaymentOrder(response.getOrder())).build();
        } catch (BrokerException e) {
            return OrderCancelResponse.newBuilder().setRet(e.getCode()).build();
        }
    }

    public OrderUnlockResponse orderUnlock(Header header, Long orderId, String clientOrderId, Long payeeUserId,
                                           String tokenId, String amount, String unlockClientOrderId) {
        try {
            io.bhex.broker.server.model.PaymentOrder localPaymentOrder = getLocalPaymentOrder(header, orderId, clientOrderId);
            UnlockRequest request = UnlockRequest.newBuilder()
                    .setOrgId(header.getOrgId())
                    .setOrderId(localPaymentOrder.getOrderId())
                    .setReceiptAccountId(localPaymentOrder.getPayeeAccountId())
                    .setTokenId(tokenId)
                    .setAmount(amount)
                    .setClientReqId(unlockClientOrderId)
                    .build();
            UnlockReply response = grpcPayService.orderUnlock(request);
            return OrderUnlockResponse.newBuilder().setOrder(getPaymentOrder(response.getOrder())).build();
        } catch (BrokerException e) {
            return OrderUnlockResponse.newBuilder().setRet(e.getCode()).build();
        }
    }

    private OrderItem getOrderItem(PayInfo payInfo) {
        PayType payType = null;
        if (payInfo.getPayType() == PaymentPayType.PAY) {
            payType = PayType.GENERAL_PAY;
        } else if (payInfo.getPayType() == PaymentPayType.PREPAY) {
            payType = PayType.PAY_LOCK;
        }
        if (payType == null) {
            throw new BrokerException(BrokerErrorCode.PARAM_INVALID);
        }
        return OrderItem.newBuilder()
                .setType(payType)
                .setTokenId(payInfo.getTokenId())
                .setAmount(payInfo.getAmount())
                .setMarkedAmount(payInfo.getAmount())
                .build();
    }

    private PayInfo getPayInfo(OrderItem orderItem) {
        PaymentPayType payType = PaymentPayType.PAY_TYPE_UNKNOWN;
        if (orderItem.getType() == PayType.GENERAL_PAY) {
            payType = PaymentPayType.PAY;
        } else if (orderItem.getType() == PayType.PAY_LOCK) {
            payType = PaymentPayType.PREPAY;
        } else if (orderItem.getType() == PayType.PAY_COMMODITY) {
            payType = PaymentPayType.PAY;
        }
        return PayInfo.newBuilder()
                .setItemId(orderItem.getUniqueItemId())
                .setPayType(payType)
                .setTokenId(orderItem.getTokenId())
                .setAmount(orderItem.getAmount())
                .setPaidAmount(orderItem.getPaidAmount())
                .setRefundedAmount(orderItem.getRefundedAmount())
                .setUnlockedAmount(orderItem.getUnlockedAmount())
                .build();
    }

    private PaymentOrderStatus getOrderStatus(PayOrderDetail order) {
        if (order.getStatus() == PayOrderStatus.PAY_STATUS_NEW && order.getExpireTime() < System.currentTimeMillis()) {
            return PaymentOrderStatus.EXPIRED;
        }
        PayOrderStatus orderStatus = order.getStatus();
        if (orderStatus == PayOrderStatus.PAY_STATUS_NEW) {
            return PaymentOrderStatus.WAIT_FOR_PAYMENT;
        } else if (orderStatus == PayOrderStatus.PAY_STATUS_DEDUCTED) {
            return PaymentOrderStatus.PROCESSING;
        } else if (orderStatus == PayOrderStatus.PAY_STATUS_FINISHED) {
            return PaymentOrderStatus.COMPLETED;
        } else if (orderStatus == PayOrderStatus.PAY_STATUS_EXPIRED) {
            return PaymentOrderStatus.EXPIRED;
        } else if (orderStatus == PayOrderStatus.PAY_STATUS_CANCELLED) {
            return PaymentOrderStatus.CANCELLED;
        }
        return PaymentOrderStatus.WAIT_FOR_PAYMENT;
    }

    private PayOrderStatus getPayOrderStatus(PaymentOrderStatus orderStatus) {
        if (orderStatus == PaymentOrderStatus.WAIT_FOR_PAYMENT) {
            return PayOrderStatus.PAY_STATUS_NEW;
        } else if (orderStatus == PaymentOrderStatus.PROCESSING) {
            return PayOrderStatus.PAY_STATUS_DEDUCTED;
        } else if (orderStatus == PaymentOrderStatus.COMPLETED) {
            return PayOrderStatus.PAY_STATUS_FINISHED;
        } else if (orderStatus == PaymentOrderStatus.EXPIRED) {
            return PayOrderStatus.PAY_STATUS_EXPIRED;
        } else if (orderStatus == PaymentOrderStatus.CANCELLED) {
            return PayOrderStatus.PAY_STATUS_CANCELLED;
        }
        return null;
    }

    private PaymentOrder getPaymentOrder(PayOrderDetail order) {
        boolean isMapping = order.getItemsList().stream().anyMatch(item -> item.getType() == PayType.PAY_COMMODITY);
        List<PayInfo> payInfoList = Lists.newArrayList();
        List<PayInfo> productInfoList = Lists.newArrayList();
        for (OrderItem orderItem : order.getItemsList()) {
            if (orderItem.getType() == PayType.PAY_COMMODITY) {
                productInfoList.add(getPayInfo(orderItem));
            } else {
                payInfoList.add(getPayInfo(orderItem));
            }
        }
        return PaymentOrder.newBuilder()
                .setOrderId(order.getOrgId())
                .setClientOrderId(order.getClientOrderId())
                .setOrderId(order.getOrderId())
                .setPayerOrgId(order.getOrgId())
                .setPayerUserId(accountService.getUserId(order.getPayAccountId()))
                .setPayerAccountId(order.getPayAccountId())
                .setPayeeOrgId(order.getOrgId())
                .setPayeeUserId(accountService.getUserId(order.getReceiptAccountId()))
                .setPayeeAccountId(order.getReceiptAccountId())
                .setIsMapping(isMapping)
                .addAllPayInfo(payInfoList)
                .addAllProductInfo(productInfoList)
                .setPaymentEffectiveTime(order.getExpireTime())
                .setOrderStatus(getOrderStatus(order))
                .setDesc(order.getDescirbe())
                .setExtendInfo(order.getExtraMsg())
                .setCreated(order.getCreatedAt())
                .setUpdated(order.getUpdatedAt())
                .build();
    }

    private io.bhex.broker.server.model.PaymentOrder getLocalPaymentOrder(Header header, Long orderId, String clientOrderId) {
        io.bhex.broker.server.model.PaymentOrder queryObj = new io.bhex.broker.server.model.PaymentOrder();
        queryObj.setOrgId(header.getOrgId());
        if (orderId > 0) {
            queryObj.setOrderId(orderId);
        } else if (!Strings.isNullOrEmpty(clientOrderId)) {
            queryObj.setClientOrderId(clientOrderId);
        }
        io.bhex.broker.server.model.PaymentOrder paymentOrder = paymentOrderMapper.selectOne(queryObj);
        if (paymentOrder == null) {
            log.warn("cannot find payment order with orgId:{}-orderId:{}-clientOrderId:{}", header.getOrgId(), orderId, clientOrderId);
            throw new BrokerException(BrokerErrorCode.ORDER_NOT_FOUND);
        }
        return paymentOrder;
    }

}
