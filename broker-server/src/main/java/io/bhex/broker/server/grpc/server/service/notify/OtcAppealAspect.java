package io.bhex.broker.server.grpc.server.service.notify;

import com.google.protobuf.TextFormat;
import io.bhex.broker.server.grpc.client.service.GrpcOtcService;
import io.bhex.ex.otc.*;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.AfterReturning;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Objects;

@Slf4j
@Aspect
@Service
public class OtcAppealAspect extends AbstractAspect {

    private final static String TAG = "otc-appeal";

    @Resource
    private GrpcOtcService grpcOtcService;

    @Pointcut("@annotation(io.bhex.broker.server.grpc.server.service.notify.OtcAppealNotify)")
    public void notifyAppleal() {
    }


    @AfterReturning(pointcut = "notifyAppleal()", returning = "retVal")
    public void afterReturningAdvice(JoinPoint jp, Object retVal) {
        log.info("Method Signature: " + jp.getSignature());
        log.info("Returning:" + retVal.toString());

        Object[] args = jp.getArgs();
        if (Objects.nonNull(args)) {
            OTCHandleOrderRequest req = (OTCHandleOrderRequest) args[0];
            OTCOrderHandleTypeEnum handleType = req.getType();
            //非申诉订单，不处理
            if (handleType != OTCOrderHandleTypeEnum.APPEAL) {
                return;
            }

            log.info("Start OtcAppealAspect {}", TextFormat.shortDebugString(req));
            OTCHandleOrderResponse resp = (OTCHandleOrderResponse) retVal;
            //处理不成功，不处理
            if (resp.getResult() != OTCResult.SUCCESS) {
                log.info("orderId={},status={}",req.getOrderId(),resp.getResult().name());
                return;
            }

            Long brokerId = req.getBaseRequest().getOrgId();

            OTCGetOrderInfoRequest req2=OTCGetOrderInfoRequest.newBuilder()
                    .setAccountId(req.getAccountId())
                    .setOrderId(req.getOrderId())
                    .setBaseRequest(req.getBaseRequest())
                    .build();

            OTCGetOrderInfoResponse queryResp=grpcOtcService.getOrder(req2);
            if(queryResp.getResult()!=OTCResult.SUCCESS){
                log.info("Query order fail,orderId={},status={}",req.getOrderId(),queryResp.getResult().name());
                return;
            }

            OTCOrderDetail order=queryResp.getOrder();

            if(order.getOrderStatus()!=OTCOrderStatusEnum.OTC_ORDER_APPEAL){
                log.info("Wrong order status,orderId={},status={}",order.getOrderId(),order.getOrderStatus().name());
                return;
            }

/*            String tag=OtcAppealAspect.TAG+"-"+runEnv;
            log.info("runEnv={},topic={},tag={},brokerId={}",runEnv,AbstractAspect.TOPIC,tag,brokerId);
            messageProducer.sendMessage(AbstractAspect.TOPIC,tag,brokerId);*/

            String key = buildKey(OtcAppealAspect.TAG, brokerId.toString());
            log.info("OtcAppealAspect,Notify key={}", key);
            redisTemplate.opsForValue().increment(key);
            cacheNotify(OtcAppealAspect.TAG, brokerId.toString());

        }
    }

}
