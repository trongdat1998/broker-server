package io.bhex.broker.server.grpc.server.service.notify;

import com.google.common.collect.Lists;
import io.bhex.broker.server.grpc.server.service.AccountService;
import io.bhex.broker.server.primary.mapper.UnconfirmOtcOrderMapper;
import io.bhex.broker.server.model.UnconfirmOtcOrder;
import io.bhex.ex.otc.*;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.AfterReturning;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.springframework.stereotype.Service;
import tk.mybatis.mapper.entity.Example;

import javax.annotation.Resource;
import java.util.List;
import java.util.Objects;


@Slf4j
@Aspect
@Service
public class OtcOrderHandleAspect {

    @Resource
    private UnconfirmOtcOrderMapper unconfirmOtcOrderMapper;

    @Resource
    private AccountService accountService;

    private List<OTCOrderHandleTypeEnum> expectHandle=
            Lists.newArrayList(OTCOrderHandleTypeEnum.PAY,OTCOrderHandleTypeEnum.FINISH,OTCOrderHandleTypeEnum.CANCEL,OTCOrderHandleTypeEnum.APPEAL);

    @Pointcut("execution(* io.bhex.broker.server.grpc.client.service.GrpcOtcService.handleOrder(..))")
    public void payOtcOrder(){}


    @AfterReturning(pointcut="payOtcOrder()", returning="retVal")
    public void afterReturningAdvice(JoinPoint jp, Object retVal){
        log.info("Method Signature: "  + jp.getSignature());
        log.info("Returning:" + retVal.toString() );

        Object[] args=jp.getArgs();
        OTCHandleOrderRequest req=(OTCHandleOrderRequest)args[0];
        OTCHandleOrderResponse resp=(OTCHandleOrderResponse)retVal;

        if(Objects.isNull(req) || resp.getResult()!= OTCResult.SUCCESS){
            return;
        }

        OTCOrderStatusEnum status=transformStatus(req.getType());
        //状态无法转换的操作不处理
        if(Objects.isNull(status)){
            return;
        }

        Long orgId=req.getBaseRequest().getOrgId();
        Long orderId=req.getOrderId();
        Long userId=req.getBaseRequest().getUserId();
        Long accountId=accountService.getAccountId(orgId,userId);

        UnconfirmOtcOrder uoo=findOrder(orgId,orderId,accountId,status);
        //数据不存在，并且不是支付操作，返回
        if(Objects.isNull(uoo) &&
                !expectHandle.contains(req.getType())){
            return;
        }

        Long now=System.currentTimeMillis();

        log.info("After proc otc order with unpayment status,orderId={},orgId={},opsType={},status={},exist={}",
                orderId,orgId,req.getType().name(),status.name(),Objects.nonNull(uoo));

        if(Objects.isNull(uoo) &&
                req.getType()==OTCOrderHandleTypeEnum.PAY){

            UnconfirmOtcOrder obj= UnconfirmOtcOrder.builder()
                    .orderId(orderId)
                    .orgId(orgId)
                    .status(status.getNumber())
                    .accountId(accountId)
                    .createDate(now)
                    .updateDate(now)
                    .build();

            int rows=unconfirmOtcOrderMapper.insertSelective(obj);
            log.info("Save success");
            return;

        }

        if(Objects.isNull(uoo)){
            return;
        }

        UnconfirmOtcOrder obj= UnconfirmOtcOrder.builder()
                .id(uoo.getId())
                .status(status.getNumber())
                .updateDate(now)
                .build();

        int rows=unconfirmOtcOrderMapper.updateByPrimaryKeySelective(obj);
        log.info("Update success");
        return;
    }

    private UnconfirmOtcOrder findOrder(Long orgId, Long orderId, Long accountId, OTCOrderStatusEnum status) {

        Example exp=new Example(UnconfirmOtcOrder.class);
        exp.createCriteria()
                .andEqualTo("orgId",orgId)
                .andEqualTo("orderId",orderId)
                .andEqualTo("accountId",accountId)
                .andEqualTo("status",status);

        return unconfirmOtcOrderMapper.selectOneByExample(exp);
    }

    private OTCOrderStatusEnum transformStatus(OTCOrderHandleTypeEnum type) {

        OTCOrderStatusEnum status=null;
        if(type==OTCOrderHandleTypeEnum.PAY){
            status=OTCOrderStatusEnum.OTC_ORDER_UNCONFIRM;
        }

        if(type==OTCOrderHandleTypeEnum.CANCEL){
            status=OTCOrderStatusEnum.OTC_ORDER_CANCEL;
        }

        if(type==OTCOrderHandleTypeEnum.FINISH){
            status=OTCOrderStatusEnum.OTC_ORDER_FINISH;
        }

        if(type==OTCOrderHandleTypeEnum.APPEAL){
            status=OTCOrderStatusEnum.OTC_ORDER_APPEAL;
        }

        return status;
    }

}
