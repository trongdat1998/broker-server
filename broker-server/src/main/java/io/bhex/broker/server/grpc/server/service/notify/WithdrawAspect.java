package io.bhex.broker.server.grpc.server.service.notify;

import io.bhex.base.account.WithdrawalBrokerAuditEnum;
import io.bhex.broker.grpc.common.Header;
import io.bhex.broker.grpc.withdraw.OpenApiWithdrawResponse;
import io.bhex.broker.grpc.withdraw.OrgWithdrawResponse;
import io.bhex.broker.grpc.withdraw.WithdrawVerifyResponse;
import io.bhex.broker.server.primary.mapper.WithdrawOrderMapper;
import io.bhex.broker.server.model.WithdrawOrder;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.AfterReturning;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.springframework.stereotype.Service;
import tk.mybatis.mapper.entity.Example;

import javax.annotation.Resource;
import java.util.Objects;

@Slf4j
@Aspect
@Service
public class WithdrawAspect extends AbstractAspect{

    private final static String TAG="withdraw";


    @Resource
    private WithdrawOrderMapper withdrawOrderMapper;


    @Pointcut("@annotation(io.bhex.broker.server.grpc.server.service.notify.WithdrawNotify)")
    public void notifyWithdraw(){}

//    @Pointcut("@annotation(io.bhex.broker.server.grpc.server.service.notify.ApiWithdrawNotify)")
//    public void notifyApiWithdraw(){}
//
//
//    @AfterReturning(pointcut="notifyWithdraw()", returning="retVal")
//    public void afterReturningAdvice(JoinPoint jp, Object retVal){
//        log.info("WithdrawAspect,method signature: "  + jp.getSignature());
//        log.info("Returning:" + retVal.toString() );
//
//        Object[] args = jp.getArgs();
//        if(Objects.nonNull(args)){
//            WithdrawVerifyResponse resp=(WithdrawVerifyResponse)retVal;
//            //不成功，不处理
//            if(resp.getRet()!= 0){
//                return;
//            }
//            Header header = (Header)args[0];
//            Long brokerId = header.getOrgId();
//            Long userId = header.getUserId();
//            Long orderId = resp.getWithdrawOrderId();
//            sendNotify(brokerId, userId, orderId);
//        }
//    }

    @AfterReturning(pointcut="notifyWithdraw()", returning="retVal")
    public void afterReturningAdvice(JoinPoint jp, Object retVal){
        log.info("ApiWithdrawAspect,method signature: "  + jp.getSignature());
        log.info("Returning:" + retVal.toString() );

        Object[] args = jp.getArgs();
        if(Objects.isNull(args)){
            return;
        }

        Long orderId = 0L;
        if (retVal instanceof WithdrawVerifyResponse) {
            if(((WithdrawVerifyResponse) retVal).getRet()!= 0){
                return;
            }
            orderId = ((WithdrawVerifyResponse) retVal).getWithdrawOrderId();
        } else if (retVal instanceof OrgWithdrawResponse) {
            if(((OrgWithdrawResponse) retVal).getRet()!= 0){
                return;
            }
            orderId = ((OrgWithdrawResponse) retVal).getWithdrawOrderId();
        } else if (retVal instanceof OpenApiWithdrawResponse) {
            if(((OpenApiWithdrawResponse) retVal).getRet()!= 0){
                return;
            }
            orderId = ((OpenApiWithdrawResponse) retVal).getWithdrawOrderId();
        }

        for (Object arg : args) {
            if (arg instanceof Header) {
                Header header = (Header)arg;
                Long brokerId = header.getOrgId();
                Long userId = header.getUserId();
                sendNotify(brokerId, userId, orderId);
                break;
            }
        }

    }

    private void sendNotify(long brokerId, long userId, long orderId) {

        log.info("WithdrawAspect,orgId={},userId={},orderId={}", brokerId, userId, orderId);
        //查询提现订单是否是审核状态
        Example exp = new Example(WithdrawOrder.class);
        exp.createCriteria()
                .andEqualTo("orgId", brokerId)
                .andEqualTo("userId", userId)
                .andEqualTo("orderId", orderId)
                .andEqualTo("status", WithdrawalBrokerAuditEnum.BROKER_AUDITING.getNumber());


        WithdrawOrder order = withdrawOrderMapper.selectOneByExample(exp);
        if (Objects.isNull(order)) {
            log.info("WithdrawAspect,Hasn't any record");
            return;
        }

        String key = buildKey(WithdrawAspect.TAG, brokerId + "");
        log.info("WithdrawAspect,Notify key={}",key);
        redisTemplate.opsForValue().increment(key);
        cacheNotify(WithdrawAspect.TAG, brokerId + "");
    }
}
