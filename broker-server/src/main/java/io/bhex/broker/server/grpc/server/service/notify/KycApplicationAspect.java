package io.bhex.broker.server.grpc.server.service.notify;

import io.bhex.broker.grpc.common.Header;
import io.bhex.broker.grpc.user.PersonalVerifyResponse;
import io.bhex.broker.grpc.user.kyc.*;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.AfterReturning;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.springframework.stereotype.Service;

import java.util.Objects;

@Slf4j
@Aspect
@Service
public class KycApplicationAspect extends AbstractAspect {

    private final static String TAG = "kyc-application";


    @Pointcut("@annotation(io.bhex.broker.server.grpc.server.service.notify.KycApplicationNotify)")
    public void notifyKycApplication() {
    }


    @AfterReturning(pointcut = "notifyKycApplication()", returning = "retVal")
    public void afterReturningAdvice(JoinPoint jp, Object retVal) {
        log.info("Method Signature: " + jp.getSignature());
        log.info("Returning:" + retVal.toString());

        Object[] args = jp.getArgs();
        if (Objects.nonNull(args)) {
            int ret=-1;
            if(retVal instanceof PersonalVerifyResponse){
                PersonalVerifyResponse resp = (PersonalVerifyResponse) retVal;
                ret=resp.getBasicRet().getCode();
            }

            if(retVal instanceof PhotoKycVerifyResponse){
                PhotoKycVerifyResponse resp = (PhotoKycVerifyResponse) retVal;
                ret=resp.getRet();
            }

            if(retVal instanceof FaceKycVerifyResponse){
                FaceKycVerifyResponse resp = (FaceKycVerifyResponse) retVal;
                ret=resp.getRet();
            }

            if(retVal instanceof VideoKycVerifyResponse){
                VideoKycVerifyResponse resp = (VideoKycVerifyResponse) retVal;
                ret=resp.getRet();
            }

            //处理不成功，不处理
            if (ret != 0) {
                return;
            }

            Long brokerId = null;

            if (args[0] instanceof Header) {
                brokerId = ((Header) args[0]).getOrgId();
            } else if (args[0] instanceof PhotoKycVerifyRequest) {
                brokerId = ((PhotoKycVerifyRequest)args[0]).getHeader().getOrgId();
            } else if (args[0] instanceof FaceKycVerifyRequest) {
                brokerId = ((FaceKycVerifyRequest)args[0]).getHeader().getOrgId();
            } else if (args[0] instanceof  VideoKycVerifyRequest) {
                brokerId = ((VideoKycVerifyRequest)args[0]).getHeader().getOrgId();
            } else {
                log.warn("unkonwn type: {}", args[0].getClass().getName());
                return;
            }

            //String tag=KycApplicationAspect.TAG+"-"+runEnv;
            //log.info("runEnv={},topic={},tag={},brokerId={}",runEnv,AbstractAspect.TOPIC,tag,brokerId);
            //messageProducer.sendMessage(AbstractAspect.TOPIC,tag,brokerId);
            String key = buildKey(KycApplicationAspect.TAG, brokerId.toString());
            log.info("KycApplicationAspect,Notify key={}", key);
            redisTemplate.opsForValue().increment(key);
            cacheNotify(KycApplicationAspect.TAG, brokerId.toString());

        }
    }
}
