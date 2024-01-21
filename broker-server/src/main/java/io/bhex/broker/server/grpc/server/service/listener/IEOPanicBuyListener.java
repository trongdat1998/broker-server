package io.bhex.broker.server.grpc.server.service.listener;

import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

import io.bhex.broker.common.util.JsonUtil;
import io.bhex.broker.server.grpc.server.service.ActivityLockInterestService;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class IEOPanicBuyListener {

    @Resource
    private ActivityLockInterestService activityLockInterestService;

    @Async
    @EventListener
    public void invokeEvent(IEOPanicBuyEvent event) {
        try {
            //异步处理IEO抢购订单
            this.activityLockInterestService.panicBuyingAsynchronousTransfer(event.getId());
        } catch (Exception e) {
            log.error("IEOPanicBuyListener invokeEvent exception:{}", JsonUtil.defaultGson().toJson(event), e);
        }
    }
}
