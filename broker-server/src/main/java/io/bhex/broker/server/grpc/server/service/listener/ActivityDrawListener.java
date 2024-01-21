package io.bhex.broker.server.grpc.server.service.listener;

import io.bhex.broker.server.grpc.server.service.activity.ActivityDrawService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class ActivityDrawListener {

    @Autowired
    ActivityDrawService activityDrawService;

    @Async
    @EventListener
    public void sendTicket(ActivityDrawEvent event) {
        try {
            activityDrawService.sendActivityDrawTicket(event.getTicket());
        } catch (Exception e) {
            log.error(" sendTicket exception: ticket :{}", event.getTicket(), e);
        }
    }
}
