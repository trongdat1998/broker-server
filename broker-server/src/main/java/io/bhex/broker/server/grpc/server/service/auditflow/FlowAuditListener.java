package io.bhex.broker.server.grpc.server.service.auditflow;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

/**
 * receive audit record request
 *
 * @author songxd
 * @date 2021-01-13
 */
@Slf4j
@Component
public class FlowAuditListener implements ApplicationListener<FlowAuditEvent> {

    @Resource
    private FlowAuditService flowAuditService;


    /**
     * receive audit record data insert db
     *
     * @param event
     */
    @Async
    @Override
    public void onApplicationEvent(FlowAuditEvent event) {
        try {
            flowAuditService.syncAuditBizRecord(event);
        } catch (Exception e) {
            log.error("FlowAuditListener exception:{} event :{}", e, event.toString());
        }
    }
}
