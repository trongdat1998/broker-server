package io.bhex.broker.server.push.service;

import io.bhex.broker.server.model.AppDeliveryReport;
import io.bhex.broker.server.primary.mapper.AppDeliveryReportMapper;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

/**
 * @author wangsc
 * @description
 * @date 2020-07-30 10:53
 */
@Service
public class AppDeliveryReportService {
    @Resource
    private AppDeliveryReportMapper reportMapper;

    public void insertHuaweiPushDelivery(Long orgId, String reqOrderId, Long taskId, Long taskRound, String body) {
        AppDeliveryReport report = AppDeliveryReport.builder()
                .orgId(orgId)
                .reqOrderId(reqOrderId)
                .pushChannel("huawei")
                .taskId(taskId)
                .body(body)
                .created(System.currentTimeMillis())
                .build();
        reportMapper.insertSelective(report);
    }
}
