package io.bhex.broker.server.grpc.client.service;

import io.bhex.base.common.MessageServiceGrpc;
import io.bhex.base.common.SendTemplateMessageRequest;
import io.bhex.broker.common.grpc.client.annotation.GrpcLog;
import io.bhex.broker.common.grpc.client.annotation.PrometheusMetrics;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@GrpcLog
@PrometheusMetrics
public class GrpcCommonServerService extends GrpcBaseService {

    public boolean sendTemplateMessage(SendTemplateMessageRequest request) {
        MessageServiceGrpc.MessageServiceBlockingStub stub = grpcClientConfig.messageServiceBlockingStub(request.getOrgId());
        try {
            return stub.sendTemplateMessage(request).getSuccess();
        } catch (Exception e) {
            log.error("sendTemplateMessage Exception", e);
            return false;
        }
    }

}
