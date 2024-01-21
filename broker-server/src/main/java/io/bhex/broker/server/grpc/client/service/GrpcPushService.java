package io.bhex.broker.server.grpc.client.service;

import io.bhex.base.common.ApnsNotification;
import io.bhex.base.common.MessageReply;
import io.bhex.base.common.MessageServiceGrpc;
import io.bhex.base.common.SendBusinessPushRequest;
import io.bhex.base.common.SendPushRequest;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.common.grpc.client.annotation.PrometheusMetrics;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@PrometheusMetrics
public class GrpcPushService extends GrpcBaseService {

    public boolean sendApnsNotification(ApnsNotification request) {
        MessageServiceGrpc.MessageServiceBlockingStub stub = grpcClientConfig.messageServiceBlockingStub(request.getOrgId());
        try {
            return stub.sendApnsNotification(request).getSuccess();
        } catch (Exception e) {
            log.error("sendSmsNotice Exception", e);
            return false;
        }
    }

    /**
     * 发送业务推送(最大100一批)
     * @param request
     * @return
     */
    public boolean sendBusinessPush(SendBusinessPushRequest request) {
        MessageServiceGrpc.MessageServiceBlockingStub stub = grpcClientConfig.messageServiceBlockingStub(request.getOrgId());
        try {
            return stub.sendBusinessPush(request).getSuccess();
        } catch (Exception e) {
            log.error("sendBusinessPush Exception", e);
            return false;
        }
    }

    /**
     * 发送推送(最大100一批)
     * @param request
     * @return
     */
    public MessageReply sendPush(SendPushRequest request) {
        MessageServiceGrpc.MessageServiceBlockingStub stub = grpcClientConfig.messageServiceBlockingStub(request.getOrgId());
        try {
            return stub.sendPush(request);
        } catch (Exception e) {
            log.error("sendPush Exception!{},{}", request.getOrgId(), request.getReqOrderId(), e);
            throw new BrokerException(BrokerErrorCode.GRPC_SERVER_SYSTEM_ERROR);
        }
    }

}
