package io.bhex.broker.server.grpc.client.service;

import com.google.common.base.Throwables;
import io.bhex.base.proto.TimeReply;
import io.bhex.base.proto.TimeRequest;
import io.bhex.base.proto.TimeServiceGrpc;
import io.bhex.broker.common.grpc.client.annotation.GrpcLog;
import io.bhex.broker.common.grpc.client.annotation.PrometheusMetrics;
import io.bhex.broker.server.util.BaseReqUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/**
 * Class GrpcTimeService for project broker
 * Author: Lion
 * Date:   2018/10/10
 * Time:   15:21
 **/

@Service
@Slf4j
@GrpcLog
@PrometheusMetrics
public class GrpcTimeService extends GrpcBaseService {

    public TimeReply syncTime(long clientTimeMisc, String loopMessage, Long orgId) {

        TimeServiceGrpc.TimeServiceBlockingStub stub = grpcClientConfig.timeServiceBlockingStub(orgId);
        TimeReply reply = null;

        try {
            long clientTime = clientTimeMisc;

            TimeRequest request = TimeRequest.newBuilder()
                    .setBaseRequest(BaseReqUtil.getBaseRequest(orgId))
                    .setClientTimeMillis(clientTime)
                    .setLoopMessage(loopMessage)
                    .build();

            reply = stub.syncTime(request);


        } catch (Exception e) {
            log.info("Time service occured error {}", Throwables.getStackTraceAsString(e));
        }

        return reply;
    }
}
