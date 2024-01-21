/**********************************
 * @项目名称: trade
 * @文件名称: TimeServiceGrpcImpl
 * @Date 2018/7/18
 * @Author fulintang
 * @Copyright（C）: 2018 BlueHelix Inc. All rights reserved.
 * 注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/


package io.bhex.broker.server.grpc.server;

import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.broker.grpc.time.TimeReply;
import io.bhex.broker.grpc.time.TimeRequest;
import io.bhex.broker.grpc.time.TimeServiceGrpc;
import io.bhex.broker.server.grpc.client.service.GrpcTimeService;
import io.bhex.broker.server.util.BaseReqUtil;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Resource;
import java.text.SimpleDateFormat;
import java.util.Date;

@GrpcService(applyGlobalInterceptors = false)
@Slf4j
public class TimeServiceGrpcImpl extends TimeServiceGrpc.TimeServiceImplBase {

    @Resource
    private GrpcTimeService timeService;

    @Override
    public void syncTime(TimeRequest request, StreamObserver<TimeReply> responseObserver) {
        long enter = System.currentTimeMillis();
        SimpleDateFormat formatter = new SimpleDateFormat("dd-MMM-yyyy HH:mm:ss:SSS");
        String enterString = formatter.format(new Date(enter));
        //print request
        Long orgId = BaseReqUtil.getOrgIdByHeader(request.getHeader());
        io.bhex.base.proto.TimeReply baseReply = timeService.syncTime(enter, request.getLoopMessage(), orgId);

        //print respsonse
        long leave = System.currentTimeMillis();
        String leaveString = formatter.format(new Date(leave));
        long distance = leave - enter;

        String message = String.format("[BROKER SERVER] from [%s] to [%s], spend in[%d] ms\n", enterString, leaveString, distance);
        log.info(message);


        TimeReply reply = TimeReply.newBuilder().setServerTimeMillis(leave).setLoopMessage(message + baseReply.getLoopMessage()).build();

        responseObserver.onNext(reply);
        responseObserver.onCompleted();

    }

}
