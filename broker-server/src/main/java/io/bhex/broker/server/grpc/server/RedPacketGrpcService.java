package io.bhex.broker.server.grpc.server;

import com.google.common.base.Strings;
import com.google.protobuf.InvalidProtocolBufferException;
import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.base.grpc.server.interceptor.GrpcServerLogInterceptor;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.common.util.JsonUtil;
import io.bhex.broker.core.domain.BaseResult;
import io.bhex.broker.grpc.common.BasicRet;
import io.bhex.broker.grpc.red_packet.*;
import io.bhex.broker.server.grpc.server.service.RedPacketService;
import io.bhex.broker.server.model.RedPacket;
import io.bhex.broker.server.model.RedPacketReceiveDetail;
import io.bhex.broker.server.model.RedPacketTheme;
import io.bhex.broker.server.model.RedPacketTokenConfig;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.util.List;
import java.util.stream.Collectors;

@GrpcService(interceptors = GrpcServerLogInterceptor.class)
@Slf4j
public class RedPacketGrpcService extends RedPacketServiceGrpc.RedPacketServiceImplBase {

    @Resource
    private RedPacketService redPacketService;

    @Override
    public void queryRedPacketConfig(QueryRedPacketConfigRequest request, StreamObserver<QueryRedPacketConfigResponse> observer) {
        try {
            RedPacketConfig redPacketConfig = redPacketService.queryRedPacketConfig(request.getHeader());
            QueryRedPacketConfigResponse response = QueryRedPacketConfigResponse.newBuilder()
                    .setRedPacketConfig(redPacketConfig)
                    .build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("queryRedPacketConfig error", e);
            observer.onError(e);
        }
    }

    @Override
    public void queryRedPacketTheme(QueryRedPacketThemeRequest request, StreamObserver<QueryRedPacketThemeResponse> observer) {
        try {
            List<RedPacketTheme> themes = redPacketService.queryOrgRedPacketTheme(request.getHeader());
            QueryRedPacketThemeResponse response = QueryRedPacketThemeResponse.newBuilder()
                    .addAllTheme(themes.stream().map(RedPacketTheme::convertGrpcObj).collect(Collectors.toList()))
                    .build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("queryRedPacketTheme error", e);
            observer.onError(e);
        }
    }

    @Override
    public void queryRedPacketTokenConfig(QueryRedPacketTokenConfigRequest request, StreamObserver<QueryRedPacketTokenConfigResponse> observer) {
        try {
            List<RedPacketTokenConfig> tokenConfigs = redPacketService.queryOrgRedPacketTokenConfig(request.getHeader());
            QueryRedPacketTokenConfigResponse response = QueryRedPacketTokenConfigResponse.newBuilder()
                    .addAllTokenConfig(tokenConfigs.stream().map(RedPacketTokenConfig::convertGrpcObj).collect(Collectors.toList()))
                    .build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("queryRedPacketTokenConfig error", e);
            observer.onError(e);
        }
    }

    @Override
    public void sendRedPacket(SendRedPacketRequest request, StreamObserver<SendRedPacketResponse> observer) {
        SendRedPacketResponse response;
        try {
            BaseResult<RedPacket> sendResult = redPacketService.sendRedPacket(request.getHeader(), request.getRedPacketTypeValue(), request.getThemeId(), request.getNeedPassword(),
                    request.getTokenId(), request.getTotalCount(), new BigDecimal(request.getAmount()), new BigDecimal(request.getTotalAmount()),
                    request.getReceiveUserTypeValue(), request.getTradePassword());
            if (sendResult.isSuccess()) {
                response = SendRedPacketResponse.newBuilder()
                        .setRedPacket(sendResult.getData().convertGrpcObj())
                        .build();
            } else {
                response = SendRedPacketResponse.newBuilder()
                        .setRet(sendResult.getCode())
                        .setBasicRet(BasicRet.newBuilder().setCode(sendResult.getCode()).build())
                        .build();
            }
        } catch (BrokerException e) {
            response = SendRedPacketResponse.newBuilder()
                    .setRet(e.getCode())
                    .setBasicRet(BasicRet.newBuilder().setCode(e.getCode()).build())
                    .build();
        } catch (Exception e) {
            try {
                log.error("sendRedPacket error, request:{}", JsonUtil.defaultProtobufJsonPrinter().print(request), e);
            } catch (InvalidProtocolBufferException invalidProtocolBufferException) {
                //
            }
            response = SendRedPacketResponse.newBuilder()
                    .setRet(BrokerErrorCode.RED_PACKET_SEND_FAILED.code())
                    .setBasicRet(BasicRet.newBuilder().setCode(BrokerErrorCode.RED_PACKET_SEND_FAILED.code()).build())
                    .build();
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    @Override
    public void openPasswordRedPacket(OpenPasswordRedPacketRequest request, StreamObserver<OpenRedPacketResponse> observer) {
        OpenRedPacketResponse response;
        try {
            BaseResult<RedPacketReceiveDetail> openResult = redPacketService.openPasswordRedPacket(request.getHeader(),
                    Strings.nullToEmpty(request.getPassword()).toUpperCase(), request.getPassedGeeTestCheck());
            if (openResult.isSuccess()) {
                response = OpenRedPacketResponse.newBuilder()
                        .setReceiveDetail(openResult.getData().convertGrpcObj())
                        .setFirstOpen(!openResult.getData().getHasBeenOpened())
                        .build();
            } else {
                response = OpenRedPacketResponse.newBuilder()
                        .setRet(openResult.getCode())
                        .setBasicRet(BasicRet.newBuilder().setCode(openResult.getCode()).build())
                        .build();
            }
        } catch (BrokerException e) {
            response = OpenRedPacketResponse.newBuilder()
                    .setRet(e.getCode())
                    .setBasicRet(BasicRet.newBuilder().setCode(e.getCode()).build())
                    .build();
        } catch (Exception e) {
            try {
                log.error("openRedPacket error, request:{}", JsonUtil.defaultProtobufJsonPrinter().print(request), e);
            } catch (InvalidProtocolBufferException invalidProtocolBufferException) {
                //
            }
            response = OpenRedPacketResponse.newBuilder()
                    .setRet(BrokerErrorCode.RED_PACKET_RECEIVER_FAILED.code())
                    .setBasicRet(BasicRet.newBuilder().setCode(BrokerErrorCode.RED_PACKET_RECEIVER_FAILED.code()).build())
                    .build();
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    @Override
    public void queryRedPacketDetail(QueryRedPacketDetailRequest request, StreamObserver<QueryRedPacketDetailResponse> observer) {
        QueryRedPacketDetailResponse response;
        try {
            RedPacket redPacket = redPacketService.getRedPacketForDetail(request.getHeader(), request.getRedPacketId());
            if (redPacket == null) {
                response = QueryRedPacketDetailResponse.newBuilder()
                        .setRet(BrokerErrorCode.RED_PACKET_NOT_FOUND.code())
                        .setBasicRet(BasicRet.newBuilder().setCode(BrokerErrorCode.RED_PACKET_NOT_FOUND.code()).build())
                        .build();
            } else {
                List<RedPacketReceiveDetail> receiveDetailList = redPacketService.queryRedPacketReceiveDetail(request.getHeader(), request.getRedPacketId(), request.getFromId(), request.getLimit());
                response = QueryRedPacketDetailResponse.newBuilder()
                        .setRedPacket(redPacket.convertGrpcObj())
                        .addAllReceiveDetail(receiveDetailList.stream().map(RedPacketReceiveDetail::convertGrpcObj).collect(Collectors.toList()))
                        .build();
            }
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = QueryRedPacketDetailResponse.newBuilder()
                    .setRet(e.getCode())
                    .setBasicRet(BasicRet.newBuilder().setCode(e.getCode()).build())
                    .build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            try {
                log.error("queryRedPacketDetail error, request:{}", JsonUtil.defaultProtobufJsonPrinter().print(request), e);
            } catch (InvalidProtocolBufferException invalidProtocolBufferException) {
                // ignore
            }
            observer.onError(e);
        }
    }

    @Override
    public void queryMySend(QueryMySendRequest request, StreamObserver<QueryMySendResponse> observer) {
        QueryMySendResponse response;
        try {
            List<RedPacket> redPackets = redPacketService.queryRedPacketSendFromMe(request.getHeader(), request.getFromId(), request.getLimit());
            response = QueryMySendResponse.newBuilder()
                    .addAllRedPacket(redPackets.stream().map(RedPacket::convertGrpcObj).collect(Collectors.toList()))
                    .build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = QueryMySendResponse.newBuilder()
                    .setRet(e.getCode())
                    .setBasicRet(BasicRet.newBuilder().setCode(e.getCode()).build())
                    .build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            try {
                log.error("queryMySend error, request:{}", JsonUtil.defaultProtobufJsonPrinter().print(request), e);
            } catch (InvalidProtocolBufferException invalidProtocolBufferException) {
                // ignore
            }
            observer.onError(e);
        }
    }

    @Override
    public void queryMyReceive(QueryMyReceiveRequest request, StreamObserver<QueryMyReceiveResponse> observer) {
        QueryMyReceiveResponse response;
        try {
            List<RedPacketReceiveDetail> receiveDetails = redPacketService.queryMyReceivedRedPacket(request.getHeader(), request.getFromId(), request.getLimit());
            response = QueryMyReceiveResponse.newBuilder()
                    .addAllReceiveDetail(receiveDetails.stream().map(RedPacketReceiveDetail::convertGrpcObj).collect(Collectors.toList()))
                    .build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = QueryMyReceiveResponse.newBuilder()
                    .setRet(e.getCode())
                    .setBasicRet(BasicRet.newBuilder().setCode(e.getCode()).build())
                    .build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            try {
                log.error("queryMyReceive error, request:{}", JsonUtil.defaultProtobufJsonPrinter().print(request), e);
            } catch (InvalidProtocolBufferException invalidProtocolBufferException) {
                // ignore
            }
            observer.onError(e);
        }
    }
}
