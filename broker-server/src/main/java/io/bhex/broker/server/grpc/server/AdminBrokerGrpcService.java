package io.bhex.broker.server.grpc.server;

import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.common.grpc.client.annotation.GrpcLog;
import io.bhex.broker.grpc.admin.*;
import io.bhex.broker.grpc.common.BasicRet;
import io.bhex.broker.server.domain.FunctionModule;
import io.bhex.broker.server.grpc.server.service.BrokerService;
import io.bhex.broker.server.model.Broker;
import io.bhex.broker.server.util.BeanCopyUtils;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.grpc.server.service
 * @Author: ming.xu
 * @CreateDate: 16/09/2018 4:23 PM
 * @Copyright（C）: 2018 BHEX Inc. All rights reserved.
 */
@Slf4j
@GrpcLog
@GrpcService
public class AdminBrokerGrpcService extends AdminBrokerServiceGrpc.AdminBrokerServiceImplBase {

    @Autowired
    private BrokerService brokerService;

    @Override
    public void createBroker(CreateBrokerRequest request, StreamObserver<CreateBrokerReply> responseObserver) {
        Broker broker = new Broker();
        broker.setOrgId(request.getOrgId());
        broker.setBrokerName(request.getBrokerName());
        broker.setApiDomain(request.getApiDomain());
        broker.setPrivateKey(request.getPrivateKey());
        broker.setPublicKey(request.getPublicKey());
        broker.setStatus(0);
        broker.setCreated(System.currentTimeMillis());
        broker.setSignName("");
        broker.setDomainRandomKey(RandomStringUtils.randomAlphabetic(32));
        // TODO: getFromAdmin
        broker.setFunctions(FunctionModule.getDefaultFunctionConfig());
        broker.setAppRequestSignSalt("bhex.com");
        broker.setKeyVersion(1);

        Boolean isOk = brokerService.createBroker(broker);

        // init broker data

        CreateBrokerReply reply = CreateBrokerReply.newBuilder()
                .setResult(isOk)
                .build();
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void updateBroker(UpdateBrokerRequest request, StreamObserver<UpdateBrokerReply> responseObserver) {
        Broker broker = brokerService.getBrokerByOrgId(request.getOrgId());
        if (!request.getBrokerName().equals("") && !request.getBrokerName().equals(broker.getBrokerName())) {
            broker.setBrokerName(request.getBrokerName());
        }
        if (!request.getApiDomain().equals("") && !request.getApiDomain().equals(broker.getApiDomain())) {
            broker.setApiDomain(request.getApiDomain());
        }
        if (StringUtils.isNoneBlank(request.getRealtimeInterval())) {
            //具体值broker-admin控制
            broker.setRealtimeInterval(request.getRealtimeInterval());
        }
//        if (!request.getFunctions().equals("") && !request.getFunctions().equals(broker.getFunctions())) {
//            broker.setFunctions(request.getFunctions());
//        }
//        if (!request.getSupportLanguages().equals("") && !request.getSupportLanguages().equals(broker.getSupportLanguages())) {
//            broker.setSupportLanguages(request.getSupportLanguages());
//        }

        Boolean result = brokerService.updateBroker(broker);
        UpdateBrokerReply reply = UpdateBrokerReply.newBuilder().setOrgId(request.getOrgId()).setResult(result).build();
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void getBrokerByBrokerId(GetBrokerByBrokerIdRequest request, StreamObserver<BrokerDetail> responseObserver) {
        Broker broker = brokerService.getBrokerByOrgId(request.getBrokerId());
        if (broker == null) {
            responseObserver.onNext(BrokerDetail.newBuilder().build());
            responseObserver.onCompleted();
            return;
        }
        BrokerDetail.Builder builder = BrokerDetail.newBuilder();
        BeanCopyUtils.copyPropertiesIgnoreNull(broker, builder);
        builder.setCanCreateOrgApi(broker.getOrgApi() == 1);
        builder.setLoginForceNeed2Fa(broker.getLoginNeed2fa() == 1);
        builder.setFilterTopBaseToken(broker.getFilterTopBaseToken() != null && broker.getFilterTopBaseToken() == 1);
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void enableBroker(EnableBrokerRequest request, StreamObserver<EnableBrokerReply> responseObserver) {
        Broker broker = brokerService.getBrokerByOrgId(request.getBrokerId());
        broker.setStatus(request.getEnabled() ? 1 : 0);
        brokerService.updateBroker(broker);
        responseObserver.onNext(EnableBrokerReply.newBuilder().setResult(true).build());
        responseObserver.onCompleted();
    }

    @Override
    public void updateBrokeSignName(UpdateBrokeSignNameRequest request, StreamObserver<UpdateBrokeSignNameReply> responseObserver) {
        Broker broker = brokerService.getBrokerByOrgId(request.getBrokerId());
        broker.setSignName(request.getSignName());
        brokerService.updateBroker(broker);
        responseObserver.onNext(UpdateBrokeSignNameReply.newBuilder().setResult(true).build());
        responseObserver.onCompleted();
    }

    @Override
    public void updateBrokerFilterTopBaseToken(UpdateBrokerFilterTopBaseTokenRequest request, StreamObserver<UpdateBrokerFilterTopBaseTokenReply> responseObserver) {
        Broker broker = brokerService.getBrokerByOrgId(request.getBrokerId());
        broker.setFilterTopBaseToken(request.getFilterTopBaseToken() ? 1 : 0);
        responseObserver.onNext(UpdateBrokerFilterTopBaseTokenReply.newBuilder().setResult(brokerService.updateBroker(broker)).build());
        responseObserver.onCompleted();
    }

    @Deprecated
    @Override
    public void saveBrokerExt(SaveBrokerExtRequest request, StreamObserver<BasicRet> responseObserver) {
        BasicRet resp = null;
        try {
            resp = brokerService.saveBrokerExt(request);
            responseObserver.onNext(resp);
        } catch (BrokerException e) {
            log.error(e.getMessage(), e);
            resp = BasicRet.newBuilder().setCode(e.getCode()).build();
            responseObserver.onNext(resp);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        }

        responseObserver.onCompleted();
    }

    @Deprecated
    @Override
    public void getBrokerExt(GetBrokerByBrokerIdRequest request, StreamObserver<BrokerExtResponse> responseObserver) {

        BrokerExtResponse resp = null;
        try {
            resp = brokerService.getBrokerExt(request);
            responseObserver.onNext(resp);
        } catch (BrokerException e) {
            log.error(e.getMessage(), e);
            resp = BrokerExtResponse.newBuilder().setRet(BasicRet.newBuilder().setCode(e.getCode()).build()).build();
            responseObserver.onNext(resp);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        }

        responseObserver.onCompleted();
    }

    @Override
    public void updateBrokerFunctionAndLanguage(UpdateBrokerRequest request, StreamObserver<UpdateBrokerReply> responseObserver) {
        Boolean result = brokerService.updateBrokerFunctionAndLanguage(request.getOrgId(), request.getFunctions(), request.getSupportLanguages());
        UpdateBrokerReply reply = UpdateBrokerReply.newBuilder().setOrgId(request.getOrgId()).setResult(result).build();
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }
}
