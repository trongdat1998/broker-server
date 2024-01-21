package io.bhex.broker.server.grpc.server;

import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.grpc.gateway.*;
import io.bhex.broker.server.grpc.server.service.BrokerQuoteService;
import io.bhex.broker.server.model.RealQuoteEngineAddress;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Resource;

/**
 * @author wangsc
 * @description 获取quote有关信息
 * @date 2020-07-02 14:42
 */
@Slf4j
@GrpcService
public class QuoteGrpcService extends QuoteServiceGrpc.QuoteServiceImplBase {

    @Resource
    private BrokerQuoteService quoteService;

    @Override
    public void getRealQuoteEngineAddress(GetRealQuoteEngineAddressRequest request, StreamObserver<GetRealQuoteEngineAddressResponse> responseObserver) {
        GetRealQuoteEngineAddressResponse response;
        try {
            RealQuoteEngineAddress realQuoteEngineAddress = quoteService.getRealQuoteEngineAddress(request.getPlatform(), request.getAddress().getHost(), request.getAddress().getPort());
            response = GetRealQuoteEngineAddressResponse.newBuilder()
                    .setAddress(getRealQuoteEngineAddress(realQuoteEngineAddress))
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = GetRealQuoteEngineAddressResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }

    private io.bhex.broker.grpc.gateway.RealQuoteEngineAddress getRealQuoteEngineAddress(RealQuoteEngineAddress realQuoteEngineAddress) {
        return io.bhex.broker.grpc.gateway.RealQuoteEngineAddress
                .newBuilder()
                .setHost(realQuoteEngineAddress.getHost())
                .setPort(realQuoteEngineAddress.getPort())
                .setRealPort(realQuoteEngineAddress.getRealPort())
                .setRealHost(realQuoteEngineAddress.getRealHost())
                .build();
    }

    @Override
    public void getBatchRealQuoteEngineAddress(GetBatchRealQuoteEngineAddressRequest request, StreamObserver<GetBatchRealQuoteEngineAddressResponse> responseObserver) {
        GetBatchRealQuoteEngineAddressResponse response;
        try {
            String platform = request.getPlatform();
            GetBatchRealQuoteEngineAddressResponse.Builder builder = GetBatchRealQuoteEngineAddressResponse.newBuilder();
            request.getBatchAddressList()
                    .forEach(quoteEngineAddress -> {
                        RealQuoteEngineAddress realQuoteEngineAddress = quoteService.getRealQuoteEngineAddress(platform, quoteEngineAddress.getHost(), quoteEngineAddress.getPort());
                        builder.addBatchAddress(getRealQuoteEngineAddress(realQuoteEngineAddress));
                    });
            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = GetBatchRealQuoteEngineAddressResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }
}
