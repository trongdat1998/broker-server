package io.bhex.broker.server.grpc.server;

import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.grpc.admin.AdminCurrencyServiceGrpc;
import io.bhex.broker.grpc.admin.Currency;
import io.bhex.broker.grpc.admin.ListCurrencyRequest;
import io.bhex.broker.grpc.admin.ListCurrencyResponse;
import io.bhex.broker.grpc.common.BasicRet;
import io.bhex.broker.server.grpc.server.service.BasicService;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Resource;
import java.util.List;
import java.util.stream.Collectors;


@Slf4j
@GrpcService
public class AdminCurrencyGrpcService extends AdminCurrencyServiceGrpc.AdminCurrencyServiceImplBase {

    @Resource
    private BasicService basicService;

    @Override
    public void listCurrency(ListCurrencyRequest request,
                            StreamObserver<ListCurrencyResponse> responseObserver){



        ListCurrencyResponse resp = null;
        try {
            List<Currency> list =basicService.listAllCountry().stream().map(i->{
                return Currency.newBuilder()
                        .setCode(i.getCurrencyCode())
                        .setCountryCode(i.getNationalCode())
                        .setId(i.getId())
                        .build();
            }).collect(Collectors.toList());
            resp=ListCurrencyResponse.newBuilder().addAllCurrencies(list).build();
            responseObserver.onNext(resp);
        } catch (BrokerException e) {
            log.error(e.getMessage(), e);
            resp = ListCurrencyResponse.newBuilder().setRet(e.getCode()).setMessage(e.getMessage()).build();
            responseObserver.onNext(resp);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        }

        responseObserver.onCompleted();
    }


}
