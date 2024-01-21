/**********************************
 *@项目名称: broker-parent
 *@文件名称: io.bhex.broker.grpc.client
 *@Date 2018/6/25
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
package io.bhex.broker.server.grpc.client.service;

import io.bhex.base.quote.*;
import io.bhex.broker.common.grpc.client.annotation.PrometheusMetrics;
import io.bhex.broker.server.util.BaseReqUtil;
import io.grpc.StatusRuntimeException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Collections;

@Service
@Slf4j
@PrometheusMetrics
public class GrpcQuoteService extends GrpcBaseService {

    @Deprecated
    public GetRateReply getRate(String token, Long orgId) {
        GetRateRequest request = GetRateRequest.newBuilder()
                .setBaseRequest(BaseReqUtil.getBaseRequest(orgId))
                .setToken(token)
                .build();
        QuoteServiceGrpc.QuoteServiceBlockingStub stub = grpcClientConfig.quoteServiceBlockingStub(orgId);
        try {
            return stub.getFXRate(request);
        } catch (StatusRuntimeException e) {
            log.error(" getRate exception:{}", token, e);
            throw commonStatusRuntimeException(e);
        }
    }

    @Deprecated
    public GetRatesReply getRates(GetRatesRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        QuoteServiceGrpc.QuoteServiceBlockingStub stub = grpcClientConfig.quoteServiceBlockingStub(orgId);
        try {
            return stub.getRates(request);
        } catch (StatusRuntimeException e) {
            log.error(" getRates exception", e);
            throw commonStatusRuntimeException(e);
        }
    }

    public GetExchangeRateReply getRatesV3(GetExchangeRateRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        QuoteServiceGrpc.QuoteServiceBlockingStub stub = grpcClientConfig.quoteServiceBlockingStub(orgId);
        try {
            return stub.getRatesV3(request);
        } catch (StatusRuntimeException e) {
            log.error(" getRatesV3 exception", e);
            throw commonStatusRuntimeException(e);
        }
    }

    public GetRealtimeReply getRealtime(long exchangeId, String symbol, Long orgId) {
        // log.info("exchangeId {} symbol {}", exchangeId, symbol);
        GetQuoteRequest request = GetQuoteRequest.newBuilder()
                .setBaseRequest(BaseReqUtil.getBaseRequest(orgId))
                .setExchangeId(exchangeId)
                .setSymbol(symbol)
                .build();
        QuoteServiceGrpc.QuoteServiceBlockingStub stub = grpcClientConfig.quoteServiceBlockingStub(orgId);
        try {
            return stub.getRealtime(request);
        } catch (StatusRuntimeException e) {
            log.error(" getRealtime exchangeId:{} symbol:{}", exchangeId, symbol, e);
            throw commonStatusRuntimeException(e);
        }
    }

    public GetIndicesReply getIndices(String symbol, Long orgId) {
        GetIndicesRequest request = GetIndicesRequest.newBuilder()
                .setBaseRequest(BaseReqUtil.getBaseRequest(orgId))
                .addAllSymbols(Collections.singletonList(symbol))
                .build();
        QuoteServiceGrpc.QuoteServiceBlockingStub stub = grpcClientConfig.quoteServiceBlockingStub(orgId);
        try {
            return stub.getIndices(request);
        } catch (StatusRuntimeException e) {
            log.error("getIndices symbol:{}", symbol, e);
            throw commonStatusRuntimeException(e);
        }
    }

    public GetDepthReply getPartialDepth(Long exchangeId, String symbol, Integer dumpScale, Integer limitCount, Long orgId) {
        GetQuoteRequest request = GetQuoteRequest.newBuilder()
                .setBaseRequest(BaseReqUtil.getBaseRequest(orgId))
                .setExchangeId(exchangeId)
                .setSymbol(symbol)
                .setDumpScale(dumpScale)
                .setLimitCount(limitCount)
                .build();
        try {
            return grpcClientConfig.quoteServiceBlockingStub(orgId).getPartialDepth(request);
        } catch (StatusRuntimeException e) {
            log.error("getDepth symbol:{}", symbol, e);
            throw commonStatusRuntimeException(e);
        }
    }

    public GetKLineReply getLatestKLine(GetLatestKLineRequest request) {
        QuoteServiceGrpc.QuoteServiceBlockingStub stub = grpcClientConfig.quoteServiceBlockingStub(BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest()));
        try {
            return stub.getLatestKLine(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public GetKLineReply getKline(GetKLineRequest request){
        QuoteServiceGrpc.QuoteServiceBlockingStub stub = grpcClientConfig.quoteServiceBlockingStub(BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest()));
        try {
            return stub.getKLine(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

}
