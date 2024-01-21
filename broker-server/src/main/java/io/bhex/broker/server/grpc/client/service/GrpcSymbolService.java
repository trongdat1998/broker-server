/**********************************
 *@项目名称: broker-parent
 *@文件名称: io.bhex.broker.grpc.client
 *@Date 2018/6/25
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
package io.bhex.broker.server.grpc.client.service;

import org.springframework.stereotype.Service;

import java.util.List;

import io.bhex.base.token.ExchangeSymbolDetail;
import io.bhex.base.token.GetEtfSymbolPriceReply;
import io.bhex.base.token.GetEtfSymbolPriceRequest;
import io.bhex.base.token.GetExchangeSymbolsRequest;
import io.bhex.base.token.GetMakerBonusConfigReply;
import io.bhex.base.token.GetMakerBonusConfigRequest;
import io.bhex.base.token.GetSymbolMapReply;
import io.bhex.base.token.GetSymbolMapRequest;
import io.bhex.base.token.GetSymbolRequest;
import io.bhex.base.token.MakerBonusConfig;
import io.bhex.base.token.SetMakerBonusConfigReply;
import io.bhex.base.token.SymbolDetail;
import io.bhex.base.token.SymbolServiceGrpc;
import io.bhex.broker.common.grpc.client.annotation.PrometheusMetrics;
import io.bhex.broker.server.util.BaseReqUtil;
import io.grpc.StatusRuntimeException;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
@PrometheusMetrics
public class GrpcSymbolService extends GrpcBaseService {

    public GetSymbolMapReply querySymbolList(GetSymbolMapRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        SymbolServiceGrpc.SymbolServiceBlockingStub stub = grpcClientConfig.symbolServiceBlockingStub(orgId);
        try {
            return stub.getSymbolMap(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    /**
     * 特殊处理的brokerId List
     *
     * @param request
     * @param brokerId
     * @return
     */
    public GetMakerBonusConfigReply querySymbolMakerBonusConfig(GetMakerBonusConfigRequest request, long brokerId) {
        SymbolServiceGrpc.SymbolServiceBlockingStub stub = grpcClientConfig.symbolServiceBlockingStub(brokerId);
        try {
            return stub.getMakerBonusConfig(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public SymbolDetail getSymbol(GetSymbolRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        SymbolServiceGrpc.SymbolServiceBlockingStub stub = grpcClientConfig.symbolServiceBlockingStub(orgId);
        try {
            return stub.getSymbol(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public List<ExchangeSymbolDetail> getExchangeSymbols(GetExchangeSymbolsRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        SymbolServiceGrpc.SymbolServiceBlockingStub stub = grpcClientConfig.symbolServiceBlockingStub(orgId);
        try {
            return stub.getExchangeSymbols(request).getExchangeSymbolDetailsList();
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public List<GetEtfSymbolPriceReply.EtfPrice> getEtfSymbolPrice(GetEtfSymbolPriceRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        SymbolServiceGrpc.SymbolServiceBlockingStub stub = grpcClientConfig.symbolServiceBlockingStub(orgId);
        try {
            return stub.getEtfSymbolPrice(request).getEtfPriceList();
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public SetMakerBonusConfigReply setMakerBonusConfig(MakerBonusConfig request, long orgId) {
        SymbolServiceGrpc.SymbolServiceBlockingStub stub = grpcClientConfig.symbolServiceBlockingStub(orgId);
        try {
            return stub.setMakerBonusConfig(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }
}
