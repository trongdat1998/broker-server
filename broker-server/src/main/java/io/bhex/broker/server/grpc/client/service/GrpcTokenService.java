/**********************************
 *@项目名称: broker-parent
 *@文件名称: io.bhex.broker.grpc.client
 *@Date 2018/6/25
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
package io.bhex.broker.server.grpc.client.service;

import io.bhex.base.token.*;
import io.bhex.broker.common.grpc.client.annotation.GrpcLog;
import io.bhex.broker.common.grpc.client.annotation.PrometheusMetrics;
import io.bhex.broker.server.util.BaseReqUtil;
import io.grpc.StatusRuntimeException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@PrometheusMetrics
public class GrpcTokenService extends GrpcBaseService {

    public TokenList queryTokenList(GetTokenListRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        TokenServiceGrpc.TokenServiceBlockingStub stub = grpcClientConfig.tokenServiceBlockingStub(orgId);
        try {
            return stub.getTokenList(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public TokenList queryTokenListByIds(GetTokenIdsRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        TokenServiceGrpc.TokenServiceBlockingStub stub = grpcClientConfig.tokenServiceBlockingStub(orgId);
        try {
            return stub.getTokenListByIds(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public TokenDetail getToken(GetTokenRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        TokenServiceGrpc.TokenServiceBlockingStub stub = grpcClientConfig.tokenServiceBlockingStub(orgId);
        try {
            return stub.getToken(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public GetUnderlyingReply getUnderlyingList(GetUnderlyingRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        TokenServiceGrpc.TokenServiceBlockingStub stub = grpcClientConfig.tokenServiceBlockingStub(orgId);
        try {
            return stub.getUnderlyingList(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    @GrpcLog
    public ConfigTokenSwitchReply configTokenSwitch(ConfigTokenSwitchRequest request) {
        TokenServiceGrpc.TokenServiceBlockingStub stub = grpcClientConfig.tokenServiceBlockingStub(request.getBrokerId());
        try {
            return stub.configTokenSwitch(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public ChangeTokenNameReply changeTokenName(ChangeTokenNameRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        TokenServiceGrpc.TokenServiceBlockingStub stub = grpcClientConfig.tokenServiceBlockingStub(orgId);
        try {
            return stub.changeTokenName(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }
}
