package io.bhex.broker.server.grpc.server;

import io.bhex.base.account.ExchangeReply;
import io.bhex.base.account.GetExchangesByBrokerReply;
import io.bhex.base.account.GetExchangesByBrokerRequest;
import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.base.proto.BaseRequest;
import io.bhex.base.token.BrokerExchangeTokenServiceGrpc;
import io.bhex.base.token.BrokerTokenObj;
import io.bhex.base.token.SyncBrokerTokenRequest;
import io.bhex.broker.grpc.admin.*;
import io.bhex.broker.grpc.common.AdminSimplyReply;
import io.bhex.broker.server.grpc.client.config.GrpcClientConfig;
import io.bhex.broker.server.grpc.server.service.AdminTokenService;
import io.bhex.broker.server.model.Token;
import io.bhex.broker.server.primary.mapper.TokenMapper;
import io.bhex.broker.server.util.BeanCopyUtils;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.util.List;

/**
 * @ProjectName: broker-server
 * @Package: io.bhex.broker.server.grpc.server
 * @Author: ming.xu
 * @CreateDate: 20/08/2018 9:28 PM
 * @Copyright（C）: 2018 BHEX Inc. All rights reserved.
 */
@Slf4j
@GrpcService
public class AdminTokenGrpcService extends AdminTokenServiceGrpc.AdminTokenServiceImplBase {

    @Autowired
    private AdminTokenService adminTokenService;
    @Resource
    GrpcClientConfig grpcClientConfig;

    @Override
    public void queryToken(QueryTokenRequest request, StreamObserver<QueryTokenReply> responseObserver) {
        QueryTokenReply reply = adminTokenService.queryToken(request.getCurrent(), request.getPageSize(), request.getTokenId(), request.getTokenName(), request.getBrokerId(), request.getCategory());

        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void queryTokenSimple(QueryTokenSimpleRequest request, StreamObserver<QueryTokenSimpleReply> responseObserver) {
        List<SimpleToken> list = adminTokenService.getSimpleTokens(request.getBrokerId(), request.getCategory());
        QueryTokenSimpleReply reply = QueryTokenSimpleReply.newBuilder().addAllTokenDetails(list).build();
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void listTokenByOrgId(QueryTokenRequest request, StreamObserver<QueryTokenReply> responseObserver) {
        QueryTokenReply reply = adminTokenService.listTokenByOrgId(request.getCategory(), request.getBrokerId());

        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void tokenAllowDeposit(TokenAllowDepositRequest request, StreamObserver<TokenAllowDepositReply> responseObserver) {
        Boolean isOk = adminTokenService.allowDeposit(request.getTokenId(), request.getAllowTrade(), request.getBrokerId());
        syncBrokerExchangeToken(request.getBrokerId(), request.getTokenId());
        responseObserver.onNext(TokenAllowDepositReply.newBuilder().setResult(isOk).build());
        responseObserver.onCompleted();
    }

    @Override
    public void tokenAllowWithdraw(TokenAllowWithdrawRequest request, StreamObserver<TokenAllowWithdrawReply> responseObserver) {
        Boolean isOk = adminTokenService.allowWithdraw(request.getTokenId(), request.getAllowWithd(), request.getBrokerId());
        syncBrokerExchangeToken(request.getBrokerId(), request.getTokenId());
        responseObserver.onNext(TokenAllowWithdrawReply.newBuilder().setResult(isOk).build());
        responseObserver.onCompleted();
    }

    @Override
    public void tokenPublish(TokenPublishRequest request, StreamObserver<TokenPublishReply> responseObserver) {
        Boolean isOk = adminTokenService.publish(request.getTokenId(), request.getPublished(), request.getBrokerId());
        syncBrokerExchangeToken(request.getBrokerId(), request.getTokenId());
        responseObserver.onNext(TokenPublishReply.newBuilder().setResult(isOk).build());
        responseObserver.onCompleted();
    }

    @Override
    public void addToken(AddTokenRequest request, StreamObserver<AddTokenReply> responseObserver) {
        Token t = adminTokenService.getToken(request.getBrokerId(), request.getTokenId());
        if (null == t) {
            Token token = new Token();

            GetExchangesByBrokerRequest req = GetExchangesByBrokerRequest.newBuilder()
                    .setBrokerId(request.getBrokerId())
                    .build();
            GetExchangesByBrokerReply reply = grpcClientConfig.orgServiceStub(request.getBrokerId()).getExchangesByBroker(req);
            List<ExchangeReply> exchanges = reply.getExchangesList();
            if (CollectionUtils.isEmpty(exchanges)) {
                log.error("ALERT: addToken:{}-{} no exchange", request.getTokenId(), request.getBrokerId());
                AddTokenReply errorReply = AddTokenReply.newBuilder()
                        .setResult(false)
                        .setMessage("no exchange")
                        .build();

                responseObserver.onNext(errorReply);
                responseObserver.onCompleted();
                return;
            } else if (exchanges.size() > 1) {
                log.error("ALERT: addToken exchangeSize>1:{}-{}", request.getTokenId(), request.getBrokerId());
                token.setExchangeId(exchanges.get(0).getExchangeId());
            } else {
                token.setExchangeId(exchanges.get(0).getExchangeId());
            }

            token.setTokenIndex(request.getTokenIndex());
            token.setTokenId(request.getTokenId());
            token.setTokenName(request.getTokenName());
            token.setTokenFullName(request.getTokenFullName());
            token.setAllowDeposit(Token.FORBID_INT);
            token.setAllowWithdraw(Token.FORBID_INT);
            token.setTokenIcon(request.getIcon());
            token.setOrgId(request.getBrokerId());

            // todo: 平台未返回，需要后期补充的数据
            token.setFeeTokenId(request.getTokenId());
            token.setFee(BigDecimal.ZERO);
            token.setFeeTokenName(request.getTokenName());
            token.setMaxWithdrawQuota(BigDecimal.ZERO);
            token.setMinWithdrawQuantity(BigDecimal.ZERO);
            token.setNeedKycQuantity(BigDecimal.ZERO);
            token.setStatus(0);
            token.setCustomOrder(0);
            token.setCreated(System.currentTimeMillis());
            token.setUpdated(System.currentTimeMillis());
            token.setCategory(request.getCategory());
            adminTokenService.addToken(token);

            adminTokenService.bhAllowWithdraw(request.getTokenId(), false, request.getBrokerId());
            adminTokenService.bhAllowDeposit(request.getTokenId(), false, request.getBrokerId());
            syncBrokerExchangeToken(request.getBrokerId(), request.getTokenId());
        }
        AddTokenReply reply = AddTokenReply.newBuilder()
                .setResult(true)
                .build();

        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }


    public void syncBrokerExchangeToken(Long brokerId, String tokenId) {
        Token t = adminTokenService.getToken(brokerId, tokenId);
        syncBrokerExchangeToken(t);
    }

    private void syncBrokerExchangeToken(Token token) {
        BaseRequest baseRequest = BaseRequest.newBuilder().setOrganizationId(token.getOrgId()).build();
        BrokerExchangeTokenServiceGrpc.BrokerExchangeTokenServiceBlockingStub stub = grpcClientConfig
                .brokerExchangeTokenServiceBlockingStub(token.getOrgId());
        BrokerTokenObj.Builder builder = BrokerTokenObj.newBuilder();
        BeanCopyUtils.copyPropertiesIgnoreNull(token, builder);
        builder.setMaxWithdrawQuantity(token.getMaxWithdrawQuantity().stripTrailingZeros().toPlainString());
        builder.setMinWithdrawQuantity(token.getMinWithdrawQuantity().stripTrailingZeros().toPlainString());
        builder.setMaxWithdrawQuota(token.getMaxWithdrawQuota().stripTrailingZeros().toPlainString());
        builder.setNeedKycQuantity(token.getNeedKycQuantity().stripTrailingZeros().toPlainString());
        builder.setFee(token.getFee().stripTrailingZeros().toPlainString());

        stub.syncBrokerToken(SyncBrokerTokenRequest.newBuilder().setBaseRequest(baseRequest).setBrokerTokenObj(builder.build()).build());
    }

    @Override
    public void setTokenHighRisk(SetTokenHighRiskRequest request, StreamObserver<SetTokenHighRiskResponse> responseObserver) {
        boolean result = adminTokenService.setTokenIsHighRiskToken(request.getOrgId(), request.getTokenId(), request.getIsHighRiskToken());
        responseObserver.onNext(SetTokenHighRiskResponse.newBuilder().setResult(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void setTokenWithdrawFee(SetTokenWithdrawFeeRequest request, StreamObserver<SetTokenWithdrawFeeResponse> responseObserver) {
        boolean result = adminTokenService.setTokenWithdrawFee(request.getOrgId(), request.getTokenId(), request.getWithdrawFee());
        responseObserver.onNext(SetTokenWithdrawFeeResponse.newBuilder().setResult(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void editTokenName(EditTokenNameRequest request, StreamObserver<AdminSimplyReply> responseObserver) {
        AdminSimplyReply reply = adminTokenService.editTokenName(request.getOrgId(), request.getTokenId(),
                request.getTokenName(), true);
        syncBrokerExchangeToken(request.getOrgId(), request.getTokenId());
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void editTokenFullName(EditTokenFullNameRequest request, StreamObserver<AdminSimplyReply> responseObserver) {
        AdminSimplyReply reply = adminTokenService.editTokenFullName(request.getOrgId(), request.getTokenId(), request.getTokenFullName());
        syncBrokerExchangeToken(request.getOrgId(), request.getTokenId());
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void editTokenExtraTags(EditTokenExtraTagsRequest request, StreamObserver<AdminSimplyReply> responseObserver) {
        AdminSimplyReply reply = adminTokenService.editTokenExtraTags(request.getOrgId(), request.getTokenId(), request.getExtraTagMap());
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void editTokenExtraConfigs(EditTokenExtraConfigsRequest request, StreamObserver<AdminSimplyReply> responseObserver) {
        AdminSimplyReply reply = adminTokenService.editTokenExtraConfigs(request.getOrgId(), request.getTokenId(), request.getExtraConfigMap());
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void deleteToken(DeleteTokenRequest request, StreamObserver<AdminSimplyReply> responseObserver) {
        AdminSimplyReply reply = adminTokenService.deleteToken(request.getOrgId(), request.getTokenId());
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void setWithdrawMinQuantity(SetWithdrawMinQuantityRequest request, StreamObserver<AdminSimplyReply> responseObserver) {
        AdminSimplyReply reply = adminTokenService.setWithdrawMinQuantity(request.getOrgId(), request.getTokenId(), request.getWithdrawMinQuantity());
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Resource
    private TokenMapper tokenMapper;

    public void syncBrokerToken() {
        List<Token> tokens = tokenMapper.selectAll();
        for (Token token : tokens) {
            syncBrokerExchangeToken(token);
        }
    }

}
