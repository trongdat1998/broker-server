package io.bhex.broker.server.grpc.server;

import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.base.grpc.server.interceptor.GrpcServerLogInterceptor;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.grpc.oauth.*;
import io.bhex.broker.server.grpc.server.service.OAuthService;
import io.bhex.broker.server.model.GetOAuthCodeResponse;
import io.bhex.broker.server.model.OAuthAuthorizeResponse;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Resource;
import java.util.stream.Collectors;

@Slf4j
@GrpcService(interceptors = GrpcServerLogInterceptor.class)
public class OAuthGrpcService extends OAuthServiceGrpc.OAuthServiceImplBase {

    @Resource
    private OAuthService oAuthService;

    @Override
    public void oAuthAuthorize(AuthorizeRequest request, StreamObserver<AuthorizeResponse> observer) {
        AuthorizeResponse.Builder responseBuilder = AuthorizeResponse.newBuilder();
        try {
            OAuthAuthorizeResponse response = oAuthService.authorize(request.getHeader(), request.getClientId(), request.getRedirectUrl(),
                    request.getState(), request.getScope());
            responseBuilder.setRequestId(response.getRequestId());
            responseBuilder.setAppName(response.getAppName());
            responseBuilder.addAllOpenFunctions(response.getFunctions().stream().map(
                    function -> ThirdPartyAppOpenFunction.newBuilder()
                            .setId(function.getId())
                            .setFunction(function.getFunction())
                            .setName(function.getShowName())
                            .setDesc(function.getShowDesc())
                            .build()
            ).collect(Collectors.toList()));
        } catch (BrokerException e) {
            responseBuilder.setRet(e.getCode());
        } catch (Exception e) {
            log.error("oauth authorize error", e);
            responseBuilder.setRet(BrokerErrorCode.OAUTH_GET_APP_OPEN_FUNCTION_ERROR.code());
        }
        observer.onNext(responseBuilder.build());
        observer.onCompleted();
    }

    @Override
    public void getAuthorizationCode(GetAuthorizationCodeRequest request, StreamObserver<GetAuthorizationCodeResponse> observer) {
        GetAuthorizationCodeResponse.Builder responseBuilder = GetAuthorizationCodeResponse.newBuilder();
        try {
            GetOAuthCodeResponse response = oAuthService.getAuthorizeCode(request.getHeader(), request.getUserId(), request.getRequestId());
            responseBuilder.setOauthCode(response.getOauthCode());
            responseBuilder.setExpired(response.getExpired());
            responseBuilder.setState(response.getState());
            responseBuilder.setRedirectUrl(response.getRedirectUrl());
        } catch (BrokerException e) {
            responseBuilder.setRet(e.getCode());
        } catch (Exception e) {
            log.error("oauth get authorization code error", e);
            responseBuilder.setRet(BrokerErrorCode.OAUTH_GET_CODE_ERROR.code());
        }
        observer.onNext(responseBuilder.build());
        observer.onCompleted();
    }

    @Override
    public void getOAuthAccessToken(GetOAuthAccessTokenRequest request, StreamObserver<GetOAuthAccessTokenResponse> observer) {
        GetOAuthAccessTokenResponse.Builder responseBuilder = GetOAuthAccessTokenResponse.newBuilder();
        try {
            io.bhex.broker.server.model.GetOAuthAccessTokenResponse response
                    = oAuthService.getAccessToken(request.getHeader(), request.getClientId(), request.getClientSecret(),
                    request.getOauthCode(), request.getRedirectUrl());
            responseBuilder.setAccessToken(response.getAccessToken());
            responseBuilder.setExpired(response.getExpired());
            responseBuilder.setOpenId(response.getOpenId());
            responseBuilder.setUserId(response.getUserId());
        } catch (BrokerException e) {
            responseBuilder.setRet(e.getCode());
        } catch (Exception e) {
            log.error("oauth get access_token error", e);
            responseBuilder.setRet(BrokerErrorCode.OAUTH_GET_ACCESS_TOKEN_ERROR.code());
        }
        observer.onNext(responseBuilder.build());
        observer.onCompleted();
    }

    @Override
    public void checkOAuthRequest(OAuthRequest request, StreamObserver<CheckOAuthResponse> observer) {
        CheckOAuthResponse.Builder responseBuilder = io.bhex.broker.grpc.oauth.CheckOAuthResponse.newBuilder();
        try {
            io.bhex.broker.server.model.CheckOAuthResponse response = oAuthService.checkOAuthRequest(request.getHeader(), request.getClientId(),
                    request.getAccessToken(), request.getOpenId(), request.getRedirectUrl(), request.getRequiredPermission());
            responseBuilder.setUserId(response.getUserId());
        } catch (BrokerException e) {
            responseBuilder.setRet(e.getCode());
        } catch (Exception e) {
            log.error("check oauth request error", e);
            responseBuilder.setRet(BrokerErrorCode.OAUTH_CHECK_OAUTH_REQUEST_ERROR.code());
        }
        observer.onNext(responseBuilder.build());
        observer.onCompleted();
    }
}
