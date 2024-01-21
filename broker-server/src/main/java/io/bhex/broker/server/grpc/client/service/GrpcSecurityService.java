/**********************************
 * @项目名称: broker-parent
 * @文件名称: io.bhex.broker.grpc.client
 * @Date 2018/7/27
 * @Author peiwei.ren@bhex.io
 * @Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 * 注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
package io.bhex.broker.server.grpc.client.service;

import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.common.grpc.client.annotation.GrpcLog;
import io.bhex.broker.common.grpc.client.annotation.PrometheusMetrics;
import io.bhex.broker.grpc.security.*;
import io.grpc.StatusRuntimeException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@GrpcLog
@PrometheusMetrics
public class GrpcSecurityService extends GrpcBaseService {

    public SecurityRegisterResponse register(SecurityRegisterRequest request) {
        SecurityServiceGrpc.SecurityServiceBlockingStub stub = grpcClientConfig.securityServiceBlockingStub();
        try {
            SecurityRegisterResponse response = stub.register(request);
            if (response.getRet() != 0) {
                throw new BrokerException(BrokerErrorCode.fromCode(response.getRet()));
            }
            return response;
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw new BrokerException(BrokerErrorCode.BROKER_SECURITY_ERROR, e);
        }
    }

    public SecurityLoginResponse login(SecurityLoginRequest request) {
        SecurityServiceGrpc.SecurityServiceBlockingStub stub = grpcClientConfig.securityServiceBlockingStub();
        try {
            SecurityLoginResponse response = stub.login(request);
            if (response.getRet() != 0) {
                throw new BrokerException(BrokerErrorCode.fromCode(response.getRet()));
            }
            return response;
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw new BrokerException(BrokerErrorCode.BROKER_SECURITY_ERROR, e);
        }
    }

    public SecurityBeforeBindGAResponse beforeBindGA(SecurityBeforeBindGARequest request) {
        SecurityServiceGrpc.SecurityServiceBlockingStub stub = grpcClientConfig.securityServiceBlockingStub();
        try {
            SecurityBeforeBindGAResponse response = stub.beforeBindGA(request);
            if (response.getRet() != 0) {
                throw new BrokerException(BrokerErrorCode.fromCode(response.getRet()));
            }
            return response;
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw new BrokerException(BrokerErrorCode.BROKER_SECURITY_ERROR, e);
        }
    }

    public SecurityBindGAResponse bindGA(SecurityBindGARequest request) {
        SecurityServiceGrpc.SecurityServiceBlockingStub stub = grpcClientConfig.securityServiceBlockingStub();
        try {
            SecurityBindGAResponse response = stub.bindGA(request);
            if (response.getRet() != 0) {
                throw new BrokerException(BrokerErrorCode.fromCode(response.getRet()));
            }
            return response;
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw new BrokerException(BrokerErrorCode.BROKER_SECURITY_ERROR, e);
        }
    }

    public SecurityBindGAResponse bindGADirect(SecurityBindGADirectRequest request) {
        SecurityServiceGrpc.SecurityServiceBlockingStub stub = grpcClientConfig.securityServiceBlockingStub();
        try {
            SecurityBindGAResponse response = stub.bindGADirect(request);
            if (response.getRet() != 0) {
                throw new BrokerException(BrokerErrorCode.fromCode(response.getRet()));
            }
            return response;
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw new BrokerException(BrokerErrorCode.BROKER_SECURITY_ERROR, e);
        }
    }

    public SecurityVerifyGAResponse verifyGA(SecurityVerifyGARequest request) {
        SecurityServiceGrpc.SecurityServiceBlockingStub stub = grpcClientConfig.securityServiceBlockingStub();
        try {
            SecurityVerifyGAResponse response = stub.verifyGA(request);
            if (response.getRet() != 0) {
                log.warn("verify GA error userId:{} code:{} ret:{}",request.getUserId(),request.getGaCode(),response.getRet());
                throw new BrokerException(BrokerErrorCode.fromCode(response.getRet()));
            }
            return response;
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw new BrokerException(BrokerErrorCode.BROKER_SECURITY_ERROR, e);
        }
    }

    public SecurityUnBindGAResponse unBindGA(SecurityUnBindGARequest request) {
        SecurityServiceGrpc.SecurityServiceBlockingStub stub = grpcClientConfig.securityServiceBlockingStub();
        try {
            SecurityUnBindGAResponse response = stub.unBindGA(request);
            if (response.getRet() != 0) {
                throw new BrokerException(BrokerErrorCode.fromCode(response.getRet()));
            }
            return response;
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw new BrokerException(BrokerErrorCode.BROKER_SECURITY_ERROR, e);
        }
    }

    public SecurityResetPasswordResponse resetPassword(SecurityResetPasswordRequest request) {
        SecurityServiceGrpc.SecurityServiceBlockingStub stub = grpcClientConfig.securityServiceBlockingStub();
        try {
            SecurityResetPasswordResponse response = stub.resetPassword(request);
            if (response.getRet() != 0) {
                throw new BrokerException(BrokerErrorCode.fromCode(response.getRet()));
            }
            return response;
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw new BrokerException(BrokerErrorCode.BROKER_SECURITY_ERROR, e);
        }
    }

    public SecurityUpdatePasswordResponse updatePassword(SecurityUpdatePasswordRequest request) {
        SecurityServiceGrpc.SecurityServiceBlockingStub stub = grpcClientConfig.securityServiceBlockingStub();
        try {
            SecurityUpdatePasswordResponse response = stub.updatePassword(request);
            if (response.getRet() != 0) {
                throw new BrokerException(BrokerErrorCode.fromCode(response.getRet()));
            }
            return response;
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw new BrokerException(BrokerErrorCode.BROKER_SECURITY_ERROR, e);
        }
    }

    public SecurityUpdateUserStatusResponse updateUserStatus(SecurityUpdateUserStatusRequest request) {
        SecurityServiceGrpc.SecurityServiceBlockingStub stub = grpcClientConfig.securityServiceBlockingStub();
        try {
            SecurityUpdateUserStatusResponse response = stub.updateUserStatus(request);
            if (response.getRet() != 0) {
                throw new BrokerException(BrokerErrorCode.fromCode(response.getRet()));
            }
            return response;
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw new BrokerException(BrokerErrorCode.BROKER_SECURITY_ERROR, e);
        }
    }

    public SecurityValidVerifyCodeResponse validVerifyCode(SecurityValidVerifyCodeRequest request) {
        SecurityServiceGrpc.SecurityServiceBlockingStub stub = grpcClientConfig.securityServiceBlockingStub();
        try {
//            SecurityValidVerifyCodeResponse response = stub.validVerifyCode(request);
//            if (response.getRet() != 0) {
//                throw new BrokerException(BrokerErrorCode.fromCode(response.getRet()));
//            }
//            return response;
            return stub.validVerifyCode(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw new BrokerException(BrokerErrorCode.BROKER_SECURITY_ERROR, e);
        }
    }

    public SecuritySendVerifyCodeResponse sendMobileVerifyCode(SecuritySendMobileVerifyCodeRequest request) {
        SecurityServiceGrpc.SecurityServiceBlockingStub stub = grpcClientConfig.securityServiceBlockingStub();
        try {
            SecuritySendVerifyCodeResponse response = stub.sendMobileVerifyCode(request);
            if (response.getRet() != 0) {
                throw new BrokerException(BrokerErrorCode.fromCode(response.getRet()));
            }
            return response;
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public SecuritySendVerifyCodeResponse sendEmailVerifyCode(SecuritySendEmailVerifyCodeRequest request) {
        SecurityServiceGrpc.SecurityServiceBlockingStub stub = grpcClientConfig.securityServiceBlockingStub();
        try {
            SecuritySendVerifyCodeResponse response = stub.sendEmailVerifyCode(request);
            if (response.getRet() != 0) {
                throw new BrokerException(BrokerErrorCode.fromCode(response.getRet()));
            }
            return response;
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public SecurityInvalidVerifyCodeResponse invalidVerifyCode(SecurityInvalidVerifyCodeRequest request) {
        SecurityServiceGrpc.SecurityServiceBlockingStub stub = grpcClientConfig.securityServiceBlockingStub();
        try {
            SecurityInvalidVerifyCodeResponse response = stub.invalidVerifyCode(request);
            if (response.getRet() != 0) {
                throw new BrokerException(BrokerErrorCode.fromCode(response.getRet()));
            }
            return response;
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw new BrokerException(BrokerErrorCode.BROKER_SECURITY_ERROR, e);
        }
    }

    public SecurityCreateApiKeyResponse createApiKey(SecurityCreateApiKeyRequest request) {
        SecurityServiceGrpc.SecurityServiceBlockingStub stub = grpcClientConfig.securityServiceBlockingStub();
        try {
            SecurityCreateApiKeyResponse response = stub.createApiKey(request);
            if (response.getRet() != 0) {
                throw new BrokerException(BrokerErrorCode.fromCode(response.getRet()));
            }
            return response;
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw new BrokerException(BrokerErrorCode.BROKER_SECURITY_ERROR, e);
        }
    }

    public SecurityUpdateApiKeyResponse updateApiKey(SecurityUpdateApiKeyRequest request) {
        SecurityServiceGrpc.SecurityServiceBlockingStub stub = grpcClientConfig.securityServiceBlockingStub();
        try {
            SecurityUpdateApiKeyResponse response = stub.updateApiKey(request);
            if (response.getRet() != 0) {
                throw new BrokerException(BrokerErrorCode.fromCode(response.getRet()));
            }
            return response;
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw new BrokerException(BrokerErrorCode.BROKER_SECURITY_ERROR, e);
        }
    }

    public SecurityGetApiKeyResponse getApiKey(SecurityGetApiKeyRequest request) {
        SecurityServiceGrpc.SecurityServiceBlockingStub stub = grpcClientConfig.securityServiceBlockingStub();
        try {
            SecurityGetApiKeyResponse response = stub.getApiKey(request);
            if (response.getRet() != 0) {
                throw new BrokerException(BrokerErrorCode.fromCode(response.getRet()));
            }
            return response;
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw new BrokerException(BrokerErrorCode.BROKER_SECURITY_ERROR, e);
        }
    }

    public SecurityQueryUserApiKeysResponse queryApiKeys(SecurityQueryUserApiKeysRequest request) {
        SecurityServiceGrpc.SecurityServiceBlockingStub stub = grpcClientConfig.securityServiceBlockingStub();
        try {
            SecurityQueryUserApiKeysResponse response = stub.queryUserApiKeys(request);
            if (response.getRet() != 0) {
                throw new BrokerException(BrokerErrorCode.fromCode(response.getRet()));
            }
            return response;
        } catch (StatusRuntimeException e) {
            throw new BrokerException(BrokerErrorCode.BROKER_SECURITY_ERROR, e);
        }
    }

    public SecurityQueryUserApiKeysResponse queryAuthorizedAccountApiKeys(SecurityQueryOrgAuthorizedAccountApiKeysRequest request) {
        SecurityServiceGrpc.SecurityServiceBlockingStub stub = grpcClientConfig.securityServiceBlockingStub();
        try {
            SecurityQueryUserApiKeysResponse response = stub.queryOrgAuthorizedAccountApiKeys(request);
            if (response.getRet() != 0) {
                throw new BrokerException(BrokerErrorCode.fromCode(response.getRet()));
            }
            return response;
        } catch (StatusRuntimeException e) {
            throw new BrokerException(BrokerErrorCode.BROKER_SECURITY_ERROR, e);
        }
    }

    public SecurityDeleteApiKeyResponse deleteApiKey(SecurityDeleteApiKeyRequest request) {
        SecurityServiceGrpc.SecurityServiceBlockingStub stub = grpcClientConfig.securityServiceBlockingStub();
        try {
            SecurityDeleteApiKeyResponse response = stub.deleteApiKey(request);
            if (response.getRet() != 0) {
                throw new BrokerException(BrokerErrorCode.fromCode(response.getRet()));
            }
            return response;
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw new BrokerException(BrokerErrorCode.BROKER_SECURITY_ERROR, e);
        }
    }

    public SecuritySetTradePasswordResponse setTradePwd(SecuritySetTradePasswordRequest request) {
        SecurityServiceGrpc.SecurityServiceBlockingStub stub = grpcClientConfig.securityServiceBlockingStub();
        try {
            SecuritySetTradePasswordResponse response = stub.setTradePassword(request);
            if (response.getRet() != 0) {
                throw new BrokerException(BrokerErrorCode.fromCode(response.getRet()));
            }
            return response;
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw new BrokerException(BrokerErrorCode.BROKER_SECURITY_ERROR, e);
        }
    }

    public SecurityVerifyTradePasswordResponse verifyTradePwd(SecurityVerifyTradePasswordRequest request) {
        SecurityServiceGrpc.SecurityServiceBlockingStub stub = grpcClientConfig.securityServiceBlockingStub();
        try {
            return stub.verifyTradePassword(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw new BrokerException(BrokerErrorCode.BROKER_SECURITY_ERROR, e);
        }
    }

    public QueryUserTokenResponse getAuthorizeToken(QueryUserTokenRequest request) {
        SecurityServiceGrpc.SecurityServiceBlockingStub stub = grpcClientConfig.securityServiceBlockingStub();
        try {
            return stub.queryUserToken(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw new BrokerException(BrokerErrorCode.BROKER_SECURITY_ERROR, e);
        }
    }
    public SecurityBindGAResponse alterBindGA(SecurityBindGARequest request) {
        SecurityServiceGrpc.SecurityServiceBlockingStub stub = grpcClientConfig.securityServiceBlockingStub();
        try {
            SecurityBindGAResponse response = stub.alterBindGA(request);
            if (response.getRet() != 0) {
                throw new BrokerException(BrokerErrorCode.fromCode(response.getRet()));
            }
            return response;
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw new BrokerException(BrokerErrorCode.BROKER_SECURITY_ERROR, e);
        }
    }

    public ChangeUserApiLevelResponse changeUserApiLevel(ChangeUserApiLevelRequest request) {
        SecurityServiceGrpc.SecurityServiceBlockingStub stub = grpcClientConfig.securityServiceBlockingStub();
        try {
            return stub.changeUserApiLevel(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw new BrokerException(BrokerErrorCode.BROKER_SECURITY_ERROR, e);
        }
    }
}
