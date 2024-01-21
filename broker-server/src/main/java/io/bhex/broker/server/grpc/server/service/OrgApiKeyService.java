package io.bhex.broker.server.grpc.server.service;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.grpc.common.BasicRet;
import io.bhex.broker.grpc.common.Header;
import io.bhex.broker.grpc.org_api.*;
import io.bhex.broker.grpc.security.*;
import io.bhex.broker.server.domain.ApiKeyType;
import io.bhex.broker.server.domain.FunctionModule;
import io.bhex.broker.server.grpc.client.service.GrpcSecurityService;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;
import java.util.stream.Collectors;

@Service
public class OrgApiKeyService {

    @Resource
    private GrpcSecurityService grpcSecurityService;

    public CreateOrgApiKeyResponse createOrgApiKey(Header header, String tag, int type) {
        if (!BrokerService.checkHasOrgApi(header.getOrgId())) {
            throw new BrokerException(BrokerErrorCode.FEATURE_SUSPENDED);
        }
        ApiKeyType apiKeyType = ApiKeyType.forType(type);
        if (apiKeyType == null || !apiKeyType.isOrgAPiKey()) {
            throw new BrokerException(BrokerErrorCode.PARAM_INVALID);
        }
        SecurityCreateApiKeyRequest request = SecurityCreateApiKeyRequest.newBuilder()
                .setHeader(header)
                .setTag(tag)
                .setType(type)
                .build();
        SecurityCreateApiKeyResponse response = grpcSecurityService.createApiKey(request);
        return CreateOrgApiKeyResponse.newBuilder().setBasicRet(BasicRet.newBuilder().setCode(0).build())
                .setApiKey(getApiKey(response.getApiKey()))
                .build();
    }

    public UpdateOrgApiKeyResponse updateApiKeyIps(Header header, Long apiKeyId, String ipWhiteList) {
        ApiKeyInfo apiKey = grpcSecurityService.getApiKey(SecurityGetApiKeyRequest.newBuilder()
                .setHeader(header)
                .setApiKeyId(apiKeyId)
                .build()).getApiKey();
        if (apiKey == null) {
            throw new BrokerException(BrokerErrorCode.REQUEST_INVALID);
        }

        SecurityUpdateApiKeyRequest request = SecurityUpdateApiKeyRequest.newBuilder()
                .setHeader(header)
                .setApiKeyId(apiKeyId)
                .setIpWhiteList(ipWhiteList)
                .setUpdateType(SecurityUpdateApiKeyRequest.UpdateType.UPDATE_IP_WHITE_LIST)
                .build();
        SecurityUpdateApiKeyResponse securityUpdateApiKeyResponse = grpcSecurityService.updateApiKey(request);
        return UpdateOrgApiKeyResponse.getDefaultInstance();
    }

    /**
     * Modify the user's ApiKey, you can modify ipWhiteList or enable/disable the ApiKey
     * <p>If you want to modify the tag、ipWhiteList and status synchronously, this method needs to be modified.
     */
    public UpdateOrgApiKeyResponse updateApiKeyStatus(Header header, Long apiKeyId, Integer status) {
        ApiKeyInfo apiKey = grpcSecurityService.getApiKey(SecurityGetApiKeyRequest.newBuilder()
                .setHeader(header)
                .setApiKeyId(apiKeyId)
                .build()).getApiKey();
        if (apiKey == null) {
            throw new BrokerException(BrokerErrorCode.REQUEST_INVALID);
        }
        if (apiKey.getStatus().getNumber() == status) {
            return UpdateOrgApiKeyResponse.newBuilder().build();
        }

        SecurityUpdateApiKeyRequest request = SecurityUpdateApiKeyRequest.newBuilder()
                .setHeader(header)
                .setApiKeyId(apiKeyId)
                .setStatus(ApiKeyStatus.forNumber(status))
                .setUpdateType(SecurityUpdateApiKeyRequest.UpdateType.UPDATE_STATUS)
                .build();
        grpcSecurityService.updateApiKey(request);
        return UpdateOrgApiKeyResponse.newBuilder().build();
    }

    /**
     * Delete user's ApiKey
     */
    public DeleteOrgApiKeyResponse deleteApiKey(Header header, Long apiKeyId) {
        SecurityDeleteApiKeyRequest request = SecurityDeleteApiKeyRequest.newBuilder()
                .setHeader(header)
                .setApiKeyId(apiKeyId)
                .build();
        SecurityDeleteApiKeyResponse securityDeleteApiKeyResponse = grpcSecurityService.deleteApiKey(request);
        return DeleteOrgApiKeyResponse.getDefaultInstance();
    }

    /**
     * Query user's ApiKey records
     */
    public QueryOrgApiKeyResponse queryApiKeys(Header header) {
        SecurityQueryUserApiKeysRequest request = SecurityQueryUserApiKeysRequest.newBuilder()
                .setHeader(header)
                .build();
        SecurityQueryUserApiKeysResponse securityQueryUserApiKeysResponse = grpcSecurityService.queryApiKeys(request);
        List<OrgApiKey> apiKeyList = Lists.newArrayList();
        if (securityQueryUserApiKeysResponse.getApiKeyList() != null) {
            apiKeyList = securityQueryUserApiKeysResponse.getApiKeyList().stream().map(this::getApiKey).collect(Collectors.toList());
        }
        return QueryOrgApiKeyResponse.newBuilder().addAllApiKeys(apiKeyList).build();
    }

    private OrgApiKey getApiKey(ApiKeyInfo apiKey) {
        return OrgApiKey.newBuilder()
                .setId(apiKey.getId())
                .setApiKey(apiKey.getApiKey())
                .setSecretKey(Strings.nullToEmpty(apiKey.getSecretKey()))
                .setTag(apiKey.getTag())
                .setIpWhiteList(apiKey.getIpWhiteList())
                .setStatus(apiKey.getStatusValue())
                .setCreated(apiKey.getCreated())
                .setUpdated(apiKey.getUpdated())
                .setType(apiKey.getType())
                .setLevel(apiKey.getLevel())
                .build();
    }

}
