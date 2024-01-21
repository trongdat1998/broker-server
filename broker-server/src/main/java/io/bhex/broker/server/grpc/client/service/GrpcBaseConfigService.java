package io.bhex.broker.server.grpc.client.service;

import io.bhex.base.common.*;
import io.bhex.broker.common.grpc.client.annotation.GrpcLog;
import io.bhex.broker.common.grpc.client.annotation.PrometheusMetrics;
import io.bhex.broker.server.util.BaseReqUtil;
import io.bhex.broker.server.util.OrgConfigUtil;
import io.grpc.StatusRuntimeException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
@Slf4j
@GrpcLog
@PrometheusMetrics
public class GrpcBaseConfigService extends GrpcBaseService {

    private BaseConfigServiceGrpc.BaseConfigServiceBlockingStub getBhStub(Long orgId) {
        //来源broker-api和broker-admin(其中broker-admin后续直接从broker-gateway)
        return grpcClientConfig.baseConfigServiceBlockingStub(orgId);
    }

    public GetConfigMetasReply getBhConfigMetas(GetConfigMetasRequest request) {
        try {
            Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
            return getBhStub(orgId).getBhConfigMetas(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public EditReply editBhBaseConfigs(EditBaseConfigsRequest request) {
        try {
            Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
            return getBhStub(orgId).editBhBaseConfigs(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public EditReply cancelBhBaseConfig(CancelBaseConfigRequest request) {
        try {
            return getBhStub(request.getOrgId()).cancelBhBaseConfig(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public BaseConfigsReply getBhBaseConfigs(GetBaseConfigsRequest request) {
        try {
            return getBhStub(request.getOrgId()).getBhBaseConfigs(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public BaseConfig getOneBhBaseConfig(GetOneBaseConfigRequest request) {
        try {
            Long orgId = request.getOrgId();
            return getBhStub(orgId).getOneBhBaseConfig(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public ConfigSwitchReply getBhConfigSwitch(GetConfigSwitchRequest request) {
        try {
            return getBhStub(request.getOrgId()).getBhConfigSwitch(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public EditReply editBhBaseSymbolConfigs(EditBaseSymbolConfigsRequest request) {
        try {
            Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
            return getBhStub(orgId).editBhBaseSymbolConfigs(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public EditReply cancelBhBaseSymbolConfig(CancelBaseSymbolConfigRequest request) {
        try {
            return getBhStub(request.getOrgId()).cancelBhBaseSymbolConfig(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public BaseSymbolConfigsReply getBhBaseSymbolConfigs(GetBaseSymbolConfigsRequest request) {
        try {
            return getBhStub(request.getOrgId()).getBhBaseSymbolConfigs(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public BaseSymbolConfig getOneBhBaseSymbolConfig(GetOneBaseSymbolConfigRequest request) {
        try {
            Long orgId = request.getOrgId();
            return getBhStub(orgId).getOneBhBaseSymbolConfig(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public ConfigSwitchReply getBhSymbolConfigSwitch(GetSymbolConfigSwitchRequest request) {
        try {
            return getBhStub(request.getOrgId()).getBhSymbolConfigSwitch(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public EditReply editBhBaseTokenConfigs(EditBaseTokenConfigsRequest request) {
        try {
            Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
            return getBhStub(orgId).editBhBaseTokenConfigs(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public EditReply cancelBhBaseTokenConfig(CancelBaseTokenConfigRequest request) {
        try {
            return getBhStub(request.getOrgId()).cancelBhBaseTokenConfig(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public BaseTokenConfigsReply getBhBaseTokenConfigs(GetBaseTokenConfigsRequest request) {
        try {
            return getBhStub(request.getOrgId()).getBhBaseTokenConfigs(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public BaseTokenConfig getOneBhBaseTokenConfig(GetOneBaseTokenConfigRequest request) {
        try {
            return getBhStub(request.getOrgId()).getOneBhBaseTokenConfig(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public ConfigSwitchReply getBhTokenConfigSwitch(GetTokenConfigSwitchRequest request) {
        try {
            return getBhStub(request.getOrgId()).getBhTokenConfigSwitch(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public BaseConfigsReply getBhBaseConfigsByGroup(GetBaseConfigsByGroupRequest request) {
        try {
            return getBhStub(request.getOrgId()).getBhBaseConfigsByGroup(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }


    public BaseSymbolConfigsReply getBhBaseSymbolConfigsByGroup(GetBaseConfigsByGroupRequest request) {
        try {
            return getBhStub(request.getOrgId()).getBhBaseSymbolConfigsByGroup(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }


    public BaseTokenConfigsReply getBhBaseTokenConfigsByGroup(GetBaseConfigsByGroupRequest request) {
        try {
            return getBhStub(request.getOrgId()).getBhBaseTokenConfigsByGroup(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }
}
