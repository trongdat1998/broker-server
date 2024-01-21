package io.bhex.broker.server.grpc.server;

import com.beust.jcommander.internal.Lists;
import com.google.protobuf.TextFormat;
import io.bhex.base.common.*;
import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.base.grpc.server.interceptor.GrpcServerLogInterceptor;
import io.bhex.broker.server.grpc.client.service.GrpcBaseConfigService;
import io.bhex.broker.server.grpc.server.service.BaseConfigService;
import io.bhex.broker.server.grpc.server.service.BaseSymbolConfigService;
import io.bhex.broker.server.grpc.server.service.BaseTokenConfigService;
import io.bhex.broker.server.model.BaseConfigInfo;
import io.bhex.broker.server.model.BaseSymbolConfigInfo;
import io.bhex.broker.server.model.BaseTokenConfigInfo;
import io.bhex.broker.server.util.BeanCopyUtils;
import io.bhex.broker.server.util.OrgConfigUtil;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.util.CollectionUtils;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@GrpcService(interceptors = GrpcServerLogInterceptor.class)
public class BaseConfigGrpcService extends BaseConfigServiceGrpc.BaseConfigServiceImplBase {

    @Resource
    private BaseConfigService baseConfigService;
    @Resource
    private BaseSymbolConfigService baseSymbolConfigService;
    @Resource
    private BaseTokenConfigService baseTokenConfigService;
    @Resource
    private GrpcBaseConfigService bhGrpcConfigService;

    @Override
    public void getConfigMetas(GetConfigMetasRequest request, StreamObserver<GetConfigMetasReply> observer) {
        GetConfigMetasReply response;
        try {
            List<io.bhex.broker.server.model.BaseConfigMeta> list =
                    baseConfigService.getConfigMetas(request.getGroup(), request.getKey());
            List<BaseConfigMeta> configMetas;
            if (CollectionUtils.isEmpty(list)) {
                configMetas = new ArrayList<>();
            } else {
                configMetas = list.stream().map(m -> {
                    BaseConfigMeta.Builder builder = BaseConfigMeta.newBuilder();
                    BeanCopyUtils.copyPropertiesIgnoreNull(m, builder);
                    builder.setGroup(m.getConfGroup());
                    builder.setKey(m.getConfKey());
                    return builder.build();
                }).collect(Collectors.toList());
            }
            response = GetConfigMetasReply.newBuilder().addAllConfigMeta(configMetas).build();
        } catch (Exception e) {
            log.error(" getConfigMetas exception:{}", TextFormat.shortDebugString(request), e);
            response = GetConfigMetasReply.getDefaultInstance();
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    @Override
    public void editBaseConfigs(EditBaseConfigsRequest request, StreamObserver<EditReply> observer) {
        EditReply response;
        try {
            boolean r = baseConfigService.editConfigs(request);
            response = EditReply.newBuilder()
                    .setCode(r ? 0 : 1)
                    .build();
        } catch (Exception e) {
            log.error(" editBaseConfigs exception:{}", TextFormat.shortDebugString(request), e);
            response = EditReply.getDefaultInstance();
        }
        observer.onNext(response);
        observer.onCompleted();
    }



    @Override
    public void cancelBaseConfig(CancelBaseConfigRequest request, StreamObserver<EditReply> observer) {
        EditReply response;
        try {
            boolean r = baseConfigService.cancelConfig(request);
            response = EditReply.newBuilder()
                    .setCode(r ? 0 : 1)
                    .build();
        } catch (Exception e) {
            log.error(" cancelConfig exception:{}", TextFormat.shortDebugString(request), e);
            response = EditReply.getDefaultInstance();
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    @Override
    public void getBaseConfigs(GetBaseConfigsRequest request, StreamObserver<BaseConfigsReply> observer) {
        BaseConfigsReply response;
        try {
            List<BaseConfigInfo> configInfos = baseConfigService.getConfigs(request.getOrgId(), Lists.newArrayList(request.getGroup()),
                    request.getKeyList(), request.getLanguage(), request.getPageSize(), request.getLastId());
            if (CollectionUtils.isEmpty(configInfos)) {
                response = BaseConfigsReply.getDefaultInstance();
            } else {
                response = BaseConfigsReply.newBuilder()
                        .addAllBaseConfig(configInfos.stream().map(this::convertBaseConfig).collect(Collectors.toList()))
                        .build();
            }

        } catch (Exception e) {
            log.error(" getOneBaseConfig exception:{}", TextFormat.shortDebugString(request), e);
            response = BaseConfigsReply.getDefaultInstance();
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    @Override
    public void getBaseConfigsByGroup(GetBaseConfigsByGroupRequest request, StreamObserver<BaseConfigsReply> observer) {
        BaseConfigsReply response;
        try {
            List<BaseConfigInfo> configInfos = baseConfigService.getConfigs(request.getOrgId(), request.getGroupList(),
                    null, request.getLanguage(), request.getPageSize(), request.getLastId());
            if (CollectionUtils.isEmpty(configInfos)) {
                response = BaseConfigsReply.getDefaultInstance();
            } else {
                response = BaseConfigsReply.newBuilder()
                        .addAllBaseConfig(configInfos.stream().map(this::convertBaseConfig).collect(Collectors.toList()))
                        .build();
            }

        } catch (Exception e) {
            log.error(" getOneBaseConfig exception:{}", TextFormat.shortDebugString(request), e);
            response = BaseConfigsReply.getDefaultInstance();
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    @Override
    public void getOneBaseConfig(GetOneBaseConfigRequest request, StreamObserver<BaseConfig> observer) {
        BaseConfig response;
        try {
            BaseConfigInfo configInfo = baseConfigService.getOneConfig(request.getOrgId(), request.getGroup(),
                    request.getKey(), request.getLanguage(), true);
            response = convertBaseConfig(configInfo);
        } catch (Exception e) {
            log.error(" getOneBaseConfig exception:{}", TextFormat.shortDebugString(request), e);
            response = BaseConfig.getDefaultInstance();
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    @Override
    public void getConfigSwitch(GetConfigSwitchRequest request, StreamObserver<ConfigSwitchReply> observer) {
        ConfigSwitchReply response;
        try {
            Pair<Boolean, Boolean> pair = baseConfigService.getConfigSwitch(request.getOrgId(), request.getGroup(), request.getKey(), request.getLanguage());
            response = ConfigSwitchReply.newBuilder().setExisted(pair.getLeft()).setOpen(pair.getRight()).build();
        } catch (Exception e) {
            log.error("getConfigSwitch exception:{}", TextFormat.shortDebugString(request), e);
            response = ConfigSwitchReply.getDefaultInstance();
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    private BaseConfig convertBaseConfig(BaseConfigInfo configInfo) {
        BaseConfig.Builder builder = BaseConfig.newBuilder();
        if (configInfo == null) {
            return builder.build();
        }
        builder.setId(configInfo.getId())
                .setOrgId(configInfo.getOrgId())
                .setGroup(configInfo.getConfGroup())
                .setKey(configInfo.getConfKey())
                .setValue(configInfo.getConfValue())
                .setExtraValue(configInfo.getExtraValue())
                .setLanguage(configInfo.getLanguage())
                .setAdminUserName(configInfo.getAdminUserName())
                .setCreated(configInfo.getCreated())
                .setUpdated(configInfo.getUpdated())
                .setNewValue(configInfo.getNewConfValue())
                .setNewExtraValue(configInfo.getNewExtraValue())
                .setStartTime(configInfo.getNewStartTime())
                .setEndTime(configInfo.getNewEndTime())
                .setIsOpen(configInfo.getIsOpen());
        return builder.build();
    }



    @Override
    public void editBaseSymbolConfigs(EditBaseSymbolConfigsRequest request, StreamObserver<EditReply> observer) {
        EditReply response;
        try {
            boolean r = baseSymbolConfigService.editSymbolConfigs(request);
            response = EditReply.newBuilder()
                    .setCode(r ? 0 : 1)
                    .build();
        } catch (Exception e) {
            log.error(" editBaseConfig exception:{}", TextFormat.shortDebugString(request), e);
            response = EditReply.getDefaultInstance();
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    @Override
    public void cancelBaseSymbolConfig(CancelBaseSymbolConfigRequest request, StreamObserver<EditReply> observer) {
        EditReply response;
        try {
            boolean r = baseSymbolConfigService.cancelSymbolConfig(request);
            response = EditReply.newBuilder()
                    .setCode(r ? 0 : 1)
                    .build();
        } catch (Exception e) {
            log.error(" cancelConfig exception:{}", TextFormat.shortDebugString(request), e);
            response = EditReply.getDefaultInstance();
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    @Override
    public void getBaseSymbolConfigs(GetBaseSymbolConfigsRequest request, StreamObserver<BaseSymbolConfigsReply> observer) {
        BaseSymbolConfigsReply response;
        try {
            List<BaseSymbolConfigInfo> configInfos = baseSymbolConfigService.getSymbolConfigs(request.getOrgId(), request.getSymbolList(),
                    Arrays.asList(request.getGroup()), request.getKeyList(), request.getLanguage(), request.getPageSize(), request.getLastId());
            if (CollectionUtils.isEmpty(configInfos)) {
                response = BaseSymbolConfigsReply.getDefaultInstance();
            } else {
                response = BaseSymbolConfigsReply.newBuilder()
                        .addAllBaseConfig(configInfos.stream().map(this::convertBaseSymbolConfig).collect(Collectors.toList()))
                        .build();
            }

        } catch (Exception e) {
            log.error(" getOneBaseConfig exception:{}", TextFormat.shortDebugString(request), e);
            response = BaseSymbolConfigsReply.getDefaultInstance();
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    @Override
    public void getBaseSymbolConfigsByGroup(GetBaseConfigsByGroupRequest request, StreamObserver<BaseSymbolConfigsReply> observer) {
        BaseSymbolConfigsReply response;
        try {
            List<BaseSymbolConfigInfo> configInfos = baseSymbolConfigService.getSymbolConfigs(request.getOrgId(), null,
                    request.getGroupList(), null, request.getLanguage(), request.getPageSize(), request.getLastId());
            if (CollectionUtils.isEmpty(configInfos)) {
                response = BaseSymbolConfigsReply.getDefaultInstance();
            } else {
                response = BaseSymbolConfigsReply.newBuilder()
                        .addAllBaseConfig(configInfos.stream().map(this::convertBaseSymbolConfig).collect(Collectors.toList()))
                        .build();
            }

        } catch (Exception e) {
            log.error(" getOneBaseConfig exception:{}", TextFormat.shortDebugString(request), e);
            response = BaseSymbolConfigsReply.getDefaultInstance();
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    @Override
    public void getOneBaseSymbolConfig(GetOneBaseSymbolConfigRequest request, StreamObserver<BaseSymbolConfig> observer) {
        BaseSymbolConfig response;
        try {
            BaseSymbolConfigInfo configInfo = baseSymbolConfigService.getOneSymbolConfig(request.getOrgId(), request.getSymbol(),
                    request.getGroup(), request.getKey(), request.getLanguage(), true);
            response = convertBaseSymbolConfig(configInfo);
        } catch (Exception e) {
            log.error(" getOneBaseConfig exception:{}", TextFormat.shortDebugString(request), e);
            response = BaseSymbolConfig.getDefaultInstance();
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    @Override
    public void getSymbolConfigSwitch(GetSymbolConfigSwitchRequest request, StreamObserver<ConfigSwitchReply> observer) {
        ConfigSwitchReply response;
        try {
            Pair<Boolean, Boolean> pair = baseSymbolConfigService.getSymbolConfigSwitch(request.getOrgId(), request.getSymbol(),
                    request.getGroup(), request.getKey(), request.getLanguage());
            response = ConfigSwitchReply.newBuilder().setExisted(pair.getLeft()).setOpen(pair.getRight()).build();
        } catch (Exception e) {
            log.error("getConfigSwitch exception:{}", TextFormat.shortDebugString(request), e);
            response = ConfigSwitchReply.getDefaultInstance();
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    private BaseSymbolConfig convertBaseSymbolConfig(BaseSymbolConfigInfo configInfo) {
        BaseSymbolConfig.Builder builder = BaseSymbolConfig.newBuilder();
        if (configInfo == null) {
            return builder.build();
        }
        builder.setId(configInfo.getId())
                .setOrgId(configInfo.getOrgId())
                .setSymbol(configInfo.getSymbol())
                .setGroup(configInfo.getConfGroup())
                .setKey(configInfo.getConfKey())
                .setValue(configInfo.getConfValue())
                .setExtraValue(configInfo.getExtraValue())
                .setLanguage(configInfo.getLanguage())
                .setAdminUserName(configInfo.getAdminUserName())
                .setCreated(configInfo.getCreated())
                .setUpdated(configInfo.getUpdated())
                .setNewValue(configInfo.getNewConfValue())
                .setNewExtraValue(configInfo.getNewExtraValue())
                .setStartTime(configInfo.getNewStartTime())
                .setEndTime(configInfo.getNewEndTime())
                .setIsOpen(configInfo.getIsOpen());
        return builder.build();
    }


    @Override
    public void editBaseTokenConfigs(EditBaseTokenConfigsRequest request, StreamObserver<EditReply> observer) {
        EditReply response;
        try {
            boolean r = baseTokenConfigService.editTokenConfigs(request);
            response = EditReply.newBuilder()
                    .setCode(r ? 0 : 1)
                    .build();
        } catch (Exception e) {
            log.error(" editBaseConfig exception:{}", TextFormat.shortDebugString(request), e);
            response = EditReply.getDefaultInstance();
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    @Override
    public void cancelBaseTokenConfig(CancelBaseTokenConfigRequest request, StreamObserver<EditReply> observer) {
        EditReply response;
        try {
            boolean r = baseTokenConfigService.cancelTokenConfig(request);
            response = EditReply.newBuilder()
                    .setCode(r ? 0 : 1)
                    .build();
        } catch (Exception e) {
            log.error(" cancelConfig exception:{}", TextFormat.shortDebugString(request), e);
            response = EditReply.getDefaultInstance();
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    @Override
    public void getBaseTokenConfigs(GetBaseTokenConfigsRequest request, StreamObserver<BaseTokenConfigsReply> observer) {
        BaseTokenConfigsReply response;
        try {
            List<BaseTokenConfigInfo> configInfos = baseTokenConfigService.getTokenConfigs(request.getOrgId(), request.getTokenList(),
                    Arrays.asList(request.getGroup()), request.getKeyList(), request.getLanguage(), request.getPageSize(), request.getLastId());
            if (CollectionUtils.isEmpty(configInfos)) {
                response = BaseTokenConfigsReply.getDefaultInstance();
            } else {
                response = BaseTokenConfigsReply.newBuilder()
                        .addAllBaseConfig(configInfos.stream().map(this::convertBaseTokenConfig).collect(Collectors.toList()))
                        .build();
            }

        } catch (Exception e) {
            log.error(" getOneBaseConfig exception:{}", TextFormat.shortDebugString(request), e);
            response = BaseTokenConfigsReply.getDefaultInstance();
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    @Override
    public void getBaseTokenConfigsByGroup(GetBaseConfigsByGroupRequest request, StreamObserver<BaseTokenConfigsReply> observer) {
        BaseTokenConfigsReply response;
        try {
            List<BaseTokenConfigInfo> configInfos = baseTokenConfigService.getTokenConfigs(request.getOrgId(), null,
                    request.getGroupList(), null, request.getLanguage(), request.getPageSize(), request.getLastId());
            if (CollectionUtils.isEmpty(configInfos)) {
                response = BaseTokenConfigsReply.getDefaultInstance();
            } else {
                response = BaseTokenConfigsReply.newBuilder()
                        .addAllBaseConfig(configInfos.stream().map(this::convertBaseTokenConfig).collect(Collectors.toList()))
                        .build();
            }

        } catch (Exception e) {
            log.error(" getOneBaseConfig exception:{}", TextFormat.shortDebugString(request), e);
            response = BaseTokenConfigsReply.getDefaultInstance();
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    @Override
    public void getOneBaseTokenConfig(GetOneBaseTokenConfigRequest request, StreamObserver<BaseTokenConfig> observer) {
        BaseTokenConfig response;
        try {
            BaseTokenConfigInfo configInfo = baseTokenConfigService.getOneTokenConfig(request.getOrgId(), request.getToken(),
                    request.getGroup(), request.getKey(), request.getLanguage(), true);
            response = convertBaseTokenConfig(configInfo);
        } catch (Exception e) {
            log.error(" getOneBaseConfig exception:{}", TextFormat.shortDebugString(request), e);
            response = BaseTokenConfig.getDefaultInstance();
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    @Override
    public void getTokenConfigSwitch(GetTokenConfigSwitchRequest request, StreamObserver<ConfigSwitchReply> observer) {
        ConfigSwitchReply response;
        try {
            Pair<Boolean, Boolean> pair = baseTokenConfigService.getTokenConfigSwitch(request.getOrgId(), request.getToken(),
                    request.getGroup(), request.getKey(), request.getLanguage());
            response = ConfigSwitchReply.newBuilder().setExisted(pair.getLeft()).setOpen(pair.getRight()).build();
        } catch (Exception e) {
            log.error("getConfigSwitch exception:{}", TextFormat.shortDebugString(request), e);
            response = ConfigSwitchReply.getDefaultInstance();
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    private BaseTokenConfig convertBaseTokenConfig(BaseTokenConfigInfo configInfo) {
        BaseTokenConfig.Builder builder = BaseTokenConfig.newBuilder();
        if (configInfo == null) {
            return builder.build();
        }
        builder.setId(configInfo.getId())
                .setOrgId(configInfo.getOrgId())
                .setToken(configInfo.getToken())
                .setGroup(configInfo.getConfGroup())
                .setKey(configInfo.getConfKey())
                .setValue(configInfo.getConfValue())
                .setExtraValue(configInfo.getExtraValue())
                .setLanguage(configInfo.getLanguage())
                .setAdminUserName(configInfo.getAdminUserName())
                .setCreated(configInfo.getCreated())
                .setUpdated(configInfo.getUpdated())
                .setNewValue(configInfo.getNewConfValue())
                .setNewExtraValue(configInfo.getNewExtraValue())
                .setStartTime(configInfo.getNewStartTime())
                .setEndTime(configInfo.getNewEndTime())
                .setIsOpen(configInfo.getIsOpen());
        return builder.build();
    }

    @Override
    public void getBhConfigMetas(GetConfigMetasRequest request, StreamObserver<GetConfigMetasReply> responseObserver) {
        GetConfigMetasReply reply = bhGrpcConfigService.getBhConfigMetas(request);
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void editBhBaseConfigs(EditBaseConfigsRequest request, StreamObserver<EditReply> responseObserver) {
        EditReply reply = bhGrpcConfigService.editBhBaseConfigs(request);
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void cancelBhBaseConfig(CancelBaseConfigRequest request, StreamObserver<EditReply> responseObserver) {
        EditReply reply = bhGrpcConfigService.cancelBhBaseConfig(request);
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void getBhBaseConfigs(GetBaseConfigsRequest request, StreamObserver<BaseConfigsReply> responseObserver) {
        BaseConfigsReply reply = bhGrpcConfigService.getBhBaseConfigs(request);
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void getOneBhBaseConfig(GetOneBaseConfigRequest request, StreamObserver<BaseConfig> responseObserver) {
        BaseConfig reply = bhGrpcConfigService.getOneBhBaseConfig(request);
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void getBhConfigSwitch(GetConfigSwitchRequest request, StreamObserver<ConfigSwitchReply> responseObserver) {
        ConfigSwitchReply reply = bhGrpcConfigService.getBhConfigSwitch(request);
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void editBhBaseSymbolConfigs(EditBaseSymbolConfigsRequest request, StreamObserver<EditReply> responseObserver) {
        EditReply reply = bhGrpcConfigService.editBhBaseSymbolConfigs(request);
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void cancelBhBaseSymbolConfig(CancelBaseSymbolConfigRequest request, StreamObserver<EditReply> responseObserver) {
        EditReply reply = bhGrpcConfigService.cancelBhBaseSymbolConfig(request);
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void getBhBaseSymbolConfigs(GetBaseSymbolConfigsRequest request, StreamObserver<BaseSymbolConfigsReply> responseObserver) {
        BaseSymbolConfigsReply reply = bhGrpcConfigService.getBhBaseSymbolConfigs(request);
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void getOneBhBaseSymbolConfig(GetOneBaseSymbolConfigRequest request, StreamObserver<BaseSymbolConfig> responseObserver) {
        BaseSymbolConfig reply = bhGrpcConfigService.getOneBhBaseSymbolConfig(request);
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void getBhSymbolConfigSwitch(GetSymbolConfigSwitchRequest request, StreamObserver<ConfigSwitchReply> responseObserver) {
        ConfigSwitchReply reply = bhGrpcConfigService.getBhSymbolConfigSwitch(request);
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void editBhBaseTokenConfigs(EditBaseTokenConfigsRequest request, StreamObserver<EditReply> responseObserver) {
        EditReply reply = bhGrpcConfigService.editBhBaseTokenConfigs(request);
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void cancelBhBaseTokenConfig(CancelBaseTokenConfigRequest request, StreamObserver<EditReply> responseObserver) {
        EditReply reply = bhGrpcConfigService.cancelBhBaseTokenConfig(request);
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void getBhBaseTokenConfigs(GetBaseTokenConfigsRequest request, StreamObserver<BaseTokenConfigsReply> responseObserver) {
        BaseTokenConfigsReply reply = bhGrpcConfigService.getBhBaseTokenConfigs(request);
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void getOneBhBaseTokenConfig(GetOneBaseTokenConfigRequest request, StreamObserver<BaseTokenConfig> responseObserver) {
        BaseTokenConfig reply = bhGrpcConfigService.getOneBhBaseTokenConfig(request);
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void getBhTokenConfigSwitch(GetTokenConfigSwitchRequest request, StreamObserver<ConfigSwitchReply> responseObserver) {
        ConfigSwitchReply reply = bhGrpcConfigService.getBhTokenConfigSwitch(request);
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void getBhBaseConfigsByGroup(GetBaseConfigsByGroupRequest request, StreamObserver<BaseConfigsReply> responseObserver) {
        BaseConfigsReply reply = bhGrpcConfigService.getBhBaseConfigsByGroup(request);
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void getBhBaseSymbolConfigsByGroup(GetBaseConfigsByGroupRequest request, StreamObserver<BaseSymbolConfigsReply> responseObserver) {
        BaseSymbolConfigsReply reply = bhGrpcConfigService.getBhBaseSymbolConfigsByGroup(request);
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void getBhBaseTokenConfigsByGroup(GetBaseConfigsByGroupRequest request, StreamObserver<BaseTokenConfigsReply> responseObserver) {
        BaseTokenConfigsReply reply = bhGrpcConfigService.getBhBaseTokenConfigsByGroup(request);
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }
}
