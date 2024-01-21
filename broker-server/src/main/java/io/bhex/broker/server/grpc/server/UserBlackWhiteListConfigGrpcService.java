package io.bhex.broker.server.grpc.server;

import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.base.grpc.server.interceptor.GrpcServerLogInterceptor;
import io.bhex.broker.grpc.bwlist.*;
import io.bhex.broker.server.domain.UserBlackWhiteListConfig;
import io.bhex.broker.server.grpc.server.service.UserBlackWhiteListConfigService;
import io.bhex.broker.server.util.BeanCopyUtils;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.CollectionUtils;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @Description:
 * @Date: 2019/8/23 下午4:07
 * @Author: liwei
 * @Copyright（C）: 2018 BlueHelix Inc. All rights reserved.
 */

@Slf4j
@GrpcService(interceptors = GrpcServerLogInterceptor.class)
public class UserBlackWhiteListConfigGrpcService extends UserBlackWhiteListConfigServiceGrpc.UserBlackWhiteListConfigServiceImplBase {


    @Resource
    private UserBlackWhiteListConfigService userBlackWhiteListConfigService;

    @Override
    public void getUserBlackWhiteConfig(GetUserBlackWhiteConfigRequest request, StreamObserver<UserBlackWhiteConfig> observer) {
        UserBlackWhiteConfig config = userBlackWhiteListConfigService.getBlackWhiteConfig(request);
        observer.onNext(config);
        observer.onCompleted();
    }

    @Override
    public void editUserBlackWhiteConfig(EditUserBlackWhiteConfigRequest request, StreamObserver<EditUserBlackWhiteConfigResponse> observer) {
        userBlackWhiteListConfigService.edit(request);
        observer.onNext(EditUserBlackWhiteConfigResponse.newBuilder().setRet(0).build());
        observer.onCompleted();
    }

    @Override
    public void getBlackWhiteConfigs(GetBlackWhiteConfigsRequest request, StreamObserver<GetBlackWhiteConfigsResponse> observer) {
        List<UserBlackWhiteListConfig> configs = userBlackWhiteListConfigService.getBlackWhiteConfigs(request.getOrgId(), request.getListTypeValue(),
                request.getBwTypeValue(), request.getPageSize(), request.getFromId(), request.getUserId());
        List<UserBlackWhiteConfig> result = new ArrayList<>();
        if (!CollectionUtils.isEmpty(configs)) {
            result = configs.stream().map(config -> {
                UserBlackWhiteConfig.Builder builder = UserBlackWhiteConfig.newBuilder();
                BeanCopyUtils.copyPropertiesIgnoreNull(config, builder);
                builder.setBwTypeValue(config.getBwType());
                builder.setListTypeValue(config.getListType());
                return builder.build();
            }).collect(Collectors.toList());
        }
        observer.onNext(GetBlackWhiteConfigsResponse.newBuilder().addAllConfigs(result).build());
        observer.onCompleted();
    }
}
