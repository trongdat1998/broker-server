package io.bhex.broker.server.grpc.server;

import com.google.common.base.Strings;
import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.base.grpc.server.interceptor.GrpcServerLogInterceptor;
import io.bhex.broker.grpc.red_packet.*;
import io.bhex.broker.server.grpc.server.service.BasicService;
import io.bhex.broker.server.grpc.server.service.RedPacketAdminService;
import io.bhex.broker.server.model.RedPacketTheme;
import io.bhex.broker.server.model.RedPacketTokenConfig;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.util.List;
import java.util.stream.Collectors;

@GrpcService(interceptors = GrpcServerLogInterceptor.class)
@Slf4j
public class RedPacketAdminGrpcService extends RedPacketAdminServiceGrpc.RedPacketAdminServiceImplBase {

    @Resource
    private RedPacketAdminService redPacketAdminService;

    @Resource
    private BasicService basicService;

    @Override
    public void openRedPacketFunction(OpenRedPacketFunctionRequest request, StreamObserver<OpenRedPacketFunctionResponse> observer) {
        try {
            redPacketAdminService.openRedPacketFunction(request.getOrgId());
            observer.onNext(OpenRedPacketFunctionResponse.getDefaultInstance());
            observer.onCompleted();
        } catch (Exception e) {
            observer.onError(e);
        }
    }

    @Override
    public void queryRedPacketTheme(QueryRedPacketThemeRequest request, StreamObserver<QueryRedPacketThemeResponse> observer) {
        try {
            List<RedPacketTheme> themeList = redPacketAdminService.queryAllOrgTheme(request.getHeader().getOrgId());
            QueryRedPacketThemeResponse response = QueryRedPacketThemeResponse.newBuilder()
                    .addAllTheme(themeList.stream().map(RedPacketTheme::convertGrpcObj).collect(Collectors.toList()))
                    .build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            observer.onError(e);
        }
    }

    @Override
    public void queryRedPacketTokenConfig(QueryRedPacketTokenConfigRequest request, StreamObserver<QueryRedPacketTokenConfigResponse> observer) {
        try {
            List<RedPacketTokenConfig> tokenConfigList = redPacketAdminService.queryAllOrgRedPacketTokenConfig(request.getHeader().getOrgId());
            QueryRedPacketTokenConfigResponse response = QueryRedPacketTokenConfigResponse.newBuilder()
                    .addAllTokenConfig(tokenConfigList.stream().map(RedPacketTokenConfig::convertGrpcObj).collect(Collectors.toList()))
                    .build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            observer.onError(e);
        }
    }

    @Override
    public void saveOrUpdateRedPacketTheme(SaveOrUpdateRedPacketThemeRequest request, StreamObserver<SaveOrUpdateRedPacketThemeResponse> observer) {
        try {
            io.bhex.broker.grpc.red_packet.RedPacketTheme redPacketThemeGrpcObj = request.getRedPacketTheme();
            RedPacketTheme redPacketTheme = RedPacketTheme.builder()
                    .id(redPacketThemeGrpcObj.getId())
                    .orgId(redPacketThemeGrpcObj.getOrgId())
                    .themeId(redPacketThemeGrpcObj.getThemeId())
                    .themeContent(redPacketThemeGrpcObj.getTheme())
                    .status(redPacketThemeGrpcObj.getStatus())
                    .customOrder(redPacketThemeGrpcObj.getCustomIndex())
                    .position(redPacketThemeGrpcObj.getPosition())
                    .build();
            redPacketAdminService.saveOrUpdateRedPacketTheme(redPacketTheme);
            observer.onNext(SaveOrUpdateRedPacketThemeResponse.getDefaultInstance());
            observer.onCompleted();
        } catch (Exception e) {
            observer.onError(e);
        }
    }

    @Override
    public void saveOrUpdateRedPacketThemes(SaveOrUpdateRedPacketThemesRequest request, StreamObserver<SaveOrUpdateRedPacketThemeResponse> observer) {
        try {
            List<io.bhex.broker.grpc.red_packet.RedPacketTheme> redPacketThemeGrpcObjs = request.getRedPacketThemesList();
            List<RedPacketTheme> redPacketThemes =
                    redPacketThemeGrpcObjs.stream().map(redPacketThemeGrpcObj -> RedPacketTheme.builder()
                            .id(redPacketThemeGrpcObj.getId())
                            .orgId(redPacketThemeGrpcObj.getOrgId())
                            .themeId(redPacketThemeGrpcObj.getThemeId())
                            .themeContent(redPacketThemeGrpcObj.getTheme())
                            .status(redPacketThemeGrpcObj.getStatus())
                            .customOrder(redPacketThemeGrpcObj.getCustomIndex())
                            .position(redPacketThemeGrpcObj.getPosition())
                            .build()).collect(Collectors.toList());
            redPacketAdminService.saveOrUpdateRedPacketThemes(redPacketThemes);
            observer.onNext(SaveOrUpdateRedPacketThemeResponse.getDefaultInstance());
            observer.onCompleted();
        } catch (Exception e) {
            observer.onError(e);
        }
    }

    @Override
    public void saveOrUpdateRedPacketTokenConfig(SaveOrUpdateRedPacketTokenConfigRequest request, StreamObserver<SaveOrUpdateRedPacketTokenConfigResponse> observer) {
        try {
            io.bhex.broker.grpc.red_packet.RedPacketTokenConfig redPacketTokenConfigGrpcObj = request.getTokenConfig();
            RedPacketTokenConfig redPacketTokenConfig = RedPacketTokenConfig.builder()
                    .id(redPacketTokenConfigGrpcObj.getId())
                    .orgId(redPacketTokenConfigGrpcObj.getOrgId())
                    .tokenId(redPacketTokenConfigGrpcObj.getTokenId())
                    .tokenName(basicService.getTokenName(redPacketTokenConfigGrpcObj.getOrgId(), redPacketTokenConfigGrpcObj.getTokenId()))
                    .minAmount(Strings.isNullOrEmpty(redPacketTokenConfigGrpcObj.getMinAmount()) ? BigDecimal.ZERO : new BigDecimal(redPacketTokenConfigGrpcObj.getMinAmount()))
                    .maxAmount(Strings.isNullOrEmpty(redPacketTokenConfigGrpcObj.getMaxAmount()) ? BigDecimal.ZERO : new BigDecimal(redPacketTokenConfigGrpcObj.getMaxAmount()))
                    .maxCount(redPacketTokenConfigGrpcObj.getMaxCount())
                    .maxTotalAmount(Strings.isNullOrEmpty(redPacketTokenConfigGrpcObj.getMaxTotalAmount()) ? BigDecimal.ZERO : new BigDecimal(redPacketTokenConfigGrpcObj.getMaxTotalAmount()))
                    .status(redPacketTokenConfigGrpcObj.getStatus())
                    .customOrder(redPacketTokenConfigGrpcObj.getCustomIndex())
                    .build();
            redPacketAdminService.saveOrUpdateRedPacketTokenConfig(redPacketTokenConfig);
            observer.onNext(SaveOrUpdateRedPacketTokenConfigResponse.getDefaultInstance());
            observer.onCompleted();
        } catch (Exception e) {
            observer.onError(e);
        }
    }

    @Override
    public void changeThemeCustomOrder(ChangeThemeCustomOrderRequest request, StreamObserver<ChangeThemeCustomOrderResponse> observer) {
        try {
            redPacketAdminService.changeThemeOrder(request.getOrgId(), request.getCustomOrderMapMap());
            observer.onNext(ChangeThemeCustomOrderResponse.getDefaultInstance());
            observer.onCompleted();
        } catch (Exception e) {
            observer.onError(e);
        }
    }

    @Override
    public void changeTokenConfigCustomOrder(ChangeTokenConfigCustomOrderRequest request, StreamObserver<ChangeTokenConfigCustomOrderResponse> observer) {
        try {
            redPacketAdminService.changeTokenConfigOrder(request.getOrgId(), request.getCustomOrderMapMap());
            observer.onNext(ChangeTokenConfigCustomOrderResponse.getDefaultInstance());
            observer.onCompleted();
        } catch (Exception e) {
            observer.onError(e);
        }
    }

    @Override
    public void queryRedPacketList(QueryRedPacketListRequest request, StreamObserver<QueryRedPacketListResponse> observer) {
        try {
            List<io.bhex.broker.server.model.RedPacket> redPacketList
                    = redPacketAdminService.queryRedPacketList(request.getOrgId(), request.getUserId(), request.getFromId(), request.getLimit());
            QueryRedPacketListResponse response = QueryRedPacketListResponse.newBuilder()
                    .addAllRedPacket(redPacketList.stream().map(io.bhex.broker.server.model.RedPacket::convertGrpcObj).collect(Collectors.toList()))
                    .build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            observer.onError(e);
        }
    }

    @Override
    public void queryRedPacketReceiveDetailList(QueryRedPacketReceiveDetailListRequest request, StreamObserver<QueryRedPacketReceiveDetailListResponse> observer) {
        try {
            List<io.bhex.broker.server.model.RedPacketReceiveDetail> redPacketReceiveDetailList
                    = redPacketAdminService.queryRedPacketReceiveDetailList(request.getOrgId(), request.getReceiveUserId(), request.getRedPacketId(), request.getFromId(), request.getLimit());
            QueryRedPacketReceiveDetailListResponse response = QueryRedPacketReceiveDetailListResponse.newBuilder()
                    .addAllReceiveDetail(redPacketReceiveDetailList.stream().map(io.bhex.broker.server.model.RedPacketReceiveDetail::convertGrpcObj).collect(Collectors.toList()))
                    .build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            observer.onError(e);
        }
    }

}
