/**********************************
 *@项目名称: server-parent
 *@文件名称: io.bhex.broker.server.grpc.server
 *@Date 2018/10/15
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
package io.bhex.broker.server.grpc.server;

import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.broker.grpc.app_config.*;
import io.bhex.broker.grpc.common.DeviceTypeEnum;
import io.bhex.broker.grpc.common.Header;
import io.bhex.broker.server.grpc.server.service.AppConfigService;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Resource;
import java.util.List;

@GrpcService
@Slf4j
public class AppConfigGrpcService extends AppConfigServiceGrpc.AppConfigServiceImplBase {

    @Resource
    private AppConfigService appConfigService;

    @Override
    public void checkAppVersion(CheckAppVersionRequest request, StreamObserver<CheckAppVersionResponse> responseObserver) {
        ServerCallStreamObserver<CheckAppVersionResponse> observer = (ServerCallStreamObserver<CheckAppVersionResponse>) responseObserver;
        observer.setCompression("gzip");
        CheckAppVersionResponse response;
        try {
            response = appConfigService.checkAppVersion(request.getHeader(), request.getAppId(), request.getAppVersion(),
                    request.getAppChannel(), request.getDeviceType());
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("checkAppVersion exception", e);
            observer.onError(e);
        }
    }

    @Override
    public void queryAllAppVersionConfig(QueryAllAppVersionConfigRequest request, StreamObserver<QueryAllAppVersionConfigResponse> responseObserver) {
        ServerCallStreamObserver<QueryAllAppVersionConfigResponse> observer = (ServerCallStreamObserver<QueryAllAppVersionConfigResponse>) responseObserver;
        observer.setCompression("gzip");
        QueryAllAppVersionConfigResponse response;
        try {
            response = appConfigService.queryAllAppVersionConfig(request.getHeader());
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("queryAllAppVersionConfig exception", e);
            observer.onError(e);
        }
    }

    @Override
    public void editAppIndexModule(EditAppIndexModuleRequest request, StreamObserver<EditAppIndexIconResponse> observer) {
        EditAppIndexIconResponse response;
        try {
            response = appConfigService.editAppIndexModule(request);
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("editAppIndexModule exception", e);
            observer.onError(e);
        }
    }

    @Override
    public void listAppIndexModules(ListAppIndexModulesRequest request, StreamObserver<ListAppIndexModulesResponse> observer) {
        ListAppIndexModulesResponse response;
        try {
            response = appConfigService.listAppIndexModules(request);
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("listAppIndexModules exception", e);
            observer.onError(e);
        }
    }

    @Override
    public void queryAllAppIndexFunctionConfig(QueryAllAppIndexFunctionConfigRequest request, StreamObserver<QueryAllAppIndexFunctionConfigResponse> responseObserver) {
        ServerCallStreamObserver<QueryAllAppIndexFunctionConfigResponse> observer = (ServerCallStreamObserver<QueryAllAppIndexFunctionConfigResponse>) responseObserver;
        observer.setCompression("gzip");
        QueryAllAppIndexFunctionConfigResponse response;
        try {
            response = appConfigService.queryAllAppIndexFunctionConfig(request.getHeader());
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("queryAllAppIndexFunctionConfig exception", e);
            observer.onError(e);
        }
    }

    @Override
    public void saveAppDownloadInfo(AppDownloadInfoRequest request, StreamObserver<SaveAppDownloadInfoResponse> observer) {
        SaveAppDownloadInfoResponse response;
        try {
            response = appConfigService.saveNewVersion(request.getHeader().getOrgId(), request);
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("queryAllAppIndexFunctionConfig exception", e);
            observer.onError(e);
        }
    }

    @Override
    public void getAppDownloadInfos(Header request, StreamObserver<AppDownloadInfos> observer) {
        try {
            List<AppDownloadInfo> infos = appConfigService.getAppDownloadInfos(request.getOrgId());
            List<io.bhex.broker.grpc.app_config.AppDownloadLocaleInfo> localeInfos = appConfigService.getAppDownloadLocales(request.getOrgId());
            observer.onNext(AppDownloadInfos.newBuilder()
                    .addAllAppDownloadInfo(infos)
                    .addAllLocaleInfo(localeInfos)
                    .build());
            observer.onCompleted();
        } catch (Exception e) {
            log.error("queryAllAppIndexFunctionConfig exception", e);
            observer.onError(e);
        }
    }

    @Override
    public void queryAllDownloadInfo(Header header, StreamObserver<AppDownloadInfos> responseObserver) {
        ServerCallStreamObserver<AppDownloadInfos> observer = (ServerCallStreamObserver<AppDownloadInfos>) responseObserver;
        observer.setCompression("gzip");
        AppDownloadInfos response;
        try {
            response = appConfigService.queryAllDownInfo(header);
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("queryAllDownloadInfo exception", e);
            observer.onError(e);
        }
    }


    @Override
    public void saveAppUpdateInfo(SaveAppUpdateInfoRequest request, StreamObserver<SaveAppUpdateInfoResponse> observer) {
        SaveAppUpdateInfoResponse response;
        try {
            appConfigService.saveAppUpdateInfo(request);
            observer.onNext(SaveAppUpdateInfoResponse.newBuilder().setRet(0).build());
            observer.onCompleted();
        } catch (Exception e) {
            log.error("saveAppUpdateInfo exception", e);
            observer.onError(e);
        }
    }

    @Override
    public void queryAppUpdateLogs(QueryAppUpdateLogsRequest request, StreamObserver<QueryAppUpdateLogsResponse> observer) {
        try {
            List<AppUpdateInfo> list = appConfigService.queryAppUpdateLogs(request.getHeader().getOrgId());
            List<String> androidVersions = appConfigService.getAllVersions(request.getHeader().getOrgId(), DeviceTypeEnum.ANDROID.name().toLowerCase());
            List<String> iosVersions = appConfigService.getAllVersions(request.getHeader().getOrgId(), DeviceTypeEnum.IOS.name().toLowerCase());

            observer.onNext(QueryAppUpdateLogsResponse.newBuilder().setRet(0)
                    .addAllAppUpdateLog(list)
                    .addAllAndroidVersions(androidVersions)
                    .addAllIosVersions(iosVersions)
                    .build());
            observer.onCompleted();
        } catch (Exception e) {
            log.error("queryAllDownloadInfo exception", e);
            observer.onError(e);
        }
    }
}
