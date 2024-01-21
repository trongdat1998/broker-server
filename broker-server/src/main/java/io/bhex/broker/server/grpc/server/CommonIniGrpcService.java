package io.bhex.broker.server.grpc.server;

import com.google.common.base.Strings;
import com.google.protobuf.TextFormat;
import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.broker.grpc.common_ini.*;
import io.bhex.broker.server.domain.CommonIni;
import io.bhex.broker.server.grpc.server.service.CommonIniService;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@GrpcService
public class CommonIniGrpcService extends CommonIniServiceGrpc.CommonIniServiceImplBase {

    @Autowired
    private CommonIniService commonIniService;

    @Override
    public void getCommonIni(GetCommonIniRequest request, StreamObserver<GetCommonIniResponse> observer) {
        GetCommonIniResponse response = null;
        try {
            List<CommonIni> commonIniList = commonIniService.getCommonIniList(request.getOrgId(), request.getIniNamesList(), request.getLanguage());
            response = GetCommonIniResponse.newBuilder()
                    .addAllInis(commonIniList.stream().map(this::buildCommonIni).collect(Collectors.toList()))
                    .build();
        } catch (Exception e) {
            log.error(" getCommonIni exception:{}", TextFormat.shortDebugString(request), e);
            response = GetCommonIniResponse.getDefaultInstance();
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    @Override
    public void getCommonIni2(GetCommonIni2Request request, StreamObserver<GetCommonIni2Response> observer) {
        try {
            GetCommonIni2Response response;
            CommonIni commonIni = commonIniService.getCommonIni(request.getOrgId(), request.getIniName(), request.getLanguage());
            if (commonIni != null) {
                response = GetCommonIni2Response.newBuilder().setInis(buildCommonIni(commonIni)).build();
            } else {
                response = GetCommonIni2Response.newBuilder().build();
            }
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("query common_ini error", e);
            observer.onError(e);
        }
    }

    @Override
    public void queryOrgCustomCommonIni(QueryOrgCustomCommonIniRequest request, StreamObserver<QueryCommonIniResponse> observer) {
        try {
            Long orgId = request.getOrgId();
            List<CommonIni> commonIniList = commonIniService.queryOrgCustomCommonIni(orgId);
            QueryCommonIniResponse response = QueryCommonIniResponse.newBuilder()
                    .addAllCommonIni(commonIniList.stream().map(this::buildCommonIni).collect(Collectors.toList()))
                    .build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("query common_ini error", e);
            observer.onError(e);
        }
    }

    @Override
    public void queryOrgCommonIni(QueryOrgCommonIniRequest request, StreamObserver<QueryCommonIniResponse> observer) {
        try {
            Long orgId = request.getOrgId();
            List<CommonIni> commonIniList = commonIniService.queryOrgCommonIni(orgId);
            QueryCommonIniResponse response = QueryCommonIniResponse.newBuilder()
                    .addAllCommonIni(commonIniList.stream().map(this::buildCommonIni).collect(Collectors.toList()))
                    .build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("query common_ini error", e);
            observer.onError(e);
        }
    }

    @Override
    public void queryCommonIni(QueryCommonIniRequest request, StreamObserver<QueryCommonIniResponse> observer) {
        try {
            Long orgId = request.getOrgId();
            List<String> iniNames = request.getIniNameList();
            List<CommonIni> commonIniList = commonIniService.queryCommonIniByOrgIdAndIniNames(orgId, iniNames);
            QueryCommonIniResponse response = QueryCommonIniResponse.newBuilder()
                    .addAllCommonIni(commonIniList.stream().map(this::buildCommonIni).collect(Collectors.toList()))
                    .build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("query common_ini error", e);
            observer.onError(e);
        }
    }

    private io.bhex.broker.grpc.common_ini.CommonIni buildCommonIni(CommonIni commonIni) {
        return io.bhex.broker.grpc.common_ini.CommonIni.newBuilder()
                .setOrgId(commonIni.getOrgId() == null ? 0 : commonIni.getOrgId())
                .setIniName(Strings.nullToEmpty(commonIni.getIniName()))
                .setIniDesc(Strings.nullToEmpty(commonIni.getIniDesc()))
                .setIniValue(Strings.nullToEmpty(commonIni.getIniValue()))
                .setLanguage(Strings.nullToEmpty(commonIni.getLanguage()))
                .build();
    }

    @Override
    public void saveCommonIni(SaveCommonIniRequest request, StreamObserver<SaveCommonIniResponse> observer) {
        try {
            SaveCommonIniResponse response;
            CommonIni commonIni = CommonIni.builder()
                    .orgId(request.getOrgId())
                    .iniName(request.getIniName())
                    .iniDesc(request.getIniDesc())
                    .iniValue(request.getIniValue())
                    .language(Strings.nullToEmpty(request.getLanguage()))
                    .build();
            int affectedRows = commonIniService.insertOrUpdateCommonIni(commonIni);
            if (affectedRows > 0) {
                response = SaveCommonIniResponse.newBuilder().setCommonIni(buildCommonIni(commonIni)).build();
            } else {
                response = SaveCommonIniResponse.getDefaultInstance();
            }
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("query common_ini error", e);
            observer.onError(e);
        }
    }
}
