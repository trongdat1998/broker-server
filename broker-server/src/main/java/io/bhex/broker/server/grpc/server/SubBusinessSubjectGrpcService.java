package io.bhex.broker.server.grpc.server;

import com.google.common.collect.Maps;
import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.base.grpc.server.interceptor.GrpcServerLogInterceptor;
import io.bhex.broker.grpc.common.Header;
import io.bhex.broker.grpc.sub_business_subject.*;
import io.bhex.broker.server.grpc.server.service.SubBusinessSubjectService;
import io.bhex.broker.server.model.SubBusinessSubject;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@GrpcService(interceptors = GrpcServerLogInterceptor.class)
public class SubBusinessSubjectGrpcService extends SubBusinessSubjectServiceGrpc.SubBusinessSubjectServiceImplBase {

    @Resource
    private SubBusinessSubjectService subBusinessSubjectService;

    @Override
    public void querySubBusinessSubject(QuerySubBusinessSubjectRequest request, StreamObserver<QuerySubBusinessSubjectResponse> observer) {
        try {
            List<SubBusinessSubject> subBusinessSubjectList = subBusinessSubjectService.querySubBusinessSubject(request.getHeader().getOrgId(), request.getParentSubject());
            Map<String, List<SubBusinessSubject>> subjectMap = subBusinessSubjectList.stream()
                    .collect(Collectors.groupingBy(item -> String.format("%s:%s", item.getParentSubject(), item.getSubject())));

            List<io.bhex.broker.grpc.sub_business_subject.SubBusinessSubject> responseItemList = subjectMap.keySet().stream()
                    .map(item -> {
                        List<SubBusinessSubject> itemSubjectList = subjectMap.get(item);
                        Map<String, String> subjectNameMap = Maps.newHashMap();
                        for (SubBusinessSubject subject : itemSubjectList) {
                            subjectNameMap.put(subject.getLanguage(), subject.getSubjectName());
                        }
                        return io.bhex.broker.grpc.sub_business_subject.SubBusinessSubject.newBuilder()
                                .setParentSubject(Integer.parseInt(item.split(":")[0]))
                                .setSubject(Integer.parseInt(item.split(":")[1]))
                                .putAllNames(subjectNameMap)
                                .build();
                    }).collect(Collectors.toList());
            QuerySubBusinessSubjectResponse response = QuerySubBusinessSubjectResponse.newBuilder()
                    .addAllSubBusinessSubject(responseItemList).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            observer.onError(e);
        }
    }

    @Override
    public void replaceSubBusinessSubject(ReplaceSubBusinessSubjectRequest request, StreamObserver<ReplaceSubBusinessSubjectResponse> observer) {
        try {
            Header header = request.getHeader();
            subBusinessSubjectService.saveSubBusinessSubject(header.getOrgId(), request.getParentSubject(), request.getSubject(), request.getNamesMap(), request.getStatus());
            observer.onNext(ReplaceSubBusinessSubjectResponse.getDefaultInstance());
            observer.onCompleted();
        } catch (Exception e) {
            observer.onError(e);
        }
    }

}
