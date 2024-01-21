package io.bhex.broker.server.grpc.server;

import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.broker.grpc.admin.*;
import io.bhex.broker.server.grpc.server.service.AdminAirdropInfoService;
import io.grpc.stub.StreamObserver;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.admin.grpc.server
 * @Author: ming.xu
 * @CreateDate: 10/11/2018 10:42 PM
 * @Copyright（C）: 2018 BHEX Inc. All rights reserved.
 */
@GrpcService
public class AdminAirdropGrpcService extends AdminAirdropServiceGrpc.AdminAirdropServiceImplBase {

    @Autowired
    private AdminAirdropInfoService airdropInfoService;

    @Override
    public void queryAirdropInfo(QueryAirdropInfoRequest request, StreamObserver<QueryAirdropInfoReply> responseObserver) {
        QueryAirdropInfoReply reply = airdropInfoService.queryAirdropInfo(request);

        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void listScheduleAirdrop(ListScheduleAirdropRequest request, StreamObserver<QueryAirdropInfoReply> responseObserver) {
        QueryAirdropInfoReply reply = airdropInfoService.listScheduleAirdrop();

        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void createAirdropInfo(CreateAirdropInfoRequest request, StreamObserver<CreateAirdropInfoReply> responseObserver) {
        CreateAirdropInfoReply reply = airdropInfoService.createAirdropInfo(request);

        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void getAirdropInfo(GetAirdropInfoRequest request, StreamObserver<AirdropInfo> responseObserver) {
        AirdropInfo reply = airdropInfoService.getAirdropInfo(request);

        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void lockAndAirdrop(LockAndAirdropRequest request, StreamObserver<LockAndAirdropReply> responseObserver) {
        LockAndAirdropReply reply = airdropInfoService.lockAndAirdrop(request);

        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void addTransferRecord(AddTransferRecordRequest request, StreamObserver<AddTransferRecordReply> responseObserver) {
        AddTransferRecordReply reply = airdropInfoService.addTransferRecord(request);

        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void addTmplRecord(AddTmplRecordRequest request, StreamObserver<AddTmplRecordReply> responseObserver) {
        AddTmplRecordReply reply = airdropInfoService.addTmplRecord(request);

        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void listTmplRecords(ListTmplRecordsRequest request, StreamObserver<ListTmplRecordsReply> responseObserver) {
        ListTmplRecordsReply reply = airdropInfoService.listTmplRecords(request);

        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void addAssetSnapshot(AddAssetSnapshotRequest request, StreamObserver<AddAssetSnapshotReply> responseObserver) {
        AddAssetSnapshotReply reply = airdropInfoService.addAssetSnapshot(request);

        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void getTransferGroupInfo(GetTransferGroupInfoRequest request, StreamObserver<TransferGroupInfo> responseObserver) {
        TransferGroupInfo reply = airdropInfoService.getTransferGroupInfo(request);

        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void updateAirdropStatus(UpdateAirdropStatusRequest request, StreamObserver<UpdateAirdropStatusReply> responseObserver) {
        UpdateAirdropStatusReply reply = airdropInfoService.updateAirdropStatus(request);

        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void transferRecordFilter(TransferRecordFilterRequest request, StreamObserver<TransferRecordFilterReply> responseObserver) {
        TransferRecordFilterReply reply = airdropInfoService.transferRecordFilter(request);

        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void listAllTransferGroup(ListAllTransferGroupRequest request, StreamObserver<ListAllTransferGroupReply> responseObserver) {
        ListAllTransferGroupReply reply = airdropInfoService.listAllTransferGroup(request);

        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void updateTransferGroupStatus(UpdateTransferGroupStatusRequest request, StreamObserver<UpdateTransferGroupStatusReply> responseObserver) {
        UpdateTransferGroupStatusReply reply = airdropInfoService.updateTransferGroupStatus(request);

        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void listTransferRecordByGroupId(ListTransferRecordRequest request, StreamObserver<ListTransferRecordReply> responseObserver) {
        ListTransferRecordReply reply = airdropInfoService.listTransferRecordByGroupId(request);

        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }
}
