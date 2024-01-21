package io.bhex.broker.server.grpc.server;

import javax.annotation.Resource;

import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.base.grpc.server.interceptor.GrpcServerLogInterceptor;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.grpc.agent.AddAgentUserRequest;
import io.bhex.broker.grpc.agent.AddAgentUserResponse;
import io.bhex.broker.grpc.agent.AgentInfoRequest;
import io.bhex.broker.grpc.agent.AgentInfoResponse;
import io.bhex.broker.grpc.agent.AgentServiceGrpc;
import io.bhex.broker.grpc.agent.AgentUserUpgradeRequest;
import io.bhex.broker.grpc.agent.AgentUserUpgradeResponse;
import io.bhex.broker.grpc.agent.CancelAgentUserRequest;
import io.bhex.broker.grpc.agent.CancelAgentUserResponse;
import io.bhex.broker.grpc.agent.QueryAgentBrokerRequest;
import io.bhex.broker.grpc.agent.QueryAgentBrokerResponse;
import io.bhex.broker.grpc.agent.QueryAgentCommissionListRequest;
import io.bhex.broker.grpc.agent.QueryAgentCommissionListResponse;
import io.bhex.broker.grpc.agent.QueryAgentUserRequest;
import io.bhex.broker.grpc.agent.QueryAgentUserResponse;
import io.bhex.broker.grpc.agent.QueryBrokerAgentListRequest;
import io.bhex.broker.grpc.agent.QueryBrokerAgentListResponse;
import io.bhex.broker.grpc.agent.QueryBrokerUserListRequest;
import io.bhex.broker.grpc.agent.QueryBrokerUserListResponse;
import io.bhex.broker.grpc.agent.RebindAgentUserRequest;
import io.bhex.broker.grpc.agent.RebindAgentUserResponse;
import io.bhex.broker.grpc.agent.UpdateBrokerRequest;
import io.bhex.broker.grpc.agent.UpdateBrokerResponse;
import io.bhex.broker.grpc.common.BasicRet;
import io.bhex.broker.server.grpc.server.service.AgentUserService;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;


@Slf4j
@GrpcService(interceptors = GrpcServerLogInterceptor.class)
public class AgentGrpcService extends AgentServiceGrpc.AgentServiceImplBase {

    @Resource
    private AgentUserService agentUserService;

    @Override
    public void addAgentUser(AddAgentUserRequest request, StreamObserver<AddAgentUserResponse> observer) {
        AddAgentUserResponse response;
        try {
            response = agentUserService.addMasterAgentUser(request);
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = AddAgentUserResponse.newBuilder().setBasicRet(BasicRet.newBuilder().setCode(e.getCode()).build()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("addAgentUser error", e);
            observer.onError(e);
        }
    }

    @Override
    public void queryBrokerAgentList(QueryBrokerAgentListRequest request, StreamObserver<QueryBrokerAgentListResponse> observer) {
        QueryBrokerAgentListResponse response;
        try {
            response = agentUserService.queryBrokerAgentList(request);
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = QueryBrokerAgentListResponse.getDefaultInstance();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("queryBrokerAgentList error", e);
            observer.onError(e);
        }
    }

    @Override
    public void queryBrokerUserList(QueryBrokerUserListRequest request, StreamObserver<QueryBrokerUserListResponse> observer) {
        QueryBrokerUserListResponse response;
        try {
            response = agentUserService.queryBrokerUserList(request);
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = QueryBrokerUserListResponse.getDefaultInstance();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("queryBrokerUserList error", e);
            observer.onError(e);
        }
    }

    @Override
    public void queryAgentUser(QueryAgentUserRequest request, StreamObserver<QueryAgentUserResponse> observer) {
        QueryAgentUserResponse response;
        try {
            response = agentUserService.queryAgentUser(request);
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            BasicRet basicRet = BasicRet.newBuilder().setCode(e.getCode()).build();
            response = QueryAgentUserResponse.newBuilder().setBasicRet(basicRet).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("queryAgentUser error", e);
            observer.onError(e);
        }
    }

    @Override
    public void agentUserUpgrade(AgentUserUpgradeRequest request, StreamObserver<AgentUserUpgradeResponse> observer) {
        AgentUserUpgradeResponse response;
        try {
            response = agentUserService.agentUserUpgrade(request);
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            BasicRet basicRet = BasicRet.newBuilder().setCode(e.getCode()).build();
            response = AgentUserUpgradeResponse.newBuilder().setBasicRet(basicRet).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("agentUserUpgrade error", e);
            observer.onError(e);
        }
    }

    @Override
    public void agentInfo(AgentInfoRequest request, StreamObserver<AgentInfoResponse> observer) {
        AgentInfoResponse response;
        try {
            response = agentUserService.agentInfo(request);
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            BasicRet basicRet = BasicRet.newBuilder().setCode(e.getCode()).build();
            response = AgentInfoResponse.newBuilder().setBasicRet(basicRet).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("agentInfo error", e);
            observer.onError(e);
        }
    }

    @Override
    public void queryAgentBroker(QueryAgentBrokerRequest request, StreamObserver<QueryAgentBrokerResponse> observer) {
        QueryAgentBrokerResponse response;
        try {
            response = agentUserService.queryAgentBroker(request);
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            BasicRet basicRet = BasicRet.newBuilder().setCode(e.getCode()).build();
            response = QueryAgentBrokerResponse.newBuilder().setBasicRet(basicRet).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("queryAgentBroker error", e);
            observer.onError(e);
        }
    }

    @Override
    public void updateBroker(UpdateBrokerRequest request, StreamObserver<UpdateBrokerResponse> observer) {
        UpdateBrokerResponse response;
        try {
            response = agentUserService.updateBroker(request);
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            BasicRet basicRet = BasicRet.newBuilder().setCode(e.getCode()).build();
            response = UpdateBrokerResponse.newBuilder().setBasicRet(basicRet).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("updateBroker error", e);
            observer.onError(e);
        }
    }

    @Override
    public void cancelAgentUser(CancelAgentUserRequest request, StreamObserver<CancelAgentUserResponse> observer) {
        CancelAgentUserResponse response;
        try {
            response = agentUserService.cancelAgentUser(request.getOrgId(), request.getUserId());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            BasicRet basicRet = BasicRet.newBuilder().setCode(e.getCode()).build();
            response = CancelAgentUserResponse.newBuilder().setBasicRet(basicRet).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("cancelAgentUser error", e);
            observer.onError(e);
        }
    }

    @Override
    public void rebindAgentUser(RebindAgentUserRequest request, StreamObserver<RebindAgentUserResponse> observer) {
        RebindAgentUserResponse response;
        try {
            response = agentUserService.rebindAgentUser(request.getOrgId(), request.getUserId(), request.getTargetUserId());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            BasicRet basicRet = BasicRet.newBuilder().setCode(e.getCode()).build();
            response = RebindAgentUserResponse.newBuilder().setBasicRet(basicRet).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("rebindAgentUser error", e);
            observer.onError(e);
        }
    }

    @Override
    public void queryAgentCommissionList(QueryAgentCommissionListRequest request, StreamObserver<QueryAgentCommissionListResponse> observer) {
        QueryAgentCommissionListResponse response;
        try {
            response = agentUserService.queryAgentCommissionList(request.getOrgId(), request.getUserId(), request.getTargetUserId(), request
                    .getTokenId(), request.getFromId(), request.getEndId(), request.getLimit(), request.getIsAdmin(), request.getStartTime(), request.getEndTime());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = QueryAgentCommissionListResponse.getDefaultInstance();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("queryAgentCommissionList error", e);
            observer.onError(e);
        }
    }
}

