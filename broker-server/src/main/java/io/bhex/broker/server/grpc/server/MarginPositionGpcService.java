package io.bhex.broker.server.grpc.server;

import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.base.grpc.server.interceptor.GrpcServerLogInterceptor;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.grpc.margin.*;
import io.bhex.broker.server.grpc.server.service.MarginPositionService;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Resource;

/**
 * @author JinYuYuan
 * @description
 * @date 2020-06-10 16:09
 */
@Slf4j
@GrpcService(interceptors = GrpcServerLogInterceptor.class)
public class MarginPositionGpcService extends MarginPositionServiceGrpc.MarginPositionServiceImplBase {
    @Resource
    MarginPositionService marginPositionService;

    @Override
    public void loan(LoanRequest request, StreamObserver<LoanResponse> responseObserver) {
        LoanResponse response;
        try {
            response = marginPositionService.loan(request.getHeader(), request.getClientOrderId(), request.getTokenId(),
                    request.getLoanAmount(), request.getAccountId());
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = LoanResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("getInterestConfig error", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void repayByLoanId(RepayByLoanIdRequest request,
                              StreamObserver<RepayByLoanIdResponse> responseObserver) {
        RepayByLoanIdResponse response;
        try {
            response = marginPositionService.repayByLoanId(request.getHeader(), request.getClientOrderId(), request.getRepayAmount(), request.getAccountId()
                    , request.getLoanOrderId(), request.getRepayType());
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = RepayByLoanIdResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("getInterestConfig error", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void getAvailWithdrawAmount(GetAvailWithdrawAmountRequest request,
                                       StreamObserver<GetAvailWithdrawAmountResponse> responseObserver) {
        GetAvailWithdrawAmountResponse response;
        try {
            response = marginPositionService.getAvailWithdrawAmount(request.getHeader(), request.getTokenId(), request.getAccountId());
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = GetAvailWithdrawAmountResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("getInterestConfig error", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void getAllCrossLoanPosition(GetAllCrossLoanPositionRequest request, StreamObserver<GetAllCrossLoanPositionResponse> responseObserver) {
        GetAllCrossLoanPositionResponse response;
        try {
            response = marginPositionService.getAllCrossLoanPosition(request.getHeader(), request.getAccountId(), request.getTokenId());
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = GetAllCrossLoanPositionResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("getInterestConfig error", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void getAllPosition(GetAllPositionRequest request, StreamObserver<GetAllPositionResponse> responseObserver) {
        GetAllPositionResponse response;
        try {
            response = marginPositionService.getAllPosition(request.getHeader(), request.getAccountId());
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = GetAllPositionResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("getInterestConfig error", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void repayAllByLoanId(RepayAllByLoanIdRequest request, StreamObserver<RepayAllByLoanIdResponse> responseObserver) {
        RepayAllByLoanIdResponse response;
        try {
            response = marginPositionService.repayAllByLoanId(request.getHeader(), request.getAccountId(), request.getClientOrderId(), request.getLoanOrderId(), request.getExchangeId());
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = RepayAllByLoanIdResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("repayAllByLoanId error", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void recalculationRoundProfit(RecalculationRoundProfitRequest request, StreamObserver<RecalculationRoundProfitResponse> responseObserver) {
        RecalculationRoundProfitResponse response;
        try {
            response = marginPositionService.recalculationRoundProfit(request.getHeader(),false);
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = RecalculationRoundProfitResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("recalculationRoundProfit error", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void getMarginProfit(GetMarginProfitRequest request, StreamObserver<GetMarginProfitResponse> responseObserver) {
        GetMarginProfitResponse response;
        try {
            response = marginPositionService.getMarginProfit(request.getHeader());
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = GetMarginProfitResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("getMarginProfit error", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void submitProfitActivity(SubmitProfitActivityRequest request, StreamObserver<SubmitProfitActivityResponse> responseObserver) {
        SubmitProfitActivityResponse response;
        try {
            response = marginPositionService.submitProfitActivity(request.getHeader());
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = SubmitProfitActivityResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("submitProfitActivity error", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void queryProfitActivityRank(QueryProfitActivityRankRequest request, StreamObserver<QueryProfitActivityRankResponse> responseObserver) {
        QueryProfitActivityRankResponse response;
        try {
            response = marginPositionService.queryProfitActivityRank(request.getHeader(),request.getJoinDate(),request.getAccountId(),request.getLimit());
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = QueryProfitActivityRankResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("queryProfitActivityRank error", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void adminQueryProfitActivity(AdminQueryProfitActivityRequest request, StreamObserver<AdminQueryProfitActivityResponse> responseObserver) {
        AdminQueryProfitActivityResponse response;
        try {
            response = marginPositionService.adminQueryProfitActivity(request.getHeader(),request.getBeginDate(),request.getEndDate(),request.getJoinStatus(),request.getFromId(),request.getAccountId(),request.getLimit());
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = AdminQueryProfitActivityResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("queryProfitActivityRank error", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void adminSortProfitRanking(AdminSortProfitRankingRequest request, StreamObserver<AdminSortProfitRankingResponse> responseObserver) {
        try {
            marginPositionService.sortProfitActivity(request.getHeader());
            responseObserver.onNext(AdminSortProfitRankingResponse.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            AdminSortProfitRankingResponse response = AdminSortProfitRankingResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("adminSortProfitRanking error", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void adminRecalTopProfitRate(AdminRecalTopProfitRateRequst request, StreamObserver<AdminRecalTopProfitRateResponse> responseObserver) {
        try {
            marginPositionService.adminRecalculationTopProfit(request.getHeader(),request.getJoinDate(),request.getTop());
            responseObserver.onNext(AdminRecalTopProfitRateResponse.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            AdminRecalTopProfitRateResponse response = AdminRecalTopProfitRateResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("adminRecalTopProfitRate error", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void adminSetProfitRanking(AdminSetProfitRankingRequest request, StreamObserver<AdminSetProfitRankingResponse> responseObserver) {
        AdminSetProfitRankingResponse response;
        try {
            response = marginPositionService.adminSetProfitRanking(request.getHeader(),request.getJoinDate(),request.getRanking());
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = AdminSetProfitRankingResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("adminSetProfitRanking error", e);
            responseObserver.onError(e);
        }
    }
}
