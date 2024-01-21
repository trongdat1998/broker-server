package io.bhex.broker.server.grpc.server;

import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.base.grpc.server.interceptor.GrpcServerLogInterceptor;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.grpc.margin.*;
import io.bhex.broker.server.grpc.server.service.MarginService;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Resource;
import java.util.List;

/**
 * @author JinYuYuan
 * @description
 * @date 2020-05-28 15:48
 */
@Slf4j
@GrpcService(interceptors = GrpcServerLogInterceptor.class)
public class MarginGrpcService extends MarginServiceGrpc.MarginServiceImplBase {

    @Resource
    MarginService marginService;

    @Override
    public void setTokenConfig(SetTokenConfigRequest request,
                               StreamObserver<SetTokenConfigResponse> responseObserver) {
        SetTokenConfigResponse response;
        try {
            marginService.setTokenConfig(request.getHeader(), request.getTokenId(), request.getConvertRate(), request.getLeverage(), request.getCanBorrow(),
                    request.getMaxQuantity(), request.getMinQuantity(), request.getQuantityPrecision(), request.getRepayMinQuantity(), request.getIsOpen(), request.getShowInterestPeriod());
            response = SetTokenConfigResponse.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = SetTokenConfigResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("setTokenConfig error", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void getTokenConfig(GetTokenConfigRequest request,
                               StreamObserver<GetTokenConfigResponse> responseObserver) {
        GetTokenConfigResponse response;
        try {
            List<TokenConfig> result = marginService.getTokenConfig(request.getHeader(), request.getTokenId());
            response = GetTokenConfigResponse.newBuilder().addAllTokenConfig(result).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = GetTokenConfigResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("getTokenConfig error", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void setRiskConfig(SetRiskConfigRequest request,
                              StreamObserver<SetRiskConfigResponse> responseObserver) {
        SetRiskConfigResponse response;
        try {
            marginService.setRiskConfig(request.getHeader(), request.getWithdrawLine(), request.getWarnLine(), request.getAppendLine(), request.getStopLine()
                    , request.getMaxLoanLimiit(), request.getNotifyType(), request.getNotifyNumber(), request.getMaxLoanLimitVip1(), request.getMaxLoanLimitVip2(), request.getMaxLoanLimitVip3());
            response = SetRiskConfigResponse.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = SetRiskConfigResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("setRiskConfig error", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void getRiskConfig(GetRiskConfigRequest request,
                              StreamObserver<GetRiskConfigResponse> responseObserver) {
        GetRiskConfigResponse response;
        try {
            List<RiskConfig> result = marginService.getRiskConfig(request.getHeader());
            response = GetRiskConfigResponse.newBuilder().addAllRiskConfig(result).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = GetRiskConfigResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("getRiskConfig error", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void setInterestConfig(SetInterestConfigRequest request,
                                  StreamObserver<SetInterestConfigResponse> responseObserver) {
        SetInterestConfigResponse response;
        try {
            marginService.setInterestConfig(request.getHeader(), request.getTokenId(), request.getInterest(), request.getInterestPeriod(), request.getCalculationPeriod(), request.getSettlementPeriod());
            response = SetInterestConfigResponse.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = SetInterestConfigResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("setInterestConfig error", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void getInterestConfig(GetInterestConfigRequest request,
                                  StreamObserver<GetInterestConfigResponse> responseObserver) {
        GetInterestConfigResponse response;
        try {
            List<InterestConfig> result = marginService.getInterestConfig(request.getHeader(), request.getTokenId());
            response = GetInterestConfigResponse.newBuilder().addAllInterestConfig(result).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = GetInterestConfigResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("getInterestConfig error", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void setMarginSymbol(SetMarginSymbolRequest request,
                                StreamObserver<SetMarginSymbolResponse> responseObserver) {
        SetMarginSymbolResponse response;
        try {
            marginService.setMarginSymbol(request.getHeader(), request.getSymbolId(), request.getAllowMargin());
            response = SetMarginSymbolResponse.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = SetMarginSymbolResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("setMarginSymbol error", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void getPoolAccount(GetPoolAccountRequest request, StreamObserver<GetPoolAccountResponse> responseObserver) {
        GetPoolAccountResponse response;
        try {
            PoolAccount result = marginService.getPoolAccount(request.getHeader());
            response = GetPoolAccountResponse.newBuilder()
                    .setId(result.getId())
                    .setOrgId(result.getOrgId())
                    .setAccountId(result.getAccountId())
                    .setAccountName(result.getAccountName())
                    .setAccountType(result.getAccountType())
                    .setAccountIndex(result.getAccountIndex())
                    .setAuthorizedOrg(result.getAuthorizedOrg())
                    .addAllToken(result.getTokenList())
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = GetPoolAccountResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("getPoolAccount error", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void getCrossLoanOrder(GetCrossLoanOrderRequest request,
                                  StreamObserver<GetCrossLoanOrderResponse> responseObserver) {
        GetCrossLoanOrderResponse response;
        try {
            List<CrossLoanOrder> result = marginService.getCrossLoanOrder(request.getHeader(),
                    request.getAccountId(), request.getTokenId(),
                    request.getLoanId(), request.getStatus(),
                    request.getFromLoanId(), request.getEndLoanId(),
                    request.getLimit());
            response = GetCrossLoanOrderResponse.newBuilder().addAllCrossLoanOrder(result).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = GetCrossLoanOrderResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("getCrossLoanOrder error", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void getRepayRecord(GetRepayRecordRequest request,
                               StreamObserver<GetRepayRecordResponse> responseObserver) {
        GetRepayRecordResponse response;
        try {
            List<RepayRecord> result = marginService.getRepayRecord(request.getHeader(),
                    request.getAccountId(), request.getTokenId(),
                    request.getLoanOrderId(), request.getFromRepayId(),
                    request.getEndRepayId(), request.getLimit());
            response = GetRepayRecordResponse.newBuilder().addAllRepayRecord(result).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = GetRepayRecordResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("getRepayRecord error", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void getMarginSafety(GetMarginSafetyRequest request, StreamObserver<GetMarginSafetyResponse> responseObserver) {
        GetMarginSafetyResponse response;
        try {
            MarginSafety result = marginService.getMarginSafety(request.getHeader(),
                    request.getAccountId());
            response = GetMarginSafetyResponse.newBuilder().setMarginSafety(result).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = GetMarginSafetyResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("getMarginSafety error", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void getMarginAsset(GetMarginAssetRequest request, StreamObserver<GetMarginAssetResponse> responseObserver) {
        GetMarginAssetResponse response;
        try {
            List<MarginAsset> result = marginService.getMarginAsset(request.getHeader(),
                    request.getAccountIndex(), request.getTokenIds());
            response = GetMarginAssetResponse.newBuilder().addAllMarginAsset(result).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = GetMarginAssetResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("getMarginAsset error", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void getFundingCross(GetFundingCrossRequest request, StreamObserver<GetFundingCrossResponse> responseObserver) {
        GetFundingCrossResponse response;
        try {
            List<FundingCross> result = marginService.getFundingCross(request.getHeader(),
                    request.getAccountId(), request.getTokenId());
            response = GetFundingCrossResponse.newBuilder().addAllFundingCross(result).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = GetFundingCrossResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("getFundingCross error", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void queryCoinPool(QueryCoinPoolRequest request, StreamObserver<QueryCoinPoolResponse> responseObserver) {
        QueryCoinPoolResponse response;
        try {
            List<CoinPool> result = marginService.queryCoinPool(request.getHeader());
            response = QueryCoinPoolResponse.newBuilder().addAllCoinPool(result).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = QueryCoinPoolResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("queryCoinPool error", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void queryUserRisk(QueryUserRiskRequest request, StreamObserver<QueryUserRiskResponse> responseObserver) {
        QueryUserRiskResponse response;
        try {
            List<UserRisk> result = marginService.queryUserRisk(request.getHeader());
            response = QueryUserRiskResponse.newBuilder().addAllUserRisk(result).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = QueryUserRiskResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("queryUserRisk error", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void statisticsUserRisk(StatisticsUserRiskRequest request, StreamObserver<StatisticsUserRiskResponse> responseObserver) {
        StatisticsUserRiskResponse response;
        try {
            UserRiskSum result = marginService.statisticsUserRisk(request.getHeader());
            response = StatisticsUserRiskResponse.newBuilder().setUserRiskSum(result).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = StatisticsUserRiskResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("statisticsUserRisk error", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void forceClose(ForceCloseRequest request, StreamObserver<ForceCloseResponse> responseObserver) {
        ForceCloseResponse response;
        try {
            marginService.forceClose(request.getHeader(), request.getAccountId(), request.getAdminUserId(), request.getDesc());
            response = ForceCloseResponse.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = ForceCloseResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("forceClose error", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void statisticsRisk(StatisticsRiskRequest request, StreamObserver<StatisticsRiskResponse> responseObserver) {
        StatisticsRiskResponse response;
        try {
            RiskSum result = marginService.statisticsRisk(request.getHeader());
            response = StatisticsRiskResponse.newBuilder().setRiskSum(result).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = StatisticsRiskResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("statisticsRisk error", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void queryRptDailyStatisticsRisk(io.bhex.broker.grpc.margin.QueryRptDailyStatisticsRiskRequest request,
                                            io.grpc.stub.StreamObserver<io.bhex.broker.grpc.margin.QueryRptDailyStatisticsRiskResponse> responseObserver) {
        try {
            responseObserver.onNext(marginService.queryRptDailyStatisticsRisk(request.getHeader(), request.getToTime(), request.getLimit()));
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            responseObserver.onNext(QueryRptDailyStatisticsRiskResponse.newBuilder().setRet(e.getCode()).build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("DailyStatisticsRiskResponse error", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void queryPositionDetail(QueryUserPositionRequest request, StreamObserver<QueryUserPositionResponse> responseObserver) {
        QueryUserPositionResponse response;
        try {
            List<PositionDetail> result = marginService.queryPositionDetail(request.getHeader(), request.getAccountId());
            response = QueryUserPositionResponse.newBuilder().addAllPositionDetail(result).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = QueryUserPositionResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("queryPositionDetail error", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void queryForceClose(QueryForceCloseRequest request, StreamObserver<QueryForceCloseResponse> responseObserver) {
        QueryForceCloseResponse response;
        try {
            List<ForceClose> result = marginService.queryForceClose(request.getHeader(), request.getAccountId());
            response = QueryForceCloseResponse.newBuilder().addAllForceClose(result).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = QueryForceCloseResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("queryForceClose error", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void getLoanable(GetLoanableRequest request, StreamObserver<GetLoanableResponse> responseObserver) {
        GetLoanableResponse response;
        try {
            response = marginService.getLoanable(request.getHeader(), request.getAccountId(), request.getTokenId());
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = GetLoanableResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("getLoanable error", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void getLevelInterestByUserId(GetLevelInterestByUserIdRequest request, StreamObserver<GetLevelInterestByUserIdResponse> responseObserver) {
        GetLevelInterestByUserIdResponse response;
        try {
            response = marginService.getLevelInterestByUserId(request.getHeader(), request.getTokenId());
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = GetLevelInterestByUserIdResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("getLevelInterestByUserId error", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void queryInterestByLevel(QueryInterestByLevelRequest request, StreamObserver<QueryInterestByLevelResponse> responseObserver) {
        QueryInterestByLevelResponse response;
        try {
            response = marginService.queryInterestByLevel(request.getHeader(), request.getTokenId(), request.getLevelConfigId());
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = QueryInterestByLevelResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("getLevelInterestByUserId error", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void queryLoanUserList(QueryLoanUserListRequest request, StreamObserver<QueryLoanUserListResponse> responseObserver) {
        QueryLoanUserListResponse response;
        try {
            List<UserRisk> result = marginService.queryLoanUserList(request.getHeader(), request.getIsAll());
            response = QueryLoanUserListResponse.newBuilder().addAllUserRisk(result).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = QueryLoanUserListResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("queryLoanUserList error", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void adminQueryForceRecord(AdminQueryForceRecordRequest request, StreamObserver<AdminQueryForceRecordResponse> responseObserver) {
        AdminQueryForceRecordResponse response;
        try {
            List<ForceRecord> list = marginService.adminQueryForceRecord(request.getHeader(), request.getAccountId(), request.getFromId(), request.getToId(),
                    request.getStartTime(), request.getEndTime(), request.getLimit());
            response = AdminQueryForceRecordResponse.newBuilder().addAllForceRecords(list).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = AdminQueryForceRecordResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("adminQueryForceRecord error", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void adminQueryAccountStatus(AdminQueryAccountStatusRequest request, StreamObserver<AdminQueryAccountStatusResponse> responseObserver) {
        AdminQueryAccountStatusResponse response;
        try {
            response = marginService.adminQueryAccountStatus(request.getHeader());
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = AdminQueryAccountStatusResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("adminQueryAccountStatus error", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void adminChangeMarginPositionStatus(AdminChangeMarginPositionStatusRequest request, StreamObserver<AdminChangeMarginPositionStatusResponse> responseObserver) {
        AdminChangeMarginPositionStatusResponse response;
        try {
            response = marginService.adminChangeMarginPositionStatus(request.getHeader(), request.getChangeToStatus(), request.getCurStatus());
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = AdminChangeMarginPositionStatusResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("adminChangeMarginPositionStatus error", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void queryAccountLoanLimitVIP(QueryAccountLoanLimitVIPRequest request, StreamObserver<QueryAccountLoanLimitVIPResponse> responseObserver) {
        QueryAccountLoanLimitVIPResponse response;
        try {
            List<AccountLoanLimitLevel> list = marginService.queryAccountLoanLimitLevel(request.getHeader(), request.getVipLevel());
            response = QueryAccountLoanLimitVIPResponse.newBuilder().addAllDatas(list).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = QueryAccountLoanLimitVIPResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("queryAccountLoanLimitVIP error", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void setAccountLoanLimitVIP(SetAccountLoanLimitVIPRequest request, StreamObserver<SetAccountLoanLimitVIPResponse> responseObserver) {
        SetAccountLoanLimitVIPResponse response;
        try {
            response = marginService.setAccountLoanLimitVIP(request.getHeader(), request.getVipLevel(), request.getUserIds());
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = SetAccountLoanLimitVIPResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("setAccountLoanLimitVIP error", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void deleteAccountLoanLimitVIP(DeleteAccountLoanLimitVIPRequest request, StreamObserver<DeleteAccountLoanLimitVIPResponse> responseObserver) {
        DeleteAccountLoanLimitVIPResponse response;
        try {
            response = marginService.deleteAccountLoanLimitVIP(request.getHeader());
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = DeleteAccountLoanLimitVIPResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("deleteAccountLoanLimitVIP error", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void queryRptMarginPool(QueryRptMarginPoolRequest request, StreamObserver<QueryRptMarginPoolResponse> responseObserver) {
        QueryRptMarginPoolResponse response;
        try {
            response = marginService.queryRptMarginPool(request.getHeader(), request.getTokenId());
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = QueryRptMarginPoolResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("queryRptMarginPool error", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void queryMarginRiskBlackList(QueryMarginRiskBlackListRequest request, StreamObserver<QueryMarginRiskBlackListResponse> responseObserver) {
        QueryMarginRiskBlackListResponse response;
        try {
            List<MarginRiskBlack> results = marginService.queryMarginRiskBlack(request.getHeader(), request.getConfGroup());
            response = QueryMarginRiskBlackListResponse.newBuilder().addAllData(results).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = QueryMarginRiskBlackListResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("queryMarginRiskBlackList error", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void addMarginRiskBlackList(AddMarginRiskBlackListRequest request, StreamObserver<AddMarginRiskBlackListResponse> responseObserver) {
        AddMarginRiskBlackListResponse response;
        try {
            response = marginService.addMarginRiskBlackList(request.getHeader(), request.getConfGroup(), request.getAdminUserName(), request.getReason());
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = AddMarginRiskBlackListResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("addMarginRiskBlackList error", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void delMarginRiskBlackList(DelMarginRiskBlackListRequest request, StreamObserver<DelMarginRiskBlackListResponse> responseObserver) {
        DelMarginRiskBlackListResponse response;
        try {
            response = marginService.delMarginRiskBlackList(request.getHeader(), request.getConfGroup());
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = DelMarginRiskBlackListResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("delMarginRiskBlackList error", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void queryRptMarginTrade(QueryRptMarginTradeRequest request, StreamObserver<QueryRptMarginTradeResponse> responseObserver) {
        QueryRptMarginTradeResponse response;
        try {
            response = marginService.queryRptMarginTrade(request.getHeader(), request.getToTime(), request.getLimit());
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = QueryRptMarginTradeResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("queryRptMarginTrade error", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void queryRptMarginTradeDetail(QueryRptMarginTradeDetailRequest request, StreamObserver<QueryRptMarginTradeDetailResponse> responseObserver) {
        QueryRptMarginTradeDetailResponse response;
        try {
            response = marginService.queryRptMarginTradeDetail(request.getHeader(), request.getRelationId());
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = QueryRptMarginTradeDetailResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("queryRptMarginTradeDetail error", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void setSpecialInterest(SetSpecialInterestRequest request, StreamObserver<SetSpecialInterestResponse> responseObserver) {
        SetSpecialInterestResponse response;
        try {
            response = marginService.setSpecialInterest(request.getHeader(), request.getTokenId(), request.getShowInterest(), request.getEffectiveFlag());
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = SetSpecialInterestResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("setSpecialInterest error", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void querySpecialInterest(QuerySpecialInterestRequest request, StreamObserver<QuerySpecialInterestResponse> responseObserver) {
        QuerySpecialInterestResponse response;
        try {
            response = marginService.querySpecialInterest(request.getHeader(), request.getUserId(), request.getAccountId(), request.getTokenId());
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = QuerySpecialInterestResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("querySpecialInterest error", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void deleteSpecialInterest(DeleteSpecialInterestRequest request, StreamObserver<DeleteSpecialInterestResponse> responseObserver) {
        DeleteSpecialInterestResponse response;
        try {
            response = marginService.deleteSpecialInterest(request.getHeader(), request.getTokenId());
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = DeleteSpecialInterestResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("deleteSpecialInterest error", e);
            responseObserver.onError(e);
        }
    }

    /**
     * 获取杠杆活动信息  -- 新系统可暂不支持
     */
    @Override
    public void getMarginActivityInfo(io.bhex.broker.grpc.margin.GetMarginActivityInfoRequest request,
                                      io.grpc.stub.StreamObserver<io.bhex.broker.grpc.margin.GetMarginActivityInfoResponse> responseObserver) {
        GetMarginActivityInfoResponse response;
        try {
            response = marginService.getMarginActivityInfo(request.getHeader(), request.getActivityId(), request.getLanguage());
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = GetMarginActivityInfoResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("getMarginActivityInfo error", e);
        }
    }

    /**
     * 获取杠杆活动list  -- 新系统可暂不支持
     */
    @Override
    public void getMarginActivityList(io.bhex.broker.grpc.margin.GetMarginActivityListRequest request,
                                      io.grpc.stub.StreamObserver<io.bhex.broker.grpc.margin.GetMarginActivityListResponse> responseObserver) {
        GetMarginActivityListResponse response;
        try {
            response = marginService.getMarginActivityList(request.getHeader(), request.getActivityId(), request.getLanguage());
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = GetMarginActivityListResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("getMarginActivityList error", e);
        }
    }


    @Override
    public void submitOpenMarginActivity(SubmitOpenMarginActivityRequest request, StreamObserver<SubmitOpenMarginActivityResponse> responseObserver) {
        SubmitOpenMarginActivityResponse response;
        try {
            response = marginService.submitOpenMarginActivity(request.getHeader());
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = SubmitOpenMarginActivityResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("submitOpenMarginActivity error", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void getOpenMarginActivity(GetOpenMarginActivityRequest request, StreamObserver<GetOpenMarginActivityResponse> responseObserver) {
        GetOpenMarginActivityResponse response;
        try {
            response = marginService.getOpenMarginActivity(request.getHeader());
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = GetOpenMarginActivityResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("submitOpenMarginActivity error", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void adminQueryOpenMarginActivity(AdminQueryOpenMarginActivityRequest request, StreamObserver<AdminQueryOpenMarginActivityResponse> responseObserver) {
        AdminQueryOpenMarginActivityResponse response;
        try {
            response = marginService.adminQueryOpenMarginActivity(request.getHeader(), request.getAccountId(), request.getStartTime(), request.getEndTime(),
                    request.getFromId(), request.getJoinStatus(), request.getLimit());
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = AdminQueryOpenMarginActivityResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("adminQueryOpenMarginActivity error", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void adminCheckDayOpenMarginActivity(AdminCheckDayOpenMarginActivityRequest request, StreamObserver<AdminCheckDayOpenMarginActivityResponse> responseObserver) {
        try {
            marginService.checkOpenMarginActivityBalance(request.getHeader());
            responseObserver.onNext(AdminCheckDayOpenMarginActivityResponse.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            AdminCheckDayOpenMarginActivityResponse response = AdminCheckDayOpenMarginActivityResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("adminCheckDayOpenMarginActivity error", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void adminCheckMonthOpenMarginActivity(AdminCheckMonthOpenMarginActivityRequest request, StreamObserver<AdminCheckMonthOpenMarginActivityResponse> responseObserver) {
        try {
            marginService.checkMonthOpenMarginActivityBalance(request.getHeader());
            responseObserver.onNext(AdminCheckMonthOpenMarginActivityResponse.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            AdminCheckMonthOpenMarginActivityResponse response = AdminCheckMonthOpenMarginActivityResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("adminCheckMonthOpenMarginActivity error", e);
            responseObserver.onError(e);
        }
    }

    /**
     * 设置特殊借币限额 -- 新系统可暂不支持
     *
     * @param request
     * @param responseObserver
     */
    @Override
    public void adminSetSpecialLoanLimit(AdminSetSpecialLoanLimitRequest request, StreamObserver<AdminSetSpecialLoanLimitResponse> responseObserver) {
        AdminSetSpecialLoanLimitResponse response;
        try {
            response = marginService.adminSetSpecialLoanLimit(request.getHeader(), request.getLoanLimit(), request.getIsOpen());
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = AdminSetSpecialLoanLimitResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("adminSetSpecialLoanLimit error", e);
            responseObserver.onError(e);
        }
    }

    /**
     * 查询特殊借币限额 -- 新系统可暂不支持
     *
     * @param request
     * @param responseObserver
     */
    @Override
    public void adminQuerySpecialLoanLimit(AdminQuerySpecialLoanLimitRequest request, StreamObserver<AdminQuerySpecialLoanLimitResponse> responseObserver) {
        AdminQuerySpecialLoanLimitResponse response;
        try {
            response = marginService.adminQuerySpecialLoanLimit(request.getHeader());
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = AdminQuerySpecialLoanLimitResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("adminQuerySpecialLoanLimit error", e);
            responseObserver.onError(e);
        }
    }

    /**
     * 删除特殊借币限额 -- 新系统可暂不支持
     *
     * @param request
     * @param responseObserver
     */
    @Override
    public void adminDelSpecialLoanLimit(AdminDelSpecialLoanLimitRequest request, StreamObserver<AdminDelSpecialLoanLimitResponse> responseObserver) {
        AdminDelSpecialLoanLimitResponse response;
        try {
            response = marginService.adminDelSpecialLoanLimit(request.getHeader());
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = AdminDelSpecialLoanLimitResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("adminDelSpecialLoanLimit error", e);
            responseObserver.onError(e);
        }
    }

    /**
     * 管理端设置杠杆币种借贷数量上限  -- 新系统可暂不支持
     */
    public void adminSetMarginLoanLimit(io.bhex.broker.grpc.margin.AdminSetMarginLoanLimitRequest request,
                                        io.grpc.stub.StreamObserver<io.bhex.broker.grpc.margin.AdminSetMarginLoanLimitResponse> responseObserver) {
        AdminSetMarginLoanLimitResponse response;
        try {
            response = marginService.adminSetMarginLoanLimit(request.getHeader(), request.getTokenId(), request.getLimitAmount(), request.getStatus());
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = AdminSetMarginLoanLimitResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("adminSetMarginLoanLimit error", e);
            responseObserver.onError(e);
        }
    }


    /**
     * 管理端查询杠杆币种借贷数量上限  -- 新系统可暂不支持
     */
    public void adminQueryMarginLoanLimit(io.bhex.broker.grpc.margin.AdminQueryMarginLoanLimitRequest request,
                                          io.grpc.stub.StreamObserver<io.bhex.broker.grpc.margin.AdminQueryMarginLoanLimitResponse> responseObserver) {
        AdminQueryMarginLoanLimitResponse response;
        try {
            response = marginService.adminQueryMarginLoanLimit(request.getHeader(), request.getTokenId());
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = AdminQueryMarginLoanLimitResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("adminQueryMarginLoanLimit error", e);
            responseObserver.onError(e);
        }
    }


    /**
     * 管理端设置杠杆用户币种借贷数量上限  -- 新系统可暂不支持
     */
    public void adminSetMarginUserLoanLimit(io.bhex.broker.grpc.margin.AdminSetMarginUserLoanLimitRequest request,
                                            io.grpc.stub.StreamObserver<io.bhex.broker.grpc.margin.AdminSetMarginUserLoanLimitResponse> responseObserver) {
        AdminSetMarginUserLoanLimitResponse response;
        try {
            response = marginService.adminSetMarginUserLoanLimit(request.getHeader(), request.getUserId(), request.getTokenId(), request.getLimitAmount(), request.getStatus());
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = AdminSetMarginUserLoanLimitResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("adminSetMarginUserLoanLimit error", e);
            responseObserver.onError(e);
        }
    }

    /**
     * 管理端查询杠杆用户币种借贷数量上限  -- 新系统可暂不支持
     */
    public void adminQueryMarginUserLoanLimit(io.bhex.broker.grpc.margin.AdminQueryMarginUserLoanLimitRequest request,
                                              io.grpc.stub.StreamObserver<io.bhex.broker.grpc.margin.AdminQueryMarginUserLoanLimitResponse> responseObserver) {
        AdminQueryMarginUserLoanLimitResponse response;
        try {
            response = marginService.adminQueryMarginUserLoanLimit(request.getHeader(), request.getUserId(), request.getAccountId(), request.getTokenId());
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = AdminQueryMarginUserLoanLimitResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("adminQueryMarginUserLoanLimit error", e);
            responseObserver.onError(e);
        }
    }
}
