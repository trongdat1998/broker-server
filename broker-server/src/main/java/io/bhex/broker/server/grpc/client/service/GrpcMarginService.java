package io.bhex.broker.server.grpc.client.service;

import io.bhex.base.margin.*;
import io.bhex.base.margin.MarginConfigServiceGrpc.MarginConfigServiceBlockingStub;
import io.bhex.base.margin.cross.*;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.common.grpc.client.annotation.GrpcLog;
import io.bhex.broker.common.grpc.client.annotation.PrometheusMetrics;
import io.bhex.broker.grpc.margin.QueryAccountLoanLimitVIPResponse;
import io.bhex.broker.server.util.BaseReqUtil;
import io.grpc.StatusRuntimeException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;


/**
 * @author JinYuYuan
 * @description
 * @date 2020-06-09 13:51
 */
@Service
@Slf4j
@GrpcLog
@PrometheusMetrics
public class GrpcMarginService extends GrpcBaseService {

    public GetRiskConfigReply getRisk(GetRiskConfigRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        MarginConfigServiceBlockingStub stub = grpcClientConfig.marginConfigServiceBlockingStub(orgId);
        try {
            return stub.getRisk(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public GetTokenConfigReply getToken(GetTokenConfigRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        MarginConfigServiceBlockingStub stub = grpcClientConfig.marginConfigServiceBlockingStub(orgId);

        try {
            return stub.getTokenConfig(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public GetInterestConfigReply getInterest(GetInterestConfigRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        MarginConfigServiceBlockingStub stub = grpcClientConfig.marginConfigServiceBlockingStub(orgId);
        try {
            return stub.getInterestConfig(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public SetRiskConfigReply setRisk(SetRiskConfigRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        MarginConfigServiceBlockingStub stub = grpcClientConfig.marginConfigServiceBlockingStub(orgId);
        try {
            return stub.setRisk(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public SetTokenConfigReply setToken(SetTokenConfigRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        MarginConfigServiceBlockingStub stub = grpcClientConfig.marginConfigServiceBlockingStub(orgId);
        try {
            SetTokenConfigReply reply = stub.setTokenConfig(request);
            if (reply.getRet() != 0) {
                log.error("setToken error  ret {}", reply.getRet());
                if (reply.getRet() == 4673) {
                    throw new BrokerException(BrokerErrorCode.MARGIN_INDICES_NOT_EXIT);
                }
                throw new BrokerException(BrokerErrorCode.GRPC_SERVER_SYSTEM_ERROR);
            }
            return reply;
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }


    public SetInterestConfigReply setInterest(SetInterestConfigRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        MarginConfigServiceBlockingStub stub = grpcClientConfig.marginConfigServiceBlockingStub(orgId);
        try {
            return stub.setInterestConfig(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public CrossLoanOrderListReply getCrossLoanOrder(CrossLoanOrderListRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        MarginCrossServiceGrpc.MarginCrossServiceBlockingStub stub = grpcClientConfig.marginCrossServiceBlockingStub(orgId);
        try {
            return stub.getLoanOrderList(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public RepayOrderListReply getRepayRecord(RepayOrderListRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        MarginCrossServiceGrpc.MarginCrossServiceBlockingStub stub = grpcClientConfig.marginCrossServiceBlockingStub(orgId);
        try {
            return stub.getRepayRecordByLoanOrder(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public GetSafetyReply getMarginSafety(GetSafetyRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        MarginConfigServiceBlockingStub stub = grpcClientConfig.marginConfigServiceBlockingStub(orgId);
        try {
            return stub.getSafety(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public CrossLoanPositionReply getCrossLoanPosition(CrossLoanPositionRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        MarginCrossServiceGrpc.MarginCrossServiceBlockingStub stub = grpcClientConfig.marginCrossServiceBlockingStub(orgId);
        try {
            return stub.getCrossLoanPosition(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public GetAvailableAmountReply getAvailableAmount(GetAvailableAmountRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        MarginConfigServiceBlockingStub stub = grpcClientConfig.marginConfigServiceBlockingStub(orgId);
        try {
            return stub.getAvailableAmount(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public FundingCrossReply getFundingCross(FundingCrossRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        MarginCrossServiceGrpc.MarginCrossServiceBlockingStub stub = grpcClientConfig.marginCrossServiceBlockingStub(orgId);
        try {
            return stub.getFundingCross(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public GetMarginPositionStatusReply getMarginPositionStatus(GetMarginPositionStatusRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        MarginCrossServiceGrpc.MarginCrossServiceBlockingStub stub = grpcClientConfig.marginCrossServiceBlockingStub(orgId);
        try {
            return stub.getMarginPositionStatus(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }


    public GetUserRiskReply queryUserRisk(GetUserRiskRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        MarginConfigServiceBlockingStub stub = grpcClientConfig.marginConfigServiceBlockingStub(orgId);
        try {
            return stub.getUserRisk(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public SummaryRiskReply summaryRisk(SummaryRiskRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        MarginConfigServiceBlockingStub stub = grpcClientConfig.marginConfigServiceBlockingStub(orgId);
        try {
            return stub.summaryRisk(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public ForceCloseReply forceClose(ForceCloseRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        MarginConfigServiceBlockingStub stub = grpcClientConfig.marginConfigServiceBlockingStub(orgId);
        try {
            return stub.forceClose(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public SetSymbolConfigReply setSymbolConfig(SetSymbolConfigRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        MarginConfigServiceBlockingStub stub = grpcClientConfig.marginConfigServiceBlockingStub(orgId);
        try {
            SetSymbolConfigReply reply = stub.setSymbolConfig(request);
            if (reply.getRet() != 0) {
                log.error("setSymbolConfig error  ret {}", reply.getRet());
                throw new BrokerException(BrokerErrorCode.GRPC_SERVER_SYSTEM_ERROR);
            }
            return reply;
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public QueryInterestByLevelReply queryInterestByLevel(QueryInterestByLevelRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        MarginConfigServiceBlockingStub stub = grpcClientConfig.marginConfigServiceBlockingStub(orgId);
        try {
            return stub.queryInterestByLevel(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public SetLevelInterestReply setLevelInterest(SetLevelInterestRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        MarginConfigServiceBlockingStub stub = grpcClientConfig.marginConfigServiceBlockingStub(orgId);
        try {
            return stub.setLevelInterest(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public DeleteInterestByLevelReply deleteInterestByLevel(DeleteInterestByLevelRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        MarginConfigServiceBlockingStub stub = grpcClientConfig.marginConfigServiceBlockingStub(orgId);
        try {
            return stub.deleteInterestByLevel(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public QueryForceRecordReply queryForceRecord(QueryForceRecordRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        MarginConfigServiceBlockingStub stub = grpcClientConfig.marginConfigServiceBlockingStub(orgId);
        try {
            return stub.queryForceRecord(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public ChangeMarginPositionStatusReply changeMarginPositionStatus(ChangeMarginPositionStatusRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        MarginCrossServiceGrpc.MarginCrossServiceBlockingStub stub = grpcClientConfig.marginCrossServiceBlockingStub(orgId);
        try {
            return stub.changeMarginPositionStatus(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public QueryMarginSymbolReply queryMarginSymbolReply(QueryMarginSymbolRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        MarginConfigServiceBlockingStub stub = grpcClientConfig.marginConfigServiceBlockingStub(orgId);
        try {
            return stub.queryMarginSymbol(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public SetSymbolConfigReply updateMarginSymbol(SetSymbolConfigRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        MarginConfigServiceBlockingStub stub = grpcClientConfig.marginConfigServiceBlockingStub(orgId);
        try {
            return stub.updateSymbolInfo(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public QueryAccountLoanLimitVIPReply queryAccountLoanLimitVIP(QueryAccountLoanLimitVIPRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        MarginConfigServiceBlockingStub stub = grpcClientConfig.marginConfigServiceBlockingStub(orgId);
        try {
            return stub.queryAccountLoanLimitVIP(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public SetAccountLoanLimitVIPReply setAccountLoanLimitVIP(SetAccountLoanLimitVIPRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        MarginConfigServiceBlockingStub stub = grpcClientConfig.marginConfigServiceBlockingStub(orgId);
        try {
            return stub.setAccountLoanLimitVIP(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public DeleteAccountLoanLimitVIPReply deleteAccountLoanLimitVIP(DeleteAccountLoanLimitVIPRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        MarginConfigServiceBlockingStub stub = grpcClientConfig.marginConfigServiceBlockingStub(orgId);
        try {
            return stub.deleteAccountLoanLimitVIP(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public QueryMarginRiskBlackListReply queryMarginRiskBlackList(QueryMarginRiskBlackListRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        MarginConfigServiceBlockingStub stub = grpcClientConfig.marginConfigServiceBlockingStub(orgId);
        try {
            return stub.queryMarginRiskBlackList(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public AddMarginRiskBlackListReply addMarginRiskBlackList(AddMarginRiskBlackListRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        MarginConfigServiceBlockingStub stub = grpcClientConfig.marginConfigServiceBlockingStub(orgId);
        try {
            return stub.addMarginRiskBlackList(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public DelMarginRiskBlackListReply delMarginRiskBlackList(DelMarginRiskBlackListRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        MarginConfigServiceBlockingStub stub = grpcClientConfig.marginConfigServiceBlockingStub(orgId);
        try {
            return stub.delMarginRiskBlackList(request);
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public SetSpecialLoanLimitReply setSpecialLoanLimit(SetSpecialLoanLimitRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        MarginConfigServiceBlockingStub stub = grpcClientConfig.marginConfigServiceBlockingStub(orgId);
        try {
            SetSpecialLoanLimitReply reply = stub.setSpecialLoanLimit(request);
            if (reply.getRet() != 0) {
                log.error("setSpecialLoanLimit error  ret {}", reply.getRet());
                throw new BrokerException(BrokerErrorCode.GRPC_SERVER_SYSTEM_ERROR);
            }
            return reply;
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public QuerySpecialLoanLimitReply querySpecialLoanLimit(QuerySpecialLoanLimitRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        MarginConfigServiceBlockingStub stub = grpcClientConfig.marginConfigServiceBlockingStub(orgId);
        try {
            QuerySpecialLoanLimitReply reply = stub.querySpecialLoanLimit(request);
            if (reply.getRet() != 0) {
                log.error("querySpecialLoanLimit error  ret {}", reply.getRet());
                throw new BrokerException(BrokerErrorCode.GRPC_SERVER_SYSTEM_ERROR);
            }
            return reply;
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public DelSpecialLoanLimitReply delSpecialLoanLimit(DelSpecialLoanLimitRequest request) {
        Long orgId = BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        MarginConfigServiceBlockingStub stub = grpcClientConfig.marginConfigServiceBlockingStub(orgId);
        try {
            DelSpecialLoanLimitReply reply = stub.delSpecialLoanLimit(request);
            if (reply.getRet() != 0) {
                log.error("delSpecialLoanLimit error  ret {}", reply.getRet());
                throw new BrokerException(BrokerErrorCode.GRPC_SERVER_SYSTEM_ERROR);
            }
            return reply;
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }


}
