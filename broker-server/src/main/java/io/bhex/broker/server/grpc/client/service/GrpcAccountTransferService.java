package io.bhex.broker.server.grpc.client.service;

import com.google.protobuf.TextFormat;
import io.bhex.base.account.UserAccountTransferGrpc;
import io.bhex.base.account.UserAccountTransferReq;
import io.bhex.base.account.UserAccountTransferResp;
import io.bhex.base.exception.ErrorStatusRuntimeException;
import io.bhex.base.proto.ErrorCode;
import io.bhex.base.proto.ErrorStatus;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.common.grpc.client.annotation.GrpcLog;
import io.bhex.broker.common.grpc.client.annotation.PrometheusMetrics;
import io.bhex.broker.server.util.BaseReqUtil;
import io.bhex.broker.server.util.BigDecimalUtil;
import io.grpc.StatusRuntimeException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.grpc.client.service
 * @Author: ming.xu
 * @CreateDate: 15/01/2019 4:14 PM
 * @Copyright（C）: 2019 BHEX Inc. All rights reserved.
 */
@Service
@Slf4j
@GrpcLog
@PrometheusMetrics
public class GrpcAccountTransferService extends GrpcBaseService {

    public UserAccountTransferResp userAccountTransfer(UserAccountTransferReq request) {
        UserAccountTransferGrpc.UserAccountTransferBlockingStub stub = grpcClientConfig.accountTransferServiceBlockingStub(BaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest()));
        try {
            BigDecimalUtil.checkParamScale(StringUtils.isNotEmpty(request.getQuantity()) ? request.getQuantity() : "");
            return stub.userAccountTransfer(request);
        } catch (StatusRuntimeException e) {
            ErrorStatus errorStatus = e.getTrailers().get(ErrorStatusRuntimeException.ERROR_STATUS_KEY);
            if (errorStatus != null) {
                ErrorCode error = errorStatus.getCode();
                log.warn("userAccountTransfer failed: {} (request: {})", error.name(), TextFormat.shortDebugString(request));
                if (error == ErrorCode.ERR_Unknown_Account) {
                    throw new BrokerException(BrokerErrorCode.ACCOUNT_NOT_EXIST);
                } else if (error == ErrorCode.ERR_Unknown_Token) {
                    throw new BrokerException(BrokerErrorCode.TOKEN_NOT_FOUND);
                } else if (error == ErrorCode.ERR_Insufficient_Balance) {
                    throw new BrokerException(BrokerErrorCode.INSUFFICIENT_BALANCE);
                } else if (error == ErrorCode.ERR_Duplicated_Order) {
                    throw new BrokerException(BrokerErrorCode.DUPLICATED_ORDER);
                } else if (error == ErrorCode.ERR_Invalid_Argument) {
                    throw new BrokerException(BrokerErrorCode.PARAM_INVALID);
                } else if (error == ErrorCode.PRD_4604) {
                    throw new BrokerException(BrokerErrorCode.ACCOUNT_NOT_IN_SAME_SHARD);
                } else {
                    log.error("userAccountTransfer failed: {} (request: {})", error.name(), TextFormat.shortDebugString(request));
                    throw new BrokerException(BrokerErrorCode.USER_ACCOUNT_TRANSFER_FILLED, e);
                }
            }
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }
}