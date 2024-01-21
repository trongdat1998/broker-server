package io.bhex.broker.server.grpc.client.service;

import org.springframework.stereotype.Service;

import io.bhex.base.rc.ListRequest;
import io.bhex.base.rc.ListUserBizPermissionResponse;
import io.bhex.base.rc.RcBalanceResponse;
import io.bhex.base.rc.UserRequest;
import io.grpc.StatusRuntimeException;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class GrpcExchangeRCService extends GrpcBaseService {

    public RcBalanceResponse getUserRcBalance(UserRequest request) {
        try {
            return grpcClientConfig.exchangeRCServiceBlockingStub().getUserRcBalance(request);
        } catch (StatusRuntimeException e) {
            log.error("getUserRcBalance exception  ", e);
            throw commonStatusRuntimeException(e);
        }
    }

    public ListUserBizPermissionResponse getAllUserBizPermissionList(ListRequest request) {
        try {
            return grpcClientConfig.exchangeRCServiceBlockingStub().getAllUserBizPermissionList(request);
        } catch (StatusRuntimeException e) {
            log.error("getAllUserBizPermissionList exception  ", e);
            throw commonStatusRuntimeException(e);
        }
    }
}
