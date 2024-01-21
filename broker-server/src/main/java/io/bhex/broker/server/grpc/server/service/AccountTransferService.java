package io.bhex.broker.server.grpc.server.service;

import io.bhex.base.account.UserAccountTransferReq;
import io.bhex.base.account.UserAccountTransferResp;
import io.bhex.broker.grpc.transfer.UserAccountTransferRequest;
import io.bhex.broker.grpc.transfer.UserAccountTransferResponse;
import io.bhex.broker.server.grpc.client.service.GrpcAccountTransferService;
import io.bhex.broker.server.util.BaseReqUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.grpc.server.service
 * @Author: ming.xu
 * @CreateDate: 15/01/2019 4:13 PM
 * @Copyright（C）: 2019 BHEX Inc. All rights reserved.
 */
@Slf4j
@Service
public class AccountTransferService {

    @Resource
    private GrpcAccountTransferService grpcAccountTransferService;
    @Resource
    private AccountService accountService;


    public UserAccountTransferResponse userAccountTransfer(UserAccountTransferRequest request) {
        UserAccountTransferReq.TransferType transferType = UserAccountTransferReq.TransferType.UNKNOWN;
        switch (request.getTransferType()) {
            case UNKNOWN:
                break;
            case COIN_TO_FUTURE:
                transferType = UserAccountTransferReq.TransferType.COIN_TO_FUTURE;
                break;
            case COIN_TO_OPTION:
                transferType = UserAccountTransferReq.TransferType.COIN_TO_OPTION;
                break;
            case FUTURE_TO_COIN:
                transferType = UserAccountTransferReq.TransferType.FUTURE_TO_COIN;
                break;
            case OPTION_TO_COIN:
                transferType = UserAccountTransferReq.TransferType.OPTION_TO_COIN;
                break;
            case FUTURE_TO_OPTION:
                transferType = UserAccountTransferReq.TransferType.FUTURE_TO_OPTION;
                break;
            case OPTION_TO_FUTURE:
                transferType = UserAccountTransferReq.TransferType.OPTION_TO_FUTURE;
                break;
        }

        UserAccountTransferReq.Builder bhRequestBuilder = UserAccountTransferReq.newBuilder()
                .setCoinId(request.getCoinId())
                .setQuantity(request.getQuantity())
                .setTransferType(transferType)
                .setUserId(request.getHeader().getUserId())
                .setCoinAccountId(accountService.getMainAccountId(request.getHeader()))
                .setOptionAccountId(accountService.getOptionAccountId(request.getHeader()))
                .setFuturesAccountId(accountService.getFuturesAccountId(request.getHeader()));

        Long futuresAccountId = accountService.getFuturesAccountId(request.getHeader());
        if (futuresAccountId != null) {
            bhRequestBuilder.setFuturesAccountId(futuresAccountId);
        }
        bhRequestBuilder.setBaseRequest(BaseReqUtil.getBaseRequest(request.getHeader().getOrgId()));
        UserAccountTransferResp userAccountTransferResp = grpcAccountTransferService.userAccountTransfer(bhRequestBuilder.build());

        UserAccountTransferResponse.Builder replyBuilder = UserAccountTransferResponse.newBuilder();
        if (userAccountTransferResp.getErrorCode() == 0) {
            replyBuilder.setRet(0);
        } else {
            replyBuilder.setRet(1);
            log.error("userAccountTransfer: {}", userAccountTransferResp.getMsg());
        }
        return replyBuilder.build();
    }
}
