package io.bhex.broker.server.grpc.client.service;

import com.google.common.base.Stopwatch;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.annotation.Resource;

import io.bhex.base.idgen.api.ISequenceGenerator;
import io.bhex.broker.common.api.client.jiean.JieanApi;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.common.grpc.client.annotation.GrpcLog;
import io.bhex.broker.common.grpc.client.annotation.NoGrpcLog;
import io.bhex.broker.common.grpc.client.annotation.PrometheusMetrics;
import io.bhex.broker.server.domain.BrokerServerConstants;
import io.bhex.broker.server.grpc.server.service.AccountService;
import io.bhex.broker.server.grpc.server.service.OtcLegalCurrencyService;
import io.bhex.broker.server.grpc.server.service.OtcSymbolService;
import io.bhex.broker.server.grpc.server.service.UserService;
import io.bhex.broker.server.grpc.server.service.aspect.UserActionLogAnnotation;
import io.bhex.broker.server.grpc.server.service.notify.OtcAppealNotify;
import io.bhex.broker.server.model.Country;
import io.bhex.broker.server.model.OtcLegalCurrency;
import io.bhex.broker.server.model.OtcWhiteList;
import io.bhex.broker.server.model.UserVerify;
import io.bhex.broker.server.primary.mapper.CountryMapper;
import io.bhex.broker.server.primary.mapper.OtcWhiteListMapper;
import io.bhex.broker.server.primary.mapper.UserVerifyMapper;
import io.bhex.broker.server.util.CommonUtil;
import io.bhex.broker.server.util.OtcBaseReqUtil;
import io.bhex.ex.otc.BatchUpdatePaymentVisibleRequest;
import io.bhex.ex.otc.BatchUpdatePaymentVisibleResponse;
import io.bhex.ex.otc.FindOrdersByOrderIdsReponse;
import io.bhex.ex.otc.FindOrdersByOrderIdsRequest;
import io.bhex.ex.otc.GetAppealMessagesRequest;
import io.bhex.ex.otc.GetAppealMessagesResponse;
import io.bhex.ex.otc.GetBrokerPaymentConfigRequest;
import io.bhex.ex.otc.GetBrokerPaymentConfigResponse;
import io.bhex.ex.otc.GetLastNewOrderIdResponse;
import io.bhex.ex.otc.GetOTCCurrencysRequest;
import io.bhex.ex.otc.GetOTCCurrencysResponse;
import io.bhex.ex.otc.GetOTCSymbolsRequest;
import io.bhex.ex.otc.GetOTCSymbolsResponse;
import io.bhex.ex.otc.GetOTCTokensRequest;
import io.bhex.ex.otc.GetOTCTokensResponse;
import io.bhex.ex.otc.GetTradeFeeRateByTokenIdRequest;
import io.bhex.ex.otc.GetTradeFeeRateByTokenIdResponse;
import io.bhex.ex.otc.GetTradeFeeRateRequest;
import io.bhex.ex.otc.GetTradeFeeRateResponse;
import io.bhex.ex.otc.ListOtcUserRequest;
import io.bhex.ex.otc.ListOtcUserResponse;
import io.bhex.ex.otc.OTCCancelItemRequest;
import io.bhex.ex.otc.OTCCancelItemResponse;
import io.bhex.ex.otc.OTCConfigPaymentRequest;
import io.bhex.ex.otc.OTCConfigPaymentResponse;
import io.bhex.ex.otc.OTCConfigServiceGrpc;
import io.bhex.ex.otc.OTCCreateNewPaymentRequest;
import io.bhex.ex.otc.OTCCreateNewPaymentResponse;
import io.bhex.ex.otc.OTCDeleteItemRequest;
import io.bhex.ex.otc.OTCDeleteOrderRequest;
import io.bhex.ex.otc.OTCDeletePaymentRequest;
import io.bhex.ex.otc.OTCDeletePaymentResponse;
import io.bhex.ex.otc.OTCGetDepthRequest;
import io.bhex.ex.otc.OTCGetDepthResponse;
import io.bhex.ex.otc.OTCGetItemIdRequest;
import io.bhex.ex.otc.OTCGetItemIdResponse;
import io.bhex.ex.otc.OTCGetItemInfoRequest;
import io.bhex.ex.otc.OTCGetItemInfoResponse;
import io.bhex.ex.otc.OTCGetItemsRequest;
import io.bhex.ex.otc.OTCGetItemsResponse;
import io.bhex.ex.otc.OTCGetLastPriceRequest;
import io.bhex.ex.otc.OTCGetLastPriceResponse;
import io.bhex.ex.otc.OTCGetMessageRequest;
import io.bhex.ex.otc.OTCGetMessageResponse;
import io.bhex.ex.otc.OTCGetMessagesRequest;
import io.bhex.ex.otc.OTCGetMessagesResponse;
import io.bhex.ex.otc.OTCGetNickNameRequest;
import io.bhex.ex.otc.OTCGetNickNameResponse;
import io.bhex.ex.otc.OTCGetOnlineItemsRequest;
import io.bhex.ex.otc.OTCGetOrderIdRequest;
import io.bhex.ex.otc.OTCGetOrderIdResponse;
import io.bhex.ex.otc.OTCGetOrderInfoRequest;
import io.bhex.ex.otc.OTCGetOrderInfoResponse;
import io.bhex.ex.otc.OTCGetOrdersRequest;
import io.bhex.ex.otc.OTCGetOrdersResponse;
import io.bhex.ex.otc.OTCGetPaymentRequest;
import io.bhex.ex.otc.OTCGetPaymentResponse;
import io.bhex.ex.otc.OTCGetPendingCountRequest;
import io.bhex.ex.otc.OTCGetPendingCountResponse;
import io.bhex.ex.otc.OTCGetPendingOrdersRequest;
import io.bhex.ex.otc.OTCGetPendingOrdersResponse;
import io.bhex.ex.otc.OTCHandleOrderRequest;
import io.bhex.ex.otc.OTCHandleOrderResponse;
import io.bhex.ex.otc.OTCItemServiceGrpc;
import io.bhex.ex.otc.OTCMessageServiceGrpc;
import io.bhex.ex.otc.OTCNewItemRequest;
import io.bhex.ex.otc.OTCNewItemResponse;
import io.bhex.ex.otc.OTCNewMessageRequest;
import io.bhex.ex.otc.OTCNewMessageResponse;
import io.bhex.ex.otc.OTCNewOrderRequest;
import io.bhex.ex.otc.OTCNewOrderResponse;
import io.bhex.ex.otc.OTCNewPaymentRequest;
import io.bhex.ex.otc.OTCNewPaymentResponse;
import io.bhex.ex.otc.OTCNormalItemRequest;
import io.bhex.ex.otc.OTCNormalOrderRequest;
import io.bhex.ex.otc.OTCOfflineItemRequest;
import io.bhex.ex.otc.OTCOfflineItemResponse;
import io.bhex.ex.otc.OTCOnlineItemRequest;
import io.bhex.ex.otc.OTCOnlineItemResponse;
import io.bhex.ex.otc.OTCOrderServiceGrpc;
import io.bhex.ex.otc.OTCPaymentTermServiceGrpc;
import io.bhex.ex.otc.OTCPaymentTypeEnum;
import io.bhex.ex.otc.OTCResult;
import io.bhex.ex.otc.OTCSetNickNameRequest;
import io.bhex.ex.otc.OTCSetNickNameResponse;
import io.bhex.ex.otc.OTCUpdateNewPaymentRequest;
import io.bhex.ex.otc.OTCUpdateNewPaymentResponse;
import io.bhex.ex.otc.OTCUpdatePaymentRequest;
import io.bhex.ex.otc.OTCUpdatePaymentResponse;
import io.bhex.ex.otc.OTCUser;
import io.bhex.ex.otc.OTCUserContactResponse;
import io.bhex.ex.otc.OTCUserServiceGrpc;
import io.bhex.ex.otc.OrgIdRequest;
import io.bhex.ex.otc.QueryOtcPaymentTermListRequest;
import io.bhex.ex.otc.QueryOtcPaymentTermListResponse;
import io.bhex.ex.otc.SwitchPaymentVisibleRequest;
import io.bhex.ex.otc.SwitchPaymentVisibleResponse;
import io.grpc.StatusRuntimeException;
import lombok.extern.slf4j.Slf4j;

/**
 * @author lizhen
 * @date 2018-09-20
 */
@Service
@Slf4j
@GrpcLog
@PrometheusMetrics
public class GrpcOtcService extends GrpcBaseService {

    @Resource
    private AccountService accountService;

    @Resource
    private UserVerifyMapper userVerifyMapper;

    @Resource
    private CountryMapper countryMapper;

    @Resource
    private OtcSymbolService otcSymbolService;

    @Autowired
    private OtcLegalCurrencyService otcLegalCurrencyService;

    @Resource
    private UserService userService;

    @Resource
    private JieanApi jieanApi;

    @Resource
    private ISequenceGenerator sequenceGenerator;

    @Resource
    private OtcWhiteListMapper otcWhiteListMapper;

    public OTCGetNickNameResponse getUserInfo(OTCGetNickNameRequest request) {
        Long orgId = OtcBaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        OTCUserServiceGrpc.OTCUserServiceBlockingStub stub = grpcClientConfig.otcUserServiceBlockingStub(orgId);
        try {
            request = request.toBuilder()
                    .setAccountId(accountService.getAccountId(request.getBaseRequest().getOrgId(), request.getBaseRequest().getUserId()))
                    .build();
            OTCGetNickNameResponse otcGetNickNameResponse = stub.getNickName(request);
            if (otcGetNickNameResponse != null) {
                //绑定法币关系
                UserVerify dbUserVerify = userVerifyMapper.getByUserId(request.getBaseRequest().getUserId());
                if (dbUserVerify != null && dbUserVerify.getNationality() != null) {
                    Country country = countryMapper.queryCountryById(dbUserVerify.getNationality());
                    if (country != null) {
                        otcLegalCurrencyService.bindingUserLegalCurrencyV2(request.getBaseRequest().getOrgId(), dbUserVerify.getUserId(), country.getCurrencyCode());
                        OtcLegalCurrency legalCurrencyInfo
                                = otcLegalCurrencyService.queryUserLegalCurrencyInfo(request.getBaseRequest().getOrgId(), request.getBaseRequest().getUserId());
                        otcGetNickNameResponse = otcGetNickNameResponse.toBuilder().setCurrency(legalCurrencyInfo.getCode()).build();
                    }
                    if (country.getNationalCode().equalsIgnoreCase("86") || country.getDomainShortName().equalsIgnoreCase("CN")) {
                        log.info("getUserInfo accountId {} getNationalCode {} getDomainShortName {}", request.getAccountId(), country.getNationalCode(), country.getDomainShortName());
                        OTCGetNickNameResponse newOtcGetNickNameResponse = otcGetNickNameResponse.toBuilder().clearCurrencyList().build();
                        otcGetNickNameResponse = newOtcGetNickNameResponse.toBuilder().addAllCurrencyList(Arrays.asList("CNY")).build();
                    }
                }
            }
            return otcGetNickNameResponse;
        } catch (StatusRuntimeException e) {
            log.error("getUserInfo,{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        } catch (Exception e) {
            log.error("OTC:getUserInfo exception, " + e.getMessage(), e);
            throw new BrokerException(BrokerErrorCode.GRPC_SERVER_SYSTEM_ERROR);
        }
    }

    public String getOtcNickname(OTCGetNickNameRequest request) {
        Long orgId = OtcBaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        OTCUserServiceGrpc.OTCUserServiceBlockingStub stub = grpcClientConfig.otcUserServiceBlockingStub(orgId);
        try {
            request = request.toBuilder()
                    .setAccountId(accountService.getAccountId(request.getBaseRequest().getOrgId(), request.getBaseRequest().getUserId()))
                    .build();
            OTCGetNickNameResponse otcGetNickNameResponse = stub.getNickName(request);
            return otcGetNickNameResponse.getNickName();
        } catch (StatusRuntimeException e) {
            log.error("getUserInfo,{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        } catch (Exception e) {
            log.error("OTC:getUserInfo exception, " + e.getMessage(), e);
            throw new BrokerException(BrokerErrorCode.GRPC_SERVER_SYSTEM_ERROR);
        }
    }


    @UserActionLogAnnotation(actionType = BrokerServerConstants.ACTION_TYPE_SET_OTC_NICKNAME, resultSucField = "result", action = "nickname:{#request.nickName}",
            userId = "{#request.baseRequest.userId}", orgId = "{#request.baseRequest.orgId}")
    public OTCSetNickNameResponse setNickName(OTCSetNickNameRequest request) {
        Long orgId = OtcBaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        OTCUserServiceGrpc.OTCUserServiceBlockingStub stub = grpcClientConfig.otcUserServiceBlockingStub(orgId);
        try {
            request = request.toBuilder()
                    .setAccountId(accountService.getAccountId(request.getBaseRequest().getOrgId(), request.getBaseRequest().getUserId()))
                    .build();
            return stub.setNickName(request);
        } catch (StatusRuntimeException e) {
            log.error("setNickName,{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        } catch (Exception e) {
            log.error("OTC:setNickName exception, " + e.getMessage(), e);
            throw new BrokerException(BrokerErrorCode.GRPC_SERVER_SYSTEM_ERROR);
        }
    }

    public OTCNewItemResponse createItem(OTCNewItemRequest request) {
        Long orgId = OtcBaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        OTCItemServiceGrpc.OTCItemServiceBlockingStub stub = grpcClientConfig.otcItemServiceBlockingStub(orgId);
        try {
            request = request.toBuilder()
                    .setAccountId(accountService.getAccountId(request.getBaseRequest().getOrgId(), request.getBaseRequest().getUserId()))
                    .build();
            return stub.addItem(request);
        } catch (StatusRuntimeException e) {
            log.error("createItem,{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        } catch (Exception e) {
            log.error("OTC:createItem exception, " + e.getMessage(), e);
            throw new BrokerException(BrokerErrorCode.GRPC_SERVER_SYSTEM_ERROR);
        }
    }

    @GrpcLog(printNoResponse = true)
    public OTCGetItemsResponse getItemList(OTCGetItemsRequest request) {
        Long orgId = OtcBaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        OTCItemServiceGrpc.OTCItemServiceBlockingStub stub = grpcClientConfig.otcItemServiceBlockingStub(orgId);
        try {
            request = request.toBuilder()
                    .setAccountId(accountService.getAccountId(request.getBaseRequest().getOrgId(), request.getBaseRequest().getUserId()))
                    .build();
            return stub.getItems(request);
        } catch (StatusRuntimeException e) {
            log.error("getItems,{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        } catch (Exception e) {
            log.error("OTC:getItems exception, " + e.getMessage(), e);
            throw new BrokerException(BrokerErrorCode.GRPC_SERVER_SYSTEM_ERROR);
        }
    }

    public OTCGetItemsResponse getItemList(OTCGetOnlineItemsRequest request) {

/*        long brokerId=request.getBaseRequest().getOrgId();
        long exchangeId=request.getBaseRequest().getExchangeId();

        String token=request.getTokenId();
        String currency=request.getCurrencyId();

        //查询共享该币对的交易所
        List<Long> exchangeIds=otcSymbolService.listExchangeIdByShareSymbol(brokerId,exchangeId,token,currency);
        OTCGetOnlineItemsRequest req= request.toBuilder().addAllShareExchangeIds(exchangeIds).build();*/

        Long orgId = OtcBaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        OTCItemServiceGrpc.OTCItemServiceBlockingStub stub = grpcClientConfig.otcItemServiceBlockingStub(orgId);
        try {
            return stub.getOnlineItems(request);
        } catch (StatusRuntimeException e) {
            log.error("getOnlineItems,{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        } catch (Exception e) {
            log.error("OTC:getOnlineItems exception, " + e.getMessage(), e);
            throw new BrokerException(BrokerErrorCode.GRPC_SERVER_SYSTEM_ERROR);
        }
    }

    public OTCGetItemInfoResponse getItemInfo(OTCGetItemInfoRequest request) {
        Long orgId = OtcBaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        OTCItemServiceGrpc.OTCItemServiceBlockingStub stub = grpcClientConfig.otcItemServiceBlockingStub(orgId);
        try {
            //request = request.toBuilder()
            //    .setAccountId(accountService.getAccountId(request.getBaseRequest().getUserId()))
            //    .build();
            return stub.getItem(request);
        } catch (StatusRuntimeException e) {
            log.error("getItemInfo,{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        } catch (Exception e) {
            log.error("OTC:getItemInfo exception, " + e.getMessage(), e);
            throw new BrokerException(BrokerErrorCode.GRPC_SERVER_SYSTEM_ERROR);
        }
    }

    public OTCCancelItemResponse cancelItem(OTCCancelItemRequest request) {
        Long orgId = OtcBaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        OTCItemServiceGrpc.OTCItemServiceBlockingStub stub = grpcClientConfig.otcItemServiceBlockingStub(orgId);
        try {
            request = request.toBuilder()
                    .setAccountId(accountService.getAccountId(request.getBaseRequest().getOrgId(), request.getBaseRequest().getUserId()))
                    .build();
            return stub.cancelItemToDelete(request);
        } catch (StatusRuntimeException e) {
            log.error("cancelItem,{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        } catch (Exception e) {
            log.error("OTC:cancelItem exception, " + e.getMessage(), e);
            throw new BrokerException(BrokerErrorCode.GRPC_SERVER_SYSTEM_ERROR);
        }
    }

    public OTCGetDepthResponse getDepth(OTCGetDepthRequest request) {
        Long orgId = OtcBaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        OTCItemServiceGrpc.OTCItemServiceBlockingStub stub = grpcClientConfig.otcItemServiceBlockingStub(orgId);
        try {
            return stub.getDepth(request);
        } catch (StatusRuntimeException e) {
            log.error("getDepth,{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        } catch (Exception e) {
            log.error("OTC:getDepth exception, " + e.getMessage(), e);
            throw new BrokerException(BrokerErrorCode.GRPC_SERVER_SYSTEM_ERROR);
        }
    }

    public OTCOfflineItemResponse offlineItem(OTCOfflineItemRequest request) {
        Long orgId = OtcBaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        OTCItemServiceGrpc.OTCItemServiceBlockingStub stub = grpcClientConfig.otcItemServiceBlockingStub(orgId);
        try {
            request = request.toBuilder()
                    .setAccountId(accountService.getAccountId(request.getBaseRequest().getOrgId(), request.getBaseRequest().getUserId()))
                    .build();
            return stub.offlineItem(request);
        } catch (StatusRuntimeException e) {
            log.error("offlineItem,{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        } catch (Exception e) {
            log.error("OTC:offlineItem exception, " + e.getMessage(), e);
            throw new BrokerException(BrokerErrorCode.GRPC_SERVER_SYSTEM_ERROR);
        }
    }

    public OTCOnlineItemResponse onlineItem(OTCOnlineItemRequest request) {

        try {
            log.info("OTC:list online item,request={}", CommonUtil.formatMessage(request));
            Long orgId = OtcBaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
            OTCItemServiceGrpc.OTCItemServiceBlockingStub stub = grpcClientConfig.otcItemServiceBlockingStub(orgId);
            request = request.toBuilder()
                    .setAccountId(accountService.getAccountId(request.getBaseRequest().getOrgId(), request.getBaseRequest().getUserId()))
                    .build();
            return stub.onlineItem(request);
        } catch (StatusRuntimeException e) {
            log.error(e.getMessage(), e);
            log.error("OTC:list online item exception,{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        } catch (Exception e) {
            log.error("OTC:list online item exception, " + e.getMessage(), e);
            throw new BrokerException(BrokerErrorCode.GRPC_SERVER_SYSTEM_ERROR);
        }
    }

    public OTCGetLastPriceResponse getLastPrice(OTCGetLastPriceRequest request) {
        try {
            log.info("OTC:get last price,request={}", CommonUtil.formatMessage(request));
            Long orgId = OtcBaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
            OTCItemServiceGrpc.OTCItemServiceBlockingStub stub = grpcClientConfig.otcItemServiceBlockingStub(orgId);

            return stub.getLastPrice(request);
        } catch (StatusRuntimeException e) {
            log.error(e.getMessage(), e);
            log.error("OTC:get last price exception,{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        } catch (Exception e) {
            log.error("OTC:get last price exception, " + e.getMessage(), e);
            throw new BrokerException(BrokerErrorCode.GRPC_SERVER_SYSTEM_ERROR);
        }
    }

    public OTCNewOrderResponse createOrder(OTCNewOrderRequest request) {
        Stopwatch sw = Stopwatch.createStarted();
        try {
            log.info("OTC:create order, request={}", CommonUtil.formatMessage(request));
            Long orgId = OtcBaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
            OTCOrderServiceGrpc.OTCOrderServiceBlockingStub stub = grpcClientConfig.otcOrderServiceBlockingStub(orgId);
            request = request.toBuilder()
                    .setAccountId(accountService.getAccountId(request.getBaseRequest().getOrgId(), request.getBaseRequest().getUserId()))
                    .build();
            return stub.addOrder(request);
        } catch (StatusRuntimeException e) {
            log.error(e.getMessage(), e);
            log.error("OTC:create order exception, {}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        } catch (Exception e) {
            log.error("OTC:create order exception, " + e.getMessage(), e);
            throw new BrokerException(BrokerErrorCode.GRPC_SERVER_SYSTEM_ERROR);
        } finally {
            log.info("OTC:create order,consume={},request={}",
                    sw.stop().elapsed(TimeUnit.MILLISECONDS),
                    CommonUtil.formatMessage(request));
        }
    }

    @NoGrpcLog
    public OTCGetOrdersResponse getOrderList(OTCGetOrdersRequest request) {
        Stopwatch sw = Stopwatch.createStarted();
        log.info("OTC:list order, request={}", CommonUtil.formatMessage(request));
        try {
            Long orgId = OtcBaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
            Long accountId = accountService.getAccountId(orgId, request.getBaseRequest().getUserId());
            OTCOrderServiceGrpc.OTCOrderServiceBlockingStub stub = grpcClientConfig.otcOrderServiceBlockingStub(orgId);
            request = request.toBuilder()
                    .setAccountId(accountId)
                    .build();
            return stub.getOrders(request);
        } catch (StatusRuntimeException e) {
            log.error(e.getMessage(), e);
            log.error("OTC:list order exception,{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            log.error("OTC:list order exception, " + e.getMessage(), e);
            throw new BrokerException(BrokerErrorCode.GRPC_SERVER_SYSTEM_ERROR);
        } finally {
            log.info("OTC:list order,consume={},request={}",
                    sw.stop().elapsed(TimeUnit.MILLISECONDS),
                    CommonUtil.formatMessage(request));
        }
    }

    @NoGrpcLog
    public OTCGetOrdersResponse getOrderByFromId(OTCGetOrdersRequest request) {
        Stopwatch sw = Stopwatch.createStarted();
        log.info("OTC:list order, request={}", CommonUtil.formatMessage(request));
        try {
            Long orgId = OtcBaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
            Long accountId = accountService.getAccountId(orgId, request.getBaseRequest().getUserId());
            OTCOrderServiceGrpc.OTCOrderServiceBlockingStub stub = grpcClientConfig.otcOrderServiceBlockingStub(orgId);
            request = request.toBuilder()
                    .setAccountId(accountId)
                    .build();
            return stub.getOrderByFromId(request);
        } catch (StatusRuntimeException e) {
            log.error(e.getMessage(), e);
            log.error("OTC:list order exception,{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            log.error("OTC:list order exception, " + e.getMessage(), e);
            throw new BrokerException(BrokerErrorCode.GRPC_SERVER_SYSTEM_ERROR);
        } finally {
            log.info("OTC:list order,consume={},request={}",
                    sw.stop().elapsed(TimeUnit.MILLISECONDS),
                    CommonUtil.formatMessage(request));
        }
    }

    public OTCGetOrderInfoResponse getOrder(OTCGetOrderInfoRequest request) {
        Stopwatch sw = Stopwatch.createStarted();
        log.info("OTC:get order, request={}", CommonUtil.formatMessage(request));
        try {
            Long orgId = OtcBaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
            Long accountId = accountService.getAccountId(orgId, request.getBaseRequest().getUserId());
            OTCOrderServiceGrpc.OTCOrderServiceBlockingStub stub = grpcClientConfig.otcOrderServiceBlockingStub(orgId);
            request = request.toBuilder()
                    .setAccountId(accountId)
                    .build();
            return stub.getOrderInfo(request);
        } catch (StatusRuntimeException e) {
            log.error(e.getMessage(), e);
            log.error("OTC:get order exception,{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            log.error("OTC:get order exception, " + e.getMessage(), e);
            throw new BrokerException(BrokerErrorCode.GRPC_SERVER_SYSTEM_ERROR);
        } finally {
            log.info("OTC:get order,consume={},request={}",
                    sw.stop().elapsed(TimeUnit.MILLISECONDS),
                    CommonUtil.formatMessage(request));
        }
    }

    public OTCGetOrderInfoResponse getNewOrder(OTCGetOrderInfoRequest request) {
        Stopwatch sw = Stopwatch.createStarted();
        log.info("OTC:get new order, request={}", CommonUtil.formatMessage(request));
        try {
            Long orgId = OtcBaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
            OTCOrderServiceGrpc.OTCOrderServiceBlockingStub stub = grpcClientConfig.otcOrderServiceBlockingStub(orgId);
            request = request.toBuilder()
                    .setAccountId(accountService.getAccountId(orgId, request.getBaseRequest().getUserId()))
                    .build();
            return stub.getNewOrderInfo(request);
            //对手方账户ID
//            Long accountId = response.getOrder().getTargetAccountId();
//            if (accountId == null || accountId.equals(0L)) {
//                return response;
//            }

//            UserVerify userVerify = this.userService.queryUserVerifyInfoByAccountId(accountId);
//            if (userVerify == null) {
//            return response;
//            }
//            OTCOrderDetail otcOrderDetail = response.getOrder();
//            return OTCGetOrderInfoResponse.newBuilder()
//                    .setOrder(otcOrderDetail.toBuilder().setTargetFirstName(StringUtils.isEmpty(userVerify.getFirstName()) ? "" : userVerify.getFirstName())
//                            .setTargetSecondName(StringUtils.isEmpty(userVerify.getSecondName()) ? "" : userVerify.getSecondName()))
//                    .setResult(response.getResult()).build();
        } catch (StatusRuntimeException e) {
            log.error("OTC:get new order exception,{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        } catch (Exception e) {
            log.error("OTC:get new order exception, " + e.getMessage(), e);
            throw new BrokerException(BrokerErrorCode.GRPC_SERVER_SYSTEM_ERROR);
        } finally {
            log.info("OTC:get new order,consume={},request={}",
                    sw.stop().elapsed(TimeUnit.MILLISECONDS),
                    CommonUtil.formatMessage(request));
        }
    }

    @OtcAppealNotify
    public OTCHandleOrderResponse handleOrder(OTCHandleOrderRequest request) {
        log.info("OTC:handle order request={}", CommonUtil.formatMessage(request));
        Stopwatch sw = Stopwatch.createStarted();
        try {
            Long orgId = OtcBaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
            OTCOrderServiceGrpc.OTCOrderServiceBlockingStub stub = grpcClientConfig.otcOrderServiceBlockingStub(orgId);
            request = request.toBuilder()
                    .setAccountId(accountService.getAccountId(orgId, request.getBaseRequest().getUserId()))
                    .build();
            return stub.handleOrder(request);
        } catch (StatusRuntimeException e) {
            log.error("OTC:handle order exception,{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        } catch (Exception e) {
            log.error("OTC:handle order exception, " + e.getMessage(), e);
            throw new BrokerException(BrokerErrorCode.GRPC_SERVER_SYSTEM_ERROR);
        } finally {
            log.info("OTC:handle order,consume={},request={}",
                    sw.stop().elapsed(TimeUnit.MILLISECONDS),
                    CommonUtil.formatMessage(request));

        }
    }

    @NoGrpcLog
    public OTCGetPendingOrdersResponse getPendingOrders(OTCGetPendingOrdersRequest request) {
        log.info("OTC:list pending order, request={}", CommonUtil.formatMessage(request));
        Stopwatch sw = Stopwatch.createStarted();
        try {
            Long orgId = OtcBaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
            OTCOrderServiceGrpc.OTCOrderServiceBlockingStub stub = grpcClientConfig.otcOrderServiceBlockingStub(orgId);
            request = request.toBuilder()
                    .setAccountId(accountService.getAccountId(orgId, request.getBaseRequest().getUserId()))
                    .build();
            return stub.getPendingOrders(request);
        } catch (StatusRuntimeException e) {
            log.error("OTC:list pending order exception, {}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        } catch (Exception e) {
            log.error("OTC:list pending order exception, " + e.getMessage(), e);
            throw new BrokerException(BrokerErrorCode.GRPC_SERVER_SYSTEM_ERROR);
        } finally {
            log.info("OTC:list pending order,consume={},request={}",
                    sw.stop().elapsed(TimeUnit.MILLISECONDS),
                    CommonUtil.formatMessage(request));

        }
    }

    @NoGrpcLog
    public OTCGetPendingCountResponse getPendingOrderCount(OTCGetPendingCountRequest request) {
        log.info("OTC:count pending order, request={}", CommonUtil.formatMessage(request));
        Stopwatch sw = Stopwatch.createStarted();
        try {
            Long orgId = OtcBaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
            OTCOrderServiceGrpc.OTCOrderServiceBlockingStub stub = grpcClientConfig.otcOrderServiceBlockingStub(orgId);
            request = request.toBuilder()
                    .setAccountId(accountService.getAccountId(orgId, request.getBaseRequest().getUserId()))
                    .build();
            return stub.getPendingOrderCount(request);
        } catch (StatusRuntimeException e) {
            log.error("OTC:count pending order exception,{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        } catch (Exception e) {
            log.error("OTC:count pending order exception, " + e.getMessage(), e);
            throw new BrokerException(BrokerErrorCode.GRPC_SERVER_SYSTEM_ERROR);
        } finally {
            log.info("OTC:count pending order,consume={},request={}",
                    sw.stop().elapsed(TimeUnit.MILLISECONDS),
                    CommonUtil.formatMessage(request));
        }
    }

    public OTCNewMessageResponse sendMessage(OTCNewMessageRequest request) {
        Long orgId = OtcBaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        OTCMessageServiceGrpc.OTCMessageServiceBlockingStub stub = grpcClientConfig.otcMessageServiceBlockingStub(orgId);
        try {
            request = request.toBuilder()
                    .setAccountId(accountService.getAccountId(request.getBaseRequest().getOrgId(), request.getBaseRequest().getUserId()))
                    .build();
            return stub.addMessage(request);
        } catch (StatusRuntimeException e) {
            log.error("sendMessage,{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        } catch (Exception e) {
            log.error("OTC:sendMessage exception, " + e.getMessage(), e);
            throw new BrokerException(BrokerErrorCode.GRPC_SERVER_SYSTEM_ERROR);
        }
    }

    @NoGrpcLog
    public OTCGetMessagesResponse getMessageList(OTCGetMessagesRequest request) {
        Long orgId = OtcBaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        OTCMessageServiceGrpc.OTCMessageServiceBlockingStub stub = grpcClientConfig.otcMessageServiceBlockingStub(orgId);
        try {
            request = request.toBuilder()
                    .setAccountId(accountService.getAccountId(orgId, request.getBaseRequest().getUserId()))
                    .build();
            return stub.getMessages(request);
        } catch (StatusRuntimeException e) {
            log.error("getMessageList,{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        } catch (Exception e) {
            log.error("OTC:getMessageList exception, " + e.getMessage(), e);
            throw new BrokerException(BrokerErrorCode.GRPC_SERVER_SYSTEM_ERROR);
        }
    }

    @NoGrpcLog
    public GetAppealMessagesResponse getAppealMessageList(GetAppealMessagesRequest request) {
        Long orgId = OtcBaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        OTCMessageServiceGrpc.OTCMessageServiceBlockingStub stub = grpcClientConfig.otcMessageServiceBlockingStub(orgId);
        try {
            request = request.toBuilder()
                    .setAccountId(accountService.getAccountId(orgId, request.getBaseRequest().getUserId()))
                    .build();
            return stub.getAppealMessages(request);
        } catch (StatusRuntimeException e) {
            log.error("getAppealMessageList,{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        } catch (Exception e) {
            log.error("OTC:getAppealMessageList exception, " + e.getMessage(), e);
            throw new BrokerException(BrokerErrorCode.GRPC_SERVER_SYSTEM_ERROR);
        }
    }

    @UserActionLogAnnotation(actionType = BrokerServerConstants.ACTION_TYPE_ADD_OTC_PAYMENT, resultSucField = "result",
            action = "paymentType:{#request.paymentType}", userId = "{#request.baseRequest.userId}", orgId = "{#request.baseRequest.orgId}")
    public OTCNewPaymentResponse addPaymentTerm(OTCNewPaymentRequest request) {
        Long orgId = OtcBaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        OTCPaymentTermServiceGrpc.OTCPaymentTermServiceBlockingStub stub = grpcClientConfig
                .otcPaymentTermServiceBlockingStub(orgId);
        try {
            if (request.getPaymentType() == OTCPaymentTypeEnum.OTC_PAYMENT_BANK) {
                if (!"OTHER".equalsIgnoreCase(request.getBankName()) && !"其他".equalsIgnoreCase(request.getBankName())) {
                    if (!verifyCard(request.getBaseRequest().getUserId(), request.getAccountNo(), request.getRealName().replaceAll("\\s*", ""))) {
                        return OTCNewPaymentResponse.newBuilder().setResult(OTCResult.BIND_CARD_ERROR).build();
                    }
                }
            }
            request = request.toBuilder()
                    .setAccountId(accountService.getAccountId(orgId, request.getBaseRequest().getUserId()))
                    .setRealName(request.getRealName().trim())
                    .build();
            return stub.addPaymentTerm(request);
        } catch (StatusRuntimeException e) {
            log.error("addPaymentTerm,{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        } catch (Exception e) {
            log.error("OTC:addPaymentTerm exception, " + e.getMessage(), e);
            throw new BrokerException(BrokerErrorCode.GRPC_SERVER_SYSTEM_ERROR);
        }
    }

    public OTCGetPaymentResponse getPaymentTerms(OTCGetPaymentRequest request) {
        Long orgId = OtcBaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        OTCPaymentTermServiceGrpc.OTCPaymentTermServiceBlockingStub stub = grpcClientConfig
                .otcPaymentTermServiceBlockingStub(orgId);
        try {
            request = request.toBuilder()
                    .setAccountId(accountService.getAccountId(orgId, request.getBaseRequest().getUserId()))
                    .build();
            return stub.getPaymentTerms(request);
        } catch (StatusRuntimeException e) {
            log.error("getPaymentTerms,{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        } catch (Exception e) {
            log.error("OTC:getPaymentTerms exception, " + e.getMessage(), e);
            throw new BrokerException(BrokerErrorCode.GRPC_SERVER_SYSTEM_ERROR);
        }
    }

    public OTCUpdatePaymentResponse updatePaymentTerm(OTCUpdatePaymentRequest request) {
        Long orgId = OtcBaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        OTCPaymentTermServiceGrpc.OTCPaymentTermServiceBlockingStub stub = grpcClientConfig
                .otcPaymentTermServiceBlockingStub(orgId);
        try {
            if (request.getPaymentType() == OTCPaymentTypeEnum.OTC_PAYMENT_BANK) {
                if (!"OTHER".equalsIgnoreCase(request.getBankName()) && !"其他".equalsIgnoreCase(request.getBankName())) {
                    if (!verifyCard(request.getBaseRequest().getUserId(), request.getAccountNo(), request.getRealName().replaceAll("\\s*", ""))) {
                        return OTCUpdatePaymentResponse.newBuilder().setResult(OTCResult.BIND_CARD_ERROR).build();
                    }
                }
            }
            request = request.toBuilder()
                    .setAccountId(accountService.getAccountId(orgId, request.getBaseRequest().getUserId()))
                    .setRealName(request.getRealName().trim())
                    .build();
            return stub.updatePaymentTerm(request);
        } catch (StatusRuntimeException e) {
            log.error("updatePaymentTerm,{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        } catch (Exception e) {
            log.error("OTC:updatePaymentTerm exception, " + e.getMessage(), e);
            throw new BrokerException(BrokerErrorCode.GRPC_SERVER_SYSTEM_ERROR);
        }
    }

    public OTCConfigPaymentResponse configPaymentTerm(OTCConfigPaymentRequest request) {
        Long orgId = OtcBaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        OTCPaymentTermServiceGrpc.OTCPaymentTermServiceBlockingStub stub = grpcClientConfig
                .otcPaymentTermServiceBlockingStub(orgId);
        try {
            request = request.toBuilder()
                    .setAccountId(accountService.getAccountId(orgId, request.getBaseRequest().getUserId()))
                    .build();
            return stub.configPaymentTerm(request);
        } catch (StatusRuntimeException e) {
            log.error("configPaymentTerm,{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        } catch (Exception e) {
            log.error("OTC:configPaymentTerm exception, " + e.getMessage(), e);
            throw new BrokerException(BrokerErrorCode.GRPC_SERVER_SYSTEM_ERROR);
        }
    }

    public void setItemToNormal(OTCNormalItemRequest request) {
        Long orgId = OtcBaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        OTCItemServiceGrpc.OTCItemServiceBlockingStub stub = grpcClientConfig.otcItemServiceBlockingStub(orgId);
        try {
            stub.setItemToNormal(request);
        } catch (StatusRuntimeException e) {
            log.error("setItemToNormal,{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        } catch (Exception e) {
            log.error("OTC:setItemToNormal exception, " + e.getMessage(), e);
            throw new BrokerException(BrokerErrorCode.GRPC_SERVER_SYSTEM_ERROR);
        }
    }

    public void setItemToDelete(OTCDeleteItemRequest request) {
        Long orgId = OtcBaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        OTCItemServiceGrpc.OTCItemServiceBlockingStub stub = grpcClientConfig.otcItemServiceBlockingStub(orgId);
        try {
            stub.setItemToDelete(request);
        } catch (StatusRuntimeException e) {
            log.error("setItemToDelete,{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        } catch (Exception e) {
            log.error("OTC:setItemToDelete exception, " + e.getMessage(), e);
            throw new BrokerException(BrokerErrorCode.GRPC_SERVER_SYSTEM_ERROR);
        }
    }

    public void setOrderToNormal(OTCNormalOrderRequest request) {
        Long orgId = OtcBaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        OTCOrderServiceGrpc.OTCOrderServiceBlockingStub stub = grpcClientConfig.otcOrderServiceBlockingStub(orgId);
        try {
            stub.setOrderToNormal(request);
        } catch (StatusRuntimeException e) {
            log.error("setOrderToNormal,{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        } catch (Exception e) {
            log.error("OTC:setOrderToNormal exception, " + e.getMessage(), e);
            throw new BrokerException(BrokerErrorCode.GRPC_SERVER_SYSTEM_ERROR);
        }
    }

    public void setOrderToDelete(OTCDeleteOrderRequest request) {
        Long orgId = OtcBaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        OTCOrderServiceGrpc.OTCOrderServiceBlockingStub stub = grpcClientConfig.otcOrderServiceBlockingStub(orgId);
        try {
            stub.setOrderToDelete(request);
        } catch (StatusRuntimeException e) {
            log.error("setOrderToDelete,{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        } catch (Exception e) {
            log.error("OTC:setOrderToDelete exception, " + e.getMessage(), e);
            throw new BrokerException(BrokerErrorCode.GRPC_SERVER_SYSTEM_ERROR);
        }
    }

    public OTCGetItemIdResponse getItemIdByClientId(OTCGetItemIdRequest request) {
        OTCItemServiceGrpc.OTCItemServiceBlockingStub stub = grpcClientConfig.otcItemServiceBlockingStub(request.getOrgId());
        try {
            return stub.getItemIdByClientId(request);
        } catch (StatusRuntimeException e) {
            log.error("getItemIdByClientId,{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        } catch (Exception e) {
            log.error("OTC:getItemIdByClientId exception, " + e.getMessage(), e);
            throw new BrokerException(BrokerErrorCode.GRPC_SERVER_SYSTEM_ERROR);
        }
    }

    public OTCGetOrderIdResponse getOrderIdByClientId(OTCGetOrderIdRequest request) {
        OTCOrderServiceGrpc.OTCOrderServiceBlockingStub stub = grpcClientConfig.otcOrderServiceBlockingStub(request.getOrgId());
        try {
            return stub.getOrderIdByClientId(request);
        } catch (StatusRuntimeException e) {
            log.error("getOrderIdByClientId,{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        } catch (Exception e) {
            log.error("OTC:getOrderIdByClientId exception, " + e.getMessage(), e);
            throw new BrokerException(BrokerErrorCode.GRPC_SERVER_SYSTEM_ERROR);
        }
    }

    public OTCGetMessageResponse getMessage(OTCGetMessageRequest request) {
        Long orgId = OtcBaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        OTCOrderServiceGrpc.OTCOrderServiceBlockingStub stub = grpcClientConfig.otcOrderServiceBlockingStub(orgId);
        try {
            return stub.getMessage(request);
        } catch (StatusRuntimeException e) {
            log.error("getMessage,{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        } catch (Exception e) {
            log.error("OTC:getMessage exception, " + e.getMessage(), e);
            throw new BrokerException(BrokerErrorCode.GRPC_SERVER_SYSTEM_ERROR);
        }
    }

    @NoGrpcLog
    public FindOrdersByOrderIdsReponse listOtcOrderByOrderIds(FindOrdersByOrderIdsRequest request) {
        Long orgId = OtcBaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        OTCOrderServiceGrpc.OTCOrderServiceBlockingStub stub = grpcClientConfig.otcOrderServiceBlockingStub(orgId);
        try {
            return stub.findOrdersByOrderIds(request);
        } catch (StatusRuntimeException e) {
            log.error("listOtcOrderByOrderIds,{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        } catch (Exception e) {
            log.error("OTC:listOtcOrderByOrderIds exception, " + e.getMessage(), e);
            throw new BrokerException(BrokerErrorCode.GRPC_SERVER_SYSTEM_ERROR);
        }
    }

    public boolean verifyCard(long userId, String cardNum, String realName) {
        return true;
//        UserVerify userVerify = userVerifyMapper.getByUserId(userId);
//        if (StringUtils.isEmpty(realName)) {
//            realName = userVerify.getFirstName() + userVerify.getSecondName();
//        }
//
//        BankCardVerifyRequest request = BankCardVerifyRequest.builder()
//                .orderId(sequenceGenerator.getLong().toString())
//                .idCardName(realName)
////                .idCardId(userVerify.getCardNo())
//                .bankCardId(cardNum)
//                .build();
//
//        BankCardVerifyResponse response = new BankCardVerifyResponse();
//        try {
//            response = jieanApi.bankCardVerify(request, BankCardVerifyType.CARD_2);
//        } catch (BrokerException brokerException) {
//            if (BrokerErrorCode.IDENTITY_AUTHENTICATION_TIMEOUT.code() == brokerException.getCode()) {
//                log.error("bank card verify timeout userId:{} cardNum:{} realName:{} error:{}",
//                        userId, cardNum, realName, brokerException);
//                return true;
//            } else {
//                log.warn("bank card verify fail userId:{} cardNum:{} realName:{} error:{}",
//                        userId, cardNum, realName, brokerException);
//                return false;
//            }
//        } catch (Exception ex) {
//            log.error("bank card verify error userId:{} cardNum:{} realName:{} error:{}",
//                    userId, cardNum, realName, ex);
//            return true;
//        }
//        if (response != null && "000".equals(response.getRespCode())) {
//            return true;
//        }
//        return false;
    }

    @UserActionLogAnnotation(actionType = BrokerServerConstants.ACTION_TYPE_DELETE_OTC_PAYMENT, resultSucField = "result",
            action = "paymentId:{#request.paymentId}", userId = "{#request.baseRequest.userId}", orgId = "{#request.baseRequest.orgId}")
    public OTCDeletePaymentResponse deletePaymentTerm(OTCDeletePaymentRequest request) {
        Long orgId = OtcBaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        OTCPaymentTermServiceGrpc.OTCPaymentTermServiceBlockingStub stub = grpcClientConfig
                .otcPaymentTermServiceBlockingStub(orgId);
        try {
            request = request.toBuilder()
                    .setAccountId(accountService.getAccountId(orgId, request.getBaseRequest().getUserId()))
                    .build();
            return stub.deletePaymentTerm(request);
        } catch (StatusRuntimeException e) {
            log.error("deletePaymentTerm,{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        } catch (Exception e) {
            log.error("OTC:deletePaymentTerm exception, " + e.getMessage(), e);
            throw new BrokerException(BrokerErrorCode.GRPC_SERVER_SYSTEM_ERROR);
        }
    }

    public SwitchPaymentVisibleResponse switchPaymentVisible(SwitchPaymentVisibleRequest request) {
        Long orgId = OtcBaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        OTCPaymentTermServiceGrpc.OTCPaymentTermServiceBlockingStub stub = grpcClientConfig
                .otcPaymentTermServiceBlockingStub(orgId);
        try {
            OtcWhiteList otcWhiteList
                    = this.otcWhiteListMapper.queryByUserId(request.getBaseRequest().getOrgId(), request.getBaseRequest().getUserId());
            UserVerify userVerifyInfo = userVerifyMapper.getByUserId(request.getBaseRequest().getUserId());
            UserVerify userVerify = null;
            if (userVerifyInfo != null) {
                userVerify = UserVerify.decrypt(userVerifyInfo);
            }

            String realName = "";
            if (userVerify != null && StringUtils.isNoneBlank(userVerify.getFirstName())) {
                realName = userVerify.getFirstName();
            }
            if (userVerify != null && StringUtils.isNoneBlank(userVerify.getSecondName())) {
                realName = realName + userVerify.getSecondName();
            }

            request = request.toBuilder()
                    .setAccountId(accountService.getAccountId(orgId, request.getBaseRequest().getUserId()))
                    .setRealName(realName)
                    .setIsBusines(otcWhiteList == null ? 0 : 1)
                    .build();
            return stub.switchPaymentVisible(request);
        } catch (StatusRuntimeException e) {
            log.error("switchPaymentVisible,{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        } catch (Exception e) {
            log.error("OTC:switchPaymentVisible exception, " + e.getMessage(), e);
            throw new BrokerException(BrokerErrorCode.GRPC_SERVER_SYSTEM_ERROR);
        }
    }

    public GetBrokerPaymentConfigResponse getBrokerPaymentConfig(GetBrokerPaymentConfigRequest request) {
        Long orgId = OtcBaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        OTCPaymentTermServiceGrpc.OTCPaymentTermServiceBlockingStub stub = grpcClientConfig
                .otcPaymentTermServiceBlockingStub(orgId);
        try {
            return stub.getBrokerPaymentConfig(request);
        } catch (StatusRuntimeException e) {
            log.error("switchPaymentVisible,{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        } catch (Exception e) {
            log.error("OTC:switchPaymentVisible exception, " + e.getMessage(), e);
            throw new BrokerException(BrokerErrorCode.GRPC_SERVER_SYSTEM_ERROR);
        }
    }

    @UserActionLogAnnotation(actionType = BrokerServerConstants.ACTION_TYPE_ADD_OTC_PAYMENT, resultSucField = "result",
            action = "paymentType:{#request.paymentType}", userId = "{#request.baseRequest.userId}", orgId = "{#request.baseRequest.orgId}")
    public OTCCreateNewPaymentResponse addNewPaymentTerm(OTCCreateNewPaymentRequest request) {
        Long orgId = OtcBaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        OTCPaymentTermServiceGrpc.OTCPaymentTermServiceBlockingStub stub = grpcClientConfig
                .otcPaymentTermServiceBlockingStub(orgId);
        try {
            if (request.getPaymentType() == OTCPaymentTypeEnum.OTC_PAYMENT_BANK) {
                if (!"OTHER".equalsIgnoreCase(request.getBankName()) && !"其他".equalsIgnoreCase(request.getBankName())) {
                    if (!verifyCard(request.getBaseRequest().getUserId(), request.getAccountNo(), request.getRealName().replaceAll("\\s*", ""))) {
                        return OTCCreateNewPaymentResponse.newBuilder().setResult(OTCResult.BIND_CARD_ERROR).build();
                    }
                }
            }
            request = request.toBuilder()
                    .setAccountId(accountService.getAccountId(orgId, request.getBaseRequest().getUserId()))
                    .build();
            return stub.addNewPaymentTerm(request);
        } catch (StatusRuntimeException e) {
            log.error("addNewPaymentTerm,{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        } catch (Exception e) {
            log.error("OTC:addNewPaymentTerm exception, " + e.getMessage(), e);
            throw new BrokerException(BrokerErrorCode.GRPC_SERVER_SYSTEM_ERROR);
        }
    }

    public OTCUpdateNewPaymentResponse updateNewPaymentTerm(OTCUpdateNewPaymentRequest request) {
        Long orgId = OtcBaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        OTCPaymentTermServiceGrpc.OTCPaymentTermServiceBlockingStub stub = grpcClientConfig
                .otcPaymentTermServiceBlockingStub(orgId);
        try {
            if (request.getPaymentType() == OTCPaymentTypeEnum.OTC_PAYMENT_BANK) {
                if (!"OTHER".equalsIgnoreCase(request.getBankName()) && !"其他".equalsIgnoreCase(request.getBankName())) {
                    if (!verifyCard(request.getBaseRequest().getUserId(), request.getAccountNo(), request.getRealName().replaceAll("\\s*", ""))) {
                        return OTCUpdateNewPaymentResponse.newBuilder().setResult(OTCResult.BIND_CARD_ERROR).build();
                    }
                }
            }
            request = request.toBuilder()
                    .setAccountId(accountService.getAccountId(orgId, request.getBaseRequest().getUserId()))
                    .build();
            return stub.updateNewPaymentTerm(request);
        } catch (StatusRuntimeException e) {
            log.error("updateNewPaymentTerm,{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        } catch (Exception e) {
            log.error("OTC:updateNewPaymentTerm exception, " + e.getMessage(), e);
            throw new BrokerException(BrokerErrorCode.GRPC_SERVER_SYSTEM_ERROR);
        }
    }

    public GetTradeFeeRateResponse getTradeFeeRate(GetTradeFeeRateRequest request) {
        OTCItemServiceGrpc.OTCItemServiceBlockingStub stub = grpcClientConfig
                .otcItemServiceBlockingStub(request.getOrgId());
        try {
            return stub.getTradeFeeRate(request);
        } catch (StatusRuntimeException e) {
            log.error("getTradeFeeRate,{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        } catch (Exception e) {
            log.error("OTC:getTradeFeeRate exception, " + e.getMessage(), e);
            throw new BrokerException(BrokerErrorCode.GRPC_SERVER_SYSTEM_ERROR);
        }
    }

    public GetTradeFeeRateByTokenIdResponse getTradeFeeRateByTokenId(GetTradeFeeRateByTokenIdRequest request) {
        OTCItemServiceGrpc.OTCItemServiceBlockingStub stub = grpcClientConfig
                .otcItemServiceBlockingStub(request.getOrgId());
        try {
            return stub.getTradeFeeRateByTokenId(request);
        } catch (StatusRuntimeException e) {
            log.error("getTradeFeeRateByTokenId,{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        } catch (Exception e) {
            log.error("OTC:getTradeFeeRateByTokenId exception, " + e.getMessage(), e);
            throw new BrokerException(BrokerErrorCode.GRPC_SERVER_SYSTEM_ERROR);
        }
    }

    @NoGrpcLog
    public GetOTCSymbolsResponse getBrokerSymbols(GetOTCSymbolsRequest request) {
        OTCConfigServiceGrpc.OTCConfigServiceBlockingStub stub = grpcClientConfig.otcConfigServiceBlockingStub(request.getOrgId());
        try {
            return stub.getOTCBrokerSymbols(request);
        } catch (StatusRuntimeException e) {
            log.error("getBrokerSymbols,{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        } catch (Exception e) {
            log.error("OTC:getBrokerSymbols exception, " + e.getMessage(), e);
            throw new BrokerException(BrokerErrorCode.GRPC_SERVER_SYSTEM_ERROR);
        }
    }

    @NoGrpcLog
    public GetOTCTokensResponse getBrokerTokens(GetOTCTokensRequest request) {
        OTCConfigServiceGrpc.OTCConfigServiceBlockingStub stub = grpcClientConfig.otcConfigServiceBlockingStub(request.getOrgId());
        try {
            return stub.getOTCBrokerTokens(request);
        } catch (StatusRuntimeException e) {
            log.error("getBrokerTokens,{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        } catch (Exception e) {
            log.error("OTC:getBrokerTokens exception, " + e.getMessage(), e);
            throw new BrokerException(BrokerErrorCode.GRPC_SERVER_SYSTEM_ERROR);
        }
    }

    @NoGrpcLog
    public GetOTCCurrencysResponse getBrokerCurrencies(GetOTCCurrencysRequest request) {
        OTCConfigServiceGrpc.OTCConfigServiceBlockingStub stub = grpcClientConfig.otcConfigServiceBlockingStub(request.getOrgId());
        try {
            return stub.getOTCBrokerCurrencys(request);
        } catch (StatusRuntimeException e) {
            log.error("getBrokerCurrencies,{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        } catch (Exception e) {
            log.error("OTC:getBrokerCurrencies exception, " + e.getMessage(), e);
            throw new BrokerException(BrokerErrorCode.GRPC_SERVER_SYSTEM_ERROR);
        }
    }


    public OTCUserContactResponse getUserContact(OTCGetNickNameRequest request) {
        Long orgId = OtcBaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
        OTCUserServiceGrpc.OTCUserServiceBlockingStub stub = grpcClientConfig.otcUserServiceBlockingStub(orgId);
        try {
            return stub.getUserContact(request);
        } catch (StatusRuntimeException e) {
            log.error("getUserContact,{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        } catch (Exception e) {
            log.error("OTC:getUserContact exception, " + e.getMessage(), e);
            throw new BrokerException(BrokerErrorCode.GRPC_SERVER_SYSTEM_ERROR);
        }
    }

    @NoGrpcLog
    public GetLastNewOrderIdResponse getLastNewOrderId(OTCGetPendingCountRequest request) {
        try {
            Long orgId = OtcBaseReqUtil.getOrgIdByBaseReq(request.getBaseRequest());
            OTCOrderServiceGrpc.OTCOrderServiceBlockingStub stub = grpcClientConfig.otcOrderServiceBlockingStub(orgId);
            request = request.toBuilder()
                    .setAccountId(accountService.getAccountId(orgId, request.getBaseRequest().getUserId()))
                    .build();
            return stub.getLastNewOrderId(request);
        } catch (StatusRuntimeException e) {
            log.error("OTC:getLastNewOrderId exception,{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        } catch (Exception e) {
            log.error("OTC:getLastNewOrderId exception, " + e.getMessage(), e);
            throw new BrokerException(BrokerErrorCode.GRPC_SERVER_SYSTEM_ERROR);
        }
    }

    @NoGrpcLog
    public List<OTCUser> listUser(Set<Long> userIds, Long orgId) {
        try {
            OTCUserServiceGrpc.OTCUserServiceBlockingStub stub = grpcClientConfig.otcUserServiceBlockingStub(orgId);
            ListOtcUserRequest request = ListOtcUserRequest.newBuilder()
                    .setBaseRequest(OtcBaseReqUtil.getBaseRequest(orgId))
                    .addAllUserIds(userIds)
                    .build();
            ListOtcUserResponse response = stub.listUser(request);
            if (response.getResult() == OTCResult.SUCCESS) {
                return response.getUsersList();
            }

            throw new IllegalStateException(response.getResult().name());
        } catch (StatusRuntimeException e) {
            log.error("OTC:listUser exception,{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        } catch (Exception e) {
            log.error("OTC:listUser exception, " + e.getMessage(), e);
            throw new BrokerException(BrokerErrorCode.GRPC_SERVER_SYSTEM_ERROR);
        }
    }

    public io.bhex.ex.otc.AllBrokerExtResponse getAllBrokerExt() {
        io.bhex.ex.otc.OTCConfigServiceGrpc.OTCConfigServiceBlockingStub stub = grpcClientConfig.otcConfigServiceBlockingStub();
        try {
            return stub.getAllBrokerExt(OrgIdRequest.getDefaultInstance());
        } catch (StatusRuntimeException e) {
            log.error("{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        }
    }

    public QueryOtcPaymentTermListResponse queryOtcPaymentTermList(QueryOtcPaymentTermListRequest request) {
        OTCPaymentTermServiceGrpc.OTCPaymentTermServiceBlockingStub stub = grpcClientConfig
                .otcPaymentTermServiceBlockingStub();
        try {
            return stub.queryOtcPaymentTermList(request);
        } catch (StatusRuntimeException e) {
            log.error("queryOtcPaymentTermList,{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        } catch (Exception e) {
            log.error("OTC:queryOtcPaymentTermList exception, " + e.getMessage(), e);
            throw new BrokerException(BrokerErrorCode.GRPC_SERVER_SYSTEM_ERROR);
        }
    }

    public BatchUpdatePaymentVisibleResponse batchUpdatePaymentVisible(BatchUpdatePaymentVisibleRequest request) {
        OTCPaymentTermServiceGrpc.OTCPaymentTermServiceBlockingStub stub = grpcClientConfig
                .otcPaymentTermServiceBlockingStub();
        try {
            return stub.batchUpdatePaymentVisible(request);
        } catch (StatusRuntimeException e) {
            log.error("batchUpdatePaymentVisible,{}", printStatusRuntimeException(e));
            throw commonStatusRuntimeException(e);
        } catch (Exception e) {
            log.error("OTC:batchUpdatePaymentVisible exception, " + e.getMessage(), e);
            throw new BrokerException(BrokerErrorCode.GRPC_SERVER_SYSTEM_ERROR);
        }
    }
}
