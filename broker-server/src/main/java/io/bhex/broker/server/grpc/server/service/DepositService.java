/**********************************
 *@项目名称: broker-server
 *@文件名称: io.bhex.broker.server.grpc.server.service
 *@Date 2018/8/23
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
package io.bhex.broker.server.grpc.server.service;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import io.bhex.base.account.*;
import io.bhex.base.proto.DecimalUtil;
import io.bhex.base.token.TokenTypeEnum;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.grpc.basic.Token;
import io.bhex.broker.grpc.basic.TokenChainInfo;
import io.bhex.broker.grpc.common.Header;
import io.bhex.broker.grpc.deposit.*;
import io.bhex.broker.server.grpc.client.service.GrpcDepositService;
import io.bhex.broker.server.model.Account;
import io.bhex.broker.server.model.User;
import io.bhex.broker.server.primary.mapper.AccountMapper;
import io.bhex.broker.server.util.BaseReqUtil;
import io.bhex.broker.server.util.GrpcRequestUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import tk.mybatis.mapper.util.StringUtil;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Service
public class DepositService {

    @Resource
    private GrpcDepositService grpcDepositService;

    @Resource
    private BasicService basicService;

    @Resource
    private AccountMapper accountMapper;

    @Resource
    private UserService userService;

    @Resource
    private AccountService accountService;

    public GetDepositAddressResponse getDepositAddress(Header header, String tokenId, String chainTypeName) {
        Token grpcToken = basicService.getToken(header.getOrgId(), tokenId);
        if (grpcToken == null || !grpcToken.getAllowDeposit()) {
            throw new BrokerException(BrokerErrorCode.DEPOSIT_NOT_ALLOW);
        }

        if (!Strings.isNullOrEmpty(chainTypeName) && !CollectionUtils.isEmpty(grpcToken.getTokenChainInfoList())) {
            for (TokenChainInfo tokenChainInfo : grpcToken.getTokenChainInfoList()) {
                if (tokenChainInfo.getChainType().equalsIgnoreCase(chainTypeName) && !tokenChainInfo.getAllowDeposit()) {
                    throw new BrokerException(BrokerErrorCode.DEPOSIT_NOT_ALLOW);
                }
            }
        }

//        Token token = basicService.getToken(header.getOrgId(), tokenId);
//        io.bhex.broker.server.model.Token token = tokenMapper.getToken(header.getOrgId(), tokenId);
//        if (token == null) {
//            log.error("getDepositAddress get token=null, orgId:{} tokenId:{}", header.getOrgId(), tokenId);
//            throw new BrokerException(BrokerErrorCode.PARAM_INVALID);
//        }
//        if (token.getAllowDeposit() == 0) {
//            throw new BrokerException(BrokerErrorCode.DEPOSIT_NOT_ALLOW);
//        }
//        TokenDetail tokenDetail = grpcTokenService.getToken(GetTokenRequest.newBuilder()
//                .setBaseRequest(GrpcRequestUtil.getBaseRequest(header))
//                .setTokenId(tokenId).build());
//        if (!tokenDetail.getAllowDeposit()) {
//            throw new BrokerException(BrokerErrorCode.DEPOSIT_NOT_ALLOW);
//        }
        boolean chainTypeAllowDeposit = true;
        if (!Strings.isNullOrEmpty(chainTypeName)) {
            List<TokenChainInfo> chainTypesList = grpcToken.getTokenChainInfoList();
            for (TokenChainInfo chainInfo : chainTypesList) {
                if (chainInfo.getChainType().equalsIgnoreCase(chainTypeName)) {
                    chainTypeAllowDeposit = chainInfo.getAllowDeposit();
                }
            }
        }
        GetAddressRequest request = GetAddressRequest.newBuilder()
                .setBaseRequest(GrpcRequestUtil.getBaseRequest(header))
                .setAccountId(accountService.getMainAccountId(header))
                .setLanguage(header.getLanguage())
                .setTokenId(tokenId)
                .setChainType(chainTypeName)
                .build();
        AddressReply addressReply = grpcDepositService.getAddress(request);
        String tokenType = grpcToken.getTokenType();
        if (tokenId.equalsIgnoreCase("USDT") && chainTypeName.equalsIgnoreCase("ERC20")) {
            tokenType = TokenTypeEnum.ERC20_TOKEN.name();
        } else if (tokenId.equalsIgnoreCase("USDT") && chainTypeName.equalsIgnoreCase("TRC20")) {
            tokenType = TokenTypeEnum.TRX_TOKEN.name();
        }
        User user = userService.getUser(header.getUserId());
        if (user.getBindPassword() == 0) {
            throw new BrokerException(BrokerErrorCode.NEED_BIND_PASSWORD);
        }
        return GetDepositAddressResponse.newBuilder()
                .setAllowDeposit(grpcToken.getAllowDeposit() && chainTypeAllowDeposit)
                .setAddress(addressReply.getAddress())
                .setAddressExt(addressReply.getAddressExt())
//                .setMinQuantity(new BigDecimal(grpcToken.getMinDepositQuantity()).stripTrailingZeros().toPlainString())
                .setMinQuantity(new BigDecimal(addressReply.getDepositMinQuantity()).stripTrailingZeros().toPlainString())
                .setIsEos(grpcToken.getTokenType().equalsIgnoreCase(TokenTypeEnum.EOS_TOKEN.name()))
                .setNeedAddressTag(grpcToken.getNeedAddressTag())
                .setRequiredConfirmNum(addressReply.getConfirmNumInNeed())
                .setCanWithdrawConfirmNum(addressReply.getConfirmNumCanWithdraw())
                .setTokenType(tokenType)
                .build();
    }

    public QueryUserDepositAddressResponse queryDepositAddress(Long orgId, Long userId) {
        QueryAddressRequest request = QueryAddressRequest.newBuilder()
                .setBaseRequest(BaseReqUtil.getBaseRequest(orgId))
                .setOrgId(orgId)
                .setAccountId(accountService.getAccountId(orgId, userId))
                .build();
        QueryAddressReply addressReply = grpcDepositService.queryAddress(request);
        List<DepositAddressObj> depositAddressObjList = Lists.newArrayList();
        addressReply.getTokenAddressMap().forEach((key, value) -> {
            DepositAddressObj depositAddressObj = DepositAddressObj.newBuilder()
                    .setTokenId(key)
                    .setAddress(value.getAddress())
                    .setAddressExt(value.getTag())
                    .build();
            depositAddressObjList.add(depositAddressObj);
        });
        return QueryUserDepositAddressResponse.newBuilder()
                .addAllDepositAddress(depositAddressObjList)
                .build();
    }

    public GetDepositOrderDetailResponse getDepositOrder(Header header, Long orderId) {
        GetDepositRecordsRequest request = GetDepositRecordsRequest.newBuilder()
                .setBaseRequest(GrpcRequestUtil.getBaseRequest(header))
                .setAccountId(accountService.getMainAccountId(header))
                .setOrderId(orderId)
                .build();
        List<DepositRecord> depositRecordsList = grpcDepositService.queryDepositOrder(request).getDepositRecordsList();
        if (depositRecordsList.size() <= 0) {
            throw new BrokerException(BrokerErrorCode.ORDER_NOT_FOUND);
        }
        return GetDepositOrderDetailResponse.newBuilder()
                .setOrder(getDepositOrder(header.getOrgId(), depositRecordsList.get(0)))
                .build();
    }

    public QueryDepositOrdersResponse queryDepositOrder(Header header, String tokenId, Long fromOrderId, Long endOrderId,
                                                        Long startTime, Long endTime, Integer limit) {
        return queryDepositOrders(header, accountService.getMainAccountId(header), tokenId, fromOrderId, endOrderId, startTime, endTime, limit);
    }

    public QueryDepositOrdersResponse queryDepositOrder(Header header, Long accountId, String tokenId, Long fromOrderId, Long endOrderId,
                                                        Long startTime, Long endTime, Integer limit) {
        return queryDepositOrders(header, accountId, tokenId, fromOrderId, endOrderId, startTime, endTime, limit);
    }

    public GetUserInfoByAddressResponse getUserInfoByAddress(Header header, String tokenId, String address, String addressExt, String chainType) {
        ForAddressReply forAddressReply = grpcDepositService.forAddress(ForAddressRequest.newBuilder()
                .setBaseRequest(GrpcRequestUtil.getBaseRequest(header))
                .setTokenId(tokenId)
                .setChainType(chainType)
                .setAddress(address)
                .setAddressExt(addressExt)
                .build());
        if (forAddressReply == null || StringUtils.isEmpty(forAddressReply.getBrokerUserId()) || forAddressReply.getOrgId() != header.getOrgId()) {
            return GetUserInfoByAddressResponse
                    .newBuilder()
                    .setUserId(0L)
                    .setOrgId(0L)
                    .setAccountId(0L)
                    .build();
        }
        return GetUserInfoByAddressResponse
                .newBuilder()
                .setUserId(StringUtils.isNotEmpty(forAddressReply.getBrokerUserId()) ? Long.parseLong(forAddressReply.getBrokerUserId()) : 0L)
                .setOrgId(forAddressReply.getOrgId())
                .setAccountId(forAddressReply.getAccountId())
                .build();
    }

    private QueryDepositOrdersResponse queryDepositOrders(Header header, Long accountId, String tokenId, Long fromOrderId, Long endOrderId,
                                                          Long startTime, Long endTime, Integer limit) {
        GetDepositRecordsRequest request = GetDepositRecordsRequest.newBuilder()
                .setBaseRequest(GrpcRequestUtil.getBaseRequest(header))
                .setAccountId(accountId)
                .addAllTokenId(Strings.isNullOrEmpty(tokenId) ? Lists.newArrayList() : Lists.newArrayList(tokenId))
                .setFromDepositRecordId(fromOrderId)
                .setEndDepositRecordId(endOrderId)
                .setStartTime(startTime)
                .setEndTime(endTime)
                .setLimit(limit)
                .build();
        List<DepositRecord> depositRecordsList = grpcDepositService.queryDepositOrder(request).getDepositRecordsList();
        List<DepositOrder> orderList = Lists.newArrayList();
        if (depositRecordsList != null) {
            orderList = depositRecordsList.stream().map(item -> getDepositOrder(header.getOrgId(), item)).collect(Collectors.toList());
        }
        return QueryDepositOrdersResponse.newBuilder()
                .addAllOrders(orderList)
                .build();
    }

    private DepositOrder getDepositOrder(Long orgId, DepositRecord record) {
        boolean isSameBroker = false;
        if (!record.getIsChainDeposit() && StringUtil.isNotEmpty(record.getFromAddress())) {
            try {
                Long accountId = Long.parseLong(record.getFromAddress());
                Account account = accountMapper.getAccountByAccountId(accountId);
                if (account != null && account.getOrgId().equals(orgId)) {
                    isSameBroker = true;
                }
            } catch (Exception e) {
                log.info("judge same broker exception:" + e.getMessage(), e);
            }
        }
        return DepositOrder.newBuilder()
                .setOrderId(record.getDepositRecordId())
                .setAccountId(record.getAccountId())
                .setTokenId(record.getToken().getTokenId())
                .setTokenName(basicService.getTokenName(orgId, record.getToken().getTokenId()))
                .setAddress(record.getAddress())
                .setAddressExt(Strings.nullToEmpty(record.getAddressExt()))
                .setFromAddress(record.getFromAddress())
                .setFromAddressExt(Strings.nullToEmpty(record.getFromAddressExt()))
                .setQuantity(DecimalUtil.toBigDecimal(record.getQuantity()).stripTrailingZeros().toPlainString())
                .setStatus(record.getStatusValue())
                .setStatusCode(record.getStatus().name())
                .setTime(record.getDepositTime())
                .setTxid(record.getTxId())
                .setTxidUrl(record.getTxExploreUrl())
                .setWalletHandleTime(record.getWalletHandleTime())
                .setRequiredConfirmNum(record.getTargetConfirmNum())
                .setConfirmNum(record.getConfirmNum())
                .setIsInternalTransfer(!record.getIsChainDeposit())
                .setIsSameBroker(isSameBroker)
                .build();
    }
}
