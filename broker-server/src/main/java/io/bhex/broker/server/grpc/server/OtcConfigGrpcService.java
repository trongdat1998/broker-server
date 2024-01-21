package io.bhex.broker.server.grpc.server;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import com.google.common.collect.Maps;
import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.base.grpc.server.interceptor.GrpcServerLogInterceptor;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.grpc.otc.*;
import io.bhex.broker.server.domain.OTCUserInfo;
import io.bhex.broker.server.grpc.server.service.OtcBankService;
import io.bhex.broker.server.grpc.server.service.OtcCurrencyService;
import io.bhex.broker.server.grpc.server.service.OtcSymbolService;
import io.bhex.broker.server.grpc.server.service.OtcTokenService;
import io.bhex.broker.server.grpc.server.service.OtcWhiteListService;
import io.bhex.broker.server.model.OtcBank;
import io.bhex.broker.server.model.OtcSymbol;
import io.bhex.broker.server.model.OtcWhiteList;
import io.bhex.broker.server.model.User;
import io.bhex.broker.server.util.CommonUtil;
import io.bhex.broker.server.util.OrgConfigUtil;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.util.CollectionUtils;

/**
 * @author lizhen
 * @date 2018-11-04
 */
@Slf4j
@GrpcService(interceptors = GrpcServerLogInterceptor.class)
public class OtcConfigGrpcService extends OTCConfigServiceGrpc.OTCConfigServiceImplBase {

    @Autowired
    private OtcTokenService otcTokenService;

    @Autowired
    private OtcBankService otcBankService;

    @Autowired
    private OtcCurrencyService otcCurrencyService;

    @Autowired
    private OtcSymbolService otcSymbolService;

    @Autowired
    private OtcWhiteListService otcWhiteListService;

    @Override
    public void getOTCTokens(GetOTCTokensRequest request, StreamObserver<GetOTCTokensResponse> responseObserver) {
        GetOTCTokensResponse.Builder responseBuilder = GetOTCTokensResponse.newBuilder();
        try {
            List<OTCToken> tokenList;
            tokenList = otcTokenService.getOtcTokenList(request.getOrgId() > 0 ? request.getOrgId() : null);
            if (!CollectionUtils.isEmpty(tokenList)) {
/*                List<OTCToken> list = tokenList.stream()
                        .map(otcToken -> OTCToken.newBuilder()
                                .setOrgId(otcToken.getOrgId())
                                .setTokenId(otcToken.getTokenId())
                                .setScale(otcToken.getScale())
                                .setMinQuote(otcToken.getMinQuote().stripTrailingZeros().toPlainString())
                                .setMaxQuote(otcToken.getMaxQuote().stripTrailingZeros().toPlainString())
                                .setUpRange(otcToken.getUpRange().stripTrailingZeros().toPlainString())
                                .setDownRange(otcToken.getDownRange().stripTrailingZeros().toPlainString())
                                .setStatus(otcToken.getStatus())
                                .build())
                        .collect(Collectors.toList());
                responseBuilder.addAllToken(list);
*/
                responseBuilder.addAllToken(tokenList);
            }
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void getOTCBanks(GetOTCBanksRequest request, StreamObserver<GetOTCBanksResponse> responseObserver) {
        GetOTCBanksResponse.Builder responseBuilder = GetOTCBanksResponse.newBuilder();
        try {
            List<OtcBank> bankList = otcBankService.getOtcBankList(request.getOrgId() > 0
                    ? request.getOrgId() : null);
            if (!CollectionUtils.isEmpty(bankList)) {
                Map<String, OTCBank> bankMap = Maps.newLinkedHashMap();
                for (OtcBank otcBank : bankList) {
                    String bankKey = otcBank.getCode() + "-" + otcBank.getOrgId();
                    OTCBank bank = bankMap.get(bankKey);
                    if (bank == null) {
                        bank = OTCBank.newBuilder()
                                .setOrgId(otcBank.getOrgId())
                                .setCode(otcBank.getCode())
                                .addLanguage(OTCLanguage.newBuilder()
                                        .setCode(otcBank.getLanguage())
                                        .setName(otcBank.getName())
                                        .build())
                                .build();
                    } else {
                        bank = bank.toBuilder()
                                .addLanguage(OTCLanguage.newBuilder()
                                        .setCode(otcBank.getLanguage())
                                        .setName(otcBank.getName())
                                        .build())
                                .build();
                    }
                    bankMap.put(bankKey, bank);
                }
                responseBuilder.addAllBank(bankMap.values());
            }
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void getOTCCurrencys(GetOTCCurrencysRequest request,
                                StreamObserver<GetOTCCurrencysResponse> responseObserver) {
        GetOTCCurrencysResponse.Builder responseBuilder = GetOTCCurrencysResponse.newBuilder();
        try {
            List<io.bhex.ex.otc.OTCCurrency> currencyList = otcCurrencyService.getOtcCurrencyList(request.getOrgId() > 0
                        ? request.getOrgId() : null);
            if (!CollectionUtils.isEmpty(currencyList)) {
                Map<String, OTCCurrency> currencyMap = Maps.newLinkedHashMap();
                for (io.bhex.ex.otc.OTCCurrency otcCurrency : currencyList) {
                    String currencyKey = otcCurrency.getCode() + "-" + otcCurrency.getOrgId();
                    OTCCurrency currency = currencyMap.get(currencyKey);
                    if (currency == null) {
                        currency = OTCCurrency.newBuilder()
                                .setOrgId(otcCurrency.getOrgId())
                                .setCode(otcCurrency.getCode())
                                .addLanguage(OTCLanguage.newBuilder()
                                        .setCode(otcCurrency.getLang())
                                        .setName(otcCurrency.getName())
                                        .build())
                                .setMinQuote(otcCurrency.getMinQuote())
                                .setMaxQuote(otcCurrency.getMaxQuote())
                                .setScale(otcCurrency.getScale())
                                .setAmountScale(otcCurrency.getAmountScale())
                                .setStatus(otcCurrency.getStatus())
                                .build();
                    } else {
                        currency = currency.toBuilder()
                                .addLanguage(OTCLanguage.newBuilder()
                                        .setCode(otcCurrency.getLang())
                                        .setName(otcCurrency.getName())
                                        .build())
                                .build();
                    }
                    currencyMap.put(currencyKey, currency);
                }
                responseBuilder.addAllCurrency(currencyMap.values());
            }
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        }
    }

/*    @Override
    public void getOTCCurrencys(GetOTCCurrencysRequest request,
                                StreamObserver<GetOTCCurrencysResponse> responseObserver) {
        GetOTCCurrencysResponse.Builder responseBuilder = GetOTCCurrencysResponse.newBuilder();
        try {
            List<OTCCurrency> currencyList = otcCurrencyService.getOtcCurrencyList(request.getOrgId() > 0
                    ? request.getOrgId() : null);
            responseBuilder.addAllCurrency(currencyList);
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }*/

    @Override
    public void getOTCSymbols(GetOTCSymbolsRequest request, StreamObserver<GetOTCSymbolsResponse> responseObserver) {
        GetOTCSymbolsResponse.Builder responseBuilder = GetOTCSymbolsResponse.newBuilder();
        try {
            List<OtcSymbol> symbolList = otcSymbolService.getOtcSymbolList(request.getExchangeId() > 0
                        ? request.getExchangeId() : null, request.getOrgId() > 0 ? request.getOrgId() : null);
            if (!CollectionUtils.isEmpty(symbolList)) {
                List<OTCSymbol> list = symbolList.stream()
                        .map(otcSymbol -> OTCSymbol.newBuilder()
                                .setId(otcSymbol.getId())
                                .setOrgId(otcSymbol.getOrgId())
                                .setExchangeId(otcSymbol.getExchangeId())
                                .setTokenId(otcSymbol.getTokenId())
                                .setCurrencyId(otcSymbol.getCurrencyId())
                                .build())
                        .collect(Collectors.toList());
                responseBuilder.addAllSymbol(list);
            }
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void getOTCWhiteList(GetOTCWhiteListRequest request,
                                StreamObserver<GetOTCWhiteListResponse> responseObserver) {
        GetOTCWhiteListResponse.Builder responseBuilder = GetOTCWhiteListResponse.newBuilder();
        try {
            List<OtcWhiteList> whiteLists = otcWhiteListService.getOtcWhiteList(request.getOrgId() > 0
                    ? request.getOrgId() : null);
            if (!CollectionUtils.isEmpty(whiteLists)) {
                List<OTCWhiteList> list = whiteLists.stream().map(
                        otcWhiteList -> OTCWhiteList.newBuilder()
                                .setOrgId(otcWhiteList.getOrgId())
                                .setUserId(otcWhiteList.getUserId())
                                .build()
                ).collect(Collectors.toList());
                responseBuilder.addAllWhiteList(list);
            }
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void getOTCWhiteListUser(GetOTCWhiteListUserRequest request,
                                    StreamObserver<GetOTCWhiteListUserResponse> responseObserver) {
        GetOTCWhiteListUserResponse.Builder responseBuilder = GetOTCWhiteListUserResponse.newBuilder();
        try {
            List<OTCUserInfo> userList = otcWhiteListService.getWhiteListUser(
                    request.getOrgId() > 0 ? request.getOrgId() : null,
                    request.getUserId() > 0 ? request.getUserId() : null,
                    request.getPage(),
                    request.getSize());
            if (!CollectionUtils.isEmpty(userList)) {
                List<OTCUser> list = userList.stream().map(
                        otcUser -> {
                            User brokerUser = otcUser.getUser();

                            io.bhex.broker.grpc.user.User user = io.bhex.broker.grpc.user.User.newBuilder()
                                    .setUserId(brokerUser.getUserId())
                                    .setEmail(StringUtils.defaultIfEmpty(brokerUser.getEmail(), ""))
                                    .setNationalCode(StringUtils.defaultIfEmpty(brokerUser.getNationalCode(), ""))
                                    .setMobile(StringUtils.defaultIfEmpty(brokerUser.getMobile(), ""))
                                    .setUserType(brokerUser.getUserType())
                                    .build();
                            OTCUser.Builder builder = OTCUser.newBuilder().setUser(user);
                            if (Objects.nonNull(otcUser.getOtcUser())) {
                                UserExtend extend = UserExtend.newBuilder()
                                        .setRealName(otcUser.getRealName())
                                        .setNickname(otcUser.getOtcUser().getNickname())
                                        .setUserId(otcUser.getOtcUser().getUserId())
                                        .setFinishOrderNumber30Days(otcUser.getOtcUser().getFinishOrderNumber30Days())
                                        .setFinishOrderRate30Days(otcUser.getOtcUser().getFinishOrderRate30Days())
                                        .setAccountId(otcUser.getOtcUser().getAccountId())
                                        .build();

                                return builder.setUserExt(extend).build();
                            }
                            return builder.build();
                        }
                ).collect(Collectors.toList());
                responseBuilder.addAllOtcUsers(list);
            }
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void delOTCWhiteList(DelOTCWhiteListRequest request,
                                StreamObserver<DelOTCWhiteListResponse> responseObserver) {
        DelOTCWhiteListResponse.Builder responseBuilder = DelOTCWhiteListResponse.newBuilder();
        try {
            otcWhiteListService.deleteOtcWhiteList(request.getOrgId(), request.getUserId());
            responseObserver.onNext(responseBuilder.setRet(0).build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void addOTCWhiteList(AddOTCWhiteListRequest request,
                                StreamObserver<AddOTCWhiteListResponse> responseObserver) {
        try {
            otcWhiteListService.addOtcWhiteList(request.getOrgId(), request.getUserId());
            responseObserver.onNext(AddOTCWhiteListResponse.newBuilder().setRet(0).build());
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            log.error("orgId=" + request.getOrgId() + ",userId=" + request.getUserId(), e);
            responseObserver.onNext(AddOTCWhiteListResponse.newBuilder().setRet(e.getCode()).build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        }
    }


}
