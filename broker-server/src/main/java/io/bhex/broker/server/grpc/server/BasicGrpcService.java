/**********************************
 *@项目名称: broker-server
 *@文件名称: io.bhex.broker.server.grpc.server
 *@Date 2018/8/21
 *@Author peiwei.ren@bhex.io
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
package io.bhex.broker.server.grpc.server;

import com.google.common.collect.Lists;
import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.base.grpc.server.interceptor.GrpcServerLogInterceptor;
import io.bhex.base.proto.BaseRequest;
import io.bhex.base.token.GetEtfSymbolPriceReply;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.common.util.Base62Util;
import io.bhex.broker.grpc.basic.*;
import io.bhex.broker.grpc.common.Header;
import io.bhex.broker.server.grpc.server.service.BasicService;
import io.bhex.broker.server.grpc.server.service.FuturesBasicService;
import io.bhex.broker.server.grpc.server.service.ShortUrlService;
import io.bhex.broker.server.model.SensitiveWords;
import io.bhex.broker.server.model.ShortUrl;
import io.bhex.broker.server.util.FuturesUtil;
import io.bhex.broker.server.util.OrgConfigUtil;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.util.CollectionUtils;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@GrpcService(interceptors = GrpcServerLogInterceptor.class)
public class BasicGrpcService extends BasicServiceGrpc.BasicServiceImplBase {

    @Resource
    private BasicService basicService;

    @Resource
    private ShortUrlService shortUrlService;

    @Resource
    private FuturesBasicService futuresBasicService;

    @Override
    public void queryKycCardType(QueryKycCardTypeRequest request, StreamObserver<QueryKycCardTypeResponse> observer) {
        QueryKycCardTypeResponse response;
        try {
            response = basicService.queryKycCardType(request.getHeader());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = QueryKycCardTypeResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("query kyc card type info error", e);
            observer.onError(e);
        }
    }

    @Override
    public void queryCountries(QueryCountryRequest request, StreamObserver<QueryCountryResponse> responseObserver) {
        ServerCallStreamObserver<QueryCountryResponse> observer = (ServerCallStreamObserver<QueryCountryResponse>) responseObserver;
        observer.setCompression("gzip");
        QueryCountryResponse response;
        try {
            response = basicService.queryCountries(request.getHeader());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = QueryCountryResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("query country error", e);
            observer.onError(e);
        }
    }

    @Override
    public void queryQuoteTokens(QueryQuoteTokenRequest request, StreamObserver<QueryQuoteTokenResponse> responseObserver) {
        ServerCallStreamObserver<QueryQuoteTokenResponse> observer = (ServerCallStreamObserver<QueryQuoteTokenResponse>) responseObserver;
        observer.setCompression("gzip");
        QueryQuoteTokenResponse response;
        try {
            if (request.getHeader().getOrgId() > 0) {
                response = basicService.queryQuoteTokensByOrgId(request.getCategoryList(), request.getHeader().getOrgId());
            } else {
                //TODO 暂且延续0L
                response = basicService.queryQuoteTokens(request.getCategoryList(), 0L);
            }
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = QueryQuoteTokenResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("query quote token error", e);
            observer.onError(e);
        }
    }

    @Override
    public void getToken(GetTokenRequest request, StreamObserver<GetTokenResponse> observer) {
        super.getToken(request, observer);
    }

    @Override
    public void queryTokens(QueryTokenRequest request, StreamObserver<QueryTokenResponse> responseObserver) {
        ServerCallStreamObserver<QueryTokenResponse> observer = (ServerCallStreamObserver<QueryTokenResponse>) responseObserver;
        observer.setCompression("gzip");
        QueryTokenResponse response;
        try {
            if (request.getHeader().getOrgId() > 0) {
                response = basicService.queryTokensByOrgId(request.getCategoryList(), request.getHeader().getOrgId());
            } else {
                //TODO 暂且延续0L
                response = basicService.queryTokens(request.getCategoryList(), 0L);
            }
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = QueryTokenResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("query tokens error", e);
            observer.onError(e);
        }
    }

    @Override
    public void queryCoinTokenDetails(QueryCoinTokenDetailsRequest request, StreamObserver<QueryCoinTokenDetailsResponse> observer) {
        QueryCoinTokenDetailsResponse response;
        try {
            response = basicService.queryCoinTokenDetails(request.getHeader());

            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = QueryCoinTokenDetailsResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("queryCoinTokenDetails error", e);
            observer.onError(e);
        }
    }

    @Override
    public void getSymbol(GetSymbolRequest request, StreamObserver<GetSymbolResponse> observer) {
        super.getSymbol(request, observer);
    }


    @Override
    public void querySymbols(QuerySymbolRequest request, StreamObserver<QuerySymbolResponse> responseObserver) {
        ServerCallStreamObserver<QuerySymbolResponse> observer = (ServerCallStreamObserver<QuerySymbolResponse>) responseObserver;
        observer.setCompression("gzip");
        QuerySymbolResponse response;
        try {
            observer.onNext(returnQuerySymbolResponse(request));
            observer.onCompleted();
        } catch (BrokerException e) {
            response = QuerySymbolResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("query symbols error", e);
            observer.onError(e);
        }
    }

    @Override
    public void setSymbolTradeStatus(SetSymbolTradeStatusRequest request, StreamObserver<SetSymbolTradeStatusResponse> observer) {
        SetSymbolTradeStatusResponse response;
        try {
            basicService.setSymbolTradeStatus(request.getHeader(), request.getSymbolId(), request.getStatusValue());
            observer.onNext(SetSymbolTradeStatusResponse.getDefaultInstance());
            observer.onCompleted();
        } catch (BrokerException e) {
            response = SetSymbolTradeStatusResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("setSymbolTradeStatus error", e);
            observer.onError(e);
        }
    }

    public QuerySymbolResponse returnQuerySymbolResponse(QuerySymbolRequest request) {
        List<Integer> categories = request.getCategoryList();
            if (FuturesUtil.isFutureCategory(categories)) {
                List<Symbol> futuresSymbols = filterRiskLimitSymbols(request.getHeader(), futuresBasicService.queryFuturesSymbols(request.getHeader()));
                return QuerySymbolResponse.newBuilder()
                        .addAllSymbol(futuresSymbols)
                        .build();
            }
            QuerySymbolResponse response = basicService.querySymbolsWithTokenDetail(request.getHeader().getOrgId(), categories);
            if (request.getHeader().getOrgId() > 0L) {
                List<Symbol> symbolList = response.getSymbolList().stream().filter(s -> s.getOrgId() == request.getHeader().getOrgId()).collect(Collectors.toList());
                return QuerySymbolResponse.newBuilder()
                        .addAllSymbol(symbolList)
                        .build();
            }
            return response;
    }

    /**
     * 根据白名单用户过滤风险限额配置
     *
     * @param header         Header
     * @param futuresSymbols List<Symbol>
     * @return 过滤之后的List<Symbol>
     */
    private List<Symbol> filterRiskLimitSymbols(Header header, List<Symbol> futuresSymbols) {
        return futuresSymbols.stream().map(t -> {
            if (!t.hasTokenFutures()) {
                return t;
            }

            List<FuturesRiskLimit> riskLimits = t.getTokenFutures().getRiskLimitsList();
            if (CollectionUtils.isEmpty(riskLimits)) {
                return t;
            }
            Header filterHeader = header.toBuilder().setOrgId(t.getOrgId()).build();
            List<FuturesRiskLimit> filterdRiskLimits = riskLimits.stream()
                    .filter(r -> basicService.filterRiskLimit(filterHeader, r.getRiskLimitId(), true))
                    .collect(Collectors.toList());
            TokenFuturesInfo tokenFuturesInfo = t.getTokenFutures().toBuilder().clearRiskLimits().addAllRiskLimits(filterdRiskLimits).build();

            return t.toBuilder().setTokenFutures(tokenFuturesInfo).build();
        }).collect(Collectors.toList());
    }

    @Override
    public void serverTime(GetServerTimeRequest request, StreamObserver<GetServerTimeResponse> observer) {
        GetServerTimeResponse response;
        try {
            response = basicService.getServerTime(request.getServerSleepTime());
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            observer.onError(e);
        }
    }

    @Override
    public void createShortUrl(CreateShortUrlRequest request, StreamObserver<CreateShortUrlResponse> responseObserver) {
        try {
            long urlId = -1;
            if (StringUtils.isNotBlank(request.getLongUrl())) {
                urlId = shortUrlService.createShortUrl(request.getLongUrl());
            }
            String code = "";
            if (urlId > 0) {
                code = Base62Util.encode(urlId) + Base62Util.getValid(urlId);
            }
            responseObserver.onNext(CreateShortUrlResponse.newBuilder()
                    .setUrlId(urlId)
                    .setUrlCode(code)
                    .build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void getLongUrl(GetLongUrlRequest request, StreamObserver<GetLongUrlResponse> responseObserver) {
        try {
            String url;
            if (request.getUrlId() <= 0) {
                url = "";
            } else {
                ShortUrl shortUrl = shortUrlService.getShortUrl(request.getUrlId());
                url = shortUrl == null ? "" : shortUrl.getLongUrl();
            }
            responseObserver.onNext(GetLongUrlResponse.newBuilder().setLongUrl(url).build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void getAllSensitiveWords(GetAllSensitiveWordsRequest request,
                                     StreamObserver<GetAllSensitiveWordsResponse> responseObserver) {
        try {
            List<SensitiveWords> sensitiveWordsList = basicService.getAllSensitiveWords();
            List<io.bhex.broker.grpc.basic.SensitiveWords> list = Lists.newArrayList();
            if (!CollectionUtils.isEmpty(sensitiveWordsList)) {
                list = sensitiveWordsList.stream().map(
                        sensitiveWords -> io.bhex.broker.grpc.basic.SensitiveWords.newBuilder()
                                .setType(sensitiveWords.getType())
                                .setWords(sensitiveWords.getWords())
                                .build()
                ).collect(Collectors.toList());
            }
            responseObserver.onNext(GetAllSensitiveWordsResponse.newBuilder()
                    .addAllSensitiveWord(list)
                    .build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void getUnderlying(GetUnderlyingRequest request, StreamObserver<GetUnderlyingResponse> observer) {
        GetUnderlyingResponse response;
        try {
            long orgId = request.getHeader().getOrgId();
            response = GetUnderlyingResponse.newBuilder()
                    .addAllUnderlying(basicService.getUnderlyings(request.getType(), orgId))
                    .build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = GetUnderlyingResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("getUnderlying error", e);
            observer.onError(e);
        }
    }

    @Override
    public void queryBrokerOpenSymbols(QueryBrokerOpenSymbolsRequest request, StreamObserver<QuerySymbolResponse> observer) {
        QuerySymbolResponse response;
        try {
            response = basicService.queryBrokerOpenSymbols(request.getOrgId());
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = QuerySymbolResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("queryBrokerOpenSymbols error", e);
            observer.onError(e);
        }
    }

    @Override
    public void queryAllSupportSymbols(QueryAllSupportSymbolsRequest request, StreamObserver<QuerySymbolResponse> responseObserver) {
        super.queryAllSupportSymbols(request, responseObserver);
    }

    @Override
    public void queryKycLevels(QueryKycLevelsRequest request, StreamObserver<QueryKycLevelsResponse> observer) {
        QueryKycLevelsResponse response;
        try {
            response = QueryKycLevelsResponse.newBuilder()
                    .addAllKycLevel(basicService.getKycLevelList())
                    .build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = QueryKycLevelsResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("queryKycLevels error", e);
            observer.onError(e);
        }
    }

    @Override
    public void queryKycLevelConfigs(QueryKycLevelConfigsRequest request, StreamObserver<QueryKycLevelConfigsResponse> observer) {
        QueryKycLevelConfigsResponse response;
        try {
            response = QueryKycLevelConfigsResponse.newBuilder()
                    .addAllLevelConfig(basicService.getAllKycLevelConfigList())
                    .build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = QueryKycLevelConfigsResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("queryKycLevelConfigs error", e);
            observer.onError(e);
        }
    }

    @Override
    public void queryBrokerKycConfigs(QueryBrokerKycConfigsRequest request, StreamObserver<QueryBrokerKycConfigsResponse> observer) {
        QueryBrokerKycConfigsResponse response;
        try {
            List<io.bhex.broker.grpc.basic.BrokerKycConfig> brokerKycConfigs =
                    basicService.getAllBrokerKycConfigs().stream()
                            .map(k -> basicService.toProtoBrokerKycConfig(k))
                            .collect(Collectors.toList());

            response = QueryBrokerKycConfigsResponse.newBuilder()
                    .addAllBrokerKycConfig(brokerKycConfigs)
                    .build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = QueryBrokerKycConfigsResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("queryBrokerKycConfigs error", e);
            observer.onError(e);
        }
    }

    @Override
    public void getEtfSymbolPrice(GetEtfSymbolPriceRequest request, StreamObserver<GetEtfSymbolPriceResponse> observer) {
        GetEtfSymbolPriceResponse response;
        try {
            List<io.bhex.broker.grpc.basic.GetEtfSymbolPriceResponse.EtfPrice> prices = basicService.getEtfSymbolPrice(request);
            response = GetEtfSymbolPriceResponse.newBuilder()
                    .addAllEtfPrice(prices)
                    .build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = GetEtfSymbolPriceResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("getEtfSymbolPrice error", e);
            observer.onError(e);
        }
    }
}
