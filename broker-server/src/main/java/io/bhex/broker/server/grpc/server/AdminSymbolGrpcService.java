package io.bhex.broker.server.grpc.server;

import com.google.common.collect.Lists;
import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.broker.grpc.admin.*;
import io.bhex.broker.grpc.common.AdminSimplyReply;
import io.bhex.broker.grpc.proto.AdminCommonResponse;
import io.bhex.broker.server.grpc.server.service.LetfService;
import io.bhex.broker.server.grpc.server.service.QuoteTokenService;
import io.bhex.broker.server.grpc.server.service.SymbolService;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import javax.annotation.Resource;
import java.util.List;

/**
 * @ProjectName: broker-server
 * @Package: io.bhex.broker.server.grpc.server
 * @Author: ming.xu
 * @CreateDate: 20/08/2018 9:41 PM
 * @Copyright（C）: 2018 BHEX Inc. All rights reserved.
 */
@Slf4j
@GrpcService
public class AdminSymbolGrpcService extends AdminSymbolServiceGrpc.AdminSymbolServiceImplBase {

    @Resource
    private SymbolService symbolService;
    @Resource
    private QuoteTokenService quoteTokenService;

    @Override
    public void querySymbol(QuerySymbolRequest request, StreamObserver<QuerySymbolReply> responseObserver) {
        List<String> symbols = Lists.newArrayList(request.getSymbolIdList());
        QuerySymbolReply reply = symbolService.querySymbol(request.getCurrent(), request.getPageSize(), request.getQuoteToken(),
                symbols, request.getSymbolName(), request.getBrokerId(), request.getExchangeId(), request.getCategory());

        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void queryOneSymbol(QueryOneSymbolRequest request, StreamObserver<SymbolDetail> responseObserver) {
        SymbolDetail symbolDetail = symbolService.queryOneSymbol(request.getBrokerId(), request.getSymbolId());
        responseObserver.onNext(symbolDetail);
        responseObserver.onCompleted();
    }

    @Override
    public void symbolAllowTrade(SymbolAllowTradeRequest request, StreamObserver<SymbolAllowTradeReply> responseObserver) {
        Boolean isOk = symbolService.allowTrade(request.getExchangeId(), request.getSymbolId(), request.getAllowTrade(), request.getBrokerId());

        responseObserver.onNext(SymbolAllowTradeReply.newBuilder().setResult(isOk).build());
        responseObserver.onCompleted();
    }

    @Override
    public void symbolPublish(SymbolPublishRequest request, StreamObserver<SymbolPublishReply> responseObserver) {
        Boolean isOk = symbolService.publish(request.getExchangeId(), request.getSymbolId(), request.getPublish(), request.getBrokerId());

        responseObserver.onNext(SymbolPublishReply.newBuilder().setResult(isOk).build());
        responseObserver.onCompleted();
    }

    @Override
    public void symbolAgency(SymbolAgencyRequest request, StreamObserver<SymbolAgencyReply> responseObserver) {
        SymbolAgencyReply reply = symbolService.symbolAgency(request);

        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void queryExistSymbol(QueryExistSymbolRequest request, StreamObserver<QueryExistSymbolReply> responseObserver) {
        QueryExistSymbolReply reply = symbolService.queryBySymbolIds(request.getExchangeId(), request.getBrokerId(), request.getSymbolIdList());

        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void symbolUpdateBanSellStatus(SymbolBanTypeRequest request,
                                          StreamObserver<SymbolBanSellStatusReply> responseObserver) {
        Boolean isOk = symbolService.updateBanSellStatus(request.getExchangeId(),
                request.getSymbolId(), request.getBanSellStatus(), request.getBrokerId());
        responseObserver.onNext(SymbolBanSellStatusReply.newBuilder().setResult(isOk).build());
        responseObserver.onCompleted();
    }

    @Override
    public void symbolUpdateWhiteAccountIdList(SymbolWhiteListRequest request,
                                               StreamObserver<SymbolWhiteAccountIdListReply> responseObserver) {
        Boolean isOk = symbolService.updateWhiteAccountIdList(request.getSaleWhiteList(), request.getBrokerId());
        responseObserver.onNext(SymbolWhiteAccountIdListReply.newBuilder().setResult(isOk).build());
        responseObserver.onCompleted();
    }

    @Override
    public void queryWhiteAccountIdList(QueryWhiteAccountIdListRequest request,
                                        StreamObserver<QueryWhiteAccountIdListRequestReply> responseObserver) {
        responseObserver
                .onNext(QueryWhiteAccountIdListRequestReply
                        .newBuilder()
                        .addAllSaleWhite(symbolService.queryWhiteAccountIdList(request.getOrgId()))
                        .build());
        responseObserver.onCompleted();
    }

    @Override
    public void editRecommendSymbols(EditRecommendSymbolsRequest request, StreamObserver<EditRecommendSymbolsReply> responseObserver) {
        boolean r = symbolService.editRecommendSymbols(request.getBrokerId(), request.getSymbolIdList());
        responseObserver.onNext(EditRecommendSymbolsReply.newBuilder().setResult(r).build());
        responseObserver.onCompleted();
    }

    @Override
    public void queryRecommendSymbols(QueryRecommendSymbolsRequest request, StreamObserver<QueryRecommendSymbolsReply> responseObserver) {
        List<String> symbols = symbolService.getRecommendSymbols(request.getBrokerId());
        responseObserver.onNext(QueryRecommendSymbolsReply.newBuilder().addAllSymbolId(symbols).build());
        responseObserver.onCompleted();
    }

    @Override
    public void editSymbolSwitch(EditSymbolSwitchRequest request, StreamObserver<AdminCommonResponse> responseObserver) {
        AdminCommonResponse response = symbolService.editSymbolSwitch(request);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void editQuoteTokens(EditQuoteTokensRequest request, StreamObserver<EditQuoteTokensReply> responseObserver) {
        boolean r = quoteTokenService.editQuoteTokens(request.getBrokerId(), request.getTokenIdList());
        responseObserver.onNext(EditQuoteTokensReply.newBuilder().setResult(r).build());
        responseObserver.onCompleted();
    }

    @Override
    public void editQuoteSymbols(EditQuoteSymbolsRequest request, StreamObserver<EditQuoteSymbolsReply> responseObserver) {
        boolean r = symbolService.editQuoteSymbols(request.getBrokerId(), request.getCategory(), request.getTokenId(), request.getSymbolIdList());
        responseObserver.onNext(EditQuoteSymbolsReply.newBuilder().setResult(r).build());
        responseObserver.onCompleted();
    }

    @Override
    public void editSymbolFilterTime(EditSymbolFilterTimeRequest request, StreamObserver<EditSymbolFilterTimeReply> responseObserver) {
        try {
            boolean r = symbolService.editSymbolFilterTime(request.getBrokerId(), request.getSymbolId(), request.getFilterTime());
            responseObserver.onNext(EditSymbolFilterTimeReply.newBuilder().setSymbolId(request.getSymbolId()).setResult(r).build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.warn("editSymbolFilterTime error", e);
            responseObserver.onNext(EditSymbolFilterTimeReply.newBuilder().setSymbolId(request.getSymbolId()).setResult(false).build());
            responseObserver.onCompleted();
        }
    }

    @Override
    public void queryQuoteSymbols(QueryQuoteSymbolsRequest request, StreamObserver<QueryQuoteSymbolsReply> responseObserver) {
        List<String> symbols = symbolService.getQuoteSymbols(request.getBrokerId(), request.getCategory(), request.getTokenId());
        responseObserver.onNext(QueryQuoteSymbolsReply.newBuilder().addAllSymbolId(symbols).build());
        responseObserver.onCompleted();
    }


    @Override
    public void queryFuturesCoinToken(QueryFuturesCoinTokenRequest request, StreamObserver<QueryFuturesCoinTokenReply> responseObserver) {
        List<String> symbols = symbolService.queryFuturesCoinToken(request.getBrokerId());
        responseObserver.onNext(QueryFuturesCoinTokenReply.newBuilder().addAllFuturesCoinToken(symbols).build());
        responseObserver.onCompleted();
    }

    @Override
    public void setSymbolLabel(SetSymbolLabelRequest request, StreamObserver<SetSymbolLabelResponse> responseObserver) {
        boolean result = symbolService.setSymbolLabel(request.getOrgId(), request.getSymbolId(), request.getLabelId());
        responseObserver.onNext(SetSymbolLabelResponse.newBuilder().setResult(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void hideFromOpenapi(HideFromOpenapiRequest request, StreamObserver<HideFromOpenapiResponse> responseObserver) {
        boolean result = symbolService.hideSymbolFromOpenapi(request.getOrgId(), request.getSymbolId(), request.getHideFromOpenapi());
        responseObserver.onNext(HideFromOpenapiResponse.newBuilder().setResult(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void forbidOpenapiTrade(ForbidOpenapiTradeRequest request, StreamObserver<ForbidOpenapiTradeResponse> responseObserver) {
        boolean result = symbolService.forbidOpenapiTrade(request.getOrgId(), request.getSymbolId(), request.getForbidOpenapiTrade());
        responseObserver.onNext(ForbidOpenapiTradeResponse.newBuilder().setResult(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void editSymbolExtraTags(EditSymbolExtraTagsRequest request, StreamObserver<AdminSimplyReply> responseObserver) {
        AdminSimplyReply result = symbolService.editSymbolExtraTags(request.getOrgId(), request.getSymbolId(), request.getExtraTagMap());
        responseObserver.onNext(result);
        responseObserver.onCompleted();
    }

    @Override
    public void editSymbolExtraConfigs(EditSymbolExtraConfigsRequest request, StreamObserver<AdminSimplyReply> responseObserver) {
        AdminSimplyReply result = symbolService.editSymbolExtraConfigs(request.getOrgId(), request.getSymbolId(), request.getExtraConfigMap());
        responseObserver.onNext(result);
        responseObserver.onCompleted();
    }

    @Resource
    private LetfService letfService;

    @Override
    public void getLetfInfos(GetLetfInfoRequest request, StreamObserver<LetfInfos> responseObserver) {
        LetfInfos result = letfService.getLetfInfos(request);
        responseObserver.onNext(result);
        responseObserver.onCompleted();
    }

    @Override
    public void editLetfInfo(EditLetfInfoRequest request, StreamObserver<AdminSimplyReply> responseObserver) {
        AdminSimplyReply result = letfService.editLetfInfo(request);
        responseObserver.onNext(result);
        responseObserver.onCompleted();
    }
}
