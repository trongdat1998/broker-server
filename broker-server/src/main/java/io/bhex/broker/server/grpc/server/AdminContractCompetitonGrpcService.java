package io.bhex.broker.server.grpc.server;

import com.google.common.base.Splitter;
import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.grpc.activity.contract.competition.*;
import io.bhex.broker.server.grpc.server.service.AdminContractCompetitionService;
import io.bhex.broker.server.model.TradeCompetition;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.Resource;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@GrpcService
public class AdminContractCompetitonGrpcService extends AdminContractCompetitionServiceGrpc.AdminContractCompetitionServiceImplBase {

    @Resource
    private AdminContractCompetitionService adminContractCompetitionService;

    @Override
    public void list(ListRequest request,
                     StreamObserver<ListContractCompetitionResponse> responseObserver) {

        ListContractCompetitionResponse.Builder resp=ListContractCompetitionResponse.newBuilder();
        try {
            ListContractCompetitionResponse.Page page = adminContractCompetitionService.listWithPage(
                    request.getHeader().getOrgId(),
                    request.getHeader().getLanguage(),
                    request.getPageNo(), request.getPageSize());

            resp.setPage(page);
            responseObserver.onNext(resp.build());
            responseObserver.onCompleted();
        }catch (BrokerException e) {
            responseObserver.onNext(resp.setErrorCode(e.getCode()).build());
            responseObserver.onCompleted();
        }catch (Exception e){
            log.error(e.getMessage(),e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void save(SaveRequest request,
                       StreamObserver<BoolResponse> responseObserver) {

        BoolResponse.Builder resp=BoolResponse.newBuilder();
        try {
            boolean success = adminContractCompetitionService.save(request.getHeader().getOrgId(),
                    request.getCompetition(),request.getDomain());

            resp.setSuccess(success);
            responseObserver.onNext(resp.build());
            responseObserver.onCompleted();
        }catch (BrokerException e) {
            responseObserver.onNext(
                    resp.setSuccess(false)
                    .setErrorCode(e.getCode()).build());
            responseObserver.onCompleted();
        }catch (Exception e){
            log.error(e.getMessage(),e);
            responseObserver.onError(e);
        }
    }


    @Override
    public void getDetail(ListRequest request,
                       StreamObserver<DetailResponse> responseObserver) {

        DetailResponse.Builder resp=DetailResponse.newBuilder();
        try {
            CompetitionInfo competition = adminContractCompetitionService.getDetail(request.getHeader().getOrgId(),
                    request.getExt());

            resp.setCompetition(competition);
            responseObserver.onNext(resp.build());
            responseObserver.onCompleted();
        }catch (BrokerException e) {
            responseObserver.onNext(resp.setErrorCode(e.getCode()).build());
            responseObserver.onCompleted();
        }catch (Exception e){
            log.error(e.getMessage(),e);
            responseObserver.onError(e);
        }
    }

    /**
     * <pre>
     *参赛人列表
     * </pre>
     */
    @Override
    public void listParticipant(ListRequest request,
                                StreamObserver<ListParticipantResponse> responseObserver) {

        ListParticipantResponse.Builder resp=ListParticipantResponse.newBuilder();
        try {
            ListParticipantResponse.Page page = adminContractCompetitionService.listParticipant(request.getHeader().getOrgId(),
                    request.getPageNo(),request.getPageSize(),Long.parseLong(request.getExt()));

            resp.setPage(page);
            responseObserver.onNext(resp.build());
            responseObserver.onCompleted();
        }catch (BrokerException e) {
            responseObserver.onNext(resp.setErrorCode(e.getCode()).build());
            responseObserver.onCompleted();
        }catch (Exception e){
            log.error(e.getMessage(),e);
            responseObserver.onError(e);
        }
    }

    /**
     * <pre>
     *增加参赛人
     * </pre>
     */
    @Override
    public void addParitcipant(AddParticipantRequest request,
                               StreamObserver<BoolResponse> responseObserver) {

        BoolResponse.Builder resp=BoolResponse.newBuilder();
        try {
            boolean success = adminContractCompetitionService.addParticipant(request.getHeader().getOrgId(),
                    request.getActivityId(),request.getParticipantsList(),request.getMode());

            resp.setSuccess(success);
            responseObserver.onNext(resp.build());
            responseObserver.onCompleted();
        }catch (BrokerException e) {
            resp.setSuccess(false);
            responseObserver.onNext(resp.setErrorCode(e.getCode()).build());
            responseObserver.onCompleted();
        }catch (Exception e){
            log.error(e.getMessage(),e);
            responseObserver.onError(e);
        }
    }

    /**
     * <pre>
     *交易大赛排行榜
     * </pre>
     */
    @Override
    public void rankingList(RankingListRequest request,
                            StreamObserver<RankingListResponse> responseObserver) {

        RankingListResponse.Builder resp=RankingListResponse.newBuilder();
        try {

            TradeCompetition competition = adminContractCompetitionService
                    .findTradeCompetition(request.getHeader().getOrgId(),request.getActivityId());

            List<RankType> rankTypes=Splitter.on(",").splitToList(competition.getRankTypes())
                    .stream().map(i->RankType.forNumber(Integer.parseInt(i))).collect(Collectors.toList());

            RankType rankType=request.getRankType();
            if(request.getRankType()==RankType.UNkNOW){
                rankType=rankTypes.get(0);
            }

            AdminContractCompetitionService.RankingListResult result= adminContractCompetitionService.rankingList(request.getHeader().getOrgId(),
                    request.getActivityId(),request.getDay(),rankType);

            resp.setCurrentRankType(rankType)
                .addAllList(result.getRankings())
                .addAllRankTypes(rankTypes)
                .addAllDays(result.getDays())
                .setCurrentDay(result.getCurrentDaySafe())
            ;

            responseObserver.onNext(resp.build());
            responseObserver.onCompleted();
        }catch (BrokerException e) {
            responseObserver.onNext(resp.setErrorCode(e.getCode()).build());
            responseObserver.onCompleted();
        }catch (Exception e){
            log.error(e.getMessage(),e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void getShortUrl(CompetitionShortUrlRequest request,
                            StreamObserver<CompetitionShortUrlResponse> responseObserver){

        CompetitionShortUrlResponse.Builder resp=CompetitionShortUrlResponse.newBuilder();
        try {
            Pair<String,String> pair = adminContractCompetitionService.getShortUrl(request.getHeader().getOrgId(),
                    request.getId(),request.getDomain());

            resp.setPcShortUrl(pair.getKey())
                    .setH5ShortUrl(pair.getValue());
            responseObserver.onNext(resp.build());
            responseObserver.onCompleted();
        }catch (BrokerException e) {
            responseObserver.onNext(resp.setErrorCode(e.getCode()).build());
            responseObserver.onCompleted();
        }catch (Exception e){
            log.error(e.getMessage(),e);
            responseObserver.onError(e);
        }



    }

/*    @Override
    public void listContractSymbol(ListRequest request,
              StreamObserver<ListContractSymbolResponse> responseObserver){

        ListContractSymbolResponse.Builder resp=ListContractSymbolResponse.newBuilder();
        try {
            List<ContractSymbol> list = adminContractCompetitionService.listContractSymbol(request.getHeader().getOrgId(),
                    request.getHeader().getLanguage());

            resp.addAllContractSymbols(list);
            responseObserver.onNext(resp.build());
            responseObserver.onCompleted();
        }catch (BrokerException e) {
            responseObserver.onNext(resp.setErrorCode(e.getCode()).build());
            responseObserver.onCompleted();
        }catch (Exception e){
            log.error(e.getMessage(),e);
            responseObserver.onError(e);
        }
    }*/
}
