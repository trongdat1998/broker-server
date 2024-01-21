package io.bhex.broker.server.grpc.server.service;

import io.bhex.base.account.BalanceDetail;
import io.bhex.base.account.GetBalanceDetailRequest;
import io.bhex.base.proto.DecimalUtil;
import io.bhex.broker.grpc.common.Header;
import io.bhex.broker.server.domain.AssetFutures;
import io.bhex.broker.server.domain.FunctionModule;
import io.bhex.broker.server.grpc.client.service.GrpcBalanceService;
import io.bhex.broker.server.util.BaseReqUtil;
import io.bhex.broker.server.util.FuturesUtil;
import io.bhex.broker.server.util.GrpcRequestUtil;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

@Service
public class AssetFuturesService {

    @Resource
    BasicService basicService;

    @Resource
    AccountService accountService;

    @Resource
    GrpcBalanceService grpcBalanceService;

    public BalanceDetail queryBalanceDetail(Long accountId, String tokenId, Long orgId) {
        GetBalanceDetailRequest request = GetBalanceDetailRequest.newBuilder()
                .setBaseRequest(BaseReqUtil.getBaseRequest(orgId))
                .setAccountId(accountId)
                .addAllTokenId(Collections.singletonList(tokenId))
                .build();
        List<BalanceDetail> balanceDetails = grpcBalanceService.getBalanceDetail(request).getBalanceDetailsList();
        if (CollectionUtils.isEmpty(balanceDetails)) {
            return null;
        }
        Optional<BalanceDetail> balanceDetail = balanceDetails
                .stream()
                .filter(t -> t.getTokenId().equalsIgnoreCase(tokenId))
                .findFirst();
        return balanceDetail.orElse(null);
    }

    public AssetFutures getAssetFutures(Header header, Long accountId) {
        if (!BrokerService.checkModule(header, FunctionModule.FUTURES)) {
            return null;
        }
        if (accountId == null) {
            return null;
        }
        GetBalanceDetailRequest request = GetBalanceDetailRequest.newBuilder()
                .setBaseRequest(GrpcRequestUtil.getBaseRequest(header))
                .setAccountId(accountId)
                .build();
        List<BalanceDetail> balanceDetails = grpcBalanceService.getBalanceDetail(request).getBalanceDetailsList();
        if (CollectionUtils.isEmpty(balanceDetails)) {
            return null;
        }

        BigDecimal usdtValue = BigDecimal.ZERO;
        BigDecimal btcValue = BigDecimal.ZERO;
        for (BalanceDetail detail : balanceDetails) {
            if (Stream.of(AccountService.TEST_TOKENS).anyMatch(testToken -> testToken.equalsIgnoreCase(detail.getTokenId()))) {
                continue;
            }
            BigDecimal usdtRate = basicService.getFXRate(header.getOrgId(), detail.getTokenId(), FuturesUtil.USDT);
            BigDecimal btcRate = basicService.getFXRate(header.getOrgId(), detail.getTokenId(), FuturesUtil.BTC);
            usdtValue = usdtValue.add(DecimalUtil.toBigDecimal(detail.getTotal()).multiply(usdtRate));
            btcValue = btcValue.add(DecimalUtil.toBigDecimal(detail.getTotal()).multiply(btcRate));
        }
        return AssetFutures.builder()
                .futuresCoinUSDTTotal(usdtValue)
                .futuresCoinBTCTotal(btcValue)
                .build();
    }
}
