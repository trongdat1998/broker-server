package io.bhex.broker.server.grpc.server.service;

import io.bhex.base.account.OptionAccountDetailList;
import io.bhex.base.account.OptionAccountDetailReq;
import io.bhex.base.account.OptionAssetList;
import io.bhex.base.account.OptionAssetReq;
import io.bhex.base.proto.DecimalUtil;
import io.bhex.base.quote.FXRate;
import io.bhex.base.quote.Rate;
import io.bhex.base.token.SymbolDetail;
import io.bhex.base.token.TokenDetail;
import io.bhex.base.token.TokenOptionInfo;
import io.bhex.broker.grpc.account.*;
import io.bhex.broker.server.domain.AssetOption;
import io.bhex.broker.server.domain.BrokerServerConstants;
import io.bhex.broker.server.domain.SellAbleValueDetail;
import io.bhex.broker.server.domain.TokenCategory;
import io.bhex.broker.server.grpc.client.service.GrpcBalanceService;
import io.bhex.broker.server.model.Symbol;
import io.bhex.broker.server.util.OptionUtil;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class AssetOptionService {

    @Resource
    BasicService basicService;

    @Resource
    AccountService accountService;

    @Resource
    GrpcBalanceService grpcBalanceService;

    @Resource
    OptionPriceService optionPriceService;

    public AssetOption getAssetOption(Long optionAccountId, Long orgId, boolean filterExplore, BigDecimal usdtBtcRate) {
        BigDecimal optionUSDTTotal = BigDecimal.ZERO;
        BigDecimal optionCoinUSDTTotal = BigDecimal.ZERO;

        // 2.获取期权资产列表
        // 仓位权益折合: 所以期权的仓位权益总和
        OptionAssetListResponse optionAssetList = getOptionAssetList(optionAccountId, orgId, null, filterExplore);
        List<OptionAssetListResponse.OptionAsset> optionAssets = optionAssetList.getOptionAssetList();

        for (OptionAssetListResponse.OptionAsset optionAsset : optionAssets) {
            // 非USDT需要汇率转换
            Symbol symbol = getSymbolIdByTokenId(optionAsset.getTokenId(), orgId);
            String quoteTokenId = symbol.getQuoteTokenId();
            Rate quoteRate = basicService.getV3Rate(orgId, quoteTokenId);
            BigDecimal price = quoteRate == null ? BigDecimal.ZERO : DecimalUtil.toBigDecimal(quoteRate.getRatesMap().get("USD"));
            optionUSDTTotal = optionUSDTTotal.add(new BigDecimal(optionAsset.getEquity()).multiply(price));
        }

        // 3.获取期权钱包资产
        // 期权资产估值: 可用+冻结+仓位权益折合
        OptionAccountDetailListResponse optionAccountDetail = getOptionAccountDetail(orgId, optionAccountId, null, filterExplore);
        List<OptionAccountDetailListResponse.OptionAccountDetail> optionAccountDetails = optionAccountDetail.getOptionAccountDetailList();

        for (OptionAccountDetailListResponse.OptionAccountDetail optionCoin : optionAccountDetails) {
            // 非USDT需要汇率转换
            Rate tokenRate = basicService.getV3Rate(orgId, optionCoin.getTokenId());
            BigDecimal price = tokenRate == null ? BigDecimal.ZERO : DecimalUtil.toBigDecimal(tokenRate.getRatesMap().get("USD"));
            optionCoinUSDTTotal = optionCoinUSDTTotal.add(new BigDecimal(optionCoin.getAvailable()).multiply(price));
            optionCoinUSDTTotal = optionCoinUSDTTotal.add(new BigDecimal(optionCoin.getLocked()).multiply(price));
        }

        return AssetOption.builder()
                .optionUSDTTotal(optionUSDTTotal)
                .optionCoinUSDTTotal(optionCoinUSDTTotal)
                .optionBTCTotal(optionUSDTTotal.multiply(usdtBtcRate))
                .optionCoinBTCTotal(optionCoinUSDTTotal.multiply(usdtBtcRate))
                .build();
    }


    public OptionAssetListResponse getOptionAssetList(OptionAssetRequest request) {
        return getOptionAssetList(accountService.getOptionAccountId(request.getHeader()), request.getHeader().getOrgId(), request.getTokenIdsList(), request.getFilterExplore());
    }

    public OptionAssetListResponse getOptionAssetList(Long optionAccountId, Long orgId, List<String> tokenIds, boolean filterExplore) {
        tokenIds = CollectionUtils.isEmpty(tokenIds) ? new ArrayList<>() : tokenIds;
        OptionAssetReq bhRequest = OptionAssetReq.newBuilder()
                .setAccountId(optionAccountId)
                .addAllTokenIds(tokenIds)
                .setFilterExplore(filterExplore)
                .build();

        OptionAssetList optionAssetList = grpcBalanceService.getOptionAssetList(bhRequest);
        List<OptionAssetListResponse.OptionAsset> assetList = new ArrayList<>();
        optionAssetList.getOptionAssetList().forEach(optionAsset -> {
            Symbol symbol = getSymbolIdByTokenId(optionAsset.getTokenId(), orgId);
            // 持仓量为0的期权不显示
            if (Objects.nonNull(symbol) && !(BigDecimal.ZERO.compareTo(new BigDecimal(optionAsset.getTotal())) == 0)) {
                String symbolId = symbol.getSymbolId();
                Long exchangeId = symbol.getExchangeId();

                //当前价格
                BigDecimal currentOptionPrice = optionPriceService.getCurrentOptionPrice(symbolId, exchangeId, orgId);
                //全部数量
                BigDecimal total = new BigDecimal(optionAsset.getTotal());
                //期权估值
                BigDecimal valuation = optionPriceService.getValuation(total, currentOptionPrice);
                //保证金
                BigDecimal margin = new BigDecimal(optionAsset.getMargin());
                //持仓均价
                BigDecimal cost = new BigDecimal(optionAsset.getCost());
                //账户权益
                BigDecimal equity = optionPriceService.getEquity(total, margin, currentOptionPrice);
                //持仓盈亏
                BigDecimal profit = optionPriceService.getProfit(total, cost, currentOptionPrice);
                //盈亏百分比
                BigDecimal profitPercentage = optionPriceService.getProfitPercentage(total, cost, margin, currentOptionPrice);

                assetList.add(OptionAssetListResponse.OptionAsset.newBuilder()
                        .setTokenId(optionAsset.getTokenId())
                        .setTokenName(optionAsset.getTokenName())
                        .setTokenFullName(optionAsset.getTokenFullName())
                        .setIsCall(optionAsset.getIsCall())
                        .setCurrentPrice(currentOptionPrice.setScale(BrokerServerConstants.OPTION_PRICE_PRECISION, BigDecimal.ROUND_DOWN).toPlainString())
                        .setQuantity(DecimalUtil.toTrimString(total.setScale(BrokerServerConstants.OPTION_QUANTITY_PRECISION, BigDecimal.ROUND_DOWN)))
                        .setValuation(DecimalUtil.toTrimString(valuation.setScale(BrokerServerConstants.BASE_OPTION_PRECISION, BigDecimal.ROUND_DOWN)))
                        .setMargin(DecimalUtil.toTrimString(margin.setScale(BrokerServerConstants.BASE_OPTION_PRECISION, BigDecimal.ROUND_DOWN)))
                        .setEquity(DecimalUtil.toTrimString(equity.setScale(BrokerServerConstants.BASE_OPTION_PRECISION, BigDecimal.ROUND_DOWN)))
                        .setProfit(profit.setScale(BrokerServerConstants.OPTION_PROFIT_PRECISION, BigDecimal.ROUND_DOWN).toPlainString())
                        .setProfitPercentage(profitPercentage.setScale(BrokerServerConstants.OPTION_PROFIT_PRECISION, BigDecimal.ROUND_DOWN).toPlainString())
                        .setUnit(symbol.getQuoteTokenId())
                        .build());
            }

        });
        return OptionAssetListResponse.newBuilder()
                .addAllOptionAsset(assetList)
                .build();
    }

    public OptionAccountDetailListResponse getOptionAccountDetail(OptionAccountDetailRequest request) {
        return getOptionAccountDetail(request.getHeader().getOrgId(), accountService.getOptionAccountId(request.getHeader()), request.getTokenIdsList(), request.getFilterExplore());
    }

    // 获取期权钱包内的余额
    public OptionAccountDetailListResponse getOptionAccountDetail(Long orgId, Long optionAccountId, List<String> tokenIds, boolean filterExplore) {
        tokenIds = CollectionUtils.isEmpty(tokenIds) ? new ArrayList<>() : tokenIds;
        OptionAccountDetailReq bhRequest = OptionAccountDetailReq.newBuilder()
                .setAccountId(optionAccountId)
                .addAllTokenIds(tokenIds)
                .setFilterExplore(filterExplore)
                .build();
        OptionAccountDetailList detailList = grpcBalanceService.getOptionAccountDetail(bhRequest);
        List<OptionAccountDetailListResponse.OptionAccountDetail> accountDetails = new ArrayList<>();
        detailList.getOptionAccountDetailList().forEach(detail -> {
            TokenDetail tokenDetail = getTokenDetailByTokenId(detail.getTokenId());
            if (Objects.isNull(tokenDetail) || (Objects.nonNull(tokenDetail) && !TokenCategory.isOption(tokenDetail.getType().name()))) {
                // 换算为USDT价格
                Rate rate = basicService.getV3Rate(orgId, detail.getTokenId());
                BigDecimal btcUsdtPrice = rate == null ? BigDecimal.ZERO : DecimalUtil.toBigDecimal(rate.getRatesMap().get("USDT"));
                BigDecimal usdtValue = new BigDecimal(detail.getAvailable()).add(new BigDecimal(detail.getLocked())).multiply(btcUsdtPrice);

                accountDetails.add(OptionAccountDetailListResponse.OptionAccountDetail.newBuilder()
                        .setTokenId(detail.getTokenId())
                        .setTokenName(detail.getTokenName())
                        .setTokenFullName(detail.getTokenFullName())
                        .setTotal(DecimalUtil.toTrimString(new BigDecimal(detail.getTotal()).setScale(BrokerServerConstants.BASE_OPTION_PRECISION, BigDecimal.ROUND_DOWN)))
                        .setAvailable(DecimalUtil.toTrimString(new BigDecimal(detail.getAvailable()).setScale(BrokerServerConstants.BASE_OPTION_PRECISION, BigDecimal.ROUND_DOWN)))
                        .setLocked(DecimalUtil.toTrimString(new BigDecimal(detail.getLocked()).setScale(BrokerServerConstants.BASE_OPTION_PRECISION, BigDecimal.ROUND_DOWN)))
                        .setMargin(DecimalUtil.toTrimString(new BigDecimal(detail.getMargin()).setScale(BrokerServerConstants.BASE_OPTION_PRECISION, BigDecimal.ROUND_DOWN)))
                        .setUsdtValue(DecimalUtil.toTrimString(usdtValue.setScale(BrokerServerConstants.USDT_PRECISION, BigDecimal.ROUND_DOWN)))
                        .build());
            }
        });

        return OptionAccountDetailListResponse.newBuilder()
                .addAllOptionAccountDetail(accountDetails)
                .build();
    }

    private TokenDetail getTokenDetailByTokenId(String tokenId) {
        return basicService.getTokenDetailOptionMap().get(tokenId);
    }

    private Symbol getSymbolIdByTokenId(String tokenId, Long orgId) {
        return basicService.getTokenSymbolOptionMap().get(orgId + "_" + tokenId);
    }

    public GetOptionTradeableResponse getOptionTradeAble(GetOptionTradeableRequest request) {
        Long optionAccountId = accountService.getOptionAccountId(request.getHeader());
        List<String> baseTokenIds = request.getTokenIdsList();
        Long orgId = request.getHeader().getOrgId();

        if (CollectionUtils.isEmpty(baseTokenIds)) {
            return GetOptionTradeableResponse.newBuilder().build();
        }

        //期权资产(base token balance)
        Map<String, OptionAssetList.OptionAsset> baseBalanceMap = getOptionBaseBalanceMap(optionAccountId);

        //base token ids
        List<TokenDetail> baseTokens = baseTokenIds
                .stream()
                .map(tokenId -> basicService.getTokenDetailOptionMap().get(tokenId))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        //quote token ids
        List<String> quoteTokenIds = getQuoteTokenIds(orgId, baseTokenIds);

        //当前资产余额（币) (quote token balance)
        Map<String, OptionAccountDetailList.OptionAccountDetail> quoteTokenBalanceMap = getOptionQuoteBalanceMap(optionAccountId, quoteTokenIds);

        //期权资产、当前资产余额 --计算--> 可卖期权，当前资产余额
        List<OptionBalance> result = baseTokens
                .stream()
                .map(token -> {
                    String baseTokenId = token.getTokenId();
                    Symbol symbol = basicService.getTokenSymbolOptionMap().get(String.format("%s_%s", orgId, baseTokenId));
                    if (symbol == null) {
                        return null;
                    }
                    SymbolDetail symbolDetail = basicService.getSymbolDetailOption(symbol.getExchangeId(), symbol.getSymbolId());
                    if (symbolDetail == null) {
                        return null;
                    }

                    OptionAssetList.OptionAsset baseBalance = baseBalanceMap.get(baseTokenId);
                    OptionAccountDetailList.OptionAccountDetail quoteBalance = quoteTokenBalanceMap.get(symbol.getQuoteTokenId());

                    BigDecimal baseBalanceTotal = (baseBalance != null ? new BigDecimal(baseBalance.getTotal()) : BigDecimal.ZERO);
                    BigDecimal baseBalanceAvailable = (baseBalance != null ? new BigDecimal(baseBalance.getAvailable()) : BigDecimal.ZERO);
                    BigDecimal baseAvailPosition = (baseBalance != null ? new BigDecimal(baseBalance.getAvailPosition()) : BigDecimal.ZERO);
                    BigDecimal baseBalanceMargin = (baseBalance != null ? new BigDecimal(baseBalance.getMargin()) : BigDecimal.ZERO);

                    BigDecimal quoteBalanceTotal = (quoteBalance != null ? new BigDecimal(quoteBalance.getTotal()) : BigDecimal.ZERO);
                    BigDecimal quoteBalanceAvailable = (quoteBalance != null ? new BigDecimal(quoteBalance.getAvailable()) : BigDecimal.ZERO);


                    return buildOptionBalance(
                            symbolDetail,
                            baseTokenId,
                            token.getTokenFullName(),
                            token.getTokenFullName(),
                            baseBalanceTotal,
                            baseBalanceAvailable,
                            baseAvailPosition,
                            baseBalanceMargin,
                            quoteBalanceTotal,
                            quoteBalanceAvailable
                    );
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        return GetOptionTradeableResponse.newBuilder().addAllOptionBalance(result).build();
    }

    /**
     * 期权资产(base token balance)
     *
     * @param optionAccountId option account id
     * @return base token balance
     */
    private Map<String, OptionAssetList.OptionAsset> getOptionBaseBalanceMap(Long optionAccountId) {
        OptionAssetReq baseRequest = OptionAssetReq.newBuilder().setAccountId(optionAccountId).build();
        List<OptionAssetList.OptionAsset> baseBalances = grpcBalanceService.getOptionAssetList(baseRequest)
                .getOptionAssetList();
        return Optional.ofNullable(baseBalances)
                .orElse(new ArrayList<>())
                .stream()
                .collect(
                        Collectors.toMap(OptionAssetList.OptionAsset::getTokenId, asset -> asset, (p, q) -> p)
                );
    }

    /**
     * 当前资产余额（币) (quote token balance)
     *
     * @param optionAccountId option account id
     * @param quoteTokenIds   quote token ids
     * @return quote token balance
     */
    private Map<String, OptionAccountDetailList.OptionAccountDetail> getOptionQuoteBalanceMap(Long optionAccountId, List<String> quoteTokenIds) {
        OptionAccountDetailReq quoteRequest = OptionAccountDetailReq.newBuilder()
                .setAccountId(optionAccountId)
                .addAllTokenIds(quoteTokenIds)
                .build();
        List<OptionAccountDetailList.OptionAccountDetail> quoteBalances = grpcBalanceService.getOptionAccountDetail(quoteRequest)
                .getOptionAccountDetailList();
        return Optional.ofNullable(quoteBalances)
                .orElse(new ArrayList<>())
                .stream()
                .collect(
                        Collectors.toMap(OptionAccountDetailList.OptionAccountDetail::getTokenId, detail -> detail, (p, q) -> p)
                );
    }

    private List<String> getQuoteTokenIds(Long orgId, List<String> baseTokenIds) {
        return baseTokenIds
                .stream()
                .map(baseTokenId -> {
                    Symbol symbol = basicService.getTokenSymbolOptionMap()
                            .get(String.format("%s_%s", orgId, baseTokenId));
                    return symbol != null ? symbol.getQuoteTokenId() : null;
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    /**
     * 获取可卖期权
     */
    private OptionBalance buildOptionBalance(SymbolDetail symbolDetail, String baseTokenId,
                                             String baseTokenName,
                                             String baseTokenFullName,
                                             BigDecimal baseTotal,
                                             BigDecimal baseAvailable,
                                             BigDecimal baseAvailPosition,
                                             BigDecimal baseMargin,
                                             BigDecimal quoteTotal,
                                             BigDecimal quoteAvailable) {
        BigDecimal maxPayOff = BigDecimal.ZERO;
        TokenOptionInfo tokenOptionInfo = basicService.getTokenOptionInfo(baseTokenId);
        if (tokenOptionInfo != null) {
            maxPayOff = DecimalUtil.toBigDecimal(tokenOptionInfo.getMaxPayOff());
        }

        SellAbleValueDetail sellAbleValueDetail = OptionUtil.getSellAbleValue(quoteAvailable, maxPayOff, baseTotal, baseAvailable);
        BigDecimal quoteLocked = quoteTotal.subtract(quoteAvailable);


        int basePrecision = BrokerServerConstants.OPTION_QUANTITY_PRECISION;
        if (symbolDetail != null) {
            basePrecision = DecimalUtil.toBigDecimal(symbolDetail.getBasePrecision()).stripTrailingZeros().scale();
        }

        BigDecimal sellAble = sellAbleValueDetail.getSellAbleQuantity().setScale(basePrecision, RoundingMode.DOWN).stripTrailingZeros();
        quoteAvailable = quoteAvailable.setScale(BrokerServerConstants.BASE_OPTION_PRECISION, RoundingMode.DOWN).stripTrailingZeros();
        quoteLocked = quoteLocked.setScale(BrokerServerConstants.BASE_OPTION_PRECISION, RoundingMode.DOWN).stripTrailingZeros();

        return OptionBalance.newBuilder()
                //option base
                .setTokenId(baseTokenId)//期权 token id
                .setTokenName(baseTokenName)//期权 token name
                .setTokenFullName(baseTokenFullName)//期权 token full name
                .setSellable(DecimalUtil.toTrimString(sellAble)) //可卖期权
                .setAvailPosition(DecimalUtil.toTrimString(baseAvailPosition))//期权可平量
                .setOptionTotal(DecimalUtil.toTrimString(baseTotal))//期权total
                .setOptionAvailable(DecimalUtil.toTrimString(baseAvailable))//期权available
                .setMaxOff(DecimalUtil.toTrimString(maxPayOff))//最大赔付（收益）
                .setRemainQuantity(DecimalUtil.toTrimString(sellAbleValueDetail.getRemainQuantity()))//余额可卖张数
                .setCloseQuantity(DecimalUtil.toTrimString(sellAbleValueDetail.getCloseQuantity()))//平仓期权的张数
                .setMargin(DecimalUtil.toTrimString(baseMargin)) //期权，可用保证金
                //option quote
                .setAvailable(DecimalUtil.toTrimString(quoteAvailable)) //当前资产余额（币), 可用
                .setLocked(DecimalUtil.toTrimString(quoteLocked)) //当前资产余额（币) ,锁定

                .build();
    }
}
