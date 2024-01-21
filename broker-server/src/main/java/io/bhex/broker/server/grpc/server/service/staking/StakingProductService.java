package io.bhex.broker.server.grpc.server.service.staking;

import io.bhex.base.proto.DecimalUtil;
import io.bhex.base.quote.Rate;
import io.bhex.broker.grpc.common.Header;
import io.bhex.broker.grpc.staking.*;

import io.bhex.broker.server.grpc.server.service.BasicService;
import io.bhex.broker.server.model.staking.*;
import io.bhex.broker.server.primary.mapper.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.session.RowBounds;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.repository.cdi.Eager;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import tk.mybatis.mapper.entity.Example;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Service
public class StakingProductService {

    @Resource
    StakingProductMapper stakingProductMapper;

    @Resource
    StakingProductLocalMapper stakingProductLocalMapper;

    @Resource
    StakingProductRebateMapper stakingProductRebateMapper;

    @Resource
    StakingAssetMapper stakingAssetMapper;

    @Resource
    StakingProductRebateDetailMapper stakingProductRebateDetailMapper;

    @Resource
    StakingProductOrderMapper stakingProductOrderMapper;

    @Resource
    StakingProductSubscribeLimitMapper stakingProductSubscribeLimitMapper;

    @Resource
    StakingProductJourMapper stakingProductJourMapper;

    @Resource
    StakingProductPermissionMapper stakingProductPermissionMapper;

    @Autowired
    BasicService basicService;

    public GetProductListReply getProductList(GetProductListRequest request) {

        GetProductListReply.Builder builder = GetProductListReply.newBuilder();

        Example productExample = new Example(StakingProduct.class);
        productExample.createCriteria().andEqualTo("orgId", request.getOrgId());
        productExample.createCriteria().andEqualTo("isShow", 1);
        List<StakingProduct> stakingProducts = stakingProductMapper.selectByExample(productExample);
        if(CollectionUtils.isEmpty(stakingProducts)){
            return builder.addAllProducts(new ArrayList<>()).build();
        }

        List<Long> productIds = stakingProducts.stream().map(StakingProduct::getId).collect(Collectors.toList());

        Example localExample = new Example(StakingProductLocal.class);
        localExample.createCriteria().andEqualTo("orgId", request.getOrgId()).andIn("productId", productIds);
        List<StakingProductLocal> stakingProductLocals = stakingProductLocalMapper.selectByExample(localExample);
        //转为proto定义的localinfo
        List<StakingProductLocalInfo> localInfos = stakingProductLocals.stream().map(StakingUtils::convertProductLocalInfo).collect(Collectors.toList());
        //按productId分组
        Map<Long, List<StakingProductLocalInfo>> localMap = localInfos.stream().collect(Collectors.groupingBy(StakingProductLocalInfo::getProductId));

        //活期产品不查询rebate
        Example rebateExample = new Example(StakingProductRebate.class);
        rebateExample.createCriteria().andEqualTo("orgId", request.getOrgId())
                .andNotEqualTo("status", StakingProductRebateStatus.STPD_REBATE_STATUS_INVALID_VALUE)
                .andEqualTo("type", StakingProductRebateType.STPD_REBATE_TYPE_INTEREST_VALUE)
                .andNotEqualTo("productType", StakingProductType.FI_CURRENT_VALUE);

        List<StakingProductRebate> stakingProductRebates = stakingProductRebateMapper.selectByExample(rebateExample);
        //转为proto定义的RebateInfo
        List<StakingProductRebateInfo> rebateInfos = stakingProductRebates.stream().map(rebate -> StakingUtils.convertProductRebateInfo(rebate, basicService)).collect(Collectors.toList());

        //按productId分组
        Map<Long, List<StakingProductRebateInfo>> rebateMap = rebateInfos.stream().collect(Collectors.groupingBy(StakingProductRebateInfo::getProductId));

        Example limitExample = new Example(StakingProductSubscribeLimit.class);
        limitExample.createCriteria().andEqualTo("orgId", request.getOrgId());
        List<StakingProductSubscribeLimit> subscribeLimits = stakingProductSubscribeLimitMapper.selectByExample(limitExample);
        //转为proto定义的limitInfo
        List<StakingProductSubscribeLimitInfo> limits = subscribeLimits.stream().map(StakingUtils::convertProductSubscribeLimitInfo).collect(Collectors.toList());
        //按productId分组
        Map<Long, StakingProductSubscribeLimitInfo> limitMap = limits.stream().collect(Collectors.toMap(StakingProductSubscribeLimitInfo::getProductId, p -> p, (p, q) -> p));

        List<StakingProductInfo> productInfos = new ArrayList<>();

        stakingProducts.forEach(product -> {
            StakingProductInfo productInfo = StakingProductInfo.newBuilder()
                    .setId(product.getId())
                    .setOrgId(product.getOrgId())
                    .setTokenId(product.getTokenId())
                    .setTokenName(basicService.getTokenName(product.getOrgId(), product.getTokenId()))
                    .setDividendType(product.getDividendType())
                    .setDividendTimes(product.getDividendTimes())
                    .setTimeLimit(product.getTimeLimit())
                    .setReferenceApr(product.getReferenceApr())
                    .setActualApr(product.getActualApr().stripTrailingZeros().toPlainString())
                    .setWeeklyApr(product.getWeeklyApr().stripTrailingZeros().toPlainString())
                    .setPerUsrLowLots(product.getPerUsrLowLots())
                    .setPerUsrUpLots(product.getPerUsrUpLots())
                    .setUpLimitLots(product.getUpLimitLots())
                    .setShowUpLimitLots(product.getShowUpLimitLots())
                    .setPerLotAmount(product.getPerLotAmount().stripTrailingZeros().toPlainString())
                    .setSoldLots(product.getSoldLots())
                    .setSubscribeStartDate(product.getSubscribeStartDate())
                    .setSubscribeEndDate(product.getSubscribeEndDate())
                    .setInterestStartDate(product.getInterestStartDate())
                    .setSort(product.getSort())
                    .setType(product.getType())
                    .setIsShow(product.getIsShow())
                    .setPrincipalAccountId(product.getPrincipalAccountId())
                    .setDividendAccountId(product.getPrincipalAccountId())
                    .setFundFlow(product.getFundFlow())
                    .setArrposid(product.getArrposid())
                    .setCreatedAt(product.getCreatedAt())
                    .setUpdatedAt(product.getUpdatedAt())
                    .addAllLocalInfos(localMap.containsKey(product.getId()) ? localMap.get(product.getId()) : new ArrayList<>())
                    .addAllRebates(rebateMap.containsKey(product.getId()) ? rebateMap.get(product.getId()) : new ArrayList<>())
                    .setLimitinfo(limitMap.containsKey(product.getId()) ? limitMap.get(product.getId()) : StakingProductSubscribeLimitInfo.newBuilder().build())
                    .setRebateCalcWay(rebateMap.containsKey(product.getId()) ? rebateMap.get(product.getId()).get(0).getRebateCalcWay() : StakingProductRebateCalcWay.STPD_REBATE_CALCWAY_RATE_VALUE)
                    .build();

            productInfos.add(productInfo);
        });
        return builder.addAllProducts(productInfos).build();
    }

    public GetStakingAssetListReply getStakingAssetList(GetStakingAssetListRequest request) {
        GetStakingAssetListReply.Builder builder = GetStakingAssetListReply.newBuilder();

        Example example = new Example(StakingAsset.class);
        Example.Criteria criteria = example.createCriteria();

        criteria.andEqualTo("orgId", request.getOrgId())
                .andEqualTo("userId", request.getUserId())
                .andGreaterThan("currentAmount", "0");

        if (request.getProductId() > 0) {
            criteria.andEqualTo("productId", request.getProductId());
        } else{
            if(!CollectionUtils.isEmpty(request.getProductTypesList())){
                if(request.getProductTypesList().size() == 1){
                    criteria.andEqualTo("productType", request.getProductTypesList().get(0));
                } else{
                    criteria.andIn("productType", request.getProductTypesList());
                }
            }
        }

        List<StakingAsset> stakingAssets = stakingAssetMapper.selectByExample(example);

        if (stakingAssets.isEmpty()) {
            return builder.addAllAssets(new ArrayList<>()).build();
        }

        List<StakingAssetInfo> assetInfos = new ArrayList<>();

        stakingAssets.forEach(asset -> {
            setValuationForAsset(asset);

            StakingAssetInfo assetInfo = StakingAssetInfo.newBuilder()
                    .setId(asset.getId())
                    .setUserId(asset.getUserId())
                    .setOrgId(asset.getOrgId())
                    .setAccountId(asset.getAccountId())
                    .setProductId(asset.getProductId())
                    .setProductType(asset.getProductType())
                    .setTokenId(asset.getTokenId())
                    .setTokenName(basicService.getTokenName(asset.getOrgId(), asset.getTokenId()))
                    .setTotalAmount(asset.getTotalAmount().stripTrailingZeros().toPlainString())
                    .setCurrentAmount(asset.getCurrentAmount().stripTrailingZeros().toPlainString())
                    .setLastProfit(asset.getLastProfit().stripTrailingZeros().toPlainString())
                    .setTotalProfit(asset.getTotalProfit().stripTrailingZeros().toPlainString())
                    .setCreatedAt(asset.getCreatedAt())
                    .setUpdatedAt(asset.getUpdatedAt())
                    .setBtcValue(asset.getBtcValue().setScale(8, BigDecimal.ROUND_DOWN).stripTrailingZeros().toPlainString())
                    .setUsdtValue(asset.getUsdtValue().setScale(2, BigDecimal.ROUND_DOWN).stripTrailingZeros().toPlainString())
                    .build();

            assetInfos.add(assetInfo);
        });

        return builder.addAllAssets(assetInfos).build();
    }

    public GetOrderRepaymentListReply getOrderRepaymentList(GetOrderRepaymentListRequest request) {

        GetOrderRepaymentListReply.Builder builder = GetOrderRepaymentListReply.newBuilder();

        Example orderExample = new Example(StakingProductOrder.class);
        orderExample.createCriteria().andEqualTo("id", request.getOrderId()).andEqualTo("userId", request.getUserId());
        StakingProductOrder order = stakingProductOrderMapper.selectOneByExample(orderExample);
        if (order == null) {
            return builder.build();
        }

        StakingProductOrderInfo orderInfo = StakingUtils.convertProductOrderInfo(order, basicService.getTokenName(order.getOrgId(), order.getTokenId()));
        builder.setOrderInfo(orderInfo);

        Example rebateDetailExample = new Example(StakingProductRebateDetail.class);
        rebateDetailExample.createCriteria().andEqualTo("orgId", request.getOrgId())
                .andEqualTo("userId", request.getUserId())
                .andEqualTo("productId", request.getProductId())
                .andEqualTo("orderId", request.getOrderId())
                .andEqualTo("status", StakingProductRebateDetailStatus.STPD_REBATE_DETAIL_STATUS_SUCCESS_VALUE);

        List<StakingProductRebateDetail> details = stakingProductRebateDetailMapper.selectByExample(rebateDetailExample);
        if (details.isEmpty()) {
            return builder.addAllRepayments(new ArrayList<>()).build();
        }

        List<StakingProductRebateDetailInfo> detailInfos = details.stream().map(detail -> StakingUtils.convertProductRebateDetailInfo(detail, basicService.getTokenName(detail.getOrgId(), detail.getTokenId()))).collect(Collectors.toList());

        return builder.addAllRepayments(detailInfos).build();
    }

    public GetProductOrderListReply getProductOrderList(GetProductOrderListRequest request) {
        GetProductOrderListReply.Builder builder = GetProductOrderListReply.newBuilder();

        Example example = new Example(StakingProductOrder.class);
        Example.Criteria criteria = example.createCriteria();

        if (request.getStartOrderId() > 0) {
            criteria.andLessThan("id", request.getStartOrderId());
        }

        criteria.andEqualTo("orgId", request.getOrgId())
                .andEqualTo("userId", request.getUserId())
                .andIn("productType", Arrays.asList(StakingProductType.FI_TIME_VALUE, StakingProductType.LOCK_POSITION_VALUE));

        if (request.getProductId() != 0) {
            criteria.andEqualTo("productId", request.getProductId());
        }

        if (request.getProductType() != -1) {
            criteria.andEqualTo("productType", request.getProductType());
        }

        if (request.getType() == 1) {
            //查询当前有效订单
            criteria.andNotIn("status", Arrays.asList(StakingProductOrderStatus.STPD_ORDER_STATUS_FAILED_VALUE, StakingProductOrderStatus.STPD_ORDER_STATUS_REDEEMED_VALUE));
        } else if (request.getType() == 2) {
            //查询历史订单
            criteria.andIn("status", Arrays.asList(StakingProductOrderStatus.STPD_ORDER_STATUS_FAILED_VALUE, StakingProductOrderStatus.STPD_ORDER_STATUS_REDEEMED_VALUE));
        }

        example.setOrderByClause("id desc");

        List<StakingProductOrder> orders = stakingProductOrderMapper.selectByExampleAndRowBounds(example, new RowBounds(0, request.getLimit()));

        List<StakingProductOrderInfo> orderInfos = orders.stream().map(order -> StakingUtils.convertProductOrderInfo(order, basicService.getTokenName(order.getOrgId(), order.getTokenId()))).collect(Collectors.toList());

        return builder.addAllOrders(orderInfos).build();
    }

    public GetProductJourListReply getProductJourList(GetProductJourListRequest request) {

        List<StakingProductJour> productJourList = stakingProductJourMapper.getProductJourList(request.getOrgId(), request.getUserId(), request.getProductId(), request.getProductType(), request.getType(), request.getStartId(), request.getLimit(), request.getJourId());

        List<StakingProductJourInfo> list = new ArrayList<>();

        productJourList.forEach(jour -> {
                    StakingProductJourInfo jourInfo = StakingProductJourInfo.newBuilder()
                            .setId(jour.getId())
                            .setOrgId(jour.getOrgId())
                            .setUserId(jour.getUserId())
                            .setAccountId(jour.getAccountId())
                            .setProductId(jour.getProductId())
                            .setProductType(jour.getProductType())
                            .setType(jour.getType())
                            .setAmount(StakingUtils.decimalScaleToString(jour.getAmount()))
                            .setTokenId(jour.getTokenId())
                            .setTokenName(basicService.getTokenName(jour.getOrgId(), jour.getTokenId()))
                            .setTransferId(jour.getTransferId())
                            .setCreatedAt(jour.getCreatedAt())
                            .setUpdatedAt(jour.getUpdatedAt())
                            .build();
                    list.add(jourInfo);
                }
        );

        return GetProductJourListReply.newBuilder().addAllJours(list).build();
    }

    /**
     * 为兼容原币多多接口功能，特殊新增此接口，其他业务不能使用
     */
    public GetSimpleFinanceRecordReply getSimpleFinanceRecord(GetSimpleFinanceRecordRequest request) {

        List<StakingProductJour> productJours = stakingProductJourMapper.getSimpleFinanceRecord(request.getOrgId(), request.getJourId(), request.getOrderId());

        StakingProductJourInfo.Builder builder = StakingProductJourInfo.newBuilder();
        int status = 1;
        if (productJours.isEmpty()) {

            StakingProductOrder order = stakingProductOrderMapper.getOrderById(request.getOrgId(), request.getOrderId());
            if (order == null) {
                return GetSimpleFinanceRecordReply.newBuilder().build();
            }

            builder.setId(order.getId())
                    .setOrgId(order.getOrgId())
                    .setUserId(order.getUserId())
                    .setAccountId(order.getAccountId())
                    .setProductId(order.getProductId())
                    .setProductType(order.getProductType())
                    .setType(order.getOrderType() == StakingProductOrderType.STPD_ORDER_TYPE_SUBSCRIBE_VALUE ? StakingProductJourType.STPD_JOUR_TYPE_SUBSCRIBE_VALUE : StakingProductJourType.STPD_JOUR_TYPE_REDEEM_VALUE)
                    .setAmount(order.getPayAmount().stripTrailingZeros().toPlainString())
                    .setTokenId(order.getTokenId())
                    .setTokenName(basicService.getTokenName(order.getOrgId(), order.getTokenId()))
                    .setTransferId(order.getTransferId())
                    .setCreatedAt(order.getCreatedAt())
                    .setUpdatedAt(order.getUpdatedAt());
            if (order.getStatus() == StakingProductOrderStatus.STPD_ORDER_STATUS_FAILED_VALUE) {
                status = 3;
            } else if (order.getStatus() == StakingProductOrderStatus.STPD_ORDER_STATUS_WAITING_VALUE) {
                status = 0;
            }

        } else {
            StakingProductJour productJour = productJours.get(0);

            builder.setId(productJour.getId())
                    .setOrgId(productJour.getOrgId())
                    .setUserId(productJour.getUserId())
                    .setAccountId(productJour.getAccountId())
                    .setProductId(productJour.getProductId())
                    .setProductType(productJour.getProductType())
                    .setType(productJour.getType())
                    .setAmount(productJour.getAmount().stripTrailingZeros().toPlainString())
                    .setTokenId(productJour.getTokenId())
                    .setTokenName(basicService.getTokenName(productJour.getOrgId(), productJour.getTokenId()))
                    .setTransferId(productJour.getTransferId())
                    .setCreatedAt(productJour.getCreatedAt())
                    .setUpdatedAt(productJour.getUpdatedAt());
        }

        return GetSimpleFinanceRecordReply.newBuilder()
                .setStatus(status)
                .setJour(builder.build())
                .build();

    }

    /**
     * get broker of permission
     *
     * @param request
     * @return
     */
    public ListBrokerPermissionReply listBorkerOfPermission(ListBrokerPermissionRequest request){
        List<StakingProductPermission> listBrokerPermission = stakingProductPermissionMapper.selectAll();
        if(CollectionUtils.isEmpty(listBrokerPermission)){
            return ListBrokerPermissionReply.newBuilder().build();
        }
        List<ListBrokerPermissionReply.BrokerPermission> listBroker = new ArrayList<>();
        listBrokerPermission.forEach(brokerPermission->{
            listBroker.add(ListBrokerPermissionReply.BrokerPermission.newBuilder()
                    .setOrgId(brokerPermission.getOrgId())
                    .setFixed(brokerPermission.getAllowFixed())
                    .setLock(brokerPermission.getAllowFixedLock())
                    .setCurrent(brokerPermission.getAllowFlexible())
                    .build());
        });
        return ListBrokerPermissionReply.newBuilder().addAllBrokers(listBroker).build();
    }


    private void setValuationForAsset(StakingAsset asset) {
        Rate fxRate = basicService.getV3Rate(asset.getOrgId(), asset.getTokenId());
        if (fxRate == null) {
            asset.setBtcValue(BigDecimal.ZERO);
            asset.setUsdtValue(BigDecimal.ZERO);
        } else {
            asset.setBtcValue(StakingUtils.decimalScale(asset.getCurrentAmount().multiply(DecimalUtil.toBigDecimal(fxRate.getRatesMap().get("BTC"))), 8));
            asset.setUsdtValue(StakingUtils.decimalScale(asset.getCurrentAmount().multiply(DecimalUtil.toBigDecimal(fxRate.getRatesMap().get("USDT"))), 2));
        }
    }

    public GetProductListReply orgQueryProductListReply(Header header, Integer type, Long fromId, Long toId, Integer limit) {
        GetProductListReply.Builder builder = GetProductListReply.newBuilder();

        Example productExample = new Example(StakingProduct.class);
        Example.Criteria criteria = productExample.createCriteria();
        if (fromId > 0) {
            criteria.andLessThan("id", fromId);
        }
        if (toId > 0) {
            criteria.andGreaterThan("id", toId);
        }
        criteria.andEqualTo("orgId", header.getOrgId());

        if (type >= 0) {
            criteria.andEqualTo("type", type);
        }
        if (toId > 0) {
            productExample.setOrderByClause("id");
        } else {
            productExample.setOrderByClause("id DESC");
        }


        List<StakingProduct> stakingProducts = stakingProductMapper.selectByExampleAndRowBounds(productExample, new RowBounds(0, limit));

        List<Long> productIds = stakingProducts.stream().map(StakingProduct::getId).collect(Collectors.toList());
        List<StakingProductSubscribeLimit> subscribeLimits = new ArrayList<>();
        if (!productIds.isEmpty()) {
            Example limitExample = new Example(StakingProductSubscribeLimit.class);
            Example.Criteria limitCriteria = limitExample.createCriteria();
            limitCriteria.andIn("id", productIds);
            limitCriteria.andEqualTo("orgId", header.getOrgId());
            subscribeLimits = stakingProductSubscribeLimitMapper.selectByExample(limitExample);
        }
        //转为proto定义的limitInfo 按productId分组
        Map<Long, StakingProductSubscribeLimitInfo> limitMap = subscribeLimits
                .stream()
                .map(StakingUtils::convertProductSubscribeLimitInfo)
                .collect(Collectors.toMap(StakingProductSubscribeLimitInfo::getProductId, p -> p, (p, q) -> p));
        List<StakingProductInfo> productInfos = stakingProducts.stream()
                .map(product -> StakingProductInfo.newBuilder()
                        .setId(product.getId())
                        .setOrgId(product.getOrgId())
                        .setTokenId(product.getTokenId())
                        .setTokenName(basicService.getTokenName(product.getOrgId(), product.getTokenId()))
                        .setDividendType(product.getDividendType())
                        .setDividendTimes(product.getDividendTimes())
                        .setTimeLimit(product.getTimeLimit())
                        .setReferenceApr(product.getReferenceApr())
                        .setActualApr(product.getActualApr().stripTrailingZeros().toPlainString())
                        .setWeeklyApr(product.getWeeklyApr().stripTrailingZeros().toPlainString())
                        .setPerUsrLowLots(product.getPerUsrLowLots())
                        .setPerUsrUpLots(product.getPerUsrUpLots())
                        .setUpLimitLots(product.getUpLimitLots())
                        .setShowUpLimitLots(product.getShowUpLimitLots())
                        .setPerLotAmount(product.getPerLotAmount().stripTrailingZeros().toPlainString())
                        .setSoldLots(product.getSoldLots())
                        .setSubscribeStartDate(product.getSubscribeStartDate())
                        .setSubscribeEndDate(product.getSubscribeEndDate())
                        .setInterestStartDate(product.getInterestStartDate())
                        .setSort(product.getSort())
                        .setType(product.getType())
                        .setIsShow(product.getIsShow())
                        .setPrincipalAccountId(product.getPrincipalAccountId())
                        .setDividendAccountId(product.getPrincipalAccountId())
                        .setFundFlow(product.getFundFlow())
                        .setArrposid(product.getArrposid())
                        .setCreatedAt(product.getCreatedAt())
                        .setUpdatedAt(product.getUpdatedAt())
                        .setLimitinfo(limitMap.containsKey(product.getId()) ? limitMap.get(product.getId()) : StakingProductSubscribeLimitInfo.newBuilder().build())
                        .build())
                .collect(Collectors.toList());
        if (toId > 0) {
            Collections.reverse(productInfos);
        }
        return builder.addAllProducts(productInfos).build();
    }

    public GetProductOrderListReply orgGetProductOrderList(GetProductOrderListRequest request) {
        GetProductOrderListReply.Builder builder = GetProductOrderListReply.newBuilder();

        Example example = new Example(StakingProductOrder.class);
        Example.Criteria criteria = example.createCriteria();

        if (request.getStartOrderId() > 0) {
            criteria.andLessThan("id", request.getStartOrderId());
        }

        criteria.andEqualTo("orgId", request.getOrgId());
        if (request.getUserId() != 0) {
            criteria.andEqualTo("userId", request.getUserId());
        }

        if (request.getProductId() != 0) {
            criteria.andEqualTo("productId", request.getProductId());
        }

        if (request.getProductType() != -1) {
            criteria.andEqualTo("productType", request.getProductType());
        }

        if (request.getType() == 1) {
            //查询当前有效订单
            criteria.andNotIn("status", Arrays.asList(StakingProductOrderStatus.STPD_ORDER_STATUS_FAILED_VALUE, StakingProductOrderStatus.STPD_ORDER_STATUS_REDEEMED_VALUE));
        } else if (request.getType() == 2) {
            //查询历史订单
            criteria.andIn("status", Arrays.asList(StakingProductOrderStatus.STPD_ORDER_STATUS_FAILED_VALUE, StakingProductOrderStatus.STPD_ORDER_STATUS_REDEEMED_VALUE));
        }

        example.setOrderByClause("id desc");

        List<StakingProductOrder> orders = stakingProductOrderMapper.selectByExampleAndRowBounds(example, new RowBounds(0, request.getLimit()));

        List<StakingProductOrderInfo> orderInfos = orders.stream().map(order -> StakingUtils.convertProductOrderInfo(order, basicService.getTokenName(order.getOrgId(), order.getTokenId()))).collect(Collectors.toList());

        return builder.addAllOrders(orderInfos).build();
    }

}
