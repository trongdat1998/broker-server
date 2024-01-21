package io.bhex.broker.server.grpc.server.service.staking;

import com.google.common.collect.Lists;
import io.bhex.base.idgen.api.ISequenceGenerator;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.grpc.proto.AdminCommonResponse;
import io.bhex.broker.grpc.staking.*;
import io.bhex.broker.server.grpc.server.service.AccountService;
import io.bhex.broker.server.grpc.server.service.BasicService;
import io.bhex.broker.server.grpc.server.service.UserService;
import io.bhex.broker.server.model.Broker;
import io.bhex.broker.server.model.User;
import io.bhex.broker.server.model.staking.*;
import io.bhex.broker.server.primary.mapper.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.ibatis.session.RowBounds;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;
import tk.mybatis.mapper.entity.Example;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
@Service
public class StakingProductAdminService {
    private final static String default_lang = "en_US";

    @Autowired
    BasicService basicService;

    @Autowired
    AccountService accountService;

    @Resource
    StakingProductMapper stakingProductMapper;

    @Resource
    StakingProductLocalMapper stakingProductLocalMapper;

    @Resource
    StakingProductRebateMapper stakingProductRebateMapper;

    @Resource
    StakingProductRebateDetailMapper stakingProductRebateDetailMapper;

    @Resource
    StakingProductSubscribeLimitMapper stakingProductSubscribeLimitMapper;

    @Resource
    StakingProductPermissionMapper stakingProductPermissionMapper;

    @Resource
    private ISequenceGenerator sequenceGenerator;

    @Resource
    StakingProductOrderMapper stakingProductOrderMapper;

    @Resource
    UserService userService;

    @Resource
    BrokerMapper brokerMapper;

    @Resource
    StakingProductRebateBatchMapper stakingProductRebateBatchMapper;

    @Resource
    StakingAssetMapper stakingAssetMapper;

    @Transactional(rollbackFor = Exception.class)
    public AdminSaveProductReply saveProduct(AdminSaveProductRequest request) {
        //返回参数code 0=成功 1=本金账户不是有效资金账户 2=派息账户不是有效资金账户 3=产品信息不存在 4=不创建产品权限

        AdminSaveProductReply.Builder builder = AdminSaveProductReply.newBuilder();

        long currentTimeMillis = request.getTimestamp();
        StakingProduct stakingProduct;

        Integer result = saveProductCheckParam(request);
        if (result != 0) {
            return builder.setCode(result).setProductId(0).build();
        }

        if (request.getId() == 0) {

            //检查是否有业务权限
            Example permissionExample = new Example(StakingProductPermission.class);
            permissionExample.createCriteria().andEqualTo("orgId", request.getOrgId());
            StakingProductPermission permission = stakingProductPermissionMapper.selectOneByExample(permissionExample);
            if ((permission == null && request.getType() != StakingProductType.LOCK_POSITION.getNumber())
                    || (request.getType() == StakingProductType.FI_TIME.getNumber() && permission.getAllowFixed() != 1)
                    || (request.getType() == StakingProductType.LOCK_POSITION.getNumber() && (permission != null && permission.getAllowFixedLock() != 1))
                    || (request.getType() == StakingProductType.FI_CURRENT.getNumber() && permission.getAllowFlexible() != 1)) {
                return builder.setCode(4).setProductId(0).build();
            }
            if (permission == null && request.getType() == StakingProductType.LOCK_POSITION.getNumber()) {
                // 因为默认是开通定期锁仓的，所以这里在添加锁仓产品时判断一下是否设置了开通权限，如果没有则新增
                stakingProductPermissionMapper.insertSelective(StakingProductPermission.builder()
                        .orgId(request.getOrgId())
                        .allowFixed(0)
                        .allowFixedLock(1)
                        .allowFlexible(0)
                        .created_at(currentTimeMillis)
                        .updated_at(currentTimeMillis)
                        .build());
            }

            stakingProduct = saveProdcut(request, currentTimeMillis);

            //新增或更新产品多语言名称及描述
            saveProductLocals(request.getLocalInfosList(), stakingProduct, currentTimeMillis);

            //新增派息记录
            if (request.getType() == StakingProductType.FI_CURRENT_VALUE) {
                insertCurrnetProductRebates(stakingProduct, stakingProduct.getSubscribeStartDate() + 86400_000L, 0, currentTimeMillis);
            } else {
                insertProductRebates(request.getRebatesList(), stakingProduct, currentTimeMillis);
            }

            //新增或更新认购条件
            AdminSaveProductRequest.ProductSubscribeLimitInfo limitInfo = request.getLimitinfo();
            saveProductSubscribeLimit(limitInfo, stakingProduct.getOrgId(), stakingProduct.getId(), currentTimeMillis);

        } else {
            //更新理财产品信息
            stakingProduct = stakingProductMapper.getProductById(request.getOrgId(), request.getId());

            if (stakingProduct == null) {
                return builder.setCode(3).setProductId(0).build();
            }

            Long oldSubscribeStartDate = stakingProduct.getSubscribeStartDate();
            Long oldSubscribeEndDate = stakingProduct.getSubscribeEndDate();
            BigDecimal oldActualApr = stakingProduct.getActualApr();

            if (stakingProduct.getSubscribeStartDate() > currentTimeMillis) {
                //认购开始时间之前

                stakingProduct = saveProdcut(request, currentTimeMillis);

                //新增或更新产品多语言名称及描述
                saveProductLocals(request.getLocalInfosList(), stakingProduct, currentTimeMillis);

                if (request.getType() == StakingProductType.FI_CURRENT_VALUE) {
                    updateCurrnetProductRebates(stakingProduct, oldSubscribeStartDate, oldSubscribeEndDate, oldActualApr, false, currentTimeMillis);

                } else {
                    //先取消原先的记录，再新增派息记录
                    stakingProductRebateMapper.updateRebateStatusById(stakingProduct.getOrgId(), stakingProduct.getId(), StakingProductRebateStatus.STPD_REBATE_STATUS_INVALID_VALUE, currentTimeMillis);

                    insertProductRebates(request.getRebatesList(), stakingProduct, currentTimeMillis);
                }

                //新增或更新认购条件
                AdminSaveProductRequest.ProductSubscribeLimitInfo limitInfo = request.getLimitinfo();
                saveProductSubscribeLimit(limitInfo, stakingProduct.getOrgId(), stakingProduct.getId(), currentTimeMillis);
            } else {
                //认购开始时间之后

                if (stakingProduct.getSubscribeEndDate() > currentTimeMillis) {
                    //认购结束时间之前，可修改认购结束时间、实际发售份额
                    stakingProduct.setSubscribeEndDate(request.getSubscribeEndDate());
                    stakingProduct.setUpLimitLots(request.getUpLimitLots());
                }

                if (stakingProduct.getType() == StakingProductType.FI_CURRENT_VALUE) {
                    stakingProduct.setTimeLimit((int) TimeUnit.MILLISECONDS.toDays(request.getSubscribeEndDate() - request.getSubscribeStartDate()));
                    stakingProduct.setActualApr(new BigDecimal(request.getActualApr()));
                } else{
                    stakingProduct.setShowUpLimitLots(request.getShowUpLimitLots());
                }

                stakingProduct.setReferenceApr(request.getReferenceApr());
                stakingProduct.setUpdatedAt(currentTimeMillis);
                stakingProductMapper.updateByPrimaryKeySelective(stakingProduct);

                //新增或更新产品多语言名称及描述
                saveProductLocals(request.getLocalInfosList(), stakingProduct, currentTimeMillis);

                if (stakingProduct.getType() == StakingProductType.FI_CURRENT_VALUE) {
                    updateCurrnetProductRebates(stakingProduct, oldSubscribeStartDate, oldSubscribeEndDate, oldActualApr, true, currentTimeMillis);
                } else {
                    //更新派息记录
                    updateProductRebates(request.getRebatesList(), stakingProduct, currentTimeMillis);

                }
                //新增或更新认购条件
                AdminSaveProductRequest.ProductSubscribeLimitInfo limitInfo = request.getLimitinfo();
                saveProductSubscribeLimit(limitInfo, stakingProduct.getOrgId(), stakingProduct.getId(), currentTimeMillis);
            }
        }

        return builder.setCode(0).setProductId(stakingProduct.getId()).build();
    }

    public AdminGetProductDetailReply getProductDetail(AdminGetProductDetailRequest request) {
        AdminGetProductDetailReply.Builder builder = AdminGetProductDetailReply.newBuilder();

        StakingProduct stakingProduct = stakingProductMapper.getProductById(request.getOrgId(), request.getProductId());

        if (stakingProduct == null) {
            return builder.build();
        }

        Example localExample = new Example(StakingProductLocal.class);
        localExample.createCriteria().andEqualTo("orgId", stakingProduct.getOrgId()).andEqualTo("productId", stakingProduct.getId());
        List<StakingProductLocal> localList = stakingProductLocalMapper.selectByExample(localExample);
        //转为proto 的localinfo
        List<StakingProductLocalInfo> localInfos = localList.stream().map(StakingUtils::convertProductLocalInfo).collect(Collectors.toList());

        List<StakingProductRebateInfo> rebateInfos = new ArrayList<>();
        if (stakingProduct.getType() != StakingProductType.FI_CURRENT_VALUE) {
            //非活期产品才去查询rebatelist
            Example rebateExample = new Example(StakingProductRebate.class);
            rebateExample.createCriteria().andEqualTo("orgId", stakingProduct.getOrgId())
                    .andEqualTo("productId", stakingProduct.getId())
                    .andNotEqualTo("status", StakingProductRebateStatus.STPD_REBATE_STATUS_INVALID_VALUE)
                    .andEqualTo("type", StakingProductRebateType.STPD_REBATE_TYPE_INTEREST_VALUE);
            List<StakingProductRebate> rebateList = stakingProductRebateMapper.selectByExample(rebateExample);
            //转为proto 的rebateInfo
            rebateInfos = rebateList.stream().map(rebate -> StakingUtils.convertProductRebateInfo(rebate, basicService)).collect(Collectors.toList());
        }

        StakingProductSubscribeLimit subscribeLimit = stakingProductSubscribeLimitMapper.selectOne(StakingProductSubscribeLimit.builder().productId(request.getProductId()).build());

        StakingProductSubscribeLimitInfo limitInfo = StakingProductSubscribeLimitInfo.newBuilder()
                .setId(subscribeLimit.getId())
                .setOrgId(subscribeLimit.getOrgId())
                .setProductId(subscribeLimit.getProductId())
                .setVerifyKyc(subscribeLimit.getVerifyKyc())
                .setVerifyBindPhone(subscribeLimit.getVerifyBindPhone())
                .setVerifyBalance(subscribeLimit.getVerifyBalance())
                .setVerifyAvgBalance(subscribeLimit.getVerifyAvgBalance())
                .setBalanceRuleJson(subscribeLimit.getBalanceRuleJson())
                .setLevelLimit(subscribeLimit.getLevelLimit())
                .setUpdatedTime(subscribeLimit.getUpdatedTime())
                .setCreatedTime(subscribeLimit.getCreatedTime())
                .build();

        StakingProductInfo productInfo = StakingProductInfo.newBuilder()
                .setOrgId(stakingProduct.getOrgId())
                .setId(stakingProduct.getId())
                .setTokenId(stakingProduct.getTokenId())
                .setTokenName(basicService.getTokenName(stakingProduct.getId(), stakingProduct.getTokenId()))
                .setDividendType(stakingProduct.getDividendType())
                .setDividendTimes(stakingProduct.getDividendTimes())
                .setTimeLimit(stakingProduct.getTimeLimit())
                .setReferenceApr(stakingProduct.getReferenceApr())
                .setActualApr(stakingProduct.getActualApr().stripTrailingZeros().toPlainString())
                .setWeeklyApr(stakingProduct.getWeeklyApr().stripTrailingZeros().toPlainString())
                .setPerUsrLowLots(stakingProduct.getPerUsrLowLots())
                .setPerUsrUpLots(stakingProduct.getPerUsrUpLots())
                .setUpLimitLots(stakingProduct.getUpLimitLots())
                .setShowUpLimitLots(stakingProduct.getShowUpLimitLots())
                .setPerLotAmount(stakingProduct.getPerLotAmount().stripTrailingZeros().toPlainString())
                .setSoldLots(stakingProduct.getSoldLots())
                .setSubscribeStartDate(stakingProduct.getSubscribeStartDate())
                .setSubscribeEndDate(stakingProduct.getSubscribeEndDate())
                .setInterestStartDate(stakingProduct.getInterestStartDate())
                .setSort(stakingProduct.getSort())
                .setType(stakingProduct.getType())
                .setIsShow(stakingProduct.getIsShow())
                .setPrincipalAccountId(stakingProduct.getPrincipalAccountId())
                .setDividendAccountId(stakingProduct.getDividendAccountId())
                .setFundFlow(stakingProduct.getFundFlow())
                .setArrposid(stakingProduct.getArrposid())
                .setCreatedAt(stakingProduct.getCreatedAt())
                .setUpdatedAt(stakingProduct.getUpdatedAt())
                .addAllRebates(rebateInfos)
                .addAllLocalInfos(localInfos)
                .setLimitinfo(limitInfo)
                .setRebateCalcWay(CollectionUtils.isEmpty(rebateInfos) ? 0 : rebateInfos.get(0).getRebateCalcWay())
                .build();

        return builder.setMessage("success").setProductInfo(productInfo).build();
    }

    public AdminGetProductListReply getProductList(AdminGetProductListRequest request) {
        AdminGetProductListReply.Builder builder = AdminGetProductListReply.newBuilder();

        //查出所有产品
        Example productExample = new Example(StakingProduct.class);
        Example.Criteria productCriteria = productExample.createCriteria();
        productCriteria.andEqualTo("orgId", request.getOrgId());
        if (request.getStartProductId() > 0) {
            productCriteria.andLessThan("id", request.getStartProductId());
        }
        if (request.getProductTypeList().size() > 0) {
            productCriteria.andIn("type", request.getProductTypeList());
        }
        productExample.orderBy("id").desc();

        List<StakingProduct> productList = stakingProductMapper.selectByExampleAndRowBounds(productExample, new RowBounds(0, request.getLimit()));
        if (CollectionUtils.isEmpty(productList)) {
            return AdminGetProductListReply.newBuilder()
                    .setCode(0)
                    .setMessage("success")
                    .build();
        }

        List<Long> productIds = productList.stream().map(StakingProduct::getId).collect(Collectors.toList());

        Example localExample = new Example(StakingProductLocal.class);
        localExample.createCriteria().andEqualTo("orgId", request.getOrgId()).andIn("productId", productIds);
        List<StakingProductLocal> localList = stakingProductLocalMapper.selectByExample(localExample);
        Map<Long, List<StakingProductLocal>> localMap = localList.stream().collect(Collectors.groupingBy(local -> local.getProductId()));

        List<StakingProductProfile> productProfileList = new ArrayList<>();

        productList.forEach(product -> {
                    StakingProductProfile productProfile = StakingProductProfile.newBuilder()
                            .setProductId(product.getId())
                            .setProductName(getProductName(localMap.get(product.getId()), request.getLanguage()))
                            .setTokenId(product.getTokenId())
                            .setTokenName(basicService.getTokenName(product.getOrgId(), product.getTokenId()))
                            .setActualApr(product.getActualApr().stripTrailingZeros().toPlainString())
                            .setSubscribeStartDate(product.getSubscribeStartDate())
                            .setSubscribeEndDate(product.getSubscribeEndDate())
                            .setInterestStartDate(product.getInterestStartDate())
                            .setType(product.getType())
                            .setIsShow(product.getIsShow())
                            .setStatus(getProductStaus(product))
                            .setUpLimitLots(product.getUpLimitLots())
                            .setPerLotAmount(product.getPerLotAmount().stripTrailingZeros().toPlainString())
                            .setSoldLots(product.getSoldLots())
                            .setDividendType(product.getDividendType())
                            .build();

                    productProfileList.add(productProfile);
                }
        );
        return builder.setCode(0).setMessage("success").addAllProducts(productProfileList).build();
    }

    public AdminCommonResponse onlineProduct(AdminOnlineProductRequest request) {
        long productId = request.getProductId();
        long orgId = request.getOrgId();
        int isShow = request.getOnlineStatus() ? 1 : 0;

        Example example = new Example(StakingProduct.class);
        example.createCriteria().andEqualTo("id", productId).andEqualTo("orgId", orgId);

        StakingProduct product = new StakingProduct();
        product.setIsShow(isShow);
        product.setUpdatedAt(System.currentTimeMillis());

        stakingProductMapper.updateByExampleSelective(product, example);
        return AdminCommonResponse.newBuilder().setSuccess(true).build();
    }


    public AdminCommonResponse setBrokerFixedProductPermission(AdminSetBrokerFixedProductPermissionRequest request) {

        Example example = new Example(StakingProductPermission.class);
        example.createCriteria().andEqualTo("orgId", request.getBrokerId());

        StakingProductPermission permission = stakingProductPermissionMapper.selectOneByExample(example);

        if (permission == null) {
            permission = new StakingProductPermission();
            permission.setOrgId(request.getBrokerId());
            permission.setAllowFixed(request.getStatus());
            permission.setAllowFixedLock(1);
            permission.setAllowFlexible(0);
            permission.setCreated_at(System.currentTimeMillis());
            permission.setUpdated_at(System.currentTimeMillis());
            stakingProductPermissionMapper.insertSelective(permission);
        } else {
            permission.setAllowFixed(request.getStatus());
            permission.setUpdated_at(System.currentTimeMillis());
            stakingProductPermissionMapper.updateByExample(permission, example);
        }

        return AdminCommonResponse.newBuilder().setSuccess(true).build();

    }

    public AdminCommonResponse setBrokerFixedLockProductPermission(AdminSetBrokerFixedLockProductPermissionRequest request) {

        Example example = new Example(StakingProductPermission.class);
        example.createCriteria().andEqualTo("orgId", request.getBrokerId());

        StakingProductPermission permission = stakingProductPermissionMapper.selectOneByExample(example);

        if (permission == null) {
            permission = new StakingProductPermission();
            permission.setOrgId(request.getBrokerId());
            permission.setAllowFixed(0);
            permission.setAllowFixedLock(request.getStatus());
            permission.setAllowFlexible(0);
            permission.setCreated_at(System.currentTimeMillis());
            permission.setUpdated_at(System.currentTimeMillis());
            stakingProductPermissionMapper.insertSelective(permission);
        } else {
            permission.setAllowFixedLock(request.getStatus());
            permission.setUpdated_at(System.currentTimeMillis());
            stakingProductPermissionMapper.updateByExample(permission, example);
        }

        return AdminCommonResponse.newBuilder().setSuccess(true).build();
    }

    public AdminCommonResponse setBrokerCurrentProductPermission(AdminSetBrokerCurrentProductPermissionRequest request) {

        Example example = new Example(StakingProductPermission.class);
        example.createCriteria().andEqualTo("orgId", request.getBrokerId());

        StakingProductPermission permission = stakingProductPermissionMapper.selectOneByExample(example);

        if (permission == null) {
            permission = new StakingProductPermission();
            permission.setOrgId(request.getBrokerId());
            permission.setAllowFixed(0);
            permission.setAllowFixedLock(1);
            permission.setAllowFlexible(request.getStatus());
            permission.setCreated_at(System.currentTimeMillis());
            permission.setUpdated_at(System.currentTimeMillis());
            stakingProductPermissionMapper.insertSelective(permission);
        } else {
            permission.setAllowFlexible(request.getStatus());
            permission.setUpdated_at(System.currentTimeMillis());
            stakingProductPermissionMapper.updateByExample(permission, example);
        }

        return AdminCommonResponse.newBuilder().setSuccess(true).build();
    }

    public AdminGetBrokerProductPermissionReply getBrokerProductPermission(AdminGetBrokerProductPermissionRequest request) {

        Broker broker = brokerMapper.getByOrgId(request.getBrokerId());
        if (broker == null) {
            throw new BrokerException(BrokerErrorCode.PARAM_ERROR);
        }


        Example permissionExample = new Example(StakingProductPermission.class);
        permissionExample.createCriteria().andEqualTo("orgId", request.getBrokerId());
        StakingProductPermission permission = stakingProductPermissionMapper.selectOneByExample(permissionExample);

        if (permission == null) {
            return AdminGetBrokerProductPermissionReply.newBuilder().setAllowFixed(0).setAllowFixedLock(1).setAllowCurrent(0).setBrokerId(request.getBrokerId()).setBrokerName(broker.getBrokerName()).build();
        } else {
            return AdminGetBrokerProductPermissionReply.newBuilder().setAllowFixed(permission.getAllowFixed()).setAllowFixedLock(permission.getAllowFixedLock()).setAllowCurrent(permission.getAllowFlexible()).setBrokerId(request.getBrokerId()).setBrokerName(broker.getBrokerName()).build();
        }

    }

    public AdminQueryBrokerProductUndoRebateReply queryBrokerProductUndoRebate(AdminQueryBrokerProductUndoRebateRequest request) {
        Example rebateExample = new Example(StakingProductRebate.class);

        rebateExample.createCriteria().andEqualTo("orgId", request.getOrgId())
                .andEqualTo("status", StakingProductRebateStatus.STPD_REBATE_STATUS_CALCED_VALUE)
                .andEqualTo("productType", request.getProductType());

        rebateExample.orderBy("rebateDate").desc();

        List<StakingProductRebate> stakingProductRebates = stakingProductRebateMapper.selectByExample(rebateExample);

        if (stakingProductRebates.isEmpty()) {
            return AdminQueryBrokerProductUndoRebateReply.newBuilder().build();
        }

        stakingProductRebates.sort(Comparator.comparingLong(StakingProductRebate::getRebateDate).reversed());

        Set<Long> productIdSet = new HashSet<>();
        stakingProductRebates.forEach(
                rebate -> {
                    productIdSet.add(rebate.getProductId());
                }
        );

        Map<Long, StakingProduct> productMap = getProductMapByIds(productIdSet);

        Map<Long, List<StakingProductLocal>> localMap = getProductLocalMapByIds(productIdSet);

        List<AdminQueryBrokerProductUndoRebateReply.UndoRebate> undoRebateList = new ArrayList<>();
        stakingProductRebates.forEach(
                rebate -> {
                    StakingProduct stakingProduct = productMap.get(rebate.getProductId());
                    if (stakingProduct != null && stakingProduct.getSoldLots() > 0) {
                        AdminQueryBrokerProductUndoRebateReply.UndoRebate undoRebate = AdminQueryBrokerProductUndoRebateReply.UndoRebate.newBuilder()
                                .setId(rebate.getId())
                                .setProductId(rebate.getProductId())
                                .setProductName(getProductName(localMap.get(rebate.getProductId()), request.getLanguage()))
                                .setProductType(rebate.getProductType())
                                .setDividendType(stakingProduct.getDividendType())
                                .setTokenId(stakingProduct.getTokenId())
                                .setTokenName(basicService.getTokenName(stakingProduct.getOrgId(), stakingProduct.getTokenId()))
                                .setRebateDate(rebate.getRebateDate())
                                .setPrincipalAmount(BigDecimal.valueOf(stakingProduct.getSoldLots()).multiply(stakingProduct.getPerLotAmount()).stripTrailingZeros().toPlainString())
                                .setInterestAmount(StakingUtils.decimalScaleToString(rebate.getRebateAmount()))
                                .setInterestTokenId(rebate.getTokenId())
                                .setInterestTokenName(basicService.getTokenName(rebate.getOrgId(), rebate.getTokenId()))
                                .setRebateRate(rebate.getRebateRate().stripTrailingZeros().toPlainString())
                                .setStatus(rebate.getStatus())
                                .setNumberOfPeriods(rebate.getNumberOfPeriods())
                                .setRebateCalcWay(rebate.getRebateCalcWay())
                                .setRebateAmount(StakingUtils.decimalScaleToString(rebate.getRebateAmount()))
                                .setType(rebate.getType())
                                .build();
                        undoRebateList.add(undoRebate);
                    } else {
                        log.warn("rebate product info not exist rebateId = {}, productId = {}", rebate.getId(), rebate.getProductId());
                    }
                }
        );
        return AdminQueryBrokerProductUndoRebateReply.newBuilder().addAllRebates(undoRebateList).build();
    }

    public AdminQueryBrokerProductHistoryRebateReply queryBrokerProductHistoryRebate(AdminQueryBrokerProductHistoryRebateRequest request) {

        Example rebateExample = new Example(StakingProductRebate.class);

        rebateExample.createCriteria().andEqualTo("orgId", request.getOrgId())
                .andIn("status", Arrays.asList(StakingProductRebateStatus.STPD_REBATE_STATUS_CANCELED_VALUE, StakingProductRebateStatus.STPD_REBATE_STATUS_SUCCESS_VALUE))
                .andEqualTo("productType", request.getProductType());
        rebateExample.orderBy("updatedAt").desc();

        int startIndex = (request.getPageNo() - 1) * request.getSize();

        request.getSize();

        List<StakingProductRebate> stakingProductRebates = stakingProductRebateMapper.selectByExampleAndRowBounds(rebateExample, new RowBounds(startIndex, request.getSize()));

        if (stakingProductRebates.isEmpty()) {
            return AdminQueryBrokerProductHistoryRebateReply.newBuilder().build();
        }

        Set<Long> productIdSet = new HashSet<>();
        stakingProductRebates.forEach(
                rebate -> {
                    productIdSet.add(rebate.getProductId());
                }
        );

        Map<Long, StakingProduct> productMap = getProductMapByIds(productIdSet);

        Map<Long, List<StakingProductLocal>> localMap = getProductLocalMapByIds(productIdSet);

        List<AdminQueryBrokerProductHistoryRebateReply.HistoryRebate> historyRebateList = new ArrayList<>();
        stakingProductRebates.forEach(
                rebate -> {
                    StakingProduct stakingProduct = productMap.get(rebate.getProductId());
                    if (stakingProduct != null) {
                        AdminQueryBrokerProductHistoryRebateReply.HistoryRebate historyRebate = AdminQueryBrokerProductHistoryRebateReply.HistoryRebate.newBuilder()
                                .setId(rebate.getId())
                                .setProductId(rebate.getProductId())
                                .setProductName(getProductName(localMap.get(rebate.getProductId()), request.getLanguage()))
                                .setProductType(rebate.getProductType())
                                .setDividendType(stakingProduct.getDividendType())
                                .setTokenId(stakingProduct.getTokenId())
                                .setTokenName(basicService.getTokenName(stakingProduct.getOrgId(), stakingProduct.getTokenId()))
                                .setRebateDate(rebate.getRebateDate())
                                .setPrincipalAmount(BigDecimal.valueOf(stakingProduct.getSoldLots()).multiply(stakingProduct.getPerLotAmount()).stripTrailingZeros().toPlainString())
                                .setInterestAmount(StakingUtils.decimalScaleToString(rebate.getRebateAmount()))
                                .setInterestTokenId(stakingProduct.getTokenId())
                                .setInterestTokenName(basicService.getTokenName(stakingProduct.getOrgId(), stakingProduct.getTokenId()))
                                .setRebateRate(rebate.getRebateRate().stripTrailingZeros().toPlainString())
                                .setStatus(rebate.getStatus())
                                .setNumberOfPeriods(rebate.getNumberOfPeriods())
                                .setUpdatedAt(rebate.getUpdatedAt())
                                .setRebateCalcWay(rebate.getRebateCalcWay())
                                .setRebateAmount(StakingUtils.decimalScaleToString(rebate.getRebateAmount()))
                                .setType(rebate.getType())
                                .build();
                        historyRebateList.add(historyRebate);
                    } else {
                        log.warn("rebate product info not exist rebateId = {}, productId = {}", rebate.getId(), rebate.getProductId());
                    }
                }
        );

        return AdminQueryBrokerProductHistoryRebateReply.newBuilder().addAllRebates(historyRebateList).build();
    }

    public QueryBrokerProductOrderReply queryBrokerProductOrder(QueryBrokerProductOrderRequest request) {
        QueryBrokerProductOrderReply.Builder builder = QueryBrokerProductOrderReply.newBuilder();

        StakingProduct stakingProduct = stakingProductMapper.getProductById(request.getOrgId(), request.getProductId());
        if (stakingProduct == null) {
            return builder.build();
        }

        Long userIdCondition = 0L;

        if (StringUtils.isNotEmpty(request.getEmail()) || StringUtils.isNotEmpty(request.getPhone()) || request.getUserId() != 0) {
            User userInfo = userService.getUserInfo(request.getOrgId(), 0L, request.getUserId(), "", request.getPhone(), request.getEmail());
            if (userInfo == null) {
                return builder.build();
            }
            userIdCondition = userInfo.getUserId();
        }

        Example example = new Example(StakingProductLocal.class);
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("orgId", request.getOrgId()).andEqualTo("productId", request.getProductId()).andEqualTo("status", 1);

        List<StakingProductLocal> localList = stakingProductLocalMapper.selectByExample(example);

        List<StakingProductOrder> stakingProductOrders = stakingProductOrderMapper.queryBrokerProductOrder(request.getOrgId(), request.getProductId(), userIdCondition, request.getOrderId(), request.getStartId(), request.getLimit());

        List<QueryBrokerProductOrderReply.ProductOrder> orders = stakingProductOrders.stream().map(order -> QueryBrokerProductOrderReply.ProductOrder.newBuilder()
                .setOrderId(order.getId())
                .setUserId(order.getUserId())
                .setProductId(order.getProductId())
                .setProductName(getProductName(localList, request.getLanguage()))
                .setDividendType(stakingProduct.getDividendType())
                .setTokenId(order.getTokenId())
                .setTokenName(basicService.getTokenName(order.getOrgId(), order.getTokenId()))
                .setTimeLimit(stakingProduct.getTimeLimit())
                .setPayLots(order.getPayLots())
                .setPayAmount(order.getPayAmount().stripTrailingZeros().toPlainString())
                .setOrderType(order.getOrderType())
                .setTakeEffectDate(stakingProduct.getInterestStartDate())
                .setProductEndDate(stakingProduct.getInterestStartDate() + stakingProduct.getTimeLimit() * 86400_000L)
                .setRedemptionDate(order.getRedemptionDate())
                .setStatus(getProductStaus(stakingProduct))
                .setReferenceApr(stakingProduct.getReferenceApr())
                .setCreatedAt(order.getCreatedAt())
                .build()).collect(Collectors.toList());

        return builder.addAllOrders(orders).build();
    }

    public AdminQueryCurrentProductAssetReply queryCurrentProductAsset(AdminQueryCurrentProductAssetRequest request) {
        AdminQueryCurrentProductAssetReply.Builder builder = AdminQueryCurrentProductAssetReply.newBuilder();

        StakingProduct stakingProduct = stakingProductMapper.getProductById(request.getOrgId(), request.getProductId());
        if (stakingProduct == null) {
            return builder.build();
        }

        Long userIdCondition = 0L;

        if (StringUtils.isNotEmpty(request.getEmail()) || StringUtils.isNotEmpty(request.getPhone()) || request.getUserId() != 0) {
            User userInfo = userService.getUserInfo(request.getOrgId(), 0L, request.getUserId(), "", request.getPhone(), request.getEmail());
            if (userInfo == null) {
                return builder.build();
            }
            userIdCondition = userInfo.getUserId();
        }

        Example example = new Example(StakingProductLocal.class);
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("orgId", request.getOrgId()).andEqualTo("productId", request.getProductId()).andEqualTo("status", 1);

        List<StakingProductLocal> localList = stakingProductLocalMapper.selectByExample(example);

        List<StakingAsset> stakingAssets = stakingAssetMapper.queryStakingProductAsset(request.getOrgId(), request.getProductId(), userIdCondition, request.getStartId(), request.getLimit());

        List<AdminQueryCurrentProductAssetReply.CurrentProductAsset> assets = stakingAssets.stream().map(asset -> AdminQueryCurrentProductAssetReply.CurrentProductAsset.newBuilder()
                .setId(asset.getId())
                .setUserId(asset.getUserId())
                .setProductId(asset.getProductId())
                .setProductName(getProductName(localList, request.getLanguage()))
                .setProductEndDate(stakingProduct.getInterestStartDate() + stakingProduct.getTimeLimit() * 86400_000L)
                .setTokenId(asset.getTokenId())
                .setTokenName(basicService.getTokenName(asset.getOrgId(), asset.getTokenId()))
                .setAmount(asset.getCurrentAmount().stripTrailingZeros().toPlainString())
                .setProductStatus(getProductStaus(stakingProduct))
                .build()).collect(Collectors.toList());

        return builder.addAllAssets(assets).build();
    }

    public GetProductRepaymentScheduleReply getProductRepaymentSchedule(GetProductRepaymentScheduleRequest request) {

        StakingProductOrder stakingProductOrder = stakingProductOrderMapper.getOrderById(request.getOrgId(), request.getOrderId());

        if (stakingProductOrder == null) {
            return GetProductRepaymentScheduleReply.newBuilder().build();
        }

        if (stakingProductOrder.getStatus() != StakingProductOrderStatus.STPD_ORDER_STATUS_SUCCESS_VALUE &&
                stakingProductOrder.getStatus() != StakingProductOrderStatus.STPD_ORDER_STATUS_REDEEMED_VALUE &&
                stakingProductOrder.getStatus() != StakingProductOrderStatus.STPD_ORDER_STATUS_DIVIDEND_VALUE) {
            //不是 申购成功、计息中、已赎回的订单直接返回
            return GetProductRepaymentScheduleReply.newBuilder().build();
        }


        StakingProduct stakingProduct = stakingProductMapper.getProductById(request.getOrgId(), stakingProductOrder.getProductId());
        if (stakingProduct == null) {
            return GetProductRepaymentScheduleReply.newBuilder().build();
        }

        Example rebateExample = new Example(StakingProductRebate.class);
        rebateExample.createCriteria().andEqualTo("orgId", request.getOrgId())
                .andEqualTo("productId", stakingProductOrder.getProductId())
                .andIn("status", Arrays.asList(StakingProductRebateStatus.STPD_REBATE_STATUS_WAITING_VALUE, StakingProductRebateStatus.STPD_REBATE_STATUS_CALCED_VALUE, StakingProductRebateStatus.STPD_REBATE_STATUS_SUCCESS_VALUE))
                .andEqualTo("type", StakingProductRebateType.STPD_REBATE_TYPE_INTEREST_VALUE);
        rebateExample.orderBy("rebateDate");

        List<StakingProductRebate> rebateList = stakingProductRebateMapper.selectByExample(rebateExample);

        if (CollectionUtils.isEmpty(rebateList)) {
            return GetProductRepaymentScheduleReply.newBuilder().build();
        }

        Example rebateDetailExample = new Example(StakingProductRebateDetail.class);
        rebateDetailExample.createCriteria().andEqualTo("orgId", request.getOrgId())
                .andEqualTo("productId", stakingProductOrder.getProductId())
                .andEqualTo("orderId", stakingProductOrder.getId())
                .andEqualTo("status", StakingProductRebateDetailStatus.STPD_REBATE_DETAIL_STATUS_SUCCESS_VALUE);

        List<StakingProductRebateDetail> rebateDetailList = stakingProductRebateDetailMapper.selectByExample(rebateDetailExample);
        Map<Long, StakingProductRebateDetail> rebateDetailMap = rebateDetailList.stream().collect(Collectors.toMap(StakingProductRebateDetail::getProductRebateId, p -> p, (p, q) -> p));

        ArrayList<GetProductRepaymentScheduleReply.RepaymentSchedule> schedules = new ArrayList<>();
        int sort = 1;
        long interestStartDate = stakingProduct.getInterestStartDate();
        for (StakingProductRebate rebate : rebateList) {
            GetProductRepaymentScheduleReply.RepaymentSchedule.Builder builder = GetProductRepaymentScheduleReply.RepaymentSchedule.newBuilder();

            builder.setUserId(stakingProductOrder.getUserId())
                    .setProductId(stakingProductOrder.getProductId())
                    .setOrderId(stakingProductOrder.getId())
                    .setSort(sort)
                    .setRebateDate(rebate.getRebateDate())
                    .setRebateRate(rebate.getRebateRate().stripTrailingZeros().toPlainString());

            StakingProductRebateDetail productRebateDetail = rebateDetailMap.get(rebate.getId());
            //存在成功的派息记录，则用派息记录的值
            if (productRebateDetail != null && productRebateDetail.getStatus() == StakingProductRebateDetailStatus.STPD_REBATE_DETAIL_STATUS_SUCCESS_VALUE) {
                builder.setRebateAmount(productRebateDetail.getRebateAmount().stripTrailingZeros().toPlainString());
                builder.setTokenId(productRebateDetail.getTokenId());
                builder.setTokenName(basicService.getTokenName(productRebateDetail.getOrgId(), productRebateDetail.getTokenId()));
                builder.setStatus(productRebateDetail.getStatus());

            } else {
                //根据订单金额和利率算出预估利息值,如果是固定金额方式则为0

                BigDecimal rebateAmount = BigDecimal.ZERO;
                if (rebate.getRebateCalcWay() == StakingProductRebateCalcWay.STPD_REBATE_CALCWAY_RATE_VALUE) {
                    long days = TimeUnit.MILLISECONDS.toDays(rebate.getRebateDate() - interestStartDate);
                    rebateAmount = rebate.getRebateRate().multiply(new BigDecimal(days)).multiply(stakingProductOrder.getPayAmount()).divide(new BigDecimal("365"), StakingUtils.SCALE_NUM, BigDecimal.ROUND_DOWN);
                }

                builder.setRebateAmount(rebateAmount.stripTrailingZeros().toPlainString());
                builder.setTokenId(rebate.getTokenId());
                builder.setTokenName(basicService.getTokenName(rebate.getOrgId(), rebate.getTokenId()));
                builder.setStatus(StakingProductRebateDetailStatus.STPD_REBATE_DETAIL_STATUS_WAITING_VALUE);
            }

            interestStartDate = rebate.getRebateDate();

            schedules.add(builder.build());
            sort += 1;
        }

        return GetProductRepaymentScheduleReply.newBuilder().addAllSchedules(schedules).build();

    }

    /**
     * 获取活期产品派息记录
     */
    public GetCurrentProductRepaymentScheduleReply getCurrentProductRepaymentSchedule(GetCurrentProductRepaymentScheduleRequest request) {

        StakingProduct stakingProduct = stakingProductMapper.getProductById(request.getOrgId(), request.getProductId());
        if (stakingProduct == null) {
            return GetCurrentProductRepaymentScheduleReply.newBuilder().build();
        }

        Example detailExample = new Example(StakingProductRebateDetail.class);
        Example.Criteria detailCriteria = detailExample.createCriteria();
        detailCriteria.andEqualTo("orgId", request.getOrgId())
                .andEqualTo("userId", request.getUserId())
                .andEqualTo("productId", request.getProductId())
                .andEqualTo("status", StakingProductRebateDetailStatus.STPD_REBATE_DETAIL_STATUS_SUCCESS_VALUE)
                .andEqualTo("rebateType", StakingProductRebateDetailType.STPD_REBATE_DETAIL_TYPE_INTEREST_VALUE);

        if (request.getStartId() > 0) {
            detailCriteria.andLessThan("id", request.getStartId());
        }

        detailExample.setOrderByClause("id desc");

        List<StakingProductRebateDetail> details = stakingProductRebateDetailMapper.selectByExample(detailExample);

        ArrayList<GetCurrentProductRepaymentScheduleReply.RepaymentSchedule> schedules = new ArrayList<>();

        Set<Long> rebateIdSet = new HashSet<>();
        details.forEach(
                detail -> rebateIdSet.add(detail.getProductRebateId())
        );

        if (rebateIdSet.isEmpty()) {
            return GetCurrentProductRepaymentScheduleReply.newBuilder().addAllSchedules(schedules).build();
        }

        Example rebateExample = new Example(StakingProductRebate.class);
        rebateExample.createCriteria().andIn("id", rebateIdSet);
        List<StakingProductRebate> rebateList = stakingProductRebateMapper.selectByExample(rebateExample);
        Map<Long, StakingProductRebate> rebateMap = rebateList.stream().collect(Collectors.toMap(StakingProductRebate::getId, p -> p));

        for (StakingProductRebateDetail detail : details) {

            StakingProductRebate rebate = rebateMap.get(detail.getProductRebateId());

            GetCurrentProductRepaymentScheduleReply.RepaymentSchedule schedule = GetCurrentProductRepaymentScheduleReply.RepaymentSchedule.newBuilder()
                    .setUserId(detail.getUserId())
                    .setProductId(detail.getProductId())
                    .setRebateDate(rebate == null ? 0L : rebate.getRebateDate())
                    .setRebateAmount(detail.getRebateAmount().stripTrailingZeros().toPlainString())
                    .setRebateRate(rebate == null ? "0" : rebate.getRebateRate().stripTrailingZeros().toPlainString())
                    .setTokenId(detail.getTokenId())
                    .setTokenName(basicService.getTokenName(detail.getOrgId(), detail.getTokenId()))
                    .build();
            schedules.add(schedule);
        }

        return GetCurrentProductRepaymentScheduleReply.newBuilder().addAllSchedules(schedules).build();

    }

    /**
     * 获取活期产品派息计划
     */
    public GetCurrentProductRebateListReply getCurrentProductRebateList(GetCurrentProductRebateListRequest request) {

        Example example = new Example(StakingProductRebate.class);
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("orgId", request.getOrgId())
                .andEqualTo("productId", request.getProductId())
                .andEqualTo("type", StakingProductRebateType.STPD_REBATE_TYPE_INTEREST_VALUE)
                .andNotEqualTo("status", StakingProductRebateStatus.STPD_REBATE_STATUS_INVALID_VALUE);

        if (request.getStatus() != -1) {
            criteria.andEqualTo("status", request.getStatus());
        }

        criteria.andGreaterThan("rebateDate", request.getStartRebateDate());

        example.setOrderByClause("rebate_date");

        List<StakingProductRebate> rebateList = stakingProductRebateMapper.selectByExampleAndRowBounds(example, new RowBounds(0, request.getSize()));

        List<GetCurrentProductRebateListReply.Rebate> rebates = rebateList.stream().map(rebate -> GetCurrentProductRebateListReply.Rebate.newBuilder()
                .setRebateDate(rebate.getRebateDate())
                .setRebateRate(rebate.getRebateRate().stripTrailingZeros().toPlainString())
                .setStatus(rebate.getStatus()).build()).collect(Collectors.toList());

        return GetCurrentProductRebateListReply.newBuilder().addAllRebates(rebates).build();

    }


    @Transactional(rollbackFor = Exception.class)
    public StakingProductRebate reCalcInterestUpdateOldRebate(Long orgId, Long productId, Long productRebateId, String rebateAmount, String rebateRate, String tokenId) {

        Example rebateExample = new Example(StakingProductRebate.class);
        rebateExample.createCriteria().andEqualTo("orgId", orgId).andEqualTo("id", productRebateId).andEqualTo("productId", productId);
        StakingProductRebate stakingProductRebate = stakingProductRebateMapper.selectOneByExample(rebateExample);
        if (stakingProductRebate == null || (stakingProductRebate.getProductType() == StakingProductType.LOCK_POSITION_VALUE && stakingProductRebate.getType() == StakingProductRebateType.STPD_REBATE_TYPE_PRINCIPAL_VALUE)) {
            //不存在或者为锁仓产品还本，报错
            throw new BrokerException(BrokerErrorCode.PARAM_ERROR);
        }
        // 不是计算状态不允许重新计算 2020-11-06
        if(stakingProductRebate.getStatus().compareTo(StakingProductRebateStatus.STPD_REBATE_STATUS_CALCED_VALUE) != 0){
            log.error("staking this rebate status is not calced:orgId->{} productId->{} rebateId->{},status->{}"
                    , orgId, productId, productRebateId,stakingProductRebate.getStatus());
            throw new BrokerException(BrokerErrorCode.DB_RECORD_ERROR);
        }

        // 如果当前记录已经开始派息则不能重新计算
        int transferCount = stakingProductRebateDetailMapper.countTransferByRebateId(orgId, productId, productRebateId);
        if(transferCount > 0){
            log.error("staking this rebate is transfered:orgId->{} productId->{} rebateId->{},count->{}"
                    , orgId, productId, productRebateId, transferCount);
            throw new BrokerException(BrokerErrorCode.DB_RECORD_ERROR);
        }

        stakingProductRebate.setStatus(StakingProductRebateStatus.STPD_REBATE_STATUS_INVALID_VALUE);
        stakingProductRebate.setUpdatedAt(System.currentTimeMillis());

        stakingProductRebateMapper.updateByPrimaryKeySelective(stakingProductRebate);

        Example detailExample = new Example(StakingProductRebateDetail.class);
        detailExample.createCriteria().andEqualTo("orgId", orgId).andEqualTo("productId", productId).andEqualTo("productRebateId", productRebateId);

        StakingProductRebateDetail updateDetail = new StakingProductRebateDetail();
        updateDetail.setStatus(StakingProductRebateDetailStatus.STPD_REBATE_DETAIL_STATUS_INVALID_VALUE);
        stakingProductRebateDetailMapper.updateByExampleSelective(updateDetail, detailExample);

        StakingProductRebate newRebate = new StakingProductRebate();
        newRebate.setOrgId(stakingProductRebate.getOrgId());
        newRebate.setProductId(stakingProductRebate.getProductId());
        newRebate.setProductType(stakingProductRebate.getProductType());
        newRebate.setRebateDate(stakingProductRebate.getRebateDate());
        newRebate.setType(stakingProductRebate.getType());
        newRebate.setRebateCalcWay(stakingProductRebate.getRebateCalcWay());
        newRebate.setNumberOfPeriods(stakingProductRebate.getNumberOfPeriods());
        newRebate.setStatus(StakingProductRebateStatus.STPD_REBATE_STATUS_WAITING_VALUE);
        newRebate.setCreatedAt(System.currentTimeMillis());
        newRebate.setUpdatedAt(System.currentTimeMillis());

        if (stakingProductRebate.getRebateCalcWay() == StakingProductRebateCalcWay.STPD_REBATE_CALCWAY_RATE_VALUE) {
            //年化利率
            if (new BigDecimal(rebateRate).compareTo(BigDecimal.ZERO) <= 0) {
                throw new BrokerException(BrokerErrorCode.PARAM_ERROR);
            }
            newRebate.setTokenId(stakingProductRebate.getTokenId());
            newRebate.setRebateRate(new BigDecimal(rebateRate));
            newRebate.setRebateAmount(BigDecimal.ZERO);

        } else {
            //固定金额
            if (new BigDecimal(rebateAmount).compareTo(BigDecimal.ZERO) <= 0 || StringUtils.isEmpty(tokenId)) {
                throw new BrokerException(BrokerErrorCode.PARAM_ERROR);
            }

            newRebate.setTokenId(tokenId);
            newRebate.setRebateRate(BigDecimal.ZERO);
            newRebate.setRebateAmount(new BigDecimal(rebateAmount));

        }

        stakingProductRebateMapper.insertSelective(newRebate);

        return newRebate;
    }

    private String getProductName(List<StakingProductLocal> localList, String language) {
        if (CollectionUtils.isEmpty(localList)) {
            return "";
        } else {
            String firstName = "";
            String languageName = "";
            String defaultName = "";
            for (StakingProductLocal local : localList) {
                if (firstName.equals("") && local.getStatus() == 1) {
                    firstName = local.getProductName();
                }

                if (local.getLanguage().equals(language) && local.getStatus() == 1) {
                    languageName = local.getProductName();
                    break;
                }
                if (local.getLanguage().equals(default_lang) && local.getStatus() == 1) {
                    defaultName = local.getProductName();
                }
            }
            return !languageName.equals("") ? languageName : (!defaultName.equals("") ? defaultName : firstName);
        }
    }

    private int getProductStaus(StakingProduct product) {
        int status = 0;
        long currentTimeMillis = System.currentTimeMillis();
        if (product.getSubscribeStartDate() > currentTimeMillis) {
            status = StakingProductStatus.STPD_STATUS_SUBSCRIBE_WAITING_VALUE;
        }
        if (product.getSubscribeStartDate() < currentTimeMillis) {
            status = StakingProductStatus.STPD_STATUS_SUBSCRIBING_VALUE;
        }

        if (product.getSubscribeEndDate() < currentTimeMillis) {
            status = StakingProductStatus.STPD_STATUS_SUBSCRIBE_END_VALUE;
        }

        if (product.getSoldLots().compareTo(product.getUpLimitLots()) != -1) {
            status = StakingProductStatus.STPD_STATUS_ALL_SOLD_OUT_VALUE;
        }

        if (product.getInterestStartDate() < currentTimeMillis) {
            status = StakingProductStatus.STPD_STATUS_INTERESTING_VALUE;
        }

        if (getProductEndDate(product.getInterestStartDate(), product.getTimeLimit()) < currentTimeMillis) {
            status = StakingProductStatus.STPD_STATUS_FINISHED_VALUE;
        }
        return status;
    }

    private long getProductEndDate(long interestStartDate, int timeLimit) {
        return interestStartDate + timeLimit * 86400_000L;
    }

    private Integer saveProductCheckParam(AdminSaveProductRequest request) {

        //检查本金账户是否是有效的资金账户
        if (request.getType() == StakingProductType.FI_TIME_VALUE || request.getType() == StakingProductType.FI_CURRENT_VALUE) {
            if (!accountService.checkValidFundAccountByAccountId(request.getOrgId(), request.getPrincipalAccountId())) {
                return 1;
            }
        }

        //检查派息账户是否是有效的资金账户
        if (!accountService.checkValidFundAccountByAccountId(request.getOrgId(), request.getDividendAccountId())) {
            return 2;
        }
        return 0;
    }

    //新增或更新产品
    private StakingProduct saveProdcut(AdminSaveProductRequest request, long currentTimeMillis) {
        StakingProduct stakingProduct = new StakingProduct();
        //新建理财产品

        stakingProduct.setOrgId(request.getOrgId());
        stakingProduct.setTokenId(request.getTokenId());
        stakingProduct.setDividendType(request.getDividendType());
        stakingProduct.setReferenceApr(request.getReferenceApr());
        stakingProduct.setActualApr(new BigDecimal(request.getActualApr()));
        stakingProduct.setWeeklyApr(BigDecimal.ZERO);
        stakingProduct.setPerUsrLowLots(request.getPerUsrLowLots());
        stakingProduct.setPerUsrUpLots(request.getPerUsrUpLots());
        stakingProduct.setUpLimitLots(request.getUpLimitLots());
        stakingProduct.setPerLotAmount(new BigDecimal(request.getPerLotAmount()));
        stakingProduct.setSubscribeStartDate(request.getSubscribeStartDate());
        stakingProduct.setSubscribeEndDate(request.getSubscribeEndDate());

        //如果是活期，则开始计息日期直接取开始申购日期
        if (request.getType() == StakingProductType.FI_CURRENT_VALUE) {
            stakingProduct.setShowUpLimitLots(request.getUpLimitLots());
            stakingProduct.setTimeLimit((int) TimeUnit.MILLISECONDS.toDays(request.getSubscribeEndDate() - request.getSubscribeStartDate()));
            stakingProduct.setInterestStartDate(request.getSubscribeStartDate());
            stakingProduct.setDividendTimes(stakingProduct.getTimeLimit());

        } else {
            stakingProduct.setShowUpLimitLots(request.getShowUpLimitLots());
            stakingProduct.setTimeLimit(request.getTimeLimit());
            stakingProduct.setInterestStartDate(request.getInterestStartDate());
            stakingProduct.setDividendTimes(request.getDividendTimes());

        }

        stakingProduct.setSort(request.getSort());
        stakingProduct.setType(request.getType());
        stakingProduct.setPrincipalAccountId(request.getPrincipalAccountId());
        stakingProduct.setDividendAccountId(request.getDividendAccountId());

        if (request.getType() == StakingProductType.LOCK_POSITION_VALUE) {
            stakingProduct.setFundFlow(StakingProductFundFlow.STPD_FLOW_LOCK_VALUE);
        } else {
            stakingProduct.setFundFlow(StakingProductFundFlow.STPD_FLOW_TRANSFER_VALUE);
        }

        stakingProduct.setArrposid(request.getArrposid());
        stakingProduct.setUpdatedAt(currentTimeMillis);

        if (request.getId() != 0) {
            stakingProduct.setId(request.getId());

            Example example = new Example(StakingProduct.class);
            example.createCriteria().andEqualTo("id", request.getId()).andEqualTo("orgId", stakingProduct.getOrgId());
            stakingProductMapper.updateByExampleSelective(stakingProduct, example);
        } else {
            stakingProduct.setId(sequenceGenerator.getLong());
            stakingProduct.setSoldLots(0);
            stakingProduct.setIsShow(StakingProductIsShow.STPD_SHOW_NO_VALUE);
            stakingProduct.setCreatedAt(currentTimeMillis);

            stakingProductMapper.insertSelective(stakingProduct);
        }

        return stakingProduct;
    }

    private void saveProductSubscribeLimit(AdminSaveProductRequest.ProductSubscribeLimitInfo limitInfo, long orgId, long productId, long currentTimeMillis) {

        Example example = new Example(StakingProductSubscribeLimit.class);
        example.createCriteria()
                .andEqualTo("orgId", orgId)
                .andEqualTo("productId", productId);
        StakingProductSubscribeLimit subscribeLimit = stakingProductSubscribeLimitMapper.selectOneByExample(example);
        if (subscribeLimit != null) {

            subscribeLimit.setVerifyKyc(limitInfo.getVerifyKyc() == 1 ? 1 : 0);
            subscribeLimit.setVerifyBindPhone(limitInfo.getVerifyBindPhone() == 1 ? 1 : 0);
            subscribeLimit.setVerifyBalance(limitInfo.getVerifyBalance() == 1 ? 1 : 0);
            subscribeLimit.setVerifyAvgBalance(limitInfo.getVerifyAvgBalance() == 1 ? 1 : 0);
            subscribeLimit.setBalanceRuleJson(limitInfo.getBalanceRuleJson());
            subscribeLimit.setLevelLimit(limitInfo.getLevelLimit());
            subscribeLimit.setUpdatedTime(currentTimeMillis);

            stakingProductSubscribeLimitMapper.updateByPrimaryKeySelective(subscribeLimit);
        } else {
            StakingProductSubscribeLimit limit = new StakingProductSubscribeLimit();
            limit.setOrgId(orgId);
            limit.setProductId(productId);
            limit.setVerifyKyc(limitInfo.getVerifyKyc() == 1 ? 1 : 0);
            limit.setVerifyBindPhone(limitInfo.getVerifyBindPhone() == 1 ? 1 : 0);
            limit.setVerifyBalance(limitInfo.getVerifyBalance() == 1 ? 1 : 0);
            limit.setVerifyAvgBalance(limitInfo.getVerifyAvgBalance() == 1 ? 1 : 0);
            limit.setBalanceRuleJson(limitInfo.getBalanceRuleJson());
            limit.setLevelLimit(limitInfo.getLevelLimit());
            limit.setUpdatedTime(currentTimeMillis);
            limit.setCreatedTime(currentTimeMillis);
            stakingProductSubscribeLimitMapper.insertSelective(limit);
        }
    }

    private void insertProductRebates(List<AdminSaveProductRequest.ProductRebateInfo> rebateInfos, StakingProduct stakingProduct, long currentTimeMillis) {

        rebateInfos = Lists.newArrayList(rebateInfos);
        rebateInfos.sort(Comparator.comparingLong(AdminSaveProductRequest.ProductRebateInfo::getRebateDate));

        StakingProductRebate lastRebate = null;
        int index = 1;
        //注：broker-admin-server 不会传入orgid,productid,tokenid,这里需要单独从产品信息中获取
        for (AdminSaveProductRequest.ProductRebateInfo rebateInfo : rebateInfos) {

            StakingProductRebate rebate = new StakingProductRebate();
            rebate.setOrgId(stakingProduct.getOrgId());
            rebate.setProductId(stakingProduct.getId());
            rebate.setProductType(stakingProduct.getType());
            rebate.setTokenId(StringUtils.isBlank(rebateInfo.getTokenId()) ? stakingProduct.getTokenId() : rebateInfo.getTokenId());
            rebate.setRebateDate(rebateInfo.getRebateDate());
            rebate.setRebateRate(new BigDecimal(rebateInfo.getRebateRate()));
            rebate.setRebateAmount(new BigDecimal(rebateInfo.getRebateAmount()));
            rebate.setRebateCalcWay(rebateInfo.getRebateCalcWay());
            rebate.setStatus(StakingProductRebateStatus.STPD_REBATE_STATUS_WAITING_VALUE);
            rebate.setCreatedAt(currentTimeMillis);
            rebate.setUpdatedAt(currentTimeMillis);
            rebate.setNumberOfPeriods(index);
            rebate.setType(StakingProductRebateType.STPD_REBATE_TYPE_INTEREST_VALUE);
            stakingProductRebateMapper.insertSelective(rebate);
            index++;

            lastRebate = rebate;

        }

        StakingProductRebate rebate = new StakingProductRebate();
        rebate.setOrgId(stakingProduct.getOrgId());
        rebate.setProductId(stakingProduct.getId());
        rebate.setProductType(stakingProduct.getType());
        rebate.setTokenId(stakingProduct.getTokenId());
        rebate.setRebateDate(lastRebate.getRebateDate());
        rebate.setRebateRate(BigDecimal.ONE);
        rebate.setRebateAmount(BigDecimal.ZERO);
        rebate.setRebateCalcWay(StakingProductRebateCalcWay.STPD_REBATE_CALCWAY_RATE_VALUE);
        rebate.setStatus(StakingProductRebateStatus.STPD_REBATE_STATUS_WAITING_VALUE);
        rebate.setCreatedAt(currentTimeMillis);
        rebate.setUpdatedAt(currentTimeMillis);
        rebate.setNumberOfPeriods(lastRebate.getNumberOfPeriods());
        rebate.setType(StakingProductRebateType.STPD_REBATE_TYPE_PRINCIPAL_VALUE);

        stakingProductRebateMapper.insertSelective(rebate);

    }

    //活期产品新增rebate记录
    private void insertCurrnetProductRebates(StakingProduct stakingProduct, long rebateDate, int index, long currentTimeMillis) {

        //  long rebateDate = stakingProduct.getSubscribeStartDate() + 86400_000L;
        List<StakingProductRebate> rebateList = new ArrayList<>();
        //  int index = 0;

        while (rebateDate <= stakingProduct.getSubscribeEndDate()) {
            index++;
            StakingProductRebate rebate = new StakingProductRebate();
            rebate.setOrgId(stakingProduct.getOrgId());
            rebate.setProductId(stakingProduct.getId());
            rebate.setProductType(stakingProduct.getType());
            rebate.setTokenId(stakingProduct.getTokenId());
            rebate.setRebateDate(rebateDate);
            rebate.setType(StakingProductRebateType.STPD_REBATE_TYPE_INTEREST_VALUE);
            rebate.setRebateCalcWay(StakingProductRebateCalcWay.STPD_REBATE_CALCWAY_RATE_VALUE);
            rebate.setRebateRate(stakingProduct.getActualApr());
            rebate.setRebateAmount(BigDecimal.ZERO);
            rebate.setNumberOfPeriods(index);
            rebate.setStatus(StakingProductRebateStatus.STPD_REBATE_STATUS_WAITING_VALUE);
            rebate.setCreatedAt(currentTimeMillis);
            rebate.setUpdatedAt(currentTimeMillis);

            rebateList.add(rebate);

            rebateDate += 86400_000L;
            if (rebateList.size() >= 365) {
                stakingProductRebateBatchMapper.insertList(rebateList);
                rebateList.clear();
            }
        }

        if (rebateList.size() > 0) {
            stakingProductRebateBatchMapper.insertList(rebateList);
            rebateList.clear();
        }

        StakingProductRebate rebate = new StakingProductRebate();
        rebate.setOrgId(stakingProduct.getOrgId());
        rebate.setProductId(stakingProduct.getId());
        rebate.setProductType(stakingProduct.getType());
        rebate.setTokenId(stakingProduct.getTokenId());
        rebate.setRebateDate(stakingProduct.getSubscribeEndDate());
        rebate.setRebateRate(BigDecimal.ONE);
        rebate.setRebateAmount(BigDecimal.ZERO);
        rebate.setRebateCalcWay(StakingProductRebateCalcWay.STPD_REBATE_CALCWAY_RATE_VALUE);
        rebate.setStatus(StakingProductRebateStatus.STPD_REBATE_STATUS_WAITING_VALUE);
        rebate.setCreatedAt(currentTimeMillis);
        rebate.setUpdatedAt(currentTimeMillis);
        rebate.setNumberOfPeriods(index);
        rebate.setType(StakingProductRebateType.STPD_REBATE_TYPE_PRINCIPAL_VALUE);

        stakingProductRebateMapper.insertSelective(rebate);

    }

    //活期产品更新rebate记录
    private void updateCurrnetProductRebates(StakingProduct newProduct, long subscribeStartDate, long subscribeEndtDate, BigDecimal actualApr, boolean isStart, long currentTimeMillis) {

        //开始申购、结束申购日期，利率都一样，不需要更新
        if (newProduct.getSubscribeStartDate().equals(subscribeStartDate)
                && newProduct.getSubscribeEndDate().equals(subscribeEndtDate)
                && newProduct.getActualApr().compareTo(actualApr) == 0) {
            return;
        }

        long conditionDate = currentTimeMillis + 1800_000L;
        //开始申购、结束申购日期不变，利率变化，只需要更新利率
        if (newProduct.getSubscribeStartDate().equals(subscribeStartDate)
                && newProduct.getSubscribeEndDate().equals(subscribeEndtDate)
                && newProduct.getActualApr().compareTo(actualApr) != 0) {
            stakingProductRebateMapper.updateCurrentRebate(newProduct.getOrgId(), newProduct.getId(), conditionDate, newProduct.getActualApr(), currentTimeMillis);
            return;
        }

        if (isStart) {
            //申购已开始，开始申购时间不能变化
            if (!newProduct.getSubscribeEndDate().equals(subscribeEndtDate)) {
                stakingProductRebateMapper.updateCurrentRebateStatusByCondition(newProduct.getOrgId(), newProduct.getId(), conditionDate, currentTimeMillis, StakingProductRebateStatus.STPD_REBATE_STATUS_INVALID_VALUE);

                StakingProductRebate lastInterestRebate = stakingProductRebateMapper.getLastInterestRebate(newProduct.getOrgId(), newProduct.getId());

                int index = lastInterestRebate.getNumberOfPeriods();

                insertCurrnetProductRebates(newProduct, lastInterestRebate.getRebateDate(), index, currentTimeMillis);
            }

        } else {
            //申购未开始，开始申购时间可以变化
            stakingProductRebateMapper.updateRebateStatusById(newProduct.getOrgId(), newProduct.getId(), StakingProductRebateStatus.STPD_REBATE_STATUS_INVALID_VALUE, currentTimeMillis);
            insertCurrnetProductRebates(newProduct, newProduct.getSubscribeStartDate() + 86400_000L, 0, currentTimeMillis);
        }
    }

    private void updateProductRebates(List<AdminSaveProductRequest.ProductRebateInfo> rebateInfos, StakingProduct stakingProduct,
                                      long currentTimeMillis) {
        long conditionRebateDate = currentTimeMillis + 1800_000;

        //注：broker-admin-server 不会传入orgid,productid,这里需要单独从产品信息中获取
        rebateInfos.forEach(rebateInfo -> {
                    stakingProductRebateMapper.updateRebate(rebateInfo.getId(),
                            stakingProduct.getOrgId(),
                            stakingProduct.getId(),
                            conditionRebateDate,
                            rebateInfo.getRebateDate(),
                            rebateInfo.getTokenId(),
                            new BigDecimal(rebateInfo.getRebateRate()),
                            new BigDecimal(rebateInfo.getRebateAmount()),
                            currentTimeMillis);
                }
        );

        Example example = new Example(StakingProductRebate.class);
        example.createCriteria().andEqualTo("orgId", stakingProduct.getOrgId())
                .andEqualTo("productId", stakingProduct.getId())
                .andNotEqualTo("status", StakingProductRebateStatus.STPD_REBATE_STATUS_INVALID_VALUE)
                .andEqualTo("type", StakingProductRebateType.STPD_REBATE_TYPE_INTEREST_VALUE);

        List<StakingProductRebate> rebates = stakingProductRebateMapper.selectByExample(example);
        rebates.sort(Comparator.comparingLong(StakingProductRebate::getRebateDate));

        int index = 1;

        for (StakingProductRebate rebate : rebates) {
            rebate.setNumberOfPeriods(index);
            stakingProductRebateMapper.updateByPrimaryKey(rebate);
            index++;
        }

    }

    private void saveProductLocals(List<AdminSaveProductRequest.ProductLocalInfo> localInfos, StakingProduct
            stakingProduct, long currentTimeMillis) {

        Example localeExample = new Example(StakingProductLocal.class);
        localeExample.createCriteria()
                .andEqualTo("orgId", stakingProduct.getOrgId())
                .andEqualTo("productId", stakingProduct.getId());
        List<StakingProductLocal> stakingProductLocals = stakingProductLocalMapper.selectByExample(localeExample);
        Map<String, StakingProductLocal> productLocalMap = stakingProductLocals.stream().collect(Collectors.toMap(StakingProductLocal::getLanguage, p -> p));

        localInfos.forEach(
                localInfo -> {
                    if (productLocalMap.containsKey(localInfo.getLanguage())) {
                        StakingProductLocal local = productLocalMap.get(localInfo.getLanguage());
                        local.setProductName(localInfo.getProductName());
                        local.setProtocolUrl(localInfo.getProtocolUrl());
                        local.setDetails(localInfo.getProductDetails());
                        local.setBackgroundUrl(localInfo.getBackgroundUrl());
                        local.setStatus(1);
                        local.setUpdatedAt(currentTimeMillis);

                        stakingProductLocalMapper.updateByPrimaryKeySelective(local);
                        productLocalMap.remove(localInfo.getLanguage());
                    } else {
                        StakingProductLocal local = new StakingProductLocal();
                        local.setOrgId(stakingProduct.getOrgId());
                        local.setProductId(stakingProduct.getId());
                        local.setProductName(localInfo.getProductName());
                        local.setLanguage(localInfo.getLanguage());
                        local.setDetails(localInfo.getProductDetails());
                        local.setProtocolUrl(localInfo.getProtocolUrl());
                        local.setBackgroundUrl(localInfo.getBackgroundUrl());
                        local.setStatus(1);
                        local.setCreatedAt(currentTimeMillis);
                        local.setUpdatedAt(currentTimeMillis);
                        stakingProductLocalMapper.insertSelective(local);
                    }
                }
        );

        for (StakingProductLocal local : productLocalMap.values()) {
            local.setUpdatedAt(currentTimeMillis);
            local.setStatus(0);
            stakingProductLocalMapper.updateByPrimaryKeySelective(local);
        }
    }

    /**
     * 根据产品id集合获取产品map
     *
     * @param productIds
     * @return
     */
    private Map<Long, StakingProduct> getProductMapByIds(Collection<Long> productIds) {

        Example productExample = new Example(StakingProduct.class);
        productExample.createCriteria().andIn("id", productIds);
        List<StakingProduct> stakingProducts = stakingProductMapper.selectByExample(productExample);
        Map<Long, StakingProduct> productMap = stakingProducts.stream().collect(Collectors.toMap(StakingProduct::getId, p -> p));
        return productMap;
    }

    /**
     * 根据产品id集合获取产品多语言配置列表map
     *
     * @param productIds
     * @return
     */
    private Map<Long, List<StakingProductLocal>> getProductLocalMapByIds(Collection<Long> productIds) {
        Example productLocalExample = new Example(StakingProductLocal.class);
        productLocalExample.createCriteria().andIn("productId", productIds);
        List<StakingProductLocal> stakingProductLocals = stakingProductLocalMapper.selectByExample(productLocalExample);
        Map<Long, List<StakingProductLocal>> localMap = stakingProductLocals.stream().collect(Collectors.groupingBy(StakingProductLocal::getProductId));
        return localMap;
    }

}
