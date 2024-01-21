package io.bhex.broker.server.grpc.server.service;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.gson.Gson;

import org.apache.commons.lang3.StringUtils;
import org.apache.ibatis.session.RowBounds;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Resource;

import io.bhex.base.account.BalanceChangeRequest;
import io.bhex.base.account.BalanceChangeResponse;
import io.bhex.base.account.BalanceDetail;
import io.bhex.base.account.BusinessType;
import io.bhex.base.account.GetBalanceDetailRequest;
import io.bhex.base.account.GetOtcAvailableRequest;
import io.bhex.base.account.GetOtcAvailableResponse;
import io.bhex.base.constants.ProtoConstants;
import io.bhex.base.idgen.api.ISequenceGenerator;
import io.bhex.base.proto.DecimalUtil;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.grpc.bwlist.UserBlackWhiteType;
import io.bhex.broker.grpc.common.Header;
import io.bhex.broker.grpc.user.level.QueryMyLevelConfigResponse;
import io.bhex.broker.server.domain.BaseConfigConstants;
import io.bhex.broker.server.domain.FrozenType;
import io.bhex.broker.server.domain.SwitchStatus;
import io.bhex.broker.server.grpc.client.service.GrpcBalanceService;
import io.bhex.broker.server.grpc.client.service.GrpcOtcService;
import io.bhex.broker.server.model.Account;
import io.bhex.broker.server.model.OtcItem;
import io.bhex.broker.server.model.OtcLegalCurrency;
import io.bhex.broker.server.model.OtcOrder;
import io.bhex.broker.server.model.OtcWhiteList;
import io.bhex.broker.server.model.User;
import io.bhex.broker.server.model.UserVerify;
import io.bhex.broker.server.primary.mapper.AccountMapper;
import io.bhex.broker.server.primary.mapper.OtcItemMapper;
import io.bhex.broker.server.primary.mapper.OtcOrderMapper;
import io.bhex.broker.server.primary.mapper.OtcWhiteListMapper;
import io.bhex.broker.server.primary.mapper.UserMapper;
import io.bhex.broker.server.primary.mapper.UserVerifyMapper;
import io.bhex.broker.server.util.BaseReqUtil;
import io.bhex.broker.server.util.BigDecimalUtil;
import io.bhex.broker.server.util.OtcBaseReqUtil;
import io.bhex.ex.otc.BatchUpdatePaymentVisibleRequest;
import io.bhex.ex.otc.GetTradeFeeRateByTokenIdRequest;
import io.bhex.ex.otc.GetTradeFeeRateByTokenIdResponse;
import io.bhex.ex.otc.OTCDeleteItemRequest;
import io.bhex.ex.otc.OTCDeleteOrderRequest;
import io.bhex.ex.otc.OTCGetItemIdRequest;
import io.bhex.ex.otc.OTCGetItemIdResponse;
import io.bhex.ex.otc.OTCGetItemInfoRequest;
import io.bhex.ex.otc.OTCGetItemInfoResponse;
import io.bhex.ex.otc.OTCGetOrderIdRequest;
import io.bhex.ex.otc.OTCGetOrderIdResponse;
import io.bhex.ex.otc.OTCItemDetail;
import io.bhex.ex.otc.OTCItemStatusEnum;
import io.bhex.ex.otc.OTCNewItemRequest;
import io.bhex.ex.otc.OTCNewItemResponse;
import io.bhex.ex.otc.OTCNewOrderRequest;
import io.bhex.ex.otc.OTCNewOrderResponse;
import io.bhex.ex.otc.OTCNewPaymentRequest;
import io.bhex.ex.otc.OTCNormalItemRequest;
import io.bhex.ex.otc.OTCNormalOrderRequest;
import io.bhex.ex.otc.OTCOrderStatusEnum;
import io.bhex.ex.otc.OTCPaymentInfo;
import io.bhex.ex.otc.OTCPaymentTypeEnum;
import io.bhex.ex.otc.OTCResult;
import io.bhex.ex.otc.OTCTradeFeeRate;
import io.bhex.ex.otc.QueryOtcPaymentTermListRequest;
import io.bhex.ex.otc.QueryOtcPaymentTermListResponse;
import io.bhex.ex.proto.BaseRequest;
import io.bhex.ex.proto.Decimal;
import io.bhex.ex.proto.OrderSideEnum;
import lombok.extern.slf4j.Slf4j;
import tk.mybatis.mapper.entity.Example;

/**
 * @author lizhen
 * @date 2018-09-18
 */
@Slf4j
@Service
public class OtcService {

    public static final int PRECISION = ProtoConstants.PRECISION;
    public static final int ROUNDMODE = ProtoConstants.ROUNDMODE;

    private static final Map<Integer, String> ZH_CN_TEMPLATE = Maps.newHashMap();

    private static final Map<Integer, String> EN_US_TEMPLATE = Maps.newHashMap();


    static {
        ZH_CN_TEMPLATE.put(1010, "您已成功下单，请15分钟内向{0}支付{4}{5}。");
        ZH_CN_TEMPLATE.put(1011, "{3}购买{1}{2}，请等待付款。");
        ZH_CN_TEMPLATE.put(1021, "{3}已标记付款{4}{5}。请及时查收并尽快向{3}放行。注意，请务必登录您的账号确认收款后再操作，"
                + "否则有可能造成您财产的损失。");
        ZH_CN_TEMPLATE.put(1030, "尊敬的用户，您的投诉BHEX客服已经受理，为了避免纠纷，请您登录BHEX网站在线解决，有问题可以联系客服 "
                + "https://bhex.zendesk.com/hc/zh-cn/requests/new，感谢您的配合。");
        ZH_CN_TEMPLATE.put(1031, "尊敬的用户，您已遭到买方{3}投诉，订单将交由BHEX客服介入处理，为了避免纠纷，请您登录BHEX网站"
                + "在线解决，有问题可以联系客服 https://bhex.zendesk.com/hc/zh-cn/requests/new ，感谢您的配合。");
        ZH_CN_TEMPLATE.put(1040, "购买{1}{2}订单已经取消，请核对您的资产，不要造成经济损失。");
        ZH_CN_TEMPLATE.put(1041, "{3}已经取消{1}{2}订单，请核对您的资产，不要造成经济损失。");
        ZH_CN_TEMPLATE.put(1050, "{0}已确认收到您的{4}{5}付款，您所购买的{1}{2}已经发送到您的账户，请查收。");
        ZH_CN_TEMPLATE.put(1051, "您已点击放行，对方将收到您出售的{1}{2}。");
        ZH_CN_TEMPLATE.put(2010, "您的广告已有卖家下单，请15分钟内向{0}支付{4}{5}。");
        ZH_CN_TEMPLATE.put(2011, "您已成功下单{1}{2}，请等待{3}付款。");
        ZH_CN_TEMPLATE.put(2030, "尊敬的用户，您已遭到卖方{0}投诉，订单将交由BHEX客服介入处理，为了避免纠纷，请您登录BHEX网站"
                + "在线解决，有问题可以联系客服 https://bhex.zendesk.com/hc/zh-cn/requests/new ，感谢您的配合。");
        ZH_CN_TEMPLATE.put(2031, "尊敬的用户，您的投诉BHEX客服已经受理，为了避免纠纷，请您登录BHEX网站在线解决，有问题可以联系客服 "
                + "https://bhex.zendesk.com/hc/zh-cn/requests/new ，感谢您的配合。");
        ZH_CN_TEMPLATE.put(3040, "尊敬的用户，因为订单未在规定期限内完结，订单由系统自动取消，请核对您的资产，不要造成经济损失。");
        ZH_CN_TEMPLATE.put(3030, "尊敬的用户，因为订单未在规定期限内完结，为了避免纠纷，请您登录BHEX网站上传付款成功的证明 "
                + "https://bhex.zendesk.com/hc/zh-cn/requests/new ，感谢您的配合。");
        ZH_CN_TEMPLATE.put(3031, "尊敬的用户，因为订单未在规定期限内完结，为了避免纠纷，请您登录BHEX网站上传未收到买家付款的证明 "
                + "https://bhex.zendesk.com/hc/zh-cn/requests/new ，感谢您的配合。");
        ZH_CN_TEMPLATE.put(5001, "尊敬的用户，您当前广告资产余额小于订单最低限额，广告已被系统下架，请您及时发布新广告。");


        EN_US_TEMPLATE.put(1010, "You have placed an order ,Please pay {4}{5} to {0} within 15min.");
        EN_US_TEMPLATE.put(1011, "{3} is purchasing {1}{2}, Please wait for payment.");
        EN_US_TEMPLATE.put(1021, "{3} has marked {4}{5} as \"Confirm Payment\"。 Please confirm you have received the "
                + "payment and release to {3} in time. Remark: To avoid loss of assets, Please check your account to "
                + "ensure the payment is received. And do make sure the name of payee is same as orderer.");
        EN_US_TEMPLATE.put(1030, "Dear customer, your complaint is under review. In order to avoid dispute, "
                + "Please login to BHEX website to settle or contact customer service for clarification. Thanks for your "
                + "cooperation! https://bhex.zendesk.com/hc/zh-cn/requests/new");
        EN_US_TEMPLATE.put(1031, "Dear customer, You have been complained by buyer {3}. The order will be handled by "
                + "BHEX customer service. In order to avoid dispute, Please login to BHEX website to settle or contact "
                + "customer service for clarification. Thanks for your cooperation! https://bhex.zendesk"
                + ".com/hc/zh-cn/requests/new");
        EN_US_TEMPLATE.put(1040, "The order of {1}{2} has been cancelled, Please check your asset "
                + "to avoid any losses.");
        EN_US_TEMPLATE.put(1041, "{3} has cancelled the order of {1}{2}, Please check your asset "
                + "to avoid any losses.");
        EN_US_TEMPLATE.put(1050, "{0} has received {4}{5} payment. {1}{2} has been transferred to your "
                + "account. Please check your account balance.");
        EN_US_TEMPLATE.put(1051, "You have clicked \"Release\". The order will receive {1}{2}.");
        EN_US_TEMPLATE.put(2010, "Your have a new order, Please pay {4}{5} to {0} within 15 min.");
        EN_US_TEMPLATE.put(2011, "You have ordered {1}{2}, Please wait for payment from {3}.");
        EN_US_TEMPLATE.put(2030, "Dear customer, You have been complained by seller {0}. The order will be handled by "
                + "BHEX customer service. In order to avoid dispute, Please login to BHEX website to settle or contact "
                + "customer service for clarification. Thanks for your cooperation! https://bhex.zendesk"
                + ".com/hc/zh-cn/requests/new");
        EN_US_TEMPLATE.put(2031, "Dear customer, your complaint is under review. In order to avoid dispute, "
                + "Please login to BHEX website to settle or contact customer service for clarification. Thanks for your "
                + "cooperation! https://bhex.zendesk.com/hc/zh-cn/requests/new");
        EN_US_TEMPLATE.put(3040, "Dear customer, the order has been automatically cancelled due to exceed of "
                + "completion time. Please check your account balance to avoid any losses.");
        EN_US_TEMPLATE.put(3030, "Dear customer, the order has not been completed in time. In order to avoid any "
                + "dispute, Please login to BHEX website to settle or upload the proof of successful payment to "
                + "https://bhex.zendesk.com/hc/zh-cn/requests/newplease. For any questions, please contact our customer "
                + "service. Thanks for your cooperation!");
        EN_US_TEMPLATE.put(3031, "Dear customer, the order has not been completed in time. In order to avoid any "
                + "dispute, Please login to BHEX website to settle or upload the proof of unreceived payment to "
                + "https://bhex.zendesk.com/hc/zh-cn/requests/newplease. For any questions, please contact our customer "
                + "service. Thanks for your cooperation!");
        EN_US_TEMPLATE.put(5001, "Dear user, your advertisement balance is lower than the order minimum transaction amount. Your advertisement has been delisted by system. Please re-publish again.");
    }

    @Autowired
    private GrpcBalanceService grpcBalanceService;

    @Autowired
    private GrpcOtcService grpcOtcService;

    @Autowired
    private AccountService accountService;

    @Resource
    private OtcItemMapper otcItemMapper;

    @Resource
    private OtcOrderMapper otcOrderMapper;

    @Resource
    private ISequenceGenerator sequenceGenerator;

    @Resource
    private OtcLegalCurrencyService otcLegalCurrencyService;

    @Resource
    private OtcWhiteListMapper otcWhiteListMapper;

    @Resource
    private UserBlackWhiteListConfigService userBlackWhiteListConfigService;

    @Resource
    private UserService userService;
    @Resource
    private BaseBizConfigService baseBizConfigService;
    @Resource
    private UserLevelService userLevelService;
    @Resource
    private RiskControlBalanceService riskControlBalanceService;

    @Scheduled(cron = "0 0/2 * * * ?")
    public void handleFailedItems() {
        try {
            Example example = Example.builder(OtcItem.class).build();
            example.createCriteria()
                    .andEqualTo("status", OTCItemStatusEnum.OTC_ITEM_INIT_VALUE)
                    .andLessThan("createDate", new Date(System.currentTimeMillis() - 1000 * 60 * 2));
            List<OtcItem> itemList = otcItemMapper.selectByExampleAndRowBounds(example, new RowBounds(0, 100));
            if (CollectionUtils.isEmpty(itemList)) {
                return;
            }
            itemList.forEach(this::handleFailedItem);
        } catch (Exception e) {
            log.error("handle failed items error", e);
        }
    }

    @Scheduled(cron = "0 0/2 * * * ?")
    public void handleFailedOrders() {
        try {
            Example example = Example.builder(OtcOrder.class).build();
            example.createCriteria()
                    .andEqualTo("status", OTCOrderStatusEnum.OTC_ORDER_INIT_VALUE)
                    .andLessThan("createDate", new Date(System.currentTimeMillis() - 1000 * 60 * 2));
            List<OtcOrder> orderList = otcOrderMapper.selectByExampleAndRowBounds(example, new RowBounds(0, 100));
            if (CollectionUtils.isEmpty(orderList)) {
                return;
            }
            orderList.forEach(this::handleFailedOrder);
        } catch (Exception e) {
            log.error("handle failed orders error", e);
        }
    }

    public OTCNewItemResponse createItem(OTCNewItemRequest request) {
        OTCNewItemResponse.Builder response = OTCNewItemResponse.newBuilder();

        if (userBlackWhiteListConfigService.inWithdrawWhiteBlackList(request.getOrgId(),
                request.getTokenId(), request.getBaseRequest().getUserId(), UserBlackWhiteType.BLACK_CONFIG)) {
            return response.setResult(OTCResult.IN_WITHDRAW_BLACK_LIST).build();
        }

        User user = userService.getUser(request.getBaseRequest().getUserId());
        if (user.getBindPassword() == 0) {
            throw new BrokerException(BrokerErrorCode.NEED_BIND_PASSWORD);
        }

        //风控禁止otc广告
//        UserBizPermission userRiskControlConfig = this.riskControlService.query(request.getOrgId(), request.getBaseRequest().getUserId());
//        if (userRiskControlConfig != null && request.getSide() == OrderSideEnum.SELL && userRiskControlConfig.getOtcTradeSell() == 1) {
//            return response.setResult(OTCResult.RISK_CONTROL_INTERCEPTION_LIMIT).build();
//        }

//        if (userRiskControlConfig != null && request.getSide() == OrderSideEnum.BUY && userRiskControlConfig.getOtcTradeBuy() == 1) {
//            return response.setResult(OTCResult.RISK_CONTROL_INTERCEPTION_LIMIT).build();
//        }

        //判断是不是当前法币
        if (StringUtils.isNotEmpty(request.getCurrencyId())) {
            OtcLegalCurrency otcLegalCurrency
                    = this.otcLegalCurrencyService.queryUserLegalCurrencyInfo(request.getOrgId(),
                    request.getBaseRequest().getUserId());
            if (otcLegalCurrency == null) {
                //未找到对应的法币配置
                return response.setResult(OTCResult.NO_CURRENCY_CONFIGURATION_FOUND).build();
            }

            if (otcLegalCurrency.getCode().equalsIgnoreCase("JPY")) {
                return response.setResult(OTCResult.UNABLE_TO_DO_THIS).build();
            }

            //如果CN国籍法币CNY则不允许发其他法币广告 其他国籍放开限制
            if (otcLegalCurrency.getCode().equalsIgnoreCase("CNY")) {
                if (!otcLegalCurrency.getCode().equalsIgnoreCase(request.getCurrencyId())) {
                    return response.setResult(OTCResult.UNABLE_TO_DO_THIS).build();
                }
            } else {
                //非CN国籍也不能发CNY广告
                if (request.getCurrencyId().equalsIgnoreCase("CNY")) {
                    return response.setResult(OTCResult.UNABLE_TO_DO_THIS).build();
                }
            }
        } else {
            return response.setResult(OTCResult.NO_CURRENCY_CONFIGURATION_FOUND).build();
        }
        //卖单风控拦截
        if (request.getSide() == OrderSideEnum.SELL) {
            try {
                //校验是否被风控
                riskControlBalanceService.checkRiskControlLimit(request.getOrgId(), request.getBaseRequest().getUserId(), FrozenType.FROZEN_OTC_TRADE);
            } catch (BrokerException brokerException) {
                return response.setResult(OTCResult.forNumber(brokerException.code())).build();
            }
        }
        //如果是卖单需要计算手续费 + 到amount里面
        BigDecimal makerFee = BigDecimal.ZERO;
        if (request.getSide() == OrderSideEnum.SELL) {
            makerFee = handlingMakerSellFee(request.getTokenId(), request.getOrgId(), toBigDecimal(request.getQuantity()));
        }

        request = request.toBuilder()
                .setAccountId(accountService.getAccountId(request.getBaseRequest().getOrgId(), request.getBaseRequest().getUserId()))
                .build();
        BigDecimal balance = queryBalance(request.getAccountId(), request.getTokenId(), request.getOrgId());
        // 卖单检查余额
        if (request.getSide() == OrderSideEnum.SELL
                && toBigDecimal(request.getQuantity(), BigDecimal.ZERO).add(makerFee).compareTo(balance) > 0) {
            log.warn("Create item,insufficient balance,tokenId={},accountId={},quantity={},makefee={}",
                    request.getTokenId(), request.getAccountId(), request.getQuantity(), makerFee.stripTrailingZeros().toPlainString());
            return response.setResult(OTCResult.BALANCE_NOT_ENOUGH).build();
        }

        if (request.getSide() == OrderSideEnum.SELL) {
            //获取OTC可用余额 跟挂单金额比对 如果 < 则返回BALANCE_NOT_ENOUGH
            GetOtcAvailableRequest getOtcAvailableRequest = GetOtcAvailableRequest.newBuilder()
                    .setAccountId(request.getAccountId())
                    .setOrgId(request.getOrgId())
                    .setTokenId(request.getTokenId())
                    .build();
            GetOtcAvailableResponse getOtcAvailableResponse
                    = grpcBalanceService.getOtcAvailable(getOtcAvailableRequest);

            if (new BigDecimal(getOtcAvailableResponse.getAvailable()).compareTo(toBigDecimal(request.getQuantity()).add(makerFee)) < 0) {
                log.warn("Create item,insufficient balance,tokenId={},accountId={},quantity={},available={},makerFee={}",
                        request.getTokenId(), request.getAccountId(), request.getQuantity(), getOtcAvailableResponse.getAvailable(), makerFee.stripTrailingZeros().toPlainString());
                return response.setResult(OTCResult.BALANCE_NOT_ENOUGH).build();
            }
        }

        Date now = new Date();
        OtcItem otcItem = OtcItem.builder()
                .id(sequenceGenerator.getLong())
                .exchangeId(request.getBaseRequest().getExchangeId())
                .orgId(request.getOrgId())
                .userId(request.getBaseRequest().getUserId())
                .accountId(request.getAccountId())
                .tokenId(request.getTokenId())
                .currencyId(request.getCurrencyId())
                .side(request.getSideValue())
                .priceType(request.getPriceTypeValue())
                .price(toBigDecimal(request.getPrice(), BigDecimal.ZERO))
                .premium(toBigDecimal(request.getPrice(), BigDecimal.ZERO))
                .quantity(toBigDecimal(request.getQuantity()).add(makerFee))
                .minAmount(toBigDecimal(request.getMinAmount()))
                .maxAmount(toBigDecimal(request.getMaxAmount()))
                .remark(request.getRemark())
                .status(OTCItemStatusEnum.OTC_ITEM_INIT_VALUE)
                .recommendLevel(0)
                .paymentPeriod(60 * 15)
                .onlyHighAuth(0)
                .autoReply("")
                .createDate(now)
                .updateDate(now)
                .frozenFee(makerFee)
                .build();
        otcItemMapper.insert(otcItem);
        if (otcItem.getId() == null) {
            log.error("broker insert item failed");
            return response.setResult(OTCResult.SYS_ERROR).build();
        }

        String mobile = Strings.nullToEmpty(user.getMobile());
        String nationalCode = Strings.nullToEmpty(user.getNationalCode());
        String email = Strings.nullToEmpty(user.getEmail());

        OTCNewItemRequest.Builder builder = request.toBuilder()
                .setClientItemId(otcItem.getId())
                .setFrozenFee(fromBigDecimal(makerFee))
                .setMobile(nationalCode + " " + mobile)
                .setEmail(email);

        //校验参数精度
        BigDecimalUtil.checkParamScale(request.getPrice() != null ? request.getPrice().getStr() : "",
                request.getQuantity() != null ? request.getQuantity().getStr() : "",
                request.getMinAmount() != null ? request.getMinAmount().getStr() : "",
                request.getMaxAmount() != null ? request.getMaxAmount().getStr() : "",
                makerFee != null ? makerFee.setScale(PRECISION, ROUNDMODE).toPlainString() : "");

        OTCNewItemResponse itemResponse = grpcOtcService.createItem(builder.build());
        // exchange创建广告单失败，直接返回失败原因
        if (itemResponse.getResult() != OTCResult.SUCCESS) {
            log.warn("exchange create item failed reason: {}", itemResponse.getResult());
            otcItemMapper.updateOtcItemStatus(otcItem.getId(), OTCItemStatusEnum.OTC_ITEM_DELETE_VALUE,
                    OTCItemStatusEnum.OTC_ITEM_INIT_VALUE, now);
            return itemResponse;
        }
        otcItem.setItemId(itemResponse.getItemId());
        return normalItem(otcItem);
    }

    private OTCItemDetail findOtcItemFromPlatform(Long itemId, Long orgId) {

        OTCGetItemInfoResponse resp = grpcOtcService.getItemInfo(
                OTCGetItemInfoRequest.newBuilder()
                        .setBaseRequest(OtcBaseReqUtil.getBaseRequest(orgId))
                        .setItemId(itemId)
                        .build());

        if (resp.getResult() == OTCResult.SUCCESS && resp.getItem().getItemId() > 0L) {
            return resp.getItem();
        }

        return null;
    }


    public OTCNewOrderResponse createOrder(OTCNewOrderRequest request) {
        long orgId = request.getBaseRequest().getOrgId();
        if (request.getBaseRequest().getUserId() == 842145604923681280L) { //1.禁止这个用户一切资金操作 7070的账号
            throw new BrokerException(BrokerErrorCode.FEATURE_SUSPENDED);
        }

        OTCNewOrderResponse.Builder response = OTCNewOrderResponse.newBuilder();
        SwitchStatus switchStatus = baseBizConfigService.getConfigSwitchStatus(orgId,
                BaseConfigConstants.WHOLE_SITE_CONTROL_SWITCH_GROUP, BaseConfigConstants.STOP_OTC_TRADE_KEY);
        if (switchStatus.isOpen()) {
            log.info("org:{} otc order closed", orgId);
            return response.setResult(OTCResult.FEATURE_SUSPENDED).build();
        }

        //风控禁止otc交易
//        UserBizPermission userRiskControlConfig = this.riskControlService.query(request.getOrgId(), request.getBaseRequest().getUserId());
//        if (userRiskControlConfig != null && request.getSide() == OrderSideEnum.SELL && userRiskControlConfig.getOtcTradeSell() == 1) {
//            return response.setResult(OTCResult.RISK_CONTROL_INTERCEPTION_LIMIT).build();
//        }
//        if (userRiskControlConfig != null && request.getSide() == OrderSideEnum.BUY && userRiskControlConfig.getOtcTradeBuy() == 1) {
//            return response.setResult(OTCResult.RISK_CONTROL_INTERCEPTION_LIMIT).build();
//        }

        switchStatus = baseBizConfigService.getConfigSwitchStatus(orgId,
                BaseConfigConstants.FROZEN_USER_OTC_TRADE_GROUP, request.getBaseRequest().getUserId() + "");
        if (switchStatus.isOpen()) {
            log.info("org:{} user:{} otc order frozen", orgId, request.getBaseRequest().getUserId());
            return response.setResult(OTCResult.ORDER_FROZEN_BY_ADMIN).build();
        }

        if (request.getSide() == OrderSideEnum.SELL &&
                userBlackWhiteListConfigService.inWithdrawWhiteBlackList(request.getOrgId(),
                        request.getTokenId(), request.getBaseRequest().getUserId(), UserBlackWhiteType.BLACK_CONFIG)) { //禁止出金
            return response.setResult(OTCResult.IN_WITHDRAW_BLACK_LIST).build();
        }

        User user = userService.getUser(request.getBaseRequest().getUserId());
        if (user.getBindPassword() == 0) {
            throw new BrokerException(BrokerErrorCode.NEED_BIND_PASSWORD);
        }

        Example exp = new Example(OtcItem.class);
        exp.selectProperties("itemId", "orgId", "exchangeId", "userId", "accountId", "currencyId", "tokenId", "side");
        exp.createCriteria().andEqualTo("itemId", request.getItemId());

        String currencyId = null;
        OtcItem otcItem = otcItemMapper.selectOneByExample(exp);
        if (Objects.isNull(otcItem)) {
            //共享广告从otc平台查询
            OTCItemDetail detail = this.findOtcItemFromPlatform(request.getItemId(), orgId);
            if (detail.getItemId() == 0L) {
                throw new BrokerException(BrokerErrorCode.OTC_ITEM_NOT_EXIST);
            }
            currencyId = detail.getCurrencyId();

        } else {
            currencyId = otcItem.getCurrencyId();
        }

        //判断是不是当前法币
        if (StringUtils.isNotEmpty(currencyId)) {
            OtcLegalCurrency otcLegalCurrency
                    = this.otcLegalCurrencyService.queryUserLegalCurrencyInfo(request.getOrgId(), request.getBaseRequest().getUserId());
            if (otcLegalCurrency == null) {
                //未找到对应的法币配置
                return response.setResult(OTCResult.NO_CURRENCY_CONFIGURATION_FOUND).build();
            }

            if (otcLegalCurrency.getCode().equalsIgnoreCase("JPY")) {
                return response.setResult(OTCResult.UNABLE_TO_DO_THIS).build();
            }

            log.info("otcLegalCurrency currencyId {} userCurrencyId {}", currencyId, otcLegalCurrency.getCode());
            if ("CNY".equalsIgnoreCase(currencyId)) {
                if (!otcLegalCurrency.getCode().equalsIgnoreCase(currencyId)) {
                    return response.setResult(OTCResult.UNABLE_TO_DO_THIS).build();
                }
            }
        } else {
            return response.setResult(OTCResult.NO_CURRENCY_CONFIGURATION_FOUND).build();
        }
        //卖单风控拦截
        if (request.getSide() == OrderSideEnum.SELL) {
            try {
                //校验是否被风控
                riskControlBalanceService.checkRiskControlLimit(request.getOrgId(), request.getBaseRequest().getUserId(), FrozenType.FROZEN_OTC_TRADE);
            } catch (BrokerException brokerException) {
                return response.setResult(OTCResult.forNumber(brokerException.code())).build();
            }
        }
        //判断当前用户是不是商家
        OtcWhiteList otcWhiteList
                = this.otcWhiteListMapper.queryByUserId(request.getOrgId(), request.getBaseRequest().getUserId());
        request = request.toBuilder()
                .setIsBusiness(Objects.nonNull(otcWhiteList) ? true : false)
                .setAccountId(accountService.getAccountId(request.getBaseRequest().getOrgId(), request.getBaseRequest().getUserId()))
                .build();
        BigDecimal balance = queryBalance(request.getAccountId(), request.getTokenId(), orgId);
        // 卖单检查余额
        if (request.getSide() == OrderSideEnum.SELL
                && toBigDecimal(request.getQuantity(), BigDecimal.ZERO).compareTo(balance) > 0) {

            log.warn("Create order,insufficient balance,tokenId={},accountId={},quantity={}",
                    request.getTokenId(), request.getAccountId(), request.getQuantity());
            return response.setResult(OTCResult.BALANCE_NOT_ENOUGH).build();
        }

        //获取OTC可用余额 跟挂单金额比对 如果 < 则返回BALANCE_NOT_ENOUGH
        if (request.getSide() == OrderSideEnum.SELL) {
            GetOtcAvailableRequest getOtcAvailableRequest = GetOtcAvailableRequest.newBuilder()
                    .setAccountId(request.getAccountId())
                    .setOrgId(request.getOrgId())
                    .setTokenId(request.getTokenId())
                    .build();

            GetOtcAvailableResponse getOtcAvailableResponse
                    = grpcBalanceService.getOtcAvailable(getOtcAvailableRequest);

            if (new BigDecimal(getOtcAvailableResponse.getAvailable()).compareTo(toBigDecimal(request.getQuantity())) < 0) {
                log.warn("Create order,insufficient balance,tokenId={},accountId={},quantity={},available={}",
                        request.getTokenId(), request.getAccountId(), request.getQuantity(), getOtcAvailableResponse.getAvailable());
                return response.setResult(OTCResult.BALANCE_NOT_ENOUGH).build();
            }
        }

        Date now = new Date();
        OtcOrder otcOrder = OtcOrder.builder()
                .id(sequenceGenerator.getLong())
                .exchangeId(request.getBaseRequest().getExchangeId())
                .orgId(request.getOrgId())
                .accountId(request.getAccountId())
                .itemId(request.getItemId())
                .tokenId(request.getTokenId())
                .side(request.getSideValue())
                .price(toBigDecimal(request.getPrice(), BigDecimal.ZERO))
                .quantity(toBigDecimal(request.getQuantity()))
                .status(OTCOrderStatusEnum.OTC_ORDER_INIT_VALUE)
                .createDate(now)
                .updateDate(now)
                .build();
        otcOrderMapper.insert(otcOrder);
        if (otcOrder.getId() == null) {
            log.error("broker insert order failed");
            return response.setResult(OTCResult.SYS_ERROR).build();
        }

        String mobile = Strings.nullToEmpty(user.getMobile());
        String nationalCode = Strings.nullToEmpty(user.getNationalCode());
        String email = Strings.nullToEmpty(user.getEmail());

        OTCNewOrderRequest.Builder builder = request.toBuilder()
                .setClientOrderId(otcOrder.getId())
                .setMobile(nationalCode + " " + mobile)
                .setEmail(email);

        //校验价格数量精度
        BigDecimalUtil.checkParamScale(request.getPrice() != null ? request.getPrice().getStr() : "",
                request.getQuantity() != null ? request.getQuantity().getStr() : "",
                request.getAmount() != null ? request.getAmount().getStr() : "");

        QueryMyLevelConfigResponse myVipLevel = userLevelService.queryMyLevelConfig(orgId, user.getUserId(), false, false);
        if (myVipLevel != null && myVipLevel.getCancelOtc24HWithdrawLimit()) {
            builder.setRiskBalanceType(OTCNewOrderRequest.RiskBalanceType.NOT_RISK_BALANCE);
        }
        UserVerify userVerify = this.userService.queryUserVerifyInfoByAccountId(request.getAccountId());
        log.info("createOrder userId {} userVerify {} ", request.getBaseRequest().getUserId(), new Gson().toJson(userVerify));
        builder.setUserFirstName(userVerify != null && StringUtils.isNotEmpty(userVerify.getFirstName()) ? userVerify.getFirstName() : "");
        builder.setUserSecondName(userVerify != null && StringUtils.isNotEmpty(userVerify.getSecondName()) ? userVerify.getSecondName() : "");
        OTCNewOrderResponse orderResponse = grpcOtcService.createOrder(builder.build());
        // exchange创建订单失败，直接返回失败原因
        if (orderResponse.getResult() != OTCResult.SUCCESS) {
            log.warn("exchange create order failed reason: {}", orderResponse.getResult());
            otcItemMapper.updateOtcItemStatus(otcOrder.getId(), OTCOrderStatusEnum.OTC_ORDER_DELETE_VALUE,
                    OTCOrderStatusEnum.OTC_ORDER_INIT_VALUE, new Date());
            return orderResponse;
        }
        otcOrder.setOrderId(orderResponse.getOrderId());
        return normalOrder(otcOrder);
    }

    private void handleFailedItem(OtcItem otcItem) {
        try {
            if (otcItem.getItemId() == null || otcItem.getItemId() <= 0) {
                OTCGetItemIdResponse response = grpcOtcService.getItemIdByClientId(OTCGetItemIdRequest.newBuilder()
                        .setOrgId(otcItem.getOrgId())
                        .setClientItemId(otcItem.getId())
                        .build());
                if (response.getResult() == OTCResult.ITEM_NOT_EXIST) {
                    otcItemMapper.updateOtcItemStatus(otcItem.getId(), OTCItemStatusEnum.OTC_ITEM_DELETE_VALUE,
                            OTCItemStatusEnum.OTC_ITEM_INIT_VALUE, new Date());
                    return;
                }
                otcItem.setItemId(response.getItemId());
            }
            normalItem(otcItem);
        } catch (Exception e) {
            log.error("handle failed item error, id={}", otcItem.getId(), e);
        }
    }

    private void handleFailedOrder(OtcOrder otcOrder) {
        try {
            if (otcOrder.getOrderId() == null || otcOrder.getOrderId() <= 0) {
                OTCGetOrderIdResponse response = grpcOtcService.getOrderIdByClientId(OTCGetOrderIdRequest.newBuilder()
                        .setOrgId(otcOrder.getOrgId())
                        .setClientOrderId(otcOrder.getId())
                        .build());
                if (response.getResult() == OTCResult.ORDER_NOT_EXIST) {
                    otcOrderMapper.updateOtcOrderStatus(otcOrder.getId(), OTCOrderStatusEnum.OTC_ORDER_DELETE_VALUE,
                            OTCOrderStatusEnum.OTC_ORDER_INIT_VALUE, new Date());
                    return;
                }
                otcOrder.setOrderId(response.getOrderId());
            }
            normalOrder(otcOrder);
        } catch (Exception e) {
            log.error("handle failed order error, id={}", otcOrder.getId(), e);
        }
    }

    private OTCNewItemResponse normalItem(OtcItem otcItem) {
        OTCNewItemResponse.Builder response = OTCNewItemResponse.newBuilder();
        // 回填exchange返回的主键id
        OtcItem updater = OtcItem.builder()
                .id(otcItem.getId())
                .itemId(otcItem.getItemId())
                .build();
        otcItemMapper.updateByPrimaryKeySelective(updater);
        // 卖单需要冻结资产
        if (otcItem.getSide() == OrderSideEnum.SELL_VALUE) {
            BalanceChangeRequest balanceChangeRequest = BalanceChangeRequest.newBuilder()
                    .setAccountId(otcItem.getAccountId())
                    .setBusinessId(otcItem.getItemId())
                    .setBusinessType(BusinessType.OTC_ITEM_FROZEN)
                    .setTokenId(otcItem.getTokenId())
                    .setAmount(DecimalUtil.fromBigDecimal(otcItem.getQuantity()))
                    .setOrgId(otcItem.getOrgId() != null ? otcItem.getOrgId() : 0)
                    .build();
            log.info("item changeBalance accountId {} businessId {} tokenId {} amount {} orgId {}",
                    otcItem.getAccountId(), otcItem.getItemId(), otcItem.getTokenId(), otcItem.getQuantity(), otcItem.getOrgId());

            BalanceChangeResponse changeResponse = grpcBalanceService.changeBalance(balanceChangeRequest);
            // 冻结失败，可能余额不足
            if (changeResponse.getChangeId() <= 0) {
                log.warn("bh frozen balance failed itemId: {}", otcItem.getItemId());
                grpcOtcService.setItemToDelete(OTCDeleteItemRequest.newBuilder()
                        .setBaseRequest(OtcBaseReqUtil.getBaseRequest(otcItem.getOrgId()))
                        .setItemId(otcItem.getItemId())
                        .build());
                otcItemMapper.updateOtcItemStatus(otcItem.getId(), OTCItemStatusEnum.OTC_ITEM_DELETE_VALUE,
                        OTCItemStatusEnum.OTC_ITEM_INIT_VALUE, new Date());
                return response.setResult(OTCResult.BALANCE_NOT_ENOUGH).build();
            }
        }
        // 改exchange中item状态为正常
        grpcOtcService.setItemToNormal(OTCNormalItemRequest.newBuilder()
                .setBaseRequest(OtcBaseReqUtil.getBaseRequest(otcItem.getOrgId()))
                .setItemId(otcItem.getItemId()).build());
        otcItemMapper.updateOtcItemStatus(otcItem.getId(), OTCItemStatusEnum.OTC_ITEM_NORMAL_VALUE,
                OTCItemStatusEnum.OTC_ITEM_INIT_VALUE, new Date());
        return response.setResult(OTCResult.SUCCESS).setItemId(otcItem.getItemId()).build();
    }

    private BigDecimal handlingMakerSellFee(String tokenId, Long orgId, BigDecimal amount) {
        GetTradeFeeRateByTokenIdRequest getTradeFeeRateByTokenIdRequest = GetTradeFeeRateByTokenIdRequest
                .newBuilder()
                .setOrgId(orgId)
                .setTokenId(tokenId)
                .build();
        GetTradeFeeRateByTokenIdResponse tradeFeeRateByTokenIdResponse
                = grpcOtcService.getTradeFeeRateByTokenId(getTradeFeeRateByTokenIdRequest);

        if (tradeFeeRateByTokenIdResponse != null) {
            OTCTradeFeeRate otcTradeFeeRate = tradeFeeRateByTokenIdResponse.getRate();
            if (otcTradeFeeRate != null) {
                if (otcTradeFeeRate.getMakerSellFeeRate() != null) {
                    return amount.multiply(toBigDecimal(otcTradeFeeRate.getMakerSellFeeRate()));
                }
            } else {
                return BigDecimal.ZERO;
            }
        }
        return BigDecimal.ZERO;
    }


    private OTCNewOrderResponse normalOrder(OtcOrder otcOrder) {
        OTCNewOrderResponse.Builder response = OTCNewOrderResponse.newBuilder();
        // 回填exchange返回的主键id
        OtcOrder updater = OtcOrder.builder()
                .id(otcOrder.getId())
                .orderId(otcOrder.getOrderId())
                .build();
        otcOrderMapper.updateByPrimaryKeySelective(updater);
        // 卖单需要冻结资产
        if (otcOrder.getSide() == OrderSideEnum.SELL_VALUE) {
            BalanceChangeRequest balanceChangeRequest = BalanceChangeRequest.newBuilder()
                    .setAccountId(otcOrder.getAccountId())
                    .setBusinessId(otcOrder.getOrderId())
                    .setBusinessType(BusinessType.OTC_ORDER_FROZEN)
                    .setTokenId(otcOrder.getTokenId())
                    .setAmount(DecimalUtil.fromBigDecimal(otcOrder.getQuantity()))
                    .setOrgId(otcOrder.getOrgId() != null ? otcOrder.getOrgId() : 0)
                    .build();

            log.info("order changeBalance accountId {} businessId {} tokenId {} amount {} orgId {}",
                    otcOrder.getAccountId(), otcOrder.getOrderId(), otcOrder.getTokenId(), otcOrder.getQuantity(), otcOrder.getOrgId());
            BalanceChangeResponse changeResponse = grpcBalanceService.changeBalance(balanceChangeRequest);
            // 冻结失败，可能余额不足
            if (changeResponse.getChangeId() <= 0) {
                log.warn("bh frozen balance failed orderId: {}", otcOrder.getOrderId());
                grpcOtcService.setOrderToDelete(OTCDeleteOrderRequest.newBuilder()
                        .setBaseRequest(OtcBaseReqUtil.getBaseRequest(otcOrder.getOrgId()))
                        .setOrderId(otcOrder.getOrderId())
                        .build());
                otcOrderMapper.updateOtcOrderStatus(otcOrder.getId(), OTCOrderStatusEnum.OTC_ORDER_DELETE_VALUE,
                        OTCOrderStatusEnum.OTC_ORDER_INIT_VALUE, new Date());
                return response.setResult(OTCResult.BALANCE_NOT_ENOUGH).build();
            }
        }
        // 改exchange中order状态为正常
        grpcOtcService.setOrderToNormal(OTCNormalOrderRequest.newBuilder()
                .setOrderId(otcOrder.getOrderId())
                .build());
        otcOrderMapper.updateOtcOrderStatus(otcOrder.getId(), OTCOrderStatusEnum.OTC_ORDER_NORMAL_VALUE,
                OTCOrderStatusEnum.OTC_ORDER_INIT_VALUE, new Date());
        return response.setResult(OTCResult.SUCCESS).setOrderId(otcOrder.getOrderId()).build();
    }

    private BigDecimal queryBalance(Long accountId, String tokenId, Long orgId) {
        if (StringUtils.isBlank(tokenId)) {
            return BigDecimal.ZERO;
        }
        GetBalanceDetailRequest request = GetBalanceDetailRequest.newBuilder()
                .setBaseRequest(BaseReqUtil.getBaseRequest(orgId))
                .setAccountId(accountId)
                .addTokenId(tokenId)
                .build();
        List<BalanceDetail> balanceDetails = grpcBalanceService.getBalanceDetail(request).getBalanceDetailsList();
        if (CollectionUtils.isEmpty(balanceDetails)) {
            return BigDecimal.ZERO;
        }
        return DecimalUtil.toBigDecimal(balanceDetails.get(0).getAvailable());
    }

    private BigDecimal toBigDecimal(Decimal decimalValue) {
        if (null != decimalValue.getStr() && !"".equals(decimalValue.getStr().trim())) {
            return new BigDecimal(decimalValue.getStr()).setScale(PRECISION, ROUNDMODE);
        }
        return BigDecimal.valueOf(decimalValue.getUnscaledValue(),
                decimalValue.getScale()).setScale(PRECISION, ROUNDMODE);
    }

    private BigDecimal toBigDecimal(Decimal decimalValue, BigDecimal defaultValue) {
        try {
            return toBigDecimal(decimalValue);
        } catch (Exception e) {
            return defaultValue;
        }
    }

    private Decimal fromBigDecimal(BigDecimal bigDecimalValue) {
        return Decimal.newBuilder()
                .setStr(bigDecimalValue.toPlainString())
                .setScale(bigDecimalValue.scale())
                .build();
    }

    public void importPayment(Header header, Integer paymentType, String realName, String bankName, String branchName,
                              String accountNo, String qrcode) {

        if (paymentType.equals(OTCPaymentTypeEnum.OTC_PAYMENT_BANK_VALUE)) {
            if (StringUtils.isBlank(bankName)) {
                throw new BrokerException(BrokerErrorCode.PARAM_INVALID);
            }
            if (StringUtils.isNoneBlank(branchName) && branchName.length() > 255) {
                throw new BrokerException(BrokerErrorCode.PARAM_INVALID);
            }
        } else {
            if (StringUtils.isBlank(qrcode)) {
                throw new BrokerException(BrokerErrorCode.PARAM_INVALID);
            }
        }

        if (StringUtils.isBlank(accountNo)) {
            throw new BrokerException(BrokerErrorCode.PARAM_INVALID);
        }

        OTCNewPaymentRequest.Builder builder = OTCNewPaymentRequest.newBuilder()
                .setBaseRequest(BaseRequest.newBuilder()
                        .setLanguage(header.getLanguage())
                        .setUserId(header.getUserId())
                        .setOrgId(header.getOrgId())
                        .build())
                .setPaymentTypeValue(paymentType)
                .setRealName(realName)
                .setAccountNo(accountNo);

        if (StringUtils.isNotBlank(bankName)) {
            builder.setBankName(bankName);
        }
        if (StringUtils.isNotBlank(branchName)) {
            builder.setBranchName(branchName);
        }
        if (StringUtils.isNotBlank(qrcode)) {
            builder.setQrcode(qrcode);
        }
        grpcOtcService.addPaymentTerm(builder.build());

    }

    @Resource
    private GrpcOtcService getGrpcOtcService;
    @Resource
    private UserMapper userMapper;
    @Resource
    private AccountMapper accountMapper;

    @Resource
    private UserVerifyMapper userVerifyMapper;

    public void cleanOtcPaymentTerm() {
        long startTime = System.currentTimeMillis();
        int page = 0;
        int size = 1000;
        int allSize = 0;
        while (true) {
            List<Long> idList = new ArrayList<>();
            QueryOtcPaymentTermListResponse response
                    = this.getGrpcOtcService.queryOtcPaymentTermList(QueryOtcPaymentTermListRequest.newBuilder().setPage(page).setSize(size).build());
            if (response.getPaymentInfoCount() == 0) {
                log.info("cleanOtcPaymentTerm list is null");
                break;
            }
            for (int i = 0; i < response.getPaymentInfoList().size(); i++) {
                allSize++;
                try {
                    OTCPaymentInfo s = response.getPaymentInfoList().get(i);
                    log.info("OTCPaymentInfo info id {} accountId {} type {} realName {} ", s.getId(), s.getAccountId(), s.getPaymentType(), s.getRealName());
                    Account account = null;
                    try {
                        account = this.accountMapper.getAccountByAccountId(s.getAccountId());
                    } catch (Exception ex) {

                    }
                    if (account != null && account.getUserId() > 0) {
                        OtcWhiteList otcWhiteList
                                = this.otcWhiteListMapper.queryByUserId(account.getOrgId(), account.getUserId());
                        if (otcWhiteList != null) {
                            log.info("Is a merchant cleanOtcPaymentTerm orgId {} userId {}", account.getOrgId(), account.getUserId());
                            continue;
                        }
                        UserVerify userVerify = UserVerify.decrypt(userVerifyMapper.getByUserId(account.getUserId()));
                        if (userVerify == null) {
                            log.info("cleanOtcPaymentTerm userVerify is null userId {} ", account.getUserId());
                            continue;
                        }

                        String realName = "";
                        if (userVerify != null && StringUtils.isNoneBlank(userVerify.getFirstName())) {
                            realName = userVerify.getFirstName();
                        }
                        if (userVerify != null && StringUtils.isNoneBlank(userVerify.getSecondName())) {
                            realName = realName + userVerify.getSecondName();
                        }

                        String realName_ = "";
                        if (userVerify != null && StringUtils.isNoneBlank(userVerify.getFirstName())) {
                            realName_ = userVerify.getFirstName();
                        }
                        if (userVerify != null && StringUtils.isNoneBlank(userVerify.getSecondName())) {
                            realName_ = realName_ + "_" + userVerify.getSecondName();
                        }
                        if (StringUtils.isBlank(s.getRealName())) {
                            idList.add(s.getId());
                        } else {
                            if (StringUtils.deleteWhitespace(s.getRealName()).equalsIgnoreCase(StringUtils.deleteWhitespace(realName_))) {
                                continue;
                            }
                            if (StringUtils.deleteWhitespace(s.getRealName()).equalsIgnoreCase(StringUtils.deleteWhitespace(realName))) {
                                continue;
                            }
                            idList.add(s.getId());
                            log.info("different name !!! userId {} payRealName {} kycName {}", account.getUserId(), s.getRealName(), realName);
                        }
                    }
                } catch (Exception ex) {
                    log.info("cleanOtcPaymentTerm error {}", ex);
                }
            }
            if (org.apache.commons.collections4.CollectionUtils.isNotEmpty(idList)) {
                Set<Long> idSetList = new HashSet<>(idList);
                Joiner joiner = Joiner.on(',');
                joiner.skipNulls();
                String idString = joiner.join(idSetList);
                log.info("cleanOtcPaymentTerm IDString {}", idString);
                if (StringUtils.isNotEmpty(idString)) {
                    //根据ID批量更新
                    this.getGrpcOtcService.batchUpdatePaymentVisible(BatchUpdatePaymentVisibleRequest.newBuilder().setIds(idString).build());
                }
            }
            page++;
        }
        long endTime = System.currentTimeMillis();
        log.info("cleanOtcPaymentTerm use time {} allSize {} ", (endTime - startTime), allSize);
    }
}
