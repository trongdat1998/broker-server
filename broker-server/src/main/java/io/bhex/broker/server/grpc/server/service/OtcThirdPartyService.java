package io.bhex.broker.server.grpc.server.service;

import com.google.protobuf.TextFormat;
import io.bhex.base.account.LockBalanceReply;
import io.bhex.base.account.UnlockBalanceResponse;
import io.bhex.base.constants.ProtoConstants;
import io.bhex.base.idgen.api.ISequenceGenerator;
import io.bhex.base.proto.DecimalUtil;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.grpc.account.Balance;
import io.bhex.broker.grpc.basic.Token;
import io.bhex.broker.grpc.common.Header;
import io.bhex.broker.grpc.deposit.GetDepositAddressResponse;
import io.bhex.broker.grpc.otc.third.party.*;
import io.bhex.broker.grpc.user.GetUserInfoResponse;
import io.bhex.broker.server.domain.OtcThirdPartyControlType;
import io.bhex.broker.server.domain.OtcThirdPartyId;
import io.bhex.broker.server.domain.OtcThirdPartyOrderStatus;
import io.bhex.broker.server.domain.OtcThirdPartyStatus;
import io.bhex.broker.server.grpc.server.service.po.*;
import io.bhex.broker.server.model.OtcThirdParty;
import io.bhex.broker.server.model.OtcThirdPartyDisclaimer;
import io.bhex.broker.server.model.OtcThirdPartyOrder;
import io.bhex.broker.server.model.OtcThirdPartyPayment;
import io.bhex.broker.server.model.OtcThirdPartySymbol;
import io.bhex.broker.server.model.*;
import io.bhex.broker.server.primary.mapper.*;
import io.bhex.broker.server.util.RedisLockUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.session.RowBounds;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import tk.mybatis.mapper.entity.Example;
import tk.mybatis.mapper.util.StringUtil;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * @author cookie.yuan
 * @description
 * @date 2020-08-13
 */
@Slf4j
@Service
public class OtcThirdPartyService {
    @Resource
    private OtcThirdPartyMapper otcThirdPartyMapper;

    @Resource
    private OtcThirdPartyChainMapper otcThirdPartyChainMapper;

    @Resource
    private OtcThirdPartyPaymentMapper otcThirdPartyPaymentMapper;

    @Resource
    private OtcThirdPartySymbolMapper otcThirdPartySymbolMapper;

    @Resource
    private OtcThirdPartyOrderMapper otcThirdPartyOrderMapper;

    @Resource
    private OtcThirdPartyDisclaimerMapper otcThirdPartyDisclaimerMapper;

    @Resource
    private ISequenceGenerator iSequenceGenerator;

    @Resource
    AccountService accountService;

    @Resource
    UserService userService;

    @Resource
    BalanceService balanceService;

    @Resource
    DepositService depositService;

    @Resource
    BasicService basicService;

    @Resource
    BanxaService banxaService;

    @Resource
    MoonpayService moonpayService;

    @Resource(name = "stringRedisTemplate")
    private StringRedisTemplate redisTemplate;

    // key: orgId+thirdPartyId+side
    private static Map<String, OtcThirdParty> otcThirdPartyMap = new ConcurrentHashMap<>();
    // key: orgId+otcSymbolId+side
    private static Map<String, OtcThirdPartySymbol> otcThirdPartySymbolMap = new ConcurrentHashMap<>();
    // key: orgId+tokenId
    private static Map<String, String> otcChainMap = new ConcurrentHashMap<>();

    private static Map<Long, MoonpayPayment> moonpayPaymentMap = new ConcurrentHashMap<>();

    @PostConstruct
    public void initMoonpayPayment() {
        MoonpayPayment cardPayment = new MoonpayPayment();
        cardPayment.setId(102001L);
        cardPayment.setType("credit_debit_card");
        cardPayment.setName("Visa/Mastercard");
        moonpayPaymentMap.put(cardPayment.getId(), cardPayment);

        MoonpayPayment applePayment = new MoonpayPayment();
        applePayment.setId(102002L);
        applePayment.setType("apple_pay");
        applePayment.setName("Apple Pay");
        moonpayPaymentMap.put(applePayment.getId(), applePayment);

        MoonpayPayment googlePayment = new MoonpayPayment();
        googlePayment.setId(102003L);
        googlePayment.setType("google_pay");
        googlePayment.setName("Google Pay");
        moonpayPaymentMap.put(googlePayment.getId(), googlePayment);
    }

    @PostConstruct
    @Scheduled(fixedDelay = 60_000)
    public void loadOtcThirdPartyChain() {
        Map<String, String> chainMap = new ConcurrentHashMap<>();
        List<OtcThirdPartyChain> chainList = otcThirdPartyChainMapper.queryAllRecord(OtcThirdPartyStatus.ENABLE.getStatus());
        for (OtcThirdPartyChain chain : chainList) {
            chainMap.put(chain.getOrgId() + "_" + chain.getTokenId(), chain.getChainType());
        }
        otcChainMap = chainMap;
    }

    public String getChainType(Long orgId, String tokenId) {
        // 根据orgId和tokenId查询入金的链类型
        String chainType = otcChainMap.get(orgId.toString() + "_" + tokenId);
        if (chainType != null) {
            return chainType;
        }
        // 未查找到值，则查询通用的链类型
        chainType = otcChainMap.get("0_" + tokenId);
        if (chainType != null) {
            return chainType;
        }
        return "";
    }

    @PostConstruct
    @Scheduled(fixedDelay = 60_000)
    public void loadLocalOtcThirdParty() {
        List<OtcThirdParty> thirdPartyList = otcThirdPartyMapper.selectAll();
        thirdPartyList.forEach(thirdParty -> otcThirdPartyMap.put(thirdParty.getOrgId().toString() +
                thirdParty.getThirdPartyId().toString() + thirdParty.getSide().toString(), thirdParty));
    }

    public OtcThirdParty getLocalOtcThirdParty(OtcThirdPartySymbol symbol) {
        Long orgId = symbol.getOrgId();
        Long thirdPartyId = symbol.getThirdPartyId();
        Integer side = symbol.getSide();
        OtcThirdParty thirdParty = otcThirdPartyMap.get(orgId.toString() + thirdPartyId.toString() + side.toString());
        if (thirdParty == null) {
            thirdParty = otcThirdPartyMapper.getByThirdPartyId(orgId, thirdPartyId, side);
        }
        return thirdParty;
    }

    @PostConstruct
    @Scheduled(fixedDelay = 60_000)
    public void loadLocalOtcThirdPartySymbol() {
        List<OtcThirdPartySymbol> symbolList = otcThirdPartySymbolMapper.selectAll();
        symbolList.forEach(symbol -> otcThirdPartySymbolMap.put(symbol.getOrgId().toString() + symbol.getOtcSymbolId().toString()
                + symbol.getSide().toString(), symbol));
    }

    public OtcThirdPartySymbol getLocalOtcThirdPartySymbol(Long orgId, Long otcSymbolId, Integer side) {
        String key = orgId.toString() + otcSymbolId.toString() + side.toString();
        OtcThirdPartySymbol symbol = otcThirdPartySymbolMap.get(key);
        if (symbol == null) {
            symbol = otcThirdPartySymbolMapper.getBySymbolId(orgId, otcSymbolId, side);
        }
        return symbol;
    }

    @Scheduled(initialDelay = 10_000, fixedDelay = 60 * 60_000)
    public void syncOtcThirdPartyPayment() {
        List<OtcThirdParty> thirdPartyList = otcThirdPartyMapper.selectAll();
        // 如果第三方处于未启用状态，则不查询更新其币对信息
        thirdPartyList.stream().filter(o -> o.getStatus().equals(OtcThirdPartyStatus.ENABLE.getStatus()))
                .forEach(thirdParty -> {
                    if (thirdParty.getThirdPartyId().equals(OtcThirdPartyId.BANXA.getId())) {
                        //当前第三方机构是banxa，则查询banxa当前开通的支持买入币种和法币等信息
                        banxaQueryPayment(thirdParty);
                    } else if (thirdParty.getThirdPartyId().equals(OtcThirdPartyId.MOONPAY.getId())) {
                        moonpayQueryPayment(thirdParty);
                    }
                });
    }

    public void moonpayQueryPayment(OtcThirdParty thirdParty) {
        Long timeStamp = System.currentTimeMillis();
        List<MoonpayCurrency> result = moonpayService.queryCurrencies(thirdParty);
        if (result == null) {
            return;
        }
        // 数字币币种
        List<MoonpayCurrency> coins = new ArrayList<>();
        // 法币币种
        List<MoonpayCurrency> fiats = new ArrayList<>();
        result.forEach(moonpayCurrency -> {
            if (moonpayCurrency.getType().equalsIgnoreCase("crypto")) {
                coins.add(moonpayCurrency);
            } else if (moonpayCurrency.getType().equalsIgnoreCase("fiat")) {
                fiats.add(moonpayCurrency);
            }
        });

        for (Long paymentId : moonpayPaymentMap.keySet()) {
            MoonpayPayment payment = moonpayPaymentMap.get(paymentId);
            if (payment == null) {
                continue;
            }
            for (MoonpayCurrency fiat : fiats) {
                for (MoonpayCurrency coin : coins) {
                    String coinCode = coin.getCode().toUpperCase();
                    String fiatCode = fiat.getCode().toUpperCase();
                    BigDecimal minAmount = fiat.getMinAmount();
                    BigDecimal maxAmount = fiat.getMaxAmount();
                    OtcThirdPartyPayment paymentItem = otcThirdPartyPaymentMapper.getByPaymentId(thirdParty.getThirdPartyId(),
                            payment.getId(), coinCode, fiatCode);
                    //更新 payment信息
                    if (paymentItem == null) {
                        paymentItem = new OtcThirdPartyPayment();
                        paymentItem.setThirdPartyId(thirdParty.getThirdPartyId());
                        paymentItem.setTokenId(coinCode);
                        paymentItem.setCurrencyId(fiatCode);
                        paymentItem.setPaymentId(payment.getId());
                        paymentItem.setPaymentType(payment.getType());
                        paymentItem.setPaymentName(payment.getName());
                        paymentItem.setStatus(1);
                        if (minAmount != null) {
                            paymentItem.setMinAmount(minAmount);
                        }
                        if (maxAmount != null) {
                            paymentItem.setMaxAmount(maxAmount);
                        }
                        paymentItem.setCreated(timeStamp);
                        paymentItem.setUpdated(timeStamp);
                        otcThirdPartyPaymentMapper.insertSelective(paymentItem);
                    } else {
                        // 最大最小值发生变化时更新信息
                        if ((minAmount != null && paymentItem.getMinAmount().compareTo(minAmount) != 0) ||
                                (maxAmount != null && paymentItem.getMaxAmount().compareTo(maxAmount) != 0)) {
                            OtcThirdPartyPayment updatePayment = new OtcThirdPartyPayment();
                            updatePayment.setId(paymentItem.getId());
                            updatePayment.setMaxAmount(minAmount);
                            updatePayment.setMaxAmount(maxAmount);
                            updatePayment.setUpdated(timeStamp);
                            otcThirdPartyPaymentMapper.updateByPrimaryKeySelective(paymentItem);
                        }
                    }

                    // 更新otc_symbol信息
                    OtcThirdPartySymbol symbol = otcThirdPartySymbolMapper.getByTokenAndPayment(thirdParty.getOrgId(),
                            thirdParty.getThirdPartyId(), coinCode, fiatCode, payment.getId(), 0);
                    if (symbol != null) {
                        if ((minAmount != null && symbol.getMinAmount().compareTo(minAmount) != 0) ||
                                (maxAmount != null && symbol.getMaxAmount().compareTo(maxAmount) != 0)) {
                            OtcThirdPartySymbol updateSymbol = new OtcThirdPartySymbol();
                            updateSymbol.setId(symbol.getId());
                            updateSymbol.setMinAmount(paymentItem.getMinAmount());
                            updateSymbol.setMaxAmount(paymentItem.getMaxAmount());
                            updateSymbol.setUpdated(timeStamp);
                            otcThirdPartySymbolMapper.updateByPrimaryKeySelective(updateSymbol);
                        }
                    }


                    // moonpay更新最小最小卖币数量
                    BigDecimal sellMinAmount = coin.getMinAmount();
                    BigDecimal sellMaxAmount = coin.getMaxAmount();
                    OtcThirdPartySymbol sellSymbol = otcThirdPartySymbolMapper.getByTokenAndPayment(thirdParty.getOrgId(),
                            thirdParty.getThirdPartyId(), coinCode, fiatCode, payment.getId(), 1);
                    if (sellSymbol != null) {
                        if ((sellMinAmount != null && sellSymbol.getMinAmount().compareTo(sellMinAmount) != 0) ||
                                (sellMaxAmount != null && sellSymbol.getMaxAmount().compareTo(sellMaxAmount) != 0)) {
                            OtcThirdPartySymbol updateSymbol = new OtcThirdPartySymbol();
                            updateSymbol.setId(sellSymbol.getId());
                            updateSymbol.setMinAmount(sellMinAmount);
                            updateSymbol.setMaxAmount(sellMaxAmount);
                            updateSymbol.setUpdated(timeStamp);
                            otcThirdPartySymbolMapper.updateByPrimaryKeySelective(updateSymbol);
                        }
                    }
                }
            }
        }
    }

    public void banxaQueryPayment(OtcThirdParty thirdParty) {
        Long timeStamp = System.currentTimeMillis();
        BanxaPaymentResult banxaPaymentResult = banxaService.queryPayment(thirdParty);
        if (banxaPaymentResult == null || banxaPaymentResult.getData() == null || banxaPaymentResult.getData().getPayment_methods() == null) {
            return;
        }
        banxaPaymentResult.getData().getPayment_methods().forEach(payment -> {
            payment.getTransaction_fees().forEach(fee -> {
                // 解析费用
                BigDecimal paymentFee = null;
                if (fee.getFees() != null && fee.getFees().size() > 0) {
                    paymentFee = new BigDecimal(fee.getFees().get(0).getAmount());
                }
                // 解析最大最小金额
                BigDecimal minAmount = null;
                BigDecimal maxAmount = null;
                if (payment.getTransaction_limits() != null) {
                    for (BanxaTransactionLimits limit : payment.getTransaction_limits()) {
                        if (limit.getFiat_code().equalsIgnoreCase(fee.getFiat_code())) {
                            minAmount = new BigDecimal(limit.getMin());
                            maxAmount = new BigDecimal(limit.getMax());
                            break;
                        }
                    }
                }
                OtcThirdPartyPayment paymentItem = otcThirdPartyPaymentMapper.getByPaymentId(thirdParty.getThirdPartyId(),
                        payment.getId(), fee.getCoin_code(), fee.getFiat_code());
                //更新 payment信息
                if (paymentItem == null) {
                    paymentItem = new OtcThirdPartyPayment();
                    paymentItem.setThirdPartyId(thirdParty.getThirdPartyId());
                    paymentItem.setPaymentId(payment.getId());
                    paymentItem.setTokenId(fee.getCoin_code());
                    paymentItem.setCurrencyId(fee.getFiat_code());
                    paymentItem.setPaymentType(payment.getPaymentType());
                    paymentItem.setPaymentName(payment.getName());
                    paymentItem.setStatus(1);
                    if (paymentFee != null) {
                        paymentItem.setFee(paymentFee);
                    }
                    if (minAmount != null) {
                        paymentItem.setMinAmount(minAmount);
                    }
                    if (maxAmount != null) {
                        paymentItem.setMaxAmount(maxAmount);
                    }
                    paymentItem.setCreated(timeStamp);
                    paymentItem.setUpdated(timeStamp);
                    otcThirdPartyPaymentMapper.insertSelective(paymentItem);
                } else {
                    // 已存在对应币对的支付方式，判断是否需要更新费用和订单最大最小金额值
                    if ((paymentFee != null && paymentFee.compareTo(paymentItem.getFee()) != 0) ||
                            (minAmount != null && minAmount.compareTo(paymentItem.getMinAmount()) != 0) ||
                            (maxAmount != null && maxAmount.compareTo(paymentItem.getMaxAmount()) != 0)) {
                        OtcThirdPartyPayment updatePayment = new OtcThirdPartyPayment();
                        updatePayment.setId(paymentItem.getId());
                        updatePayment.setFee(paymentFee);
                        updatePayment.setMinAmount(minAmount);
                        updatePayment.setMaxAmount(maxAmount);
                        updatePayment.setUpdated(timeStamp);
                        otcThirdPartyPaymentMapper.updateByPrimaryKeySelective(updatePayment);
                    }
                }

                // 更新otc_symbol信息
                OtcThirdPartySymbol symbol = otcThirdPartySymbolMapper.getByTokenAndPayment(thirdParty.getOrgId(),
                        thirdParty.getThirdPartyId(), fee.getCoin_code(), fee.getFiat_code(), payment.getId(), 0);
                if (symbol != null) {
                    // 当币对的最大最小值发生变化时才更新数据库
                    if (symbol.getMinAmount().compareTo(minAmount) != 0 ||
                            symbol.getMaxAmount().compareTo(maxAmount) != 0) {
                        OtcThirdPartySymbol updateSymbol = new OtcThirdPartySymbol();
                        updateSymbol.setId(symbol.getId());
                        updateSymbol.setMinAmount(minAmount);
                        updateSymbol.setMaxAmount(maxAmount);
                        updateSymbol.setUpdated(timeStamp);
                        otcThirdPartySymbolMapper.updateByPrimaryKeySelective(updateSymbol);
                    }
                }
            });
        });
    }

    @Scheduled(initialDelay = 10_000, fixedDelay = 2 * 60_000)
    public void syncOtcThirdPartyOrderStatusTask() {
        // 定时同步第三方otc订单的状态
        Boolean locked = RedisLockUtils.tryLock(redisTemplate, "otc-third-party-order-status-sync", 60000L);
        if (!locked) {
            return;
        }
        try {
            // 查询出本地未到终态的第三方otc订单，去第三方查询对应订单的状态
            Example example = new Example(OtcThirdPartyOrder.class);
            Example.Criteria criteria = example.createCriteria();
            List<Integer> statusList = new ArrayList<>();
            statusList.add(OtcThirdPartyOrderStatus.PENDING_PAYMENT.getStatus());
            statusList.add(OtcThirdPartyOrderStatus.WAITING_PAYMENT.getStatus());
            statusList.add(OtcThirdPartyOrderStatus.IN_PROGRESS.getStatus());
            criteria.andIn("status", statusList);
            List<OtcThirdPartyOrder> orderList = otcThirdPartyOrderMapper.selectByExample(example);
            orderList.forEach(order -> {
                syncThirdPartyOrderStatus(order);
            });
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            RedisLockUtils.releaseLock(redisTemplate, "otc-third-party-order-status-sync");
        }
    }

    private void syncThirdPartyOrderStatus(OtcThirdPartyOrder order) {
        OtcThirdPartySymbol symbol = getLocalOtcThirdPartySymbol(order.getOrgId(), order.getOtcSymbolId(), order.getSide());
        if (symbol == null) {
            return;
        }
        OtcThirdParty thirdParty = getLocalOtcThirdParty(symbol);
        if (thirdParty == null) {
            return;
        }
        if (thirdParty.getThirdPartyId().equals(OtcThirdPartyId.BANXA.getId())) {
            // 如果订单为banxa，则调用banxa接口查询订单状态
            banxaQueryOrderStatus(order, thirdParty);
        } else if (thirdParty.getThirdPartyId().equals(OtcThirdPartyId.MOONPAY.getId())) {
            moonpayQueryOrderStatus(order, thirdParty);
        }
    }

    private void banxaQueryOrderStatus(OtcThirdPartyOrder order, OtcThirdParty thirdParty) {
        try {
            // 查询banxa的订单
            BanxaOrderResult orderResult = banxaService.queryOrder(thirdParty, order.getThirdPartyOrderId());
            if (orderResult == null || orderResult.getData() == null || orderResult.getData().getOrder() == null) {
                throw new BrokerException(BrokerErrorCode.OTC_THIRD_PARTY_REQUEST_FAIL);
            }
            BanxaOrder banxaOrder = orderResult.getData().getOrder();
            if (StringUtil.isEmpty(banxaOrder.getStatus())) {
                return;
            }
            switch (banxaOrder.getStatus()) {
                case "pendingPayment": {
                    // banxa订单状态为支付中，则不更新数据库订单状态
                    break;
                }
                case "waitingPayment":
                case "paymentReceived": {
                    if (order.getStatus().equals(OtcThirdPartyOrderStatus.PENDING_PAYMENT.getStatus())) {
                        // 只有未付款状态会更新为等待付款确认状态
                        updateOrderStatus(order, OtcThirdPartyOrderStatus.WAITING_PAYMENT.getStatus());
                    }
                    break;
                }
                case "inProgress":
                case "coinTransferred": {
                    if (order.getStatus().equals(OtcThirdPartyOrderStatus.PENDING_PAYMENT.getStatus()) ||
                            order.getStatus().equals(OtcThirdPartyOrderStatus.WAITING_PAYMENT.getStatus())) {
                        // banxa订单为处理和放币完成则更新状态为放币处理中
                        updateOrderStatus(order, OtcThirdPartyOrderStatus.IN_PROGRESS.getStatus());
                    }
                }
                case "complete": {
                    if (order.getStatus().equals(OtcThirdPartyOrderStatus.PENDING_PAYMENT.getStatus()) ||
                            order.getStatus().equals(OtcThirdPartyOrderStatus.WAITING_PAYMENT.getStatus()) ||
                            order.getStatus().equals(OtcThirdPartyOrderStatus.IN_PROGRESS.getStatus())) {
                        updateBanxaOrderSuccess(order, OtcThirdPartyOrderStatus.COIN_TRANSFERRED.getStatus(), banxaOrder);
                    }
                    break;
                }
                case "refunded":
                case "cancelled": {
                    if (order.getStatus().equals(OtcThirdPartyOrderStatus.PENDING_PAYMENT.getStatus()) ||
                            order.getStatus().equals(OtcThirdPartyOrderStatus.WAITING_PAYMENT.getStatus()) ||
                            order.getStatus().equals(OtcThirdPartyOrderStatus.IN_PROGRESS.getStatus())) {
                        // banxa订单为已取消，则更新本地订单为取消
                        updateOrderStatus(order, OtcThirdPartyOrderStatus.CANCELLED.getStatus());
                    }
                    break;
                }
                case "declined": {
                    if (order.getStatus().equals(OtcThirdPartyOrderStatus.PENDING_PAYMENT.getStatus()) ||
                            order.getStatus().equals(OtcThirdPartyOrderStatus.WAITING_PAYMENT.getStatus()) ||
                            order.getStatus().equals(OtcThirdPartyOrderStatus.IN_PROGRESS.getStatus())) {
                        // banxa订单状态为拒绝，则更新本地订单为拒绝
                        updateOrderStatus(order, OtcThirdPartyOrderStatus.DECLINED.getStatus());
                    }
                    break;
                }
                case "expired": {
                    if (order.getStatus().equals(OtcThirdPartyOrderStatus.PENDING_PAYMENT.getStatus()) ||
                            order.getStatus().equals(OtcThirdPartyOrderStatus.WAITING_PAYMENT.getStatus()) ||
                            order.getStatus().equals(OtcThirdPartyOrderStatus.IN_PROGRESS.getStatus())) {
                        // banxa订单状态为超时，则更新本地订单为超时
                        updateOrderStatus(order, OtcThirdPartyOrderStatus.EXPIRED.getStatus());
                    }
                    break;
                }
                default: {
                    log.info("Unknown banxa order status:{}", orderResult.getData().getOrder().getStatus());
                    break;
                }
            }
        } catch (Exception e) {
            log.info("banxaQueryOrder Exception:" + e.getMessage(), e);
        }
    }


    private void moonpayQueryOrderStatus(OtcThirdPartyOrder order, OtcThirdParty thirdParty) {
        try {
            if (StringUtil.isEmpty(order.getThirdPartyOrderId())) {
                return;
            }
            MoonpayTransaction transaction = moonpayService.getTransaction(thirdParty, order.getThirdPartyOrderId(), order.getSide());
            if (transaction == null || transaction.getStatus() == null) {
                throw new BrokerException(BrokerErrorCode.OTC_THIRD_PARTY_REQUEST_FAIL);
            }
            switch (transaction.getStatus()) {
                case "pending": {
                    //订单状态为支付中，则不更新数据库订单状态
                    break;
                }
                case "waitingForDeposit": {
                    if (order.getStatus().equals(OtcThirdPartyOrderStatus.PENDING_PAYMENT.getStatus())) {
                        // 只有未付款状态会更新为等待付款确认状态
                        updateOrderStatus(order, OtcThirdPartyOrderStatus.WAITING_PAYMENT.getStatus());
                    }
                    break;
                }
                case "completed": {
                    if (order.getStatus().equals(OtcThirdPartyOrderStatus.PENDING_PAYMENT.getStatus()) ||
                            order.getStatus().equals(OtcThirdPartyOrderStatus.WAITING_PAYMENT.getStatus()) ||
                            order.getStatus().equals(OtcThirdPartyOrderStatus.IN_PROGRESS.getStatus())) {
                        updateMoonpayOrderSuccess(order, OtcThirdPartyOrderStatus.COIN_TRANSFERRED.getStatus(), transaction);
                    }
                    break;
                }
                case "failed": {
                    if (order.getStatus().equals(OtcThirdPartyOrderStatus.PENDING_PAYMENT.getStatus()) ||
                            order.getStatus().equals(OtcThirdPartyOrderStatus.WAITING_PAYMENT.getStatus()) ||
                            order.getStatus().equals(OtcThirdPartyOrderStatus.IN_PROGRESS.getStatus())) {
                        // 订单为已取消，则更新本地订单为取消
                        String failReason = StringUtil.isEmpty(transaction.getFailureReason()) ? "" : transaction.getFailureReason();
                        updateOrderFailed(order, OtcThirdPartyOrderStatus.CANCELLED.getStatus(), failReason);
                    }
                    break;
                }
                default: {
                    log.info("Unknown moonpay order status:{}", transaction.getStatus());
                    break;
                }
            }
        } catch (Exception e) {
            log.info("moonpayQueryOrderStatus Exception:" + e.getMessage(), e);
        }
    }

    private void updateOrderStatus(OtcThirdPartyOrder order, Integer orderStatus) {
        int ret = otcThirdPartyOrderMapper.updateOrderStatus(orderStatus, order.getId(), order.getOrgId(), order.getStatus());
        if (ret != 1) {
            log.info("updateOrderStatus fail:id:{}, status:{}, oldStatus:{}", order.getId(), order.getStatus(), orderStatus);
        }
    }

    private void updateOrderFailed(OtcThirdPartyOrder order, Integer orderStatus, String failReason) {
        int ret = otcThirdPartyOrderMapper.updateOrderFailed(orderStatus, failReason, order.getId(), order.getOrgId(), order.getStatus());
        if (ret != 1) {
            log.info("updateOrderStatus fail:id:{}, status:{}, oldStatus:{}", order.getId(), order.getStatus(), orderStatus);
        }
    }

    private void updateBanxaOrderSuccess(OtcThirdPartyOrder order, Integer orderStatus, BanxaOrder banxaOrder) {
        BigDecimal feeAmount = BigDecimal.ZERO;
        String fee = banxaOrder.getFee();
        if (StringUtil.isNotEmpty(banxaOrder.getFee())) {
            feeAmount = feeAmount.add(new BigDecimal(fee));
        }
        String paymentFee = banxaOrder.getPayment_fee();
        if (StringUtil.isNotEmpty(paymentFee)) {
            feeAmount = feeAmount.add(new BigDecimal(paymentFee));
        }
        if (feeAmount.compareTo(BigDecimal.ZERO) > 0) {
            feeAmount = feeAmount.setScale(ProtoConstants.PRECISION, RoundingMode.DOWN);
        }
        BigDecimal tokenAmount = banxaOrder.getCoin_amount().setScale(ProtoConstants.PRECISION, RoundingMode.DOWN);
        BigDecimal currencyAmount = banxaOrder.getFiat_amount().setScale(ProtoConstants.PRECISION, RoundingMode.DOWN);
        int ret = otcThirdPartyOrderMapper.updateOrderSuccess(orderStatus, currencyAmount, tokenAmount, feeAmount,
                order.getId(), order.getOrgId(), order.getStatus());
        if (ret != 1) {
            log.info("updateBanxaOrderSuccess fail:id:{}, status:{}, oldStatus:{}", order.getId(), order.getStatus(), orderStatus);
        }
    }

    private void updateMoonpayOrderSuccess(OtcThirdPartyOrder order, Integer orderStatus, MoonpayTransaction transaction) {
        BigDecimal feeAmount = BigDecimal.ZERO;
        if (transaction.getFeeAmount() != null) {
            feeAmount = feeAmount.add(transaction.getFeeAmount());
        }
        if (transaction.getExtraFeeAmount() != null) {
            feeAmount = feeAmount.add(transaction.getExtraFeeAmount());
        }
        if (transaction.getNetworkFeeAmount() != null) {
            feeAmount = feeAmount.add(transaction.getNetworkFeeAmount());
        }
        if (feeAmount.compareTo(BigDecimal.ZERO) > 0) {
            feeAmount = feeAmount.setScale(ProtoConstants.PRECISION, RoundingMode.DOWN);
        }
        BigDecimal tokenAmount = transaction.getQuoteCurrencyAmount().setScale(ProtoConstants.PRECISION, RoundingMode.DOWN);
        BigDecimal currencyAmount = transaction.getBaseCurrencyAmount().setScale(ProtoConstants.PRECISION, RoundingMode.DOWN);
        int ret = otcThirdPartyOrderMapper.updateOrderSuccess(orderStatus, currencyAmount, tokenAmount, feeAmount,
                order.getId(), order.getOrgId(), order.getStatus());
        if (ret != 1) {
            log.info("updateMoonpayOrderSuccess fail:id:{}, status:{}, oldStatus:{}", order.getId(), order.getStatus(), orderStatus);
        }
    }

    public GetOtcThirdPartyConfigResponse getOtcThirdPartyConfig(Long orgId) {
        GetOtcThirdPartyConfigResponse.Builder builder = GetOtcThirdPartyConfigResponse.newBuilder();
        builder.setIsBuySupported(false);
        builder.setIsSellSupported(false);
        // 查询是否支持买入
        List<OtcThirdParty> thirdPartyBuyList = otcThirdPartyMapper.queryListByOrgId(orgId, 0);
        if (thirdPartyBuyList != null) {
            thirdPartyBuyList = thirdPartyBuyList.stream()
                    .filter(thirdParty -> thirdParty.getStatus().equals(OtcThirdPartyStatus.ENABLE.getStatus()))
                    .collect(Collectors.toList());
            if (thirdPartyBuyList.size() > 0) {
                builder.setIsBuySupported(true);
            }
        }
        // 查询是否支持卖出
        List<OtcThirdParty> thirdPartySellList = otcThirdPartyMapper.queryListByOrgId(orgId, 1);
        if (thirdPartySellList != null) {
            thirdPartySellList = thirdPartySellList.stream()
                    .filter(thirdParty -> thirdParty.getStatus().equals(OtcThirdPartyStatus.ENABLE.getStatus()))
                    .collect(Collectors.toList());
            if (thirdPartySellList.size() > 0) {
                builder.setIsSellSupported(true);
            }
        }
        return builder.build();
    }

    public List<io.bhex.broker.grpc.otc.third.party.OtcThirdParty> getOtcThirdParty(Long orgId, Integer side) {
        List<io.bhex.broker.grpc.otc.third.party.OtcThirdParty> findList = new ArrayList<>();
        // 查询币对
        List<OtcThirdParty> thirdPartyList = otcThirdPartyMapper.queryListByOrgId(orgId, side);
        thirdPartyList.stream()
                .filter(thirdParty -> thirdParty.getStatus().equals(OtcThirdPartyStatus.ENABLE.getStatus()))
                .forEach(thirdParty -> findList.add(io.bhex.broker.grpc.otc.third.party.OtcThirdParty.newBuilder()
                        .setThirdPartyId(thirdParty.getThirdPartyId())
                        .setThirdPartyName(thirdParty.getThirdPartyName())
                        .setThirdPartyIcon(thirdParty.getIconUrl())
                        .build()));

        return findList;
    }

    public List<io.bhex.broker.grpc.otc.third.party.OtcThirdPartySymbol> getOtcThirdPartySymbols(Long orgId, Long thirdPartyId, Integer side) {
        List<OtcThirdPartySymbol> thirdPartySymbolList = otcThirdPartySymbolMapper.queryListByOrgId(orgId, thirdPartyId, side);
        return thirdPartySymbolList.stream()
                .filter(o -> o.getStatus().equals(OtcThirdPartyStatus.ENABLE.getStatus()))
                .map(this::buildThirdPartySymbol).collect(Collectors.toList());
    }

    private io.bhex.broker.grpc.otc.third.party.OtcThirdPartySymbol buildThirdPartySymbol(OtcThirdPartySymbol symbol) {
        Token grpcToken = basicService.getToken(symbol.getOrgId(), symbol.getTokenId());
        if (grpcToken == null) {
            return null;
        }
        return io.bhex.broker.grpc.otc.third.party.OtcThirdPartySymbol.newBuilder()
                .setOtcSymbolId(symbol.getOtcSymbolId())
                .setOrgId(symbol.getOrgId())
                .setThirdPartyId(symbol.getThirdPartyId())
                .setTokenId(symbol.getTokenId())
                .setCurrencyId(symbol.getCurrencyId())
                .setTokenIconUrl(grpcToken.getIconUrl())
                .build();
    }

    public List<io.bhex.broker.grpc.otc.third.party.OtcThirdPartyPayment>
    getOtcThirdPartyPayments(Long orgId, Long thirdPartyId, String tokenId, String currencyId, Integer side) {
        List<OtcThirdPartySymbol> thirdPartySymbolList = otcThirdPartySymbolMapper.queryListByTokenId(orgId, tokenId,
                currencyId, thirdPartyId, side);
        return thirdPartySymbolList.stream()
                .filter(o -> o.getStatus().equals(OtcThirdPartyStatus.ENABLE.getStatus()))
                .map(this::buildThirdPartyPayment).collect(Collectors.toList());
    }

    private io.bhex.broker.grpc.otc.third.party.OtcThirdPartyPayment buildThirdPartyPayment(OtcThirdPartySymbol symbol) {
        return io.bhex.broker.grpc.otc.third.party.OtcThirdPartyPayment.newBuilder()
                .setOtcSymbolId(symbol.getOtcSymbolId())
                .setOrgId(symbol.getOrgId())
                .setThirdPartyId(symbol.getThirdPartyId())
                .setTokenId(symbol.getTokenId())
                .setCurrencyId(symbol.getCurrencyId())
                .setPaymentId(symbol.getPaymentId())
                .setPaymentType(symbol.getPaymentType())
                .setPaymentName(symbol.getPaymentName())
                .setFee(DecimalUtil.toTrimString(symbol.getFee()))
                .setMinAmount(DecimalUtil.toTrimString(symbol.getMinAmount()))
                .setMaxAmount(DecimalUtil.toTrimString(symbol.getMaxAmount()))
                .build();
    }

    public GetOtcThirdPartyPriceResponse getOtcThirdPartyPrice(Header header, Long otcSymbolId, String tokenAmount,
                                                               String currencyAmount, Integer side) {
        OtcThirdPartySymbol symbol = getLocalOtcThirdPartySymbol(header.getOrgId(), otcSymbolId, side);
        if (symbol == null || !symbol.getStatus().equals(OtcThirdPartyStatus.ENABLE.getStatus())) {
            throw new BrokerException(BrokerErrorCode.OTC_THIRD_PARTY_SYMBOL_NOT_FOUND);
        }
        OtcThirdParty thirdParty = getLocalOtcThirdParty(symbol);
        if (thirdParty == null || !thirdParty.getStatus().equals(OtcThirdPartyStatus.ENABLE.getStatus())) {
            throw new BrokerException(BrokerErrorCode.OTC_THIRD_PARTY_NOT_FOUND);
        }
        // 判断订单判断是否满足最大最小金额
        BigDecimal orderAmount;
        if (side == 1) {
            // 卖出时取数字币数量为订单数量判断
            orderAmount = new BigDecimal(tokenAmount).setScale(ProtoConstants.PRECISION, RoundingMode.DOWN);
        } else {
            // 买入时取法币数量作为订单数量判断
            orderAmount = new BigDecimal(currencyAmount).setScale(ProtoConstants.PRECISION, RoundingMode.DOWN);
        }
        if (orderAmount.compareTo(symbol.getMinAmount()) < 0) {
            throw new BrokerException(BrokerErrorCode.ORDER_AMOUNT_TOO_SMALL);
        }
        if (orderAmount.compareTo(symbol.getMaxAmount()) > 0) {
            throw new BrokerException(BrokerErrorCode.ORDER_AMOUNT_TOO_BIG);
        }

        // 根据第三方机构id调用对应服务查询价格
        if (symbol.getThirdPartyId().equals(OtcThirdPartyId.BANXA.getId())) {
            BanxaPriceResult priceResult = banxaService.getPrice(thirdParty, symbol.getTokenId(), symbol.getCurrencyId(),
                    symbol.getPaymentId(), tokenAmount, currencyAmount, side);
            if (priceResult == null || priceResult.getData() == null) {
                throw new BrokerException(BrokerErrorCode.OTC_THIRD_PARTY_REQUEST_FAIL);
            }
            List<BanxaPrice> prices = priceResult.getData().getPrices();
            if (prices == null || prices.size() <= 0) {
                throw new BrokerException(BrokerErrorCode.OTC_THIRD_PARTY_REQUEST_FAIL);
            }
            BanxaPrice otcPrice = prices.get(0);
            return GetOtcThirdPartyPriceResponse.newBuilder()
                    .setRet(0)
                    .setOtcSymbolId(otcSymbolId)
                    .setPrice(otcPrice.getSpot_price_including_fee())
                    .setFeeAmount(otcPrice.getFee_amount())
                    .setTokenId(otcPrice.getCoin_code())
                    .setTokenAmount(otcPrice.getCoin_amount())
                    .setCurrencyId(otcPrice.getFiat_code())
                    .setCurrencyAmount(otcPrice.getFiat_amount())
                    .build();
        } else if (symbol.getThirdPartyId().equals(OtcThirdPartyId.MOONPAY.getId())) {
            MoonpayPrice result = moonpayService.getPrice(thirdParty, symbol.getTokenId(), symbol.getCurrencyId(),
                    symbol.getPaymentType(), tokenAmount, currencyAmount, side);
            if (result == null || result.getQuoteCurrencyAmount() == null) {
                throw new BrokerException(BrokerErrorCode.OTC_THIRD_PARTY_REQUEST_FAIL);
            }
            // 统计费用值
            BigDecimal fee = BigDecimal.ZERO;
            if (result.getFeeAmount() != null && result.getFeeAmount().compareTo(BigDecimal.ZERO) > 0) {
                fee = fee.add(result.getFeeAmount());
            }
            if (result.getExtraFeeAmount() != null && result.getExtraFeeAmount().compareTo(BigDecimal.ZERO) > 0) {
                fee = fee.add(result.getExtraFeeAmount());
            }
            if (result.getNetworkFeeAmount() != null && result.getNetworkFeeAmount().compareTo(BigDecimal.ZERO) > 0) {
                fee = fee.add(result.getNetworkFeeAmount());
            }
            BigDecimal avgPrice;
            if (side == 1) {
                // 卖出时得到法币数量除以卖币的数字币数量
                avgPrice = result.getQuoteCurrencyAmount().add(fee).divide(result.getBaseCurrencyAmount(), 2, RoundingMode.DOWN);
            } else {
                // 计算平均价格
                avgPrice = result.getTotalAmount().divide(result.getQuoteCurrencyAmount(), 2, RoundingMode.DOWN);
            }
            String currencyAmountRet, tokenAmountRet;
            if (side == 1) {
                tokenAmountRet = DecimalUtil.toTrimString(result.getBaseCurrencyAmount());
                currencyAmountRet = DecimalUtil.toTrimString(result.getQuoteCurrencyAmount());
            } else {
                tokenAmountRet = DecimalUtil.toTrimString(result.getQuoteCurrencyAmount());
                currencyAmountRet = DecimalUtil.toTrimString(result.getTotalAmount());
            }
            return GetOtcThirdPartyPriceResponse.newBuilder()
                    .setRet(0)
                    .setOtcSymbolId(otcSymbolId)
                    .setPrice(DecimalUtil.toTrimString(avgPrice))
                    .setFeeAmount(DecimalUtil.toTrimString(fee))
                    .setTokenId(symbol.getTokenId())
                    .setTokenAmount(tokenAmountRet)
                    .setCurrencyId(symbol.getCurrencyId())
                    .setCurrencyAmount(currencyAmountRet)
                    .build();
        } else {
            throw new BrokerException(BrokerErrorCode.OTC_THIRD_PARTY_NOT_FOUND);
        }
    }


    public CreateOtcThirdPartyOrderResponse createOtcThirdPartyOrder(Header header, Long otcSymbolId, String clientOrderId,
                                                                     String tokenAmount, String currencyAmount,
                                                                     String price, Integer side) {
        log.info("createOtcThirdPartyOrder:otcSymbolId:{},clientOrderId:{},tokenAmount:{},currencyAmount:{},price:{},side:{}.",
                otcSymbolId, clientOrderId, tokenAmount, currencyAmount, price, side);
        Long orgId = header.getOrgId();
        Long userId = header.getUserId();
        // 判断用户对应外部订单号的订单是否已经存在，若已存在则直接返回对应订单信息
        OtcThirdPartyOrder order = otcThirdPartyOrderMapper.getByClientOrderId(orgId, userId, clientOrderId);
        if (order != null) {
            return CreateOtcThirdPartyOrderResponse.newBuilder()
                    .setSuccess(true)
                    .setOrderId(order.getOrderId())
                    .setOtcUrl(order.getOtcUrl())
                    .build();
        }
        // 查询第三方币对是否存在并且不处于禁用状态
        OtcThirdPartySymbol symbol = getLocalOtcThirdPartySymbol(orgId, otcSymbolId, side);
        if (symbol == null || !symbol.getStatus().equals(OtcThirdPartyStatus.ENABLE.getStatus())) {
            throw new BrokerException(BrokerErrorCode.OTC_THIRD_PARTY_SYMBOL_NOT_FOUND);
        }
        // 查询第三方机构是否存在并且不处于禁用状态
        OtcThirdParty thirdParty = getLocalOtcThirdParty(symbol);
        if (thirdParty == null || !thirdParty.getStatus().equals(OtcThirdPartyStatus.ENABLE.getStatus())) {
            throw new BrokerException(BrokerErrorCode.OTC_THIRD_PARTY_NOT_FOUND);
        }
        // 判断订单判断是否满足最大最小金额
        BigDecimal orderAmount;
        if (side == 1) {
            // 卖出时取数字币数量为订单数量判断
            orderAmount = new BigDecimal(tokenAmount).setScale(ProtoConstants.PRECISION, RoundingMode.DOWN);
        } else {
            // 买入时取法币数量作为订单数量判断
            orderAmount = new BigDecimal(currencyAmount).setScale(ProtoConstants.PRECISION, RoundingMode.DOWN);
        }
        if (orderAmount.compareTo(symbol.getMinAmount()) < 0) {
            throw new BrokerException(BrokerErrorCode.ORDER_AMOUNT_TOO_SMALL);
        }
        if (orderAmount.compareTo(symbol.getMaxAmount()) > 0) {
            throw new BrokerException(BrokerErrorCode.ORDER_AMOUNT_TOO_BIG);
        }
        // 查询用户对应币种的钱包地址
        String chainType = getChainType(orgId, symbol.getTokenId());
        GetDepositAddressResponse addressResponse;
        try {
            log.info("createOtcThirdPartyOrder getDepositAddress:{},{},{},{}.", orgId, userId, symbol.getTokenId(), chainType);
            addressResponse = depositService.getDepositAddress(header, symbol.getTokenId(), chainType);
            if (addressResponse == null || StringUtil.isEmpty(addressResponse.getAddress())) {
                throw new BrokerException(BrokerErrorCode.OTC_THIRD_PARTY_WALLET_ADDRESS_NOT_FOUND);
            }
        } catch (Exception e) {
            throw new BrokerException(BrokerErrorCode.OTC_THIRD_PARTY_WALLET_ADDRESS_NOT_FOUND);
        }
        String walletAddress = addressResponse.getAddress();
        String walletAddressExt = addressResponse.getAddressExt();
        // 获取用户信息
        GetUserInfoResponse userInfoResponse = userService.getUserInfo(header);
        if (userInfoResponse.getUser() == null) {
            throw new BrokerException(BrokerErrorCode.USER_NOT_EXIST);
        }
        Long userAccountId = userInfoResponse.getUser().getDefaultAccountId();
        // 生成系统内订单ID
        Long orderId = iSequenceGenerator.getLong();
        // otc第三方订单ID
        String thirdOrderId = "";
        BigDecimal thirdTokenAmount;
        BigDecimal thirdCurrencyAmount;
        String thirdOtcUrl;
        // 根据第三方机构id调用对应服务进行下单
        if (symbol.getThirdPartyId().equals(OtcThirdPartyId.BANXA.getId())) {
            // 如果第三方机构要求检查可用资产或冻结资产，则需要判断其可用
            switch (thirdParty.getControlType()) {
                case 0: {
                    // 不判断可用和锁仓，直接调用第三方下单
                    break;
                }
                case 1: {
                    // 查询第三方机构账户是否存在足够余额
                    Balance thirdPartyBalance = accountService.queryTokenBalance(thirdParty.getBindOrgId(),
                            thirdParty.getBindAccountId(), symbol.getTokenId());
                    if (thirdPartyBalance == null) {
                        throw new BrokerException(BrokerErrorCode.OTC_THIRD_PARTY_INSUFFICIENT_BALANCE);
                    }
                    BigDecimal feeQty = new BigDecimal(thirdPartyBalance.getFree());
                    BigDecimal orderQty = new BigDecimal(tokenAmount);
                    if (feeQty.compareTo(orderQty) < 0) {
                        throw new BrokerException(BrokerErrorCode.OTC_THIRD_PARTY_INSUFFICIENT_BALANCE);
                    }
                    break;
                }
                case 2: {
                    // 如果otc第三方需要冻结资产，当冻结失败时则认为买入订单下单失败。 若冻结成功，但是下单失败，需要解冻
                    LockBalanceReply reply = balanceService.lockBalance(thirdParty.getBindUserId(), thirdParty.getBindOrgId(),
                            tokenAmount, symbol.getTokenId(), orderId, "otc third-party");
                    if (reply == null || reply.getCode() != LockBalanceReply.ReplyCode.SUCCESS) {
                        log.error("createOtcThirdPartyOrder lockBalance: {} {}", userId, TextFormat.shortDebugString(reply));
                        throw new BrokerException(BrokerErrorCode.OTC_THIRD_PARTY_INSUFFICIENT_BALANCE);
                    }
                    break;
                }
                default: {
                    throw new BrokerException(BrokerErrorCode.OTC_THIRD_PARTY_UNKNOWN_CONTROL_TYPE);
                }
            }
            // 调用第三方订单下单
            BanxaOrderResult orderResult = banxaService.placeOrder(thirdParty, walletAddress, walletAddressExt,
                    userAccountId.toString(), symbol.getPaymentId(), side, symbol.getTokenId(), symbol.getCurrencyId(),
                    tokenAmount, currencyAmount, thirdParty.getSuccessUrl(), thirdParty.getFailureUrl(), thirdParty.getCancelUrl());
            if (orderResult == null || orderResult.getData() == null || orderResult.getData().getOrder() == null) {
                if (thirdParty.getControlType().equals(OtcThirdPartyControlType.LOCK_POSITION.getType())) {
                    // 下单失败,并且第三方机构为预先锁定模式，需要解除锁定
                    UnlockBalanceResponse reply = balanceService.unLockBalance(thirdParty.getBindUserId(), thirdParty.getBindOrgId(),
                            tokenAmount, symbol.getTokenId(), "otc third-party", orderId);
                    if (reply == null || reply.getCode() != UnlockBalanceResponse.ReplyCode.SUCCESS) {
                        log.error("unLockBalance: {} {}", userId, TextFormat.shortDebugString(reply));
                    }
                }
                throw new BrokerException(BrokerErrorCode.OTC_THIRD_PARTY_REQUEST_FAIL);
            }

            BanxaOrder banxaOrder = orderResult.getData().getOrder();
            thirdOrderId = banxaOrder.getId();
            thirdTokenAmount = banxaOrder.getCoin_amount().setScale(ProtoConstants.PRECISION, RoundingMode.DOWN);
            thirdCurrencyAmount = banxaOrder.getFiat_amount().setScale(ProtoConstants.PRECISION, RoundingMode.DOWN);
            thirdOtcUrl = banxaOrder.getCheckout_url();
        } else if (symbol.getThirdPartyId().equals(OtcThirdPartyId.MOONPAY.getId())) {
            // 拼接Moonpay的下单url
            GetOtcThirdPartyPriceResponse response = getOtcThirdPartyPrice(header, otcSymbolId, tokenAmount, currencyAmount, side);
            tokenAmount = response.getTokenAmount();
            currencyAmount = response.getCurrencyAmount();
            price = response.getPrice();
            thirdOtcUrl = moonpayService.placeOrder(thirdParty, userId.toString(), orderId.toString(),
                    walletAddress, walletAddressExt, symbol.getPaymentType(), symbol.getTokenId(), symbol.getCurrencyId(),
                    tokenAmount, currencyAmount, side, thirdParty.getSuccessUrl());
            thirdTokenAmount = new BigDecimal(tokenAmount).setScale(ProtoConstants.PRECISION, RoundingMode.DOWN);
            thirdCurrencyAmount = new BigDecimal(currencyAmount).setScale(ProtoConstants.PRECISION, RoundingMode.DOWN);
        } else {
            throw new BrokerException(BrokerErrorCode.OTC_THIRD_PARTY_NOT_FOUND);
        }

        Long curTime = System.currentTimeMillis();
        OtcThirdPartyOrder newOrder = new OtcThirdPartyOrder();
        newOrder.setOrderId(orderId);
        newOrder.setClientOrderId(clientOrderId);
        newOrder.setThirdPartyOrderId(thirdOrderId);
        newOrder.setOtcSymbolId(otcSymbolId);
        newOrder.setTokenId(symbol.getTokenId());
        newOrder.setCurrencyId(symbol.getCurrencyId());
        newOrder.setTokenAmount(thirdTokenAmount);
        newOrder.setCurrencyAmount(thirdCurrencyAmount);
        newOrder.setWalletAddress(walletAddress);
        newOrder.setWalletAddressExt(walletAddressExt);
        newOrder.setPrice(new BigDecimal(price).setScale(ProtoConstants.PRECISION, RoundingMode.DOWN));
        newOrder.setStatus(OtcThirdPartyOrderStatus.PENDING_PAYMENT.getStatus());
        newOrder.setSide(side);
        newOrder.setOrgId(orgId);
        newOrder.setUserId(userId);
        newOrder.setAccountId(userAccountId);
        newOrder.setOtcOrgId(thirdParty.getBindOrgId());
        newOrder.setOtcUserId(thirdParty.getBindUserId());
        newOrder.setOtcAccountId(thirdParty.getBindAccountId());
        newOrder.setOtcUrl(thirdOtcUrl);
        newOrder.setCreated(curTime);
        newOrder.setUpdated(curTime);
        otcThirdPartyOrderMapper.insertSelective(newOrder);

        return CreateOtcThirdPartyOrderResponse.newBuilder()
                .setSuccess(true)
                .setOrderId(orderId)
                .setOtcUrl(thirdOtcUrl)
                .build();
    }

    public List<io.bhex.broker.grpc.otc.third.party.OtcThirdPartyOrder> queryOtcThirdPartyOrder(Long orgId,
                                                                                                Long userId, Integer status,
                                                                                                Long beginTime, Long endTime,
                                                                                                Long fromOrderId, Integer limit) {
        Example example = new Example(OtcThirdPartyOrder.class);
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("orgId", orgId);
        criteria.andEqualTo("userId", userId);
        if (status != null && !status.equals(0)) {
            criteria.andEqualTo("status", status);
        }
        if (beginTime != null && !beginTime.equals(0L)) {
            criteria.andGreaterThan("created", beginTime);
        }
        if (endTime != null && !endTime.equals(0L)) {
            criteria.andLessThan("created", endTime);
        }
        if (fromOrderId != null && !fromOrderId.equals(0L)) {
            criteria.andLessThan("orderId", fromOrderId);
        }
        if (limit == null || limit == 0) {
            limit = 50;
        } else if (limit > 500) {
            limit = 500;
        }
        example.setOrderByClause("id DESC");
        List<OtcThirdPartyOrder> orderList = otcThirdPartyOrderMapper.selectByExampleAndRowBounds(example, new RowBounds(0, limit));
        return orderList.stream().map(this::buildOtcThirdPartyOrder).collect(Collectors.toList());
    }

    private io.bhex.broker.grpc.otc.third.party.OtcThirdPartyOrder buildOtcThirdPartyOrder(OtcThirdPartyOrder order) {
        OtcThirdPartySymbol symbol = getLocalOtcThirdPartySymbol(order.getOrgId(), order.getOtcSymbolId(), order.getSide());
        if (symbol == null) {
            return null;
        }
        OtcThirdParty thirdParty = getLocalOtcThirdParty(symbol);
        if (thirdParty == null) {
            return null;
        }
        return io.bhex.broker.grpc.otc.third.party.OtcThirdPartyOrder.newBuilder()
                .setOrderId(order.getOrderId())
                .setOrgId(order.getOrgId())
                .setUserId(order.getUserId())
                .setAccountId(order.getAccountId())
                .setClientOrderId(order.getClientOrderId())
                .setOtcSymbolId(order.getOtcSymbolId())
                .setTokenAmount(DecimalUtil.toTrimString(order.getTokenAmount()))
                .setCurrencyAmount(DecimalUtil.toTrimString(order.getCurrencyAmount()))
                .setPrice(DecimalUtil.toTrimString(order.getPrice()))
                .setSide(order.getSide())
                .setStatus(order.getStatus())
                .setErrorMessage(order.getErrorMessage())
                .setCreated(order.getCreated())
                .setUpdated(order.getUpdated())
                .setTokenId(order.getTokenId())
                .setCurrencyId(order.getCurrencyId())
                .setOtcUrl(order.getOtcUrl())
                .setFeeAmount(DecimalUtil.toTrimString(order.getFeeAmount()))
                .setPaymentName(symbol.getPaymentName())
                .setThirdPartyName(thirdParty.getThirdPartyName())
                .build();
    }

    public List<io.bhex.broker.grpc.otc.third.party.OtcThirdPartyOrder> queryOtcThirdPartyOrderByAdmin(Long orgId, Long userId,
                                                                                                       Long orderId, Integer status,
                                                                                                       Long beginTime, Long endTime,
                                                                                                       Long fromOrderId, Integer limit) {
        Example example = new Example(OtcThirdPartyOrder.class);
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("orgId", orgId);
        if (userId != null && !userId.equals(0L)) {
            criteria.andEqualTo("userId", userId);
        }
        if (orderId != null && !orderId.equals(0L)) {
            criteria.andEqualTo("orderId", orderId);
        }
        if (status != null && !status.equals(0)) {
            criteria.andEqualTo("status", status);
        }
        if (beginTime != null && !beginTime.equals(0L)) {
            criteria.andGreaterThan("created", beginTime);
        }
        if (endTime != null && !endTime.equals(0L)) {
            criteria.andLessThan("created", endTime);
        }
        if (fromOrderId != null && !fromOrderId.equals(0L)) {
            criteria.andLessThan("orderId", fromOrderId);
        }
        if (limit == null || limit == 0) {
            limit = 50;
        } else if (limit > 500) {
            limit = 500;
        }
        example.setOrderByClause("id DESC");
        List<OtcThirdPartyOrder> orderList = otcThirdPartyOrderMapper.selectByExampleAndRowBounds(example, new RowBounds(0, limit));
        return orderList.stream().map(this::buildOtcThirdPartyOrder).collect(Collectors.toList());
    }

    public MoonpayTransactionResponse moonpayTransaction(String transactionId, String transactionStatus, String orderId,
                                                         String feeAmount, String extraFeeAmount, String networkFeeAmount) {
        try {
            Long findOrderId = Long.parseLong(orderId);
            OtcThirdPartyOrder order = otcThirdPartyOrderMapper.getByOrderId(findOrderId);
            if (order != null && StringUtil.isEmpty(order.getThirdPartyOrderId())) {
                OtcThirdPartyOrder updateOrder = new OtcThirdPartyOrder();
                updateOrder.setId(order.getId());
                updateOrder.setThirdPartyOrderId(transactionId);
                // 累加费用值
                BigDecimal fee = BigDecimal.ZERO;
                if (StringUtil.isNotEmpty(feeAmount)) {
                    fee = fee.add(new BigDecimal(feeAmount));
                }
                if (StringUtil.isNotEmpty(extraFeeAmount)) {
                    fee = fee.add(new BigDecimal(extraFeeAmount));
                }
                if (StringUtil.isNotEmpty(networkFeeAmount)) {
                    fee = fee.add(new BigDecimal(networkFeeAmount));
                }
                if (fee.compareTo(BigDecimal.ZERO) > 0) {
                    updateOrder.setFeeAmount(fee);
                }

                switch (transactionStatus) {
                    case "pending":
                    case "failed": {
                        //订单状态为支付中，则不更新数据库订单状态
                        break;
                    }
                    case "waitingForDeposit":
                    case "waitingPayment":
                    case "waitingAuthorization": {
                        if (order.getStatus().equals(OtcThirdPartyOrderStatus.PENDING_PAYMENT.getStatus())) {
                            updateOrder.setStatus(OtcThirdPartyOrderStatus.WAITING_PAYMENT.getStatus());
                        }
                        break;
                    }
                    case "completed": {
                        if (order.getStatus().equals(OtcThirdPartyOrderStatus.PENDING_PAYMENT.getStatus()) ||
                                order.getStatus().equals(OtcThirdPartyOrderStatus.WAITING_PAYMENT.getStatus()) ||
                                order.getStatus().equals(OtcThirdPartyOrderStatus.IN_PROGRESS.getStatus())) {
                            updateOrder.setStatus(OtcThirdPartyOrderStatus.COIN_TRANSFERRED.getStatus());
                        }
                        break;
                    }
                    default: {
                        log.info("Unknown moonpay order status:{}", transactionStatus);
                        break;
                    }
                }
                otcThirdPartyOrderMapper.updateByPrimaryKeySelective(updateOrder);
            }
        } catch (Exception e) {
            log.info("moonpayTransaction update order failed: transactionId:{}", transactionId);
        }
        return MoonpayTransactionResponse.newBuilder()
                .setRet(0)
                .setErrorMessage("")
                .build();
    }


    public List<io.bhex.broker.grpc.otc.third.party.OtcThirdPartyDisclaimer> queryOtcThirdPartyDisclaimer(Long orgId,
                                                                                                          Long thirdPartyId,
                                                                                                          String language) {
        Example example = new Example(OtcThirdPartyDisclaimer.class);
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("orgId", orgId);
        if (thirdPartyId != null && !thirdPartyId.equals(0L)) {
            criteria.andEqualTo("thirdPartyId", thirdPartyId);
        }
        if (StringUtil.isNotEmpty(language)) {
            criteria.andEqualTo("language", language);
        }
        List<OtcThirdPartyDisclaimer> orderList = otcThirdPartyDisclaimerMapper.selectByExample(example);
        return orderList.stream().map(this::buildOtcThirdPartyDisclaimer).collect(Collectors.toList());
    }

    private io.bhex.broker.grpc.otc.third.party.OtcThirdPartyDisclaimer buildOtcThirdPartyDisclaimer(OtcThirdPartyDisclaimer disclaimer) {
        return io.bhex.broker.grpc.otc.third.party.OtcThirdPartyDisclaimer.newBuilder()
                .setOrgId(disclaimer.getOrgId())
                .setThirdPartyId(disclaimer.getThirdPartyId())
                .setLanguage(disclaimer.getLanguage())
                .setDisclaimer(disclaimer.getDisclaimer())
                .build();
    }

    public UpdateOtcThirdPartyDisclaimerResponse updateOtcThirdPartyDisclaimer(Long orgId,
                                                                               List<io.bhex.broker.grpc.otc.third.party.OtcThirdPartyDisclaimer> disclaimerList) {
        Long currentTime = System.currentTimeMillis();
        for (io.bhex.broker.grpc.otc.third.party.OtcThirdPartyDisclaimer disclaimer : disclaimerList) {
            OtcThirdPartyDisclaimer findDisclaimer = otcThirdPartyDisclaimerMapper.getByOrgId(orgId, disclaimer.getThirdPartyId(), disclaimer.getLanguage());
            if (findDisclaimer == null) {
                OtcThirdPartyDisclaimer newDisclaimer = new OtcThirdPartyDisclaimer();
                newDisclaimer.setOrgId(orgId);
                newDisclaimer.setThirdPartyId(disclaimer.getThirdPartyId());
                newDisclaimer.setLanguage(disclaimer.getLanguage());
                newDisclaimer.setDisclaimer(disclaimer.getDisclaimer());
                newDisclaimer.setCreated(currentTime);
                newDisclaimer.setUpdated(currentTime);
                otcThirdPartyDisclaimerMapper.insertSelective(newDisclaimer);
            } else {
                findDisclaimer.setDisclaimer(disclaimer.getDisclaimer());
                findDisclaimer.setUpdated(currentTime);
                otcThirdPartyDisclaimerMapper.updateByPrimaryKeySelective(findDisclaimer);
            }
        }

        return UpdateOtcThirdPartyDisclaimerResponse.newBuilder()
                .setRet(0)
                .setErrorMessage("")
                .build();
    }
}
