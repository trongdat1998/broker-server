package io.bhex.broker.server.grpc.server.service;

import com.google.api.client.repackaged.com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import com.google.gson.Gson;

import com.alibaba.fastjson.JSON;

import io.bhex.broker.grpc.common.AccountTypeEnum;
import io.bhex.broker.grpc.order.OrderSide;
import io.bhex.broker.grpc.order.OrderType;
import io.bhex.broker.server.grpc.server.service.po.SpotOrderNotifyPO;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Resource;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import io.bhex.base.idgen.api.ISequenceGenerator;
import io.bhex.base.mq.MessageProducer;
import io.bhex.base.proto.DecimalUtil;
import io.bhex.base.quote.Rate;
import io.bhex.broker.common.api.client.okhttp.OkHttpPrometheusInterceptor;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.common.util.JsonUtil;
import io.bhex.broker.grpc.basic.Symbol;
import io.bhex.broker.grpc.common.Header;
import io.bhex.broker.server.domain.LoginType;
import io.bhex.broker.server.domain.PushMessageType;
import io.bhex.broker.server.grpc.server.service.po.FuturesMessage;
import io.bhex.broker.server.grpc.server.service.po.PushDataResult;
import io.bhex.broker.server.grpc.server.service.po.PushResult;
import io.bhex.broker.server.grpc.server.service.po.UserKycMessage;
import io.bhex.broker.server.grpc.server.service.po.UserLoginMessagePO;
import io.bhex.broker.server.grpc.server.service.po.UserMessage;
import io.bhex.broker.server.grpc.server.service.po.UserTradeDetailMessage;
import io.bhex.broker.server.grpc.server.service.statistics.StatisticsTradeService;
import io.bhex.broker.server.message.consumer.MarginRiskAlarmConsumer;
import io.bhex.broker.server.message.consumer.OtcOrderConsumer;
import io.bhex.broker.server.message.consumer.TicketReceiveConsumer;
import io.bhex.broker.server.message.producer.UserInfoProducer;
import io.bhex.broker.server.model.Account;
import io.bhex.broker.server.model.DataPushConfig;
import io.bhex.broker.server.model.DataPushMessage;
import io.bhex.broker.server.model.MasterKeyUserConfig;
import io.bhex.broker.server.model.StatisticsTradeAllDetail;
import io.bhex.broker.server.model.ThirdPartyUser;
import io.bhex.broker.server.model.User;
import io.bhex.broker.server.model.UserVerify;
import io.bhex.broker.server.primary.mapper.AccountMapper;
import io.bhex.broker.server.primary.mapper.MasterKeyUserConfigMapper;
import io.bhex.broker.server.primary.mapper.PushDataConfigMapper;
import io.bhex.broker.server.primary.mapper.PushDataMessageMapper;
import io.bhex.broker.server.primary.mapper.ThirdPartyUserMapper;
import io.bhex.broker.server.primary.mapper.UserMapper;
import io.bhex.broker.server.primary.mapper.UserVerifyMapper;
import io.bhex.broker.server.util.SignUtils;
import lombok.extern.slf4j.Slf4j;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

@Service
@Slf4j
public class PushDataService {

    @Resource
    private PushDataMessageMapper pushDataMessageMapper;

    @Resource
    private PushDataConfigMapper pushDataConfigMapper;

    @Resource
    private UserMapper userMapper;

    @Resource
    private UserVerifyMapper userVerifyMapper;

    @Resource
    private BasicService basicService;

    @Resource
    private AccountMapper accountMapper;

    @Resource
    private ThirdPartyUserMapper thirdPartyUserMapper;

    @Resource
    private MasterKeyUserConfigMapper masterKeyUserConfigMapper;

    @Autowired
    private ISequenceGenerator sequenceGenerator;

    @Autowired
    private OtcOrderConsumer otcOrderConsumer;

    @Autowired
    private MarginRiskAlarmConsumer marginRiskAlarmConsumer;

    @Resource
    private StatisticsTradeService statisticsTradeService;

    public static final MediaType MEDIA_TYPE = MediaType.parse("application/json; charset=utf-8");

    private static final String ORG_ID_CACHE_KEY = "ORG_ID_CACHE_KEY";

    private static final Integer HTTP_SUCCESS_CODE = 200;

    private static final String PUSH_DATA_FAIL_TASK_LOCK_KEY = "PUSH::DATA::FAIL::TASK::LOCK::KEY::";

    private static final String OVERLAY = "**********";

    private static final int START = 3;

    private static final int END = 7;

    private static final BigDecimal BIG_ORDER_VALUE = new BigDecimal("1000");

    @Resource
    private TicketReceiveConsumer ticketReceiveConsumer;

    @Resource
    private UserInfoProducer userInfoProducer;

    private Cache<String, Set<DataPushConfig>> orgCache = CacheBuilder
            .newBuilder()
            .maximumSize(200)
            .expireAfterWrite(1, TimeUnit.MINUTES)
            .build();

    private static OkHttpClient mOkHttpClient = new OkHttpClient.Builder()
            .addInterceptor(OkHttpPrometheusInterceptor.getInstance())
            .connectTimeout(2, TimeUnit.SECONDS)
            .writeTimeout(2, TimeUnit.SECONDS)
            .readTimeout(2, TimeUnit.SECONDS)
            .build();

    private final ExecutorService httpThreadPool = Executors.newFixedThreadPool(5);

    private final ExecutorService taskThreadPool = Executors.newFixedThreadPool(2);

    private final ExecutorService writeMQTaskThreadPool = Executors.newFixedThreadPool(4);

    private final ThreadPoolExecutor writeOrderNotifyMQThreadPool = new ThreadPoolExecutor(4, 4,
            120, TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(5000), new BasicThreadFactory.Builder().namingPattern("OrderNotifyMQ_%d").daemon(true).build(), (r, executor) ->
            log.info("OrderNotifyMQ requests discard..........!"));

    @Resource(name = "stringRedisTemplate")
    protected StringRedisTemplate redisTemplate;

    @Resource
    private SpotQuoteService spotQuoteService;

    @Scheduled(cron = "0/10 * * * * ?")
    public void refreshQuoteConsumer() {

        if (!BasicService.marginOrgIds.isEmpty()) {
            BasicService.marginOrgIds.forEach(orgId -> {
                marginRiskAlarmConsumer.bootMarginRiskAlarmConsumer(marginRiskAlarmConsumer.getMarginRiskAlarmTopic(orgId), orgId);
            });
        }
        Set<DataPushConfig> orgIdList = getOrgList();
        if (CollectionUtils.isEmpty(orgIdList)) {
            return;
        }
        orgIdList.forEach(broker -> {
            ticketReceiveConsumer.bootTicketConsumer("BKR_TKT_" + broker.getOrgId(), broker.getOrgId());
            otcOrderConsumer.bootOtcOrderConsumer(otcOrderConsumer.getOtcOrderTopic(broker.getOrgId()), broker.getOrgId());
            userInfoProducer.bootProducer(userInfoProducer.getUserRegisterTopic(broker.getOrgId()));
            userInfoProducer.bootProducer(userInfoProducer.getUserKycTopicPrefix(broker.getOrgId()));
            userInfoProducer.bootProducer(userInfoProducer.getOtcTradeTopicPrefix(broker.getOrgId()));
        });
    }

//    @Scheduled(cron = "0 */1 * * * ? ")
//    public void pushDataFailTask() throws Exception {
//        Set<DataPushConfig> orgIdList = getOrgList();
//        orgIdList.forEach(config -> {
//            Boolean locked = RedisLockUtils.tryLock(redisTemplate, PUSH_DATA_FAIL_TASK_LOCK_KEY + config.getOrgId(), 1 * 60 * 1000);
//            if (!locked) {
//                return;
//            }
//            log.info(" **** PUSH DATA TASK START **** ID **** {} *****", config.getOrgId());
//            List<DataPushMessage> messageList = this.pushDataMessageMapper.queryMessageList(config.getOrgId(), config.getRetryCount());
//            //推送补偿
//            taskPushServer(config.getOrgId(), messageList);
//            //无论成功失败次数都+1
//            messageList.forEach(message -> {
//                pushDataMessageMapper.addRetryCount(message.getClientReqId());
//            });
//        });
//    }

    public Set<DataPushConfig> getOrgList() {
        try {
            return orgCache.get(ORG_ID_CACHE_KEY, () -> {
                List<DataPushConfig> orgIdList = this.pushDataConfigMapper.queryAllOpen();
                Set<DataPushConfig> orgSet = new HashSet<>(orgIdList);
                return orgSet;
            });
        } catch (Exception e) {
            log.error("Push data get orgId from cache error {}", e);
            return new HashSet<>();
        }
    }

    public void userRegisterMessage(Long orgId, Long userId) {
        if (!checkIsOpen(orgId)) {
            return;
        }
        writeMQTaskThreadPool.submit(new Runnable() {
            @Override
            public void run() {
                UserMessage userMessage = toUserMessage(orgId, userId);
                if (Objects.isNull(userMessage)) {
                    return;
                }
                pushUserRegisterInfoToMQ(orgId, userMessage);
            }
        });
        taskThreadPool.submit(new Runnable() {
            @Override
            public void run() {
                UserMessage userMessage = toUserMessage(orgId, userId);
                if (Objects.isNull(userMessage)) {
                    return;
                }
                //直接推送 失败入库
                pushServer(Arrays.asList(createDataPushMessage(orgId, PushMessageType.REGISTER, new Gson().toJson(userMessage), userMessage.getUserId() + "0")));
            }
        });
    }

    private UserMessage toUserMessage(Long orgId, Long userId) {
        User user = userMapper.getByUserId(userId);
        if (user == null) {
            return null;
        }

        DataPushConfig config = pushDataConfigMapper.queryConfigOpenOrgId(orgId);
        if (config == null) {
            return null;
        }
        String email;
        String mobile;
        if (config.getEncrypt().equals(1)) {
            email = StringUtils.isNotEmpty(user.getEmail()) ? maskEmail(user.getEmail()) : "";
            mobile = StringUtils.isNotEmpty(user.getMobile()) ? maskMobile(user.getMobile()) : "";
        } else {
            email = StringUtils.isNotEmpty(user.getEmail()) ? user.getEmail() : "";
            mobile = StringUtils.isNotEmpty(user.getMobile()) ? user.getMobile() : "";
        }

        return UserMessage.builder()
                .id(user.getId() != null ? user.getId() : 0L)
                .orgId(user.getOrgId() != null ? user.getOrgId() : 0L)
                .userId(user.getUserId() != null ? user.getUserId() : 0L)
                .mobile(mobile)
                .email(email)
                .userStatus(user.getUserStatus() != null ? user.getUserStatus() : 0)
                .inviteUserId(user.getInviteUserId() != null ? user.getInviteUserId() : 0L)
                .inputInviteCode(StringUtils.isNotEmpty(user.getInputInviteCode()) ? user.getInputInviteCode() : "")
                .secondLevelInviteUserId(user.getSecondLevelInviteUserId() != null ? user.getSecondLevelInviteUserId() : 0L)
                .inviteCode(StringUtils.isNotEmpty(user.getInviteCode()) ? user.getInviteCode() : "")
                .nationalCode(StringUtils.isNotEmpty(user.getNationalCode()) ? user.getNationalCode() : "")
                .registerType(user.getRegisterType() != null ? user.getRegisterType() : 0)
                .ip(StringUtils.isNotEmpty(user.getIp()) ? user.getIp() : "")
                .bindGa(user.getBindGA() != null ? user.getBindGA() : 0)
                .bindTradePwd(user.getBindTradePwd() != null ? user.getBindTradePwd() : 0)
                .authType(user.getAuthType() != null ? user.getAuthType() : 0)
                .channel(StringUtils.isNotEmpty(user.getChannel()) ? user.getChannel() : "")
                .source(StringUtils.isNotEmpty(user.getSource()) ? user.getSource() : "")
                .platform(StringUtils.isNotEmpty(user.getPlatform()) ? user.getPlatform() : "")
                .language(StringUtils.isNotEmpty(user.getLanguage()) ? user.getLanguage() : "")
                .appBaseHeader(StringUtils.isNotEmpty(user.getAppBaseHeader()) ? user.getAppBaseHeader() : "")
                .registerTime(user.getCreated() != null ? user.getCreated() : 0L)
                .updated(user.getUpdated() != null ? user.getUpdated() : 0L)
                .created(user.getCreated() != null ? user.getCreated() : 0L)
                .signature("")
                .build();
    }

    public void userKycMessage(Long orgId, Long userId) {
        if (!checkIsOpen(orgId)) {
            return;
        }

        writeMQTaskThreadPool.submit(() -> {
            UserKycMessage userKycMessage = toUserKycMessage(orgId, userId);
            if (Objects.isNull(userKycMessage)) {
                return;
            }
            UserVerify userVerify = UserVerify.decrypt(userVerifyMapper.getByUserId(userId));
            if (userVerify == null) {
                return;
            }
            if (!userVerify.isSeniorVerifyPassed()) {
                return;
            }
            log.info("userKycMessage orgId {} userId {}  info {}", userKycMessage.getOrgId(), userKycMessage.getUserId(), new Gson().toJson(userKycMessage));
            pushUserKycInfoToMQ(orgId, userKycMessage);
        });
        taskThreadPool.submit(() -> {
            UserKycMessage userKycMessage = toUserKycMessage(orgId, userId);
            if (Objects.isNull(userKycMessage)) {
                return;
            }
            UserVerify userVerify = UserVerify.decrypt(userVerifyMapper.getByUserId(userId));
            if (userVerify == null) {
                return;
            }
            if (!userVerify.isSeniorVerifyPassed()) {
                return;
            }
            //直接推送 失败入库
            pushServer(Arrays.asList(createDataPushMessage(orgId, PushMessageType.KYC, new Gson().toJson(userKycMessage), userKycMessage.getUserId() + "1")));
        });
    }

    public void userLoginMessage(Header header, Long orgId, Long userId, LoginType loginType, Long logId) {
        if (!checkIsOpen(orgId)) {
            return;
        }
        Long currentTimestamp = System.currentTimeMillis();
        writeMQTaskThreadPool.submit(() -> {
            UserLoginMessagePO loginMsg = UserLoginMessagePO.builder()
                    .id(logId)
                    .orgId(orgId)
                    .userId(userId)
                    .loginDate(currentTimestamp)
                    .loginType(loginType.name())
                    .tokenFor(header.getPlatform().name())
                    .ip(header.getRemoteIp())
                    .build();
            pushUserLoginInfoToMQ(orgId, loginMsg);
        });
    }

    public void userSpotPlanOrderNotifyMessage(Long orgId, AccountTypeEnum accountType, io.bhex.broker.grpc.order.PlanSpotOrder planSpotOrder) {
        if (!checkIsOpen(orgId) || planSpotOrder == null || planSpotOrder.getOrderId() <= 0L) {
            return;
        }
        writeOrderNotifyMQThreadPool.submit(() -> {
            try {
                //检查是否需要通知(通知触发条件：1.大单 2.价格偏移30%以上)
                OrderType orderType = planSpotOrder.getOrderType();
                if (OrderType.LIMIT.equals(orderType) || OrderType.MARKET.equals(orderType)) {
                    int notifyType = getNotifyType(orgId, orderType, planSpotOrder.getSide(), new BigDecimal(planSpotOrder.getPrice()), new BigDecimal(planSpotOrder.getOrigQty())
                            , new BigDecimal(planSpotOrder.getTriggerPrice()), planSpotOrder.getQuoteTokenId());
                    if (notifyType > 0) {
                        SpotOrderNotifyPO notifyPo = SpotOrderNotifyPO.builder()
                                .planOrder(Boolean.TRUE)
                                .orgId(orgId)
                                .orderId(planSpotOrder.getOrderId())
                                .clientOrderId(planSpotOrder.getClientOrderId())
                                .accountId(planSpotOrder.getAccountId())
                                .accountType(accountType.getNumber())
                                .symbolId(planSpotOrder.getSymbolId())
                                .symbolName(planSpotOrder.getSymbolName())
                                .orderSide(planSpotOrder.getSide().getNumber())
                                .orderType(planSpotOrder.getOrderType().getNumber())
                                .price(planSpotOrder.getPrice())
                                .origQty(planSpotOrder.getOrigQty())
                                .quotePrice(planSpotOrder.getQuotePrice())
                                .triggerPrice(planSpotOrder.getTriggerPrice())
                                .notifyType(notifyType)
                                .build();
                        pushSpotOrderNotifyToMQ(orgId, notifyPo);
                    }
                }
            } catch (Exception e) {
                log.warn("userSpotPlanOrderNotifyMessage error!orgId:{}", orgId, e);
            }
        });
    }

    /**
     * triggerPrice 普通委托是当前行情价格，计划委托是触发时的行情价格
     */
    private int getNotifyType(Long orgId, OrderType orderType, OrderSide side,
                              BigDecimal price, BigDecimal origQty, BigDecimal triggerPrice, String quoteTokenId) {
        //计算订单价值
        boolean isBigOrderNotify = Boolean.FALSE;
        BigDecimal checkAmount;
        if (OrderType.MARKET.equals(orderType) && OrderSide.BUY.equals(side)) {
            checkAmount = origQty;
        } else if (OrderSide.BUY.equals(side)) {
            checkAmount = price.multiply(origQty);
        } else {
            //卖都是base的量,转quote的量
            price = OrderType.MARKET.equals(orderType) ? triggerPrice : price;
            checkAmount = price.multiply(origQty);
        }
        if ("USDT".equalsIgnoreCase(quoteTokenId)) {
            isBigOrderNotify = checkAmount.compareTo(BIG_ORDER_VALUE) >= 0;
        } else {
            //获取quoteToken的价值
            Rate rate = basicService.getV3Rate(orgId, quoteTokenId);
            if (rate != null && rate.getRatesMap().containsKey("USDT")) {
                BigDecimal usdtValue = DecimalUtil.toBigDecimal(rate.getRatesMap().get("USDT"));
                isBigOrderNotify = checkAmount.multiply(usdtValue).compareTo(BIG_ORDER_VALUE) >= 0;
            }
        }
        //检查是否是异常单（限价且下单价格偏移触发价格30%以上）偏差
        boolean isDeviationOrderNotify = Boolean.FALSE;
        if (OrderType.LIMIT.equals(orderType)) {
            if (price.compareTo(triggerPrice) > 0 && triggerPrice.compareTo(BigDecimal.ZERO) > 0) {
                isDeviationOrderNotify = price.divide(triggerPrice, 2, RoundingMode.DOWN).compareTo(new BigDecimal("1.3")) >= 0;
            } else if (triggerPrice.compareTo(BigDecimal.ZERO) > 0) {
                isDeviationOrderNotify = price.divide(triggerPrice, 2, RoundingMode.UP).compareTo(new BigDecimal("0.7")) <= 0;
            }
        }
        return (isBigOrderNotify ? 1 : 0) + (isDeviationOrderNotify ? 2 : 0);
    }

    public void userSpotOrderNotifyMessage(Long orgId, AccountTypeEnum accountType, io.bhex.broker.grpc.order.Order spotOrder) {
        if (!checkIsOpen(orgId) || spotOrder == null || spotOrder.getOrderId() <= 0L) {
            return;
        }
        writeOrderNotifyMQThreadPool.submit(() -> {
            try {
                //检查是否需要通知(通知触发条件：1.大单 2.价格偏移30%以上)
                OrderType orderType = spotOrder.getOrderType();
                if (OrderType.LIMIT.equals(orderType) || OrderType.MARKET.equals(orderType)) {
                    //获取当前行情价格
                    BigDecimal quotePrice = spotQuoteService.getCacheCurrentPrice(spotOrder.getSymbolId(), spotOrder.getExchangeId(), orgId);
                    int notifyType = getNotifyType(orgId, orderType, spotOrder.getOrderSide(), new BigDecimal(spotOrder.getPrice()), new BigDecimal(spotOrder.getOrigQty())
                            , quotePrice, spotOrder.getQuoteTokenId());
                    if (notifyType > 0) {
                        SpotOrderNotifyPO notifyPo = SpotOrderNotifyPO.builder()
                                .planOrder(Boolean.FALSE)
                                .orgId(orgId)
                                .orderId(spotOrder.getOrderId())
                                .clientOrderId(spotOrder.getClientOrderId())
                                .accountId(spotOrder.getAccountId())
                                .accountType(accountType.getNumber())
                                .symbolId(spotOrder.getSymbolId())
                                .symbolName(spotOrder.getSymbolName())
                                .orderSide(spotOrder.getOrderSide().getNumber())
                                .orderType(spotOrder.getOrderType().getNumber())
                                .price(spotOrder.getPrice())
                                .origQty(spotOrder.getOrigQty())
                                .quotePrice(quotePrice.stripTrailingZeros().toPlainString())
                                .triggerPrice(BigDecimal.ZERO.toPlainString())
                                .notifyType(notifyType)
                                .build();
                        pushSpotOrderNotifyToMQ(orgId, notifyPo);
                    }
                }
            } catch (Exception e) {
                log.warn("userSpotOrderNotifyMessage error!orgId:{}", orgId, e);
            }
        });
    }

    private UserKycMessage toUserKycMessage(Long orgId, Long userId) {
        UserVerify userVerify = userVerifyMapper.getByUserId(userId);
        if (userVerify == null) {
            return null;
        }
        if (!userVerify.getVerifyStatus().equals(2)) {
            return null;
        }

        DataPushConfig config = pushDataConfigMapper.queryConfigOpenOrgId(orgId);
        if (config == null) {
            return null;
        }

        String idCard;
        if (config.getEncrypt().equals(1)) {
            idCard = StringUtils.isNotEmpty(userVerify.getCardNo()) ? maskIdCard(userVerify.getCardNo()) : "";
        } else {
            userVerify = UserVerify.decrypt(userVerifyMapper.getByUserId(userId));
            idCard = StringUtils.isNotEmpty(userVerify.getCardNo()) ? userVerify.getCardNo() : "";
        }
        return UserKycMessage.builder()
                .id(userVerify.getId() != null ? userVerify.getId() : 0L)
                .orgId(userVerify.getOrgId() != null ? userVerify.getOrgId() : 0L)
                .userId(userVerify.getUserId() != null ? userVerify.getUserId() : 0L)
                .verifyStatus(userVerify.getVerifyStatus() != null ? userVerify.getVerifyStatus() : 0)
                .firstName(StringUtils.isNotEmpty(userVerify.getFirstName()) ? userVerify.getFirstName() : "")
                .secondName(StringUtils.isNotEmpty(userVerify.getSecondName()) ? userVerify.getSecondName() : "")
                .gender(userVerify.getGender() != null ? userVerify.getGender() : 0)
                .countryCode(StringUtils.isNotEmpty(userVerify.getCountryCode()) ? userVerify.getCountryCode() : "")
                .cardType(userVerify.getCardType() != null ? userVerify.getCardType() : 0)
                .cardNo(idCard)
                .created(userVerify.getCreated() != null ? userVerify.getCreated() : 0L)
                .updated(userVerify.getUpdated() != null ? userVerify.getUpdated() : 0L)
                .signature("")
                .build();
    }

    private void pushUserRegisterInfoToMQ(Long brokerId, UserMessage userMessage) {
        writeMessageToMQ(userInfoProducer.getUserRegisterTopic(brokerId), userMessage);
    }

    private void pushUserKycInfoToMQ(Long brokerId, UserKycMessage userKycMessage) {
        writeMessageToMQ(userInfoProducer.getUserKycTopicPrefix(brokerId), userKycMessage);
    }


    public void pushOtcTradeInfoToMQ(Long brokerId, String tradeMessage) {
        writeStringMessageToMQ(userInfoProducer.getOtcTradeTopicPrefix(brokerId), tradeMessage);
    }

    private void writeMessageToMQ(String topic, Object message) {
        MessageProducer producer = userInfoProducer.getProducerByTopic(topic);
        if (Objects.isNull(producer)) {
            log.error("Write User Info Message To MQ Error: producer null. topic => {}, message => {}.", topic, message);
            return;
        }
        if (StringUtils.isNotEmpty(topic) && Objects.nonNull(message)) {
            String jsonMsg = JsonUtil.defaultGson().toJson(message);
            try {
                Message msg = new Message(topic, jsonMsg.getBytes(RemotingHelper.DEFAULT_CHARSET));
                SendResult sendResult = producer.send(msg);
                if (sendResult == null) {
                    log.info("send Message timeout! message:{}", message);
                    throw new IllegalStateException("Result of send message is null");
                }
                if (SendStatus.SEND_OK != sendResult.getSendStatus()
                        && SendStatus.FLUSH_SLAVE_TIMEOUT != sendResult.getSendStatus()) {
                    log.info("send Message exception! message:{}", message);
                }
                log.info("writeMessageToMQ success message {} sendResult {}", new Gson().toJson(message), new Gson().toJson(sendResult));
            } catch (UnsupportedEncodingException e) {
                log.error("Write User Info Message To MQ Error: topic => {}, message => {}.", topic, message, e);
            }
        }
    }

    private void writeStringMessageToMQ(String topic, String message) {
        MessageProducer producer = userInfoProducer.getProducerByTopic(topic);
        if (Objects.isNull(producer)) {
            log.error("Write Otc Trade Message To MQ Error: producer null. topic => {}, message => {}.", topic, message);
            return;
        }
        if (StringUtils.isNotEmpty(topic) && Objects.nonNull(message)) {
            try {
                Message msg = new Message(topic, message.getBytes(RemotingHelper.DEFAULT_CHARSET));
                SendResult sendResult = producer.send(msg);
                if (sendResult == null) {
                    log.error("send OTC Message timeout! message:{}", message);
                    throw new IllegalStateException("Result of send message is null");
                }
                if (SendStatus.SEND_OK != sendResult.getSendStatus()
                        && SendStatus.FLUSH_SLAVE_TIMEOUT != sendResult.getSendStatus()) {
                    log.error("send OTC Message exception! message:{}", message);
                }
            } catch (UnsupportedEncodingException e) {
                log.error("Write Otc Trade Message To MQ Error: topic => {}, message => {}.", topic, message, e);
            }
        }
    }

    private void pushUserLoginInfoToMQ(Long brokerId, UserLoginMessagePO userLoginMessage) {
        writeMessageToMQ(userInfoProducer.getUserLoginTopic(brokerId), userLoginMessage);
    }

    private void pushSpotOrderNotifyToMQ(Long brokerId,  SpotOrderNotifyPO spotOrderNotifyPO) {
        writeMessageToMQ(userInfoProducer.getUserSpotOrderNotifyTopic(brokerId), spotOrderNotifyPO);
    }

    /**
     * 补偿任务 失败不更新数据状态 //TODO 应该更新重试次数 超过一定得重试次数 则不再补偿 成功则更新消息状态
     */
    public void taskPushServer(Long orgId, List<DataPushMessage> messages) {
        long startTime = System.currentTimeMillis();
        log.info("Task push server message start ****** ：total：{}  ******", messages.size());
        if (CollectionUtils.isEmpty(messages)) {
            return;
        }
        DataPushConfig config = this.pushDataConfigMapper.queryConfigOpenOrgId(orgId);
        if (config == null) {
            return;
        }

        Map<String, Object> stringObjectMap = new HashMap<>();
        stringObjectMap.put("code", HTTP_SUCCESS_CODE);
        stringObjectMap.put("size", messages.size());
        stringObjectMap.put("data", messages);
        String json = JSON.toJSONString(stringObjectMap);
        RequestBody body = RequestBody.create(MEDIA_TYPE, json);
        Request request = new Request.Builder()
                .url(config.getCallBackUrl())
                .post(body)
                .build();
        try (Response response = mOkHttpClient.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                log.info("Push server http fail json info {} response body {}", body, response.body().string());
                return;
            }
            String responseData = response.body().string();
            if (StringUtils.isEmpty(responseData)) {
                return;
            }

            PushDataResult pushDataResult = new PushDataResult();
            try {
                pushDataResult = new Gson().fromJson(responseData, PushDataResult.class);
            } catch (Exception ex) {
                log.error("Task push conversion error {} ,result {}", ex, responseData);
                return;
            }

            if (!pushDataResult.getCode().equals("200")) {
                return;
            }

            if (CollectionUtils.isEmpty(pushDataResult.getData())) {
                return;
            }
            List<PushResult> resultList = pushDataResult.getData();
            List<String> successMessage = resultList.stream()
                    .map(PushResult::getClientReqId)
                    .collect(Collectors.toList());
            if (CollectionUtils.isNotEmpty(successMessage)) {
                //把状态修改为已发送
                successMessage.forEach(k -> {
                    this.pushDataMessageMapper.updateMessageStatus(k);
                });
            }

            long endTime = System.currentTimeMillis();
            log.info("Push message success  ****** time：{} total：{} success：{} ******", (endTime - startTime), messages.size(), successMessage.size());
        } catch (IOException e) {
            log.error("Push server io exception error, target url:" + config.getCallBackUrl(), e);
        }
    }

    /**
     * 消息推送 成功则不做处理 失败则落盘 异步处理
     */
    public void pushServer(List<DataPushMessage> messages) {
        pushServer(null, messages);
    }

    public void pushServer(String messageCallbackUrl, List<DataPushMessage> messages) {
        long startTime = System.currentTimeMillis();
        log.info("Push server message start ****** ：total：{}  ******", messages.size());
        if (CollectionUtils.isEmpty(messages)) {
            return;
        }
        Map<Long, List<DataPushMessage>> pushMap = messages.stream().collect(Collectors.groupingBy(DataPushMessage::getOrgId));
        pushMap.keySet().forEach(orgId -> {
            if (!checkIsOpen(orgId)) {
                return;
            }
            httpThreadPool.execute(() -> {
                String callBackUrl = messageCallbackUrl;
                if (Strings.isNullOrEmpty(callBackUrl)) {
                    DataPushConfig config = pushDataConfigMapper.queryConfigOpenOrgId(orgId);
                    if (config == null || StringUtils.isEmpty(config.getCallBackUrl())) {
                        log.info("{} Push server cannot find config", orgId);
                        return;
                    }
                    callBackUrl = config.getCallBackUrl();
                }

                List<DataPushMessage> messageList = pushMap.get(orgId);
                Map<String, Object> dataObjMap = new HashMap<>();
                dataObjMap.put("code", HTTP_SUCCESS_CODE);
                dataObjMap.put("size", messageList.size());
                dataObjMap.put("data", messageList);

                RequestBody body = RequestBody.create(MEDIA_TYPE, JsonUtil.defaultGson().toJson(dataObjMap));
                Request request = new Request.Builder()
                        .url(callBackUrl)
                        .post(body)
                        .build();

                try (Response response = mOkHttpClient.newCall(request).execute()) {
                    if (!response.isSuccessful()) {
                        log.warn("rest push: orgId:{} message:{} response code is {}", orgId, JsonUtil.defaultGson().toJson(dataObjMap), response.code());
                        batchSaveMessageInfo(messageList);
                        return;
                    }
                    String responseData = null;
                    if (response.body() == null || StringUtils.isEmpty(responseData = response.body().string())) {
                        log.warn("rest push: orgId:{} message:{} response body is null", orgId, JsonUtil.defaultGson().toJson(dataObjMap));
                        batchSaveMessageInfo(messageList);
                        return;
                    }

                    PushDataResult pushDataResult;
                    try {
                        pushDataResult = new Gson().fromJson(responseData, PushDataResult.class);
                    } catch (Exception ex) {
                        log.error("rest push: orgId:{} message:{} analyze response data:{} exception", orgId, JsonUtil.defaultGson().toJson(dataObjMap), responseData);
                        batchSaveMessageInfo(messageList);
                        return;
                    }

                    List<PushResult> resultList = pushDataResult.getData();
                    if (!pushDataResult.getCode().equals(String.valueOf(HTTP_SUCCESS_CODE))
                            || CollectionUtils.isEmpty(resultList)) {
                        batchSaveMessageInfo(messageList);
                        return;
                    }

                    //返回的数据都是已正常处理
                    List<String> successMessage = resultList.stream()
                            .map(PushResult::getClientReqId)
                            .collect(Collectors.toList());
                    successMessage.forEach(s1 -> log.info("Push message success clientReqId {}", s1));

                    Map<String, DataPushMessage> messageMap
                            = messages.stream().collect(Collectors.toMap(DataPushMessage::getClientReqId, Function.identity()));
                    //成功的移除
                    successMessage.forEach(messageMap::remove);

                    //失败的入库
                    if (messageMap.size() > 0) {
                        List<DataPushMessage> dataPushMessages = new ArrayList<>();
                        messageMap.keySet().forEach(k -> {
                            dataPushMessages.add(messageMap.get(k));
                        });
                        //批量入库再做补偿
                        batchSaveMessageInfo(dataPushMessages);
                    }

                    long endTime = System.currentTimeMillis();
                    log.info("Push message success  ****** time：{} total：{} success：{} ******", (endTime - startTime), messages.size(), successMessage.size());
                } catch (IOException e) {
                    //则入库等待重试
                    batchSaveMessageInfo(messageList);
                    log.warn("Push server io exception error, target url: " + callBackUrl, e);
                }
            });
        });
    }

    /**
     * 批量落盘消息
     */
    public void batchSaveMessageInfo(List<DataPushMessage> messages) {
        if (CollectionUtils.isNotEmpty(messages)) {
            this.pushDataMessageMapper.insertList(messages);
        }
    }

    /**
     * 构建发送消息
     */
    public DataPushMessage createDataPushMessage(Long orgId, PushMessageType messageType, String message, String clientReqId) {
        if (StringUtils.isEmpty(message) || "null".equals(message)) {
            return null;
        }
        DataPushMessage dataPushMessage = new DataPushMessage();
        dataPushMessage.setClientReqId(clientReqId);
        dataPushMessage.setMessageStatus(0);
        dataPushMessage.setMessageType(messageType.value());
        dataPushMessage.setMessage(message);
        dataPushMessage.setCreateTime(new Date());
        dataPushMessage.setPushTime(new Date());
        dataPushMessage.setUpdateTime(new Date());
        dataPushMessage.setOrgId(orgId);
        dataPushMessage.setRetryCount(0);
        return dataPushMessage;
    }

    /**
     * 是否有推送权限
     */
    private Boolean checkIsOpen(Long orgId) {
        Set<DataPushConfig> configList = getOrgList();
        DataPushConfig dataPushConfig
                = configList.stream().filter(config -> config.getOrgId().equals(orgId)).findFirst().orElse(null);
        if (dataPushConfig != null) {
            return Boolean.TRUE;
        }
        return Boolean.FALSE;
    }


    /**
     * 签名
     */
    private static String sign(TreeMap<String, Object> stringTreeMap, String secret) {
        try {
            String originalStr = Joiner.on("&").withKeyValueSeparator("=").join(stringTreeMap);
            Mac sha256_HMAC = Mac.getInstance("HmacSHA256");
            SecretKeySpec secretKeySpec = new SecretKeySpec(secret.getBytes(), "HmacSHA256");
            sha256_HMAC.init(secretKeySpec);
            return new String(Hex.encodeHex(sha256_HMAC.doFinal(originalStr.getBytes())));
        } catch (Exception e) {
            throw new RuntimeException("Sign error.", e);
        }
    }

    /**
     * 手机号打码 保留前3位后4位
     */
    public static String maskMobile(String content) {
        if (org.springframework.util.StringUtils.isEmpty(content)) {
            return "";
        }
        return StringUtils.overlay(content, OVERLAY, START, END);
    }


    /**
     * 邮箱打码
     */
    public static String maskEmail(String email) {
        if (StringUtils.isEmpty(email)) {
            return "";
        }
        String at = "@";
        if (!email.contains(at)) {
            return email;
        }
        int length = StringUtils.indexOf(email, at);
        String content = StringUtils.substring(email, 0, length);
        String mask = StringUtils.overlay(content, OVERLAY, 2, length);
        return mask + StringUtils.substring(email, length);
    }

    /**
     * 身份证打码 保留前4位后4位
     */
    public static String maskIdCard(String idCard) {
        if (StringUtils.isEmpty(idCard)) {
            return "";
        }
        return StringUtils.overlay(idCard, OVERLAY, 4, 14);
    }

    public void registerHistoryData(Long orgId) {
        int page = 0;
        int size = 100;
        boolean next = true;
        while (next) {
            int start = page * size;
            List<User> users = this.userMapper.getUserList(orgId, start, size);
            if (CollectionUtils.isEmpty(users)) {
                next = false;
                continue;
            }
            List<DataPushMessage> dataPushMessages = createRegisterList(orgId, users);
            List<List<DataPushMessage>> messageList = Lists.partition(dataPushMessages, 30);
            messageList.forEach(messages -> {
                pushServer(messages);
            });
            page = page + 1;
        }
    }

    private List<DataPushMessage> createRegisterList(Long orgId, List<User> users) {
        List<DataPushMessage> dataPushMessages = new ArrayList<>();
        DataPushConfig config = pushDataConfigMapper.queryConfigOpenOrgId(orgId);
        if (config == null) {
            return new ArrayList<>();
        }
        users.forEach(user -> {
            String email;
            String mobile;
            if (config.getEncrypt().equals(1)) {
                email = StringUtils.isNotEmpty(user.getEmail()) ? maskEmail(user.getEmail()) : "";
                mobile = StringUtils.isNotEmpty(user.getMobile()) ? maskMobile(user.getMobile()) : "";
            } else {
                email = StringUtils.isNotEmpty(user.getEmail()) ? user.getEmail() : "";
                mobile = StringUtils.isNotEmpty(user.getMobile()) ? user.getMobile() : "";
            }
            UserMessage userMessage = UserMessage.builder()
                    .id(user.getId() != null ? user.getId() : 0L)
                    .orgId(user.getOrgId() != null ? user.getOrgId() : 0L)
                    .userId(user.getUserId() != null ? user.getUserId() : 0L)
                    .mobile(mobile)
                    .email(email)
                    .userStatus(user.getUserStatus() != null ? user.getUserStatus() : 0)
                    .inviteUserId(user.getInviteUserId() != null ? user.getInviteUserId() : 0L)
                    .inputInviteCode(StringUtils.isNotEmpty(user.getInputInviteCode()) ? user.getInputInviteCode() : "")
                    .secondLevelInviteUserId(user.getSecondLevelInviteUserId() != null ? user.getSecondLevelInviteUserId() : 0L)
                    .inviteCode(StringUtils.isNotEmpty(user.getInviteCode()) ? user.getInviteCode() : "")
                    .nationalCode(StringUtils.isNotEmpty(user.getNationalCode()) ? user.getNationalCode() : "")
                    .registerType(user.getRegisterType() != null ? user.getRegisterType() : 0)
                    .ip(StringUtils.isNotEmpty(user.getIp()) ? user.getIp() : "")
                    .bindGa(user.getBindGA() != null ? user.getBindGA() : 0)
                    .bindTradePwd(user.getBindTradePwd() != null ? user.getBindTradePwd() : 0)
                    .authType(user.getAuthType() != null ? user.getAuthType() : 0)
                    .channel(StringUtils.isNotEmpty(user.getChannel()) ? user.getChannel() : "")
                    .source(StringUtils.isNotEmpty(user.getSource()) ? user.getSource() : "")
                    .platform(StringUtils.isNotEmpty(user.getPlatform()) ? user.getPlatform() : "")
                    .language(StringUtils.isNotEmpty(user.getLanguage()) ? user.getLanguage() : "")
                    .appBaseHeader(StringUtils.isNotEmpty(user.getAppBaseHeader()) ? user.getAppBaseHeader() : "")
                    .registerTime(user.getCreated() != null ? user.getCreated() : 0L)
                    .updated(user.getUpdated() != null ? user.getUpdated() : 0L)
                    .created(user.getCreated() != null ? user.getCreated() : 0L)
                    .signature("")
                    .build();
            dataPushMessages.add(createDataPushMessage(orgId, PushMessageType.REGISTER, new Gson().toJson(userMessage), userMessage.getUserId() + "0"));
        });
        return dataPushMessages;
    }

    public void kycHistoryData(Long orgId) {
        int page = 0;
        int size = 100;
        boolean next = true;
        while (next) {
            int start = page * size;
            List<UserVerify> userVerifyList = this.userVerifyMapper.getUserVerifyListByPage(orgId, start, size);
            if (CollectionUtils.isEmpty(userVerifyList)) {
                next = false;
                continue;
            }
            List<DataPushMessage> dataPushMessages = createKycList(orgId, userVerifyList);
            List<List<DataPushMessage>> messageList = Lists.partition(dataPushMessages, 30);
            messageList.forEach(messages -> {
                pushServer(messages);
            });
            page = page + 1;
        }
    }

    public List<DataPushMessage> createKycList(Long orgId, List<UserVerify> verifies) {
        List<DataPushMessage> dataPushMessages = new ArrayList<>();
        DataPushConfig config = pushDataConfigMapper.queryConfigOpenOrgId(orgId);
        if (config == null) {
            return new ArrayList<>();
        }
        verifies.forEach(originUserVerify -> {
            UserVerify userVerify = UserVerify.decrypt(originUserVerify);
            if (!userVerify.isSeniorVerifyPassed()) {
                return;
            }
            String idCard;
            if (config.getEncrypt().equals(1)) {
                idCard = StringUtils.isNotEmpty(userVerify.getCardNo()) ? maskIdCard(userVerify.getCardNo()) : "";
            } else {
                idCard = StringUtils.isNotEmpty(userVerify.getCardNo()) ? userVerify.getCardNo() : "";
            }
            UserKycMessage userKycMessage = UserKycMessage.builder()
                    .id(userVerify.getId() != null ? userVerify.getId() : 0L)
                    .orgId(userVerify.getOrgId() != null ? userVerify.getOrgId() : 0L)
                    .userId(userVerify.getUserId() != null ? userVerify.getUserId() : 0L)
                    .verifyStatus(userVerify.getVerifyStatus() != null ? userVerify.getVerifyStatus() : 0)
                    .firstName(StringUtils.isNotEmpty(userVerify.getFirstName()) ? userVerify.getFirstName() : "")
                    .secondName(StringUtils.isNotEmpty(userVerify.getSecondName()) ? userVerify.getSecondName() : "")
                    .gender(userVerify.getGender() != null ? userVerify.getGender() : 0)
                    .countryCode(StringUtils.isNotEmpty(userVerify.getCountryCode()) ? userVerify.getCountryCode() : "")
                    .cardType(userVerify.getCardType() != null ? userVerify.getCardType() : 0)
                    .cardNo(idCard)
                    .created(userVerify.getCreated() != null ? userVerify.getCreated() : 0L)
                    .updated(userVerify.getUpdated() != null ? userVerify.getUpdated() : 0L)
                    .signature("")
                    .build();
            dataPushMessages.add(createDataPushMessage(orgId, PushMessageType.KYC, new Gson().toJson(userKycMessage), userKycMessage.getUserId() + "1"));
        });
        return dataPushMessages;
    }

    public void tradeHistoryData(Long orgId, String time) {
        int page = 0;
        int size = 100;
        boolean next = true;
        while (next) {
            int start = page * size;
            List<StatisticsTradeAllDetail> tradeAllDetails = statisticsTradeService.queryTradeDetailByOrgId(orgId, time, start, size);
            if (CollectionUtils.isEmpty(tradeAllDetails)) {
                next = false;
                continue;
            }
            List<DataPushMessage> dataPushMessages = createTradeDetailList(orgId, tradeAllDetails);
            List<List<DataPushMessage>> messageList = Lists.partition(dataPushMessages, 30);
            messageList.forEach(messages -> {
                pushServer(messages);
            });
            page = page + 1;
        }
    }


    public List<DataPushMessage> createTradeDetailList(Long orgId, List<StatisticsTradeAllDetail> tradeAllDetailList) {
        List<DataPushMessage> dataPushMessages = new ArrayList<>();
        DataPushConfig config = pushDataConfigMapper.queryConfigOpenOrgId(orgId);
        if (config == null) {
            return new ArrayList<>();
        }

        tradeAllDetailList.stream().forEach(trade -> {
            Symbol symbol = basicService.getOrgSymbol(orgId, trade.getSymbolId());
            if (symbol == null) {
                log.error("Push data build user trade detail message fail not find symbol orgId {} symbolId {} orderId {}",
                        orgId, trade.getSymbolId(), trade.getOrderId());
                throw new BrokerException(BrokerErrorCode.SYSTEM_ERROR);
            }

            Rate fxRate = basicService.getV3Rate(orgId, symbol.getQuoteTokenId());
            if (fxRate == null) {
                throw new BrokerException(BrokerErrorCode.EXCHANGE_RATE_NOT_EXIST);
            }

            BigDecimal usdAmount = trade.getAmount().multiply(DecimalUtil.toBigDecimal(fxRate.getRatesMap().get("USD")))
                    .stripTrailingZeros()
                    .setScale(8, RoundingMode.DOWN);

            String feeTokenId = trade.getSide().equals(0) ? symbol.getBaseTokenId() : symbol.getQuoteTokenId();
            Rate feeRate = basicService.getV3Rate(orgId, feeTokenId);
            if (feeRate == null) {
                throw new BrokerException(BrokerErrorCode.EXCHANGE_RATE_NOT_EXIST);
            }

            BigDecimal usdFee = trade.getTokenFee()
                    .multiply(DecimalUtil.toBigDecimal(feeRate.getRatesMap().get("USD")))
                    .stripTrailingZeros()
                    .setScale(8, RoundingMode.DOWN);
            UserTradeDetailMessage tradeDetailMessage = UserTradeDetailMessage.builder()
                    .accountId(trade.getAccountId())
                    .userId(trade.getBrokerUserId())
                    .quoteToken(symbol.getQuoteTokenId())
                    .baseToken(symbol.getBaseTokenId())
                    .amount(trade.getAmount())
                    .symbolId(trade.getSymbolId())
                    .isMaker(trade.getIsMaker())
                    .orderId(trade.getOrderId())
                    .price(trade.getPrice())
                    .quantity(trade.getQuantity())
                    .feeToken(feeTokenId)
                    .fee(trade.getTokenFee())
                    .usdFee(usdFee)
                    .usdAmount(usdAmount)
                    .createTime(trade.getCreatedAt().getTime())
                    .side(trade.getSide())
                    .ticketId(trade.getTicketId())
                    .targetOrderId(0l)
                    .targetAccountId(0l)
                    .targetUserId(0l)
                    .signature("")
                    .build();
            DataPushMessage dataPushMessage = new DataPushMessage();
            if (trade.getIsMaker().equals(1)) {
                dataPushMessage = createDataPushMessage(orgId, PushMessageType.TRADE_SPOT, new Gson().toJson(tradeDetailMessage), trade.getTicketId() + "1");
            } else if (trade.getIsMaker().equals(0)) {
                dataPushMessage = createDataPushMessage(orgId, PushMessageType.TRADE_SPOT, new Gson().toJson(tradeDetailMessage), trade.getTicketId() + "0");
            }
            if (dataPushMessage != null) {
                dataPushMessages.add(dataPushMessage);
            }
        });
        return dataPushMessages;
    }

    /**
     * 爆仓短信对外的call back 失败会自动补偿 直到成功
     */
    public void futuresMessagePush(Long orgId, FuturesMessage futuresMessage) {
        try {
            log.info("Futures message push orgId {} userId {}  message json info {}", futuresMessage.getOrgId(), futuresMessage.getUserId(), JSON.toJSONString(futuresMessage));
            //根据accountId获取到对应的userId和三方用户id
            Account account = accountMapper.getAccountByAccountId(futuresMessage.getAccountId());
            if (account == null) {
                return;
            }
            ThirdPartyUser thirdPartyUser
                    = thirdPartyUserMapper.getByUserId(futuresMessage.getOrgId(), account.getUserId());
            if (thirdPartyUser == null) {
                log.info("Futures message push not find third party user orgId {} userId {}", futuresMessage.getOrgId(), futuresMessage.getUserId());
                return;
            }

            MasterKeyUserConfig config = masterKeyUserConfigMapper.selectMasterKeyUserConfigByUserId(futuresMessage.getOrgId(), thirdPartyUser.getMasterUserId());
            if (config == null || Strings.isNullOrEmpty(config.getCallback())) {
                log.info("Futures message push not find master key user config orgId {} masterUserId {}", futuresMessage.getOrgId(), thirdPartyUser.getMasterUserId());
                return;
            }
            futuresMessage.setUserId(account.getUserId());
            futuresMessage.setThirdUserId(thirdPartyUser.getThirdUserId());
            Long messageId = sequenceGenerator.getLong();
            futuresMessage.setMessageId(messageId);
            DataPushMessage pushMessage = createDataPushMessage(orgId, PushMessageType.FUTURES_MESSAGE, new Gson().toJson(futuresMessage), String.valueOf(messageId));
            pushMessage.setCallbackUrl(config.getCallback());
            log.info("Futures message push pushMessage json {}", new Gson().toJson(pushMessage));
            pushServer(config.getCallback(), Lists.newArrayList(pushMessage));
        } catch (Exception ex) {
            log.error("Futures message push futuresMessage {} error {} ", new Gson().toJson(futuresMessage), ex);
        }
    }


    public static void main(String[] args) {
        try {
            UserMessage userInfo = UserMessage
                    .builder()
                    .id(83242883087859712L)
                    .userId(83242883087859712L)
                    .inviteUserId(83242883087859712L)
                    .nationalCode("86")
                    .orgId(6001L)
                    .registerTime(System.currentTimeMillis())
                    .created(System.currentTimeMillis())
                    .updated(System.currentTimeMillis())
                    .build();


            UserKycMessage userVerify = UserKycMessage
                    .builder()
                    .id(215926901586526208L)
                    .userId(215926901586526208L)
                    .verifyStatus(2)
                    .firstName("yue")
                    .secondName("hao")
                    .gender(1)
                    .updated(System.currentTimeMillis())
                    .created(System.currentTimeMillis())
                    .orgId(6001L)
                    .cardNo("12343232")
                    .signature(SignUtils.encryptDataWithAES("bobo", "bobo"))
                    .build();

            UserTradeDetailMessage userTradeDetailMessage = UserTradeDetailMessage
                    .builder()
                    .id(86937183730532352L)
                    .userId(86937183730532352L)
                    .accountId(86937183730532352L)
                    .amount(new BigDecimal("0.999999999"))
                    .baseToken("")
                    .quoteToken("")
                    .fee(new BigDecimal("0.00001"))
                    .feeToken("USDT")
                    .createTime(System.currentTimeMillis())
                    .isMaker(1)
                    .orderId(86937183730532352L)
                    .price(new BigDecimal("0.001"))
                    .symbolId("")
                    .quantity(new BigDecimal("1.01"))
                    .usdAmount(new BigDecimal("0.001"))
                    .signature(SignUtils.encryptDataWithAES("bobo", "bobo"))
                    .build();


            FuturesMessage futuresMessage1 = FuturesMessage.builder()
                    .accountId(83242883087859712L)
                    .liquidationType("LIQUIDATION_ALERT")
                    .marginRatePercent("15%")
                    .orgId(6001L)
                    .thirdUserId("215926901586526208")
                    .userId(86937183730532352L)
                    .symbolId("BTC-PERP-BUSDT")
                    .isLong(true)
                    .build();

            List<DataPushMessage> messages = new ArrayList<>();
            DataPushMessage userMessage = new DataPushMessage();
            userMessage.setClientReqId("97351718878576640");//消息唯一ID
            userMessage.setMessageType("USER.REGISTER");
            userMessage.setMessage(JSON.toJSONString(userInfo));
//            messages.add(userMessage);

            DataPushMessage keyMessage = new DataPushMessage();
            keyMessage.setClientReqId("97351718878576640");//消息唯一ID
            keyMessage.setMessageType("USER.KYC");
            keyMessage.setMessage(JSON.toJSONString(userVerify));
//            messages.add(keyMessage);

            DataPushMessage tradeMessage = new DataPushMessage();
            tradeMessage.setClientReqId("97351718878576640");//消息唯一ID
            tradeMessage.setMessageType("TRADE.SPOT");
            tradeMessage.setMessage(JSON.toJSONString(userTradeDetailMessage));
//            messages.add(tradeMessage);


            DataPushMessage futuresMessage = new DataPushMessage();
            futuresMessage.setClientReqId("97351718878576640");//消息唯一ID
            futuresMessage.setMessageType(PushMessageType.FUTURES_MESSAGE.name());
            futuresMessage.setMessage(JSON.toJSONString(futuresMessage1));
            futuresMessage.setCreateTime(new Date());
            futuresMessage.setRetryCount(0);
            futuresMessage.setMessageStatus(0);
            futuresMessage.setPushTime(new Date());
            messages.add(futuresMessage);


            Map<String, Object> parameterMap = new HashMap<>();
            parameterMap.put("code", HTTP_SUCCESS_CODE);
            parameterMap.put("size", messages.size());
            parameterMap.put("data", messages);

            log.info("message {} ", JSON.toJSONString(parameterMap));
        } catch (Exception ex) {
            log.info("error", ex);
        }

        long startTime = System.currentTimeMillis();
        log.info(maskMobile("13811532884"));
        long endTime = System.currentTimeMillis();
        log.info("time {}", (endTime - startTime));


        long start = System.currentTimeMillis();
        log.info(maskEmail("goo_mao@163.com"));
        long end = System.currentTimeMillis();
        log.info("time {}", (end - start));


        long s = System.currentTimeMillis();
        log.info(maskIdCard("372922198812143971"));
        long e = System.currentTimeMillis();
        log.info("time {}", (e - s));

        System.out.println("200".equals(String.valueOf(200)));
    }

}
