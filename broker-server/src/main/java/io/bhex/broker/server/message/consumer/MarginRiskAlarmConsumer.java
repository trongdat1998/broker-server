package io.bhex.broker.server.message.consumer;

import com.google.gson.JsonObject;
import io.bhex.base.margin.*;
import io.bhex.base.margin.cross.CrossLoanPosition;
import io.bhex.base.margin.cross.CrossLoanPositionReply;
import io.bhex.base.margin.cross.CrossLoanPositionRequest;
import io.bhex.base.mq.config.MQProperties;
import io.bhex.base.proto.BaseRequest;
import io.bhex.base.proto.DecimalUtil;
import io.bhex.base.quote.Rate;
import io.bhex.broker.common.util.JsonUtil;
import io.bhex.broker.grpc.common.Header;
import io.bhex.broker.grpc.margin.RiskConfig;
import io.bhex.broker.server.domain.NoticeBusinessType;
import io.bhex.broker.server.grpc.client.service.GrpcMarginService;
import io.bhex.broker.server.grpc.server.service.*;
import io.bhex.broker.server.message.MessageMarginRiskAlarmDetail;
import io.bhex.broker.server.message.service.NotifyUserStrategy;
import io.bhex.broker.server.model.Account;
import io.bhex.broker.server.model.Broker;
import io.bhex.broker.server.model.LoginLog;
import io.bhex.broker.server.model.User;
import io.bhex.broker.server.primary.mapper.AccountMapper;
import io.bhex.broker.server.primary.mapper.BrokerMapper;
import io.bhex.broker.server.primary.mapper.LoginLogMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * @author JinYuYuan
 * @description
 * @date 2020-06-30 10:20
 */
@Slf4j
@Component
public class MarginRiskAlarmConsumer {
    @Resource
    MQProperties mqProperties;

    @Resource
    private NoticeTemplateService noticeTemplateService;

    @Resource
    private UserService userService;

    @Resource
    private BasicService basicService;

    @Resource
    private AccountMapper accountMapper;

    @Resource
    private LoginLogMapper loginLogMapper;
    @Resource
    private GrpcMarginService grpcMarginService;
    @Resource
    private BrokerMapper brokerMapper;

    @Resource
    private MarginPositionService marginPositionService;

    private final static String MARGIN_RISK_TOPIC_PREFIX = "MARGIN_RISK_ALARM_";

    private static final String NOTIFY_MARGIN_RISK_ALARM_CONSUMER_GROUP = "_NOTIFY_MARGIN_RISK_ALARM_CONSUMER_GROUP_RESEND";

    private static final Map<String, DefaultMQPushConsumer> consumerMap = new HashMap<>();

    private static final String LANG_ZH_CN = "zh_CN";
    private static final String LANG_EN_US = "en_US";

    public void bootMarginRiskAlarmConsumer(String topic, Long brokerId) {
        try {
            //判断当前机构是否可用
            Broker broker = brokerMapper.getByOrgIdAndStatus(brokerId);
            if (broker == null) {
                if (consumerMap.containsKey(topic)) {
                    DefaultMQPushConsumer consumer = consumerMap.get(topic);
                    consumer.shutdown();
                    consumerMap.remove(topic);
                }
                return;
            }
            DefaultMQPushConsumer consumer = consumerMap.get(topic);
            if (consumer == null) {
                consumer = createConsumer(topic, brokerId);
                if (consumer != null) {
                    consumerMap.put(topic, consumer);
                    log.info("bootMarginRiskAlarmConsumer done. topic: {}", topic);
                } else {
                    log.error("bootMarginRiskAlarmConsumer error. topic: {}", topic);
                }
            }
        } catch (Exception e) {
            log.error(String.format("bootMarginRiskAlarmConsumer error. topic: %s", topic), e);
        }
    }

    private DefaultMQPushConsumer createConsumer(String topic, Long brokerId) {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(brokerId + NOTIFY_MARGIN_RISK_ALARM_CONSUMER_GROUP);
        consumer.setConsumeThreadMax(20);
        consumer.setNamesrvAddr(mqProperties.getNameServers());
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.setMessageModel(MessageModel.CLUSTERING);
        try {
            consumer.subscribe(topic, brokerId + "");
            consumer.registerMessageListener((MessageListenerConcurrently) (list, consumeConcurrentlyContext) -> {
                try {
                    handleMessage(brokerId, list);
                } catch (Exception e) {
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            });
            consumer.start();
        } catch (Exception e) {
            log.error("listener error", e);
            consumer.shutdown();
            consumer = null;
        }
        return consumer;
    }

    public void handleMessage(Long brokerId, List<MessageExt> messages) throws Exception {
        for (MessageExt message : messages) {
            String alarmJson = "";
            try {
                byte[] bytes = message.getBody();
                alarmJson = new String(bytes, "UTF-8");
                log.info("MarginRiskAlarmConsumer message :{}", alarmJson);
                MessageMarginRiskAlarmDetail alarm = JsonUtil.defaultGson().fromJson(alarmJson, MessageMarginRiskAlarmDetail.class);
                Account account = accountMapper.getAccountByAccountId(alarm.getAccountId());
                if (account == null) {
                    log.error("MarginRiskAlarmConsumer: can not find account by accountId: {}", alarm.getAccountId());
                    return;
                }
                User user = userService.getUser(account.getUserId());
                if (user == null) {
                    log.error("MarginRiskAlarmConsumer: can not find user by userId: {}", account.getUserId());
                    return;
                }

                // 获取语言环境
                String language = getUserLanguage(user);
                String safety = new BigDecimal(alarm.getSafety()).multiply(new BigDecimal("100")).stripTrailingZeros().toPlainString() + "%";
                JsonObject jsonObject = new JsonObject();
                jsonObject.addProperty("safety", safety);
                NoticeBusinessType noticeBusinessType = null;
                switch (alarm.getLoanRiskNotifyType()) {
                    case "MARGIN_WARN":
                        noticeBusinessType = NoticeBusinessType.MARGIN_WARN;
                        break;
                    case "MARGIN_APPEND":
                        noticeBusinessType = NoticeBusinessType.MARGIN_APPEND;
                        break;
                    case "MARGIN_LIQUIDATION_ALERT":
                        sendAdminNotify(user.getOrgId(), account.getAccountId(), user, NoticeBusinessType.ADMIN_LIQUIDATION_ALERT);
                        noticeBusinessType = NoticeBusinessType.MARGIN_LIQUIDATION_ALERT;
                        break;
                    case "MARGIN_LIQUIDATED_NOTIFY":
                        sendAdminNotify(user.getOrgId(), account.getAccountId(), user, NoticeBusinessType.ADMIN_LIQUIDATED_NOTIFY);
                        noticeBusinessType = NoticeBusinessType.MARGIN_LIQUIDATED_NOTIFY;
                        break;
                    case "LIQUIDATED_TO_UPDATE_MARGIN":
                        //完全强平结束，需更新初始保证金
                        marginPositionService.calRepaidMarginAsset(user.getOrgId(), user.getUserId(), account.getAccountId(), 0);
                        break;
                    default:
                        log.error("sendMarginRiskAlarmMessage error: Unkonwn message type: {}  orgId:{}", alarm.getLoanRiskNotifyType(),user.getOrgId());

                }
                NotifyUserStrategy notifyStrategy = getNotifyStrategy(user);
                if (noticeBusinessType != null) {
                    switch (notifyStrategy) {
                        case MOBILE:
                            noticeTemplateService.sendSmsNotice(toHeader(user, language), user.getUserId(), noticeBusinessType,
                                    user.getNationalCode(), user.getMobile(), jsonObject);
                            noticeTemplateService.sendBizPushNotice(toHeader(user, language), user.getUserId(), noticeBusinessType,
                                    message.getMsgId(), jsonObject, null);
                            log.info("sendMarginRiskAlarmMessage {} ok. userId:{} language: {}", noticeBusinessType.name(), user.getUserId(), language);
                            break;
                        case EMAIL:
                            noticeTemplateService.sendEmailNotice(toHeader(user, language), user.getUserId(), noticeBusinessType,
                                    user.getEmail(), jsonObject);
                            noticeTemplateService.sendBizPushNotice(toHeader(user, language), user.getUserId(), noticeBusinessType,
                                    message.getMsgId(), jsonObject, null);
                            log.info("sendMarginRiskAlarmMessage {} ok. userId:{} language: {}", noticeBusinessType.name(), user.getUserId(), language);
                            break;
                        default:
                            log.warn("sendLMarginWarnMessage: get NotifyStrategy: {} user: {}",
                                    notifyStrategy, user);
                    }
                }
            } catch (Exception e) {
                log.error("MarginRiskAlarmConsumer handleMessage error, messageId:" + message.getMsgId(), e);
            }
        }
    }

    public String getMarginRiskAlarmTopic(Long brokerId) {
        return MARGIN_RISK_TOPIC_PREFIX + brokerId;
    }

    /**
     * 预警通知
     *
     * @param user
     * @param language
     * @param safety
     */
    private void sendLMarginWarnMessage(User user, String language, String safety) {
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("safety", safety);

        NotifyUserStrategy notifyStrategy = getNotifyStrategy(user);
        switch (notifyStrategy) {
            case MOBILE:
                noticeTemplateService.sendSmsNotice(toHeader(user, language), user.getUserId(), NoticeBusinessType.MARGIN_WARN,
                        user.getNationalCode(), user.getMobile(), jsonObject);
                break;
            case EMAIL:
                noticeTemplateService.sendEmailNotice(toHeader(user, language), user.getUserId(), NoticeBusinessType.MARGIN_WARN,
                        user.getEmail(), jsonObject);
                break;
            default:
                log.warn("sendLMarginWarnMessage: get NotifyStrategy: {} user: {}",
                        notifyStrategy, user);
        }
    }

    /**
     * 获取用户当前语言环境
     */
    private String getUserLanguage(User user) {
        String language;

        // 优先获取用户最近一次登录所使用的语言
        List<LoginLog> loginLogs = loginLogMapper.queryLastLoginLogs(user.getUserId(), 1);
        if (!CollectionUtils.isEmpty(loginLogs)) {
            language = loginLogs.get(0).getLanguage();
            if (StringUtils.isNotEmpty(language)) {
                return language;
            }
        }

        // 获取用户的默认语言
        language = user.getLanguage();
        if (StringUtils.isNotEmpty(language)) {
            return language;
        }

        return LANG_ZH_CN;
    }

    private Header toHeader(User user, String language) {
        // 注：这里不使用User实体里面的language属性
        return Header.newBuilder().setOrgId(user.getOrgId()).setLanguage(language).build();
    }

    //给管理员发生提醒短信
    @Async
    public void sendAdminNotify(Long orgId, Long accountId, User user, NoticeBusinessType type) {
        try {
            List<RiskConfig> riskConfig = basicService.getOrgMarginRiskConfig(orgId);
            if (riskConfig.isEmpty()) {
                return;
            }
            RiskConfig config = riskConfig.get(0);
            JsonObject jsonObject = null;
            //开始强平
            if (type.equals(NoticeBusinessType.ADMIN_LIQUIDATION_ALERT)) {
                //获取强平记录
                QueryForceRecordRequest request = QueryForceRecordRequest.newBuilder()
                        .setBaseRequest(BaseRequest.newBuilder().setOrganizationId(orgId).build())
                        .setAccountId(accountId)
                        .setLimit(1)
                        .build();
                QueryForceRecordReply reply = grpcMarginService.queryForceRecord(request);
                if (reply.getForceRecordsCount() > 0) {
                    jsonObject = new JsonObject();
                    ForceRecord record = reply.getForceRecords(0);
                    jsonObject.addProperty("safety", DecimalUtil.toBigDecimal(record.getSafety()).multiply(new BigDecimal("100")).stripTrailingZeros().toPlainString());
                    jsonObject.addProperty("userId", user.getUserId());
                    jsonObject.addProperty("loan", DecimalUtil.toTrimString(record.getAllLoan()));
                }
            } else if (type.equals(NoticeBusinessType.ADMIN_LIQUIDATED_NOTIFY)) {
                //获取安全度
                GetSafetyRequest request = GetSafetyRequest.newBuilder()
                        .setBaseRequest(BaseRequest.newBuilder().setOrganizationId(orgId).build())
                        .setAccountId(accountId)
                        .build();
                GetSafetyReply response = grpcMarginService.getMarginSafety(request);
                BigDecimal safety = response.getSaftety() == null ? BigDecimal.ZERO : DecimalUtil.toBigDecimal(response.getSaftety());
                //获取借贷资产
                CrossLoanPositionRequest requestPosi = CrossLoanPositionRequest.newBuilder()
                        .setBaseRequest(BaseRequest.newBuilder().setOrganizationId(orgId).build())
                        .setAccountId(accountId)
                        .build();
                CrossLoanPositionReply responsePosi = grpcMarginService.getCrossLoanPosition(requestPosi);
                List<CrossLoanPosition> loanPositionList = responsePosi.getCrossLoanPositionList();
                BigDecimal allLoanAmount = BigDecimal.ZERO;
                for (CrossLoanPosition posi : loanPositionList) {
                    if (Stream.of(AccountService.TEST_TOKENS).anyMatch(testToken -> testToken.equalsIgnoreCase(posi.getTokenId()))) {
                        continue;
                    }
                    Rate rate = basicService.getV3Rate(orgId, posi.getTokenId());
                    BigDecimal btcRate = rate == null ? BigDecimal.ZERO : DecimalUtil.toBigDecimal(rate.getRatesMap().get("BTC"));
                    if (btcRate == null || btcRate.compareTo(BigDecimal.ZERO) <= 0) {
                        log.warn("{} {} btc rate {} is not exit", orgId, posi.getTokenId(), btcRate);
                        continue;
                    }
                    BigDecimal loanAmount = DecimalUtil.toBigDecimal(posi.getLoanTotal())
                            .add(DecimalUtil.toBigDecimal(posi.getInterestUnpaid()));
                    allLoanAmount = allLoanAmount.add(loanAmount.multiply(btcRate));
                }
                jsonObject = new JsonObject();
                jsonObject.addProperty("safety", safety.multiply(new BigDecimal("100")).stripTrailingZeros().toPlainString());
                jsonObject.addProperty("userId", user.getUserId());
                jsonObject.addProperty("loan", DecimalUtil.toTrimString(allLoanAmount));
            }
            if (jsonObject != null) {
                if (config.getNotifyType() == 1 && !config.getNotifyNumber().isBlank()) {
                    String[] notifyNumbers = config.getNotifyNumber().split(";");
                    for (String notifyNumber : notifyNumbers) {
                        try {
                            //手机
                            String[] mobiles = notifyNumber.split("-");
                            noticeTemplateService.sendSmsNotice(toHeader(user, "zh_CN"), 0L, type,
                                    mobiles[0], mobiles[1], jsonObject);
                        } catch (Exception e) {
                            log.error("send admin sms notify error {} {}", notifyNumber, e.getMessage(), e);
                        }
                    }
                } else if (config.getNotifyType() == 2 && !config.getNotifyNumber().isBlank()) {
                    //邮件
                    noticeTemplateService.sendEmailNotice(toHeader(user, "zh_CN"), 0L, type,
                            config.getNotifyNumber(), jsonObject);
                }

            }

        } catch (Exception e) {
            log.warn("sendAdminNotify error {} {} {}", orgId, accountId, e.getMessage(), e);
        }
    }

    /**
     * 获取通知策略
     */
    private NotifyUserStrategy getNotifyStrategy(User user) {
        if (StringUtils.isNotEmpty(user.getMobile())) {
            return NotifyUserStrategy.MOBILE;
        } else if (StringUtils.isNotEmpty(user.getEmail())) {
            return NotifyUserStrategy.EMAIL;
        } else {
            return NotifyUserStrategy.NONE;
        }
    }
}
