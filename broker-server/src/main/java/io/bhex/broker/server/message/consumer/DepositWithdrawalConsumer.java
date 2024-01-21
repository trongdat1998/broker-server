package io.bhex.broker.server.message.consumer;

import com.google.common.collect.Lists;
import com.google.gson.JsonObject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

import io.bhex.base.account.DepositRecord;
import io.bhex.base.account.WithdrawalOrderDetail;
import io.bhex.base.constants.DataSyncTopic;
import io.bhex.base.mq.config.MQProperties;
import io.bhex.base.proto.DecimalUtil;
import io.bhex.broker.grpc.common.Header;
import io.bhex.broker.server.BrokerServerProperties;
import io.bhex.broker.server.domain.NoticeBusinessType;
import io.bhex.broker.server.grpc.server.service.NoticeTemplateService;
import io.bhex.broker.server.grpc.server.service.UserService;
import io.bhex.broker.server.model.Broker;
import io.bhex.broker.server.model.User;
import io.bhex.broker.server.primary.mapper.BrokerMapper;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class DepositWithdrawalConsumer {
    @Resource
    private BrokerServerProperties brokerServerProperties;

    private String consumerTag;
    private final static String DEPOSIT_TOPIC = DataSyncTopic.TOPIC_DATA_NOTIFY_DEPOSIT;
    private final static String WITHDRAW_TOPIC = DataSyncTopic.TOPIC_DATA_NOTIFY_WITHDRAW;
    public static final String DEPOSIT_WITHDRAW_GROUP = "DEPOSIT_WITHDRAW_GROUP";

    @Resource
    private BrokerMapper brokerMapper;


    //private DefaultMQPushConsumer consumer;
    private static final Map<String, DefaultMQPushConsumer> consumerMap = new HashMap<>();

    @Resource
    private MQProperties mqProperties;
    @Autowired
    private NoticeTemplateService noticeTemplateService;

    @Resource
    private UserService userService;

    @PostConstruct
    @Scheduled(cron = "0 0/5 * * * ?")
    private void refreshConsumer() {
        String newConsumerTag = getConsumerTag();
        log.info("refreshConsumer: get comsumerTag: {}", newConsumerTag);
        if (StringUtils.isEmpty(newConsumerTag)) {
            log.error("refreshConsumer: do not start consumer for consumerTag is empty.");
            unsubscribeIfNeed();
            return;
        }

        // 比较订阅的TAG是否发生了变化
        String currentConsumerTag = consumerTag;
        consumerTag = newConsumerTag;
        if (newConsumerTag.equals(currentConsumerTag) && !consumerMap.isEmpty()) {
            log.info("refreshConsumer: consumer tags has no change.");
            return;
        }

        // 如果订阅的TAG是否发生了变化，需要重新订阅（如果之前已经订阅，先取消订阅）
        startSubscribe();
    }

    private void unsubscribeIfNeed() {
        if (!consumerMap.isEmpty()) {
            for (String topic : consumerMap.keySet()) {
                DefaultMQPushConsumer consumer = consumerMap.get(topic);
                consumer.unsubscribe(topic);
                consumer.shutdown();
                log.info("unsubscribe topic: {} {} success.", DEPOSIT_TOPIC, WITHDRAW_TOPIC);
            }
            consumerMap.clear();
        }
    }

    private void startSubscribe() {
        if (!consumerMap.isEmpty()) {
            unsubscribeIfNeed();
        }


        for (String topic : Lists.newArrayList(DEPOSIT_TOPIC, WITHDRAW_TOPIC)) {
            String group = brokerServerProperties.getMqGroupPrefix() + topic;

            DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(group);
            consumer.setConsumeThreadMax(20);
            consumer.setNamesrvAddr(mqProperties.getNameServers());
            consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
            consumer.setMessageModel(MessageModel.CLUSTERING);

            try {
                consumer.subscribe(topic, consumerTag);
                consumer.registerMessageListener(this::handlerMessage);
                consumer.start();
                consumerMap.put(topic, consumer);
                log.warn("start mq consumer success. consumerGroup: {} topic: {} tag: {}",
                        group, topic, consumerTag);
            } catch (MQClientException e) {
                log.error(String.format("consume mq topic: %s error", topic), e);
            }
        }
    }


    private String getConsumerTag() {
        List<Broker> brokers = brokerMapper.queryAvailableBroker();
        if (CollectionUtils.isEmpty(brokers)) {
            return "";
        }

        Long[] orgIds = brokers.stream()
                .filter(b -> b.getStatus() == 1)
                .map(Broker::getOrgId)
                .sorted()
                .toArray(Long[]::new);
        return StringUtils.join(orgIds, "||");
    }

    private ConsumeConcurrentlyStatus handlerMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
        try {
            for (MessageExt msg : msgs) {
                long orgId = Long.parseLong(msg.getTags());
                if (msg.getTopic().equals(WITHDRAW_TOPIC)) {

                    WithdrawalOrderDetail order = WithdrawalOrderDetail.parseFrom(msg.getBody());
                    long userId = Long.parseLong(order.getBrokerUserId());
                    if (userId == 0) {
                        continue;
                    }
                    if (!noticeTemplateService.canSendPush(orgId, userId, NoticeBusinessType.WITHDRAW_SUCCESS_WITH_DETAIL)) {
                        continue;
                    }
                    Header header = Header.newBuilder().setOrgId(orgId).setUserId(Long.parseLong(order.getBrokerUserId())).build();
                    JsonObject jsonObject = new JsonObject();
                    BigDecimal quantity = DecimalUtil.toBigDecimal(order.getArriveQuantity()).setScale(8, RoundingMode.DOWN).stripTrailingZeros();
                    jsonObject.addProperty("quantity", quantity.toPlainString());
                    jsonObject.addProperty("token", order.getToken().getTokenName());
                    //log.info("WithdrawOrder:{} {} {}", TextFormat.shortDebugString(header), jsonObject.toString(), order.getWithdrawalOrderId());
                    noticeTemplateService.sendBizPushNotice(header, userId, NoticeBusinessType.WITHDRAW_SUCCESS_WITH_DETAIL,
                            order.getWithdrawalOrderId() + "", jsonObject, null);
                } else if (msg.getTopic().equals(DEPOSIT_TOPIC)) {
                    DepositRecord depositRecord = DepositRecord.parseFrom(msg.getBody());
                    long userId = Long.parseLong(depositRecord.getBrokerUserId());
                    if (userId == 0) {
                        continue;
                    }
                    try {
                        handleDepositInviteRelation(depositRecord, userId);
                    } catch (Exception ex) {
                        log.info("handleDepositInviteRelation fail userId {} error {}", userId, ex);
                    }
                    if (!noticeTemplateService.canSendPush(orgId, userId, NoticeBusinessType.DEPOSIT_SUCCESS)) {
                        continue;
                    }
                    Header header = Header.newBuilder().setOrgId(orgId).setUserId(userId).build();
                    JsonObject jsonObject = new JsonObject();
                    BigDecimal quantity = DecimalUtil.toBigDecimal(depositRecord.getQuantity()).setScale(8, RoundingMode.DOWN).stripTrailingZeros();
                    jsonObject.addProperty("quantity", quantity.toPlainString());
                    jsonObject.addProperty("token", depositRecord.getToken().getTokenName());
                    //log.info("DepositOrder:{} {} {}", TextFormat.shortDebugString(header), jsonObject.toString(), depositRecord.getDepositRecordId());
                    noticeTemplateService.sendBizPushNotice(header, userId, NoticeBusinessType.DEPOSIT_SUCCESS,
                            depositRecord.getDepositRecordId() + "", jsonObject, null);
                }
            }
        } catch (Throwable e) {
            log.error("handleMessages error.", e);
            return ConsumeConcurrentlyStatus.RECONSUME_LATER;
        }
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    }

    private void handleDepositInviteRelation(DepositRecord depositRecord, Long userId) {
        if (!depositRecord.getTokenId().equalsIgnoreCase("SNP")) {
            return;
        }
        User user = userService.getUser(Long.parseLong(depositRecord.getBrokerUserId()));
        if (user == null) {
            return;
        }
        if (user.getInviteUserId() != null && user.getInviteUserId() > 0) {
            return;
        }
        if (System.currentTimeMillis() - user.getCreated() > 86400_000L) {
            return;
        }
        userService.rebuildUserInviteRelation(depositRecord.getOrgId(), userId, 769393970989124096L);
    }
}
