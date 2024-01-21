package io.bhex.broker.server.message.consumer;

import io.bhex.base.common.CommonNotifyMessage;
import io.bhex.base.mq.config.MQProperties;
import io.bhex.broker.common.util.JsonUtil;
import io.bhex.broker.server.domain.CommonIni;
import io.bhex.broker.server.grpc.server.service.CommonIniService;
import io.bhex.broker.server.message.RocketMQConstants;
import io.bhex.broker.server.message.service.MessageHandlerFactory;
import io.bhex.broker.server.model.Broker;
import io.bhex.broker.server.primary.mapper.BrokerMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.List;

@Slf4j
@Component
public class NotifyMessageConsumer {

    private static final int MAX_CONSUME_THREAD_NUM = 32;
    private static final String INI_NAME_NOTIFY_MESSAGE_CONSUMER_GROUP = "notify_message_consumer_group";

    private DefaultMQPushConsumer consumer;
    private String consumerTag;

    @Resource
    private MQProperties mqProperties;

    @Resource
    private MessageHandlerFactory messageHandlerFactory;

    @Resource
    private BrokerMapper brokerMapper;

    @Resource
    private CommonIniService commonIniService;

    private ConsumeConcurrentlyStatus handlerMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
        try {
            for (MessageExt msg : msgs) {
                CommonNotifyMessage notifyMessage = CommonNotifyMessage.parseFrom(msg.getBody());
                log.info("receive notify message: {}", JsonUtil.defaultGson().toJson(notifyMessage));
                messageHandlerFactory.handleMessage(notifyMessage);
            }
        } catch (Throwable e) {
            log.error("handleMessages error.", e);
            return ConsumeConcurrentlyStatus.RECONSUME_LATER;
        }
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    }

    @PostConstruct
    private void init() {
        refreshConsumer();
    }

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
        if (newConsumerTag.equals(currentConsumerTag) && consumer != null) {
            log.info("refreshConsumer: consumer tags has no change.");
            return;
        }

        // 如果订阅的TAG是否发生了变化，需要重新订阅（如果之前已经订阅，先取消订阅）
        startSubscribe();
    }

    private void unsubscribeIfNeed() {
        if (consumer != null) {
            consumer.unsubscribe(RocketMQConstants.MQ_NOTIFY_BROKER_TOPIC);
            consumer.shutdown();
            consumer = null;
            log.info("unsubscribe topic: {} success.", RocketMQConstants.MQ_NOTIFY_BROKER_TOPIC);
        }
    }

    private void startSubscribe() {
        if (consumer != null) {
            unsubscribeIfNeed();
        }

        String consumerGroup = getConsumerGroup();
        consumer = new DefaultMQPushConsumer(consumerGroup);
        consumer.setConsumeThreadMax(MAX_CONSUME_THREAD_NUM);
        consumer.setNamesrvAddr(mqProperties.getNameServers());
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        consumer.setMessageModel(MessageModel.CLUSTERING);

        try {
            consumer.subscribe(RocketMQConstants.MQ_NOTIFY_BROKER_TOPIC, consumerTag);
            consumer.registerMessageListener(this::handlerMessage);
            consumer.start();
            log.warn("start mq consumer success. consumerGroup: {} topic: {} tag: {}",
                    consumerGroup, RocketMQConstants.MQ_NOTIFY_BROKER_TOPIC, consumerTag);
        } catch (MQClientException e) {
            consumer = null;
            log.error(String.format("consume mq topic: %s error", RocketMQConstants.MQ_NOTIFY_BROKER_TOPIC), e);
        }
    }

    /**
     * 根据所有可用的broker的orgId来订阅TAG
     */
    private String getConsumerTag() {
        List<Broker> brokers = brokerMapper.selectAll();
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

    private String getConsumerGroup() {
        CommonIni commonIni = commonIniService.getCommonIni(0L, INI_NAME_NOTIFY_MESSAGE_CONSUMER_GROUP);
        if (commonIni == null || StringUtils.isEmpty(commonIni.getIniValue())) {
            log.warn("can not find consumer group from db. use default: {}", RocketMQConstants.MQ_NOTIFY_BROKER_GROUP_DEFAULT);
            return RocketMQConstants.MQ_NOTIFY_BROKER_GROUP_DEFAULT;
        }

        return commonIni.getIniValue();
    }
}
