package io.bhex.broker.server.message.consumer;

import io.bhex.broker.server.message.TopicConsumer;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Resource;

import io.bhex.base.idgen.api.ISequenceGenerator;
import io.bhex.base.mq.config.MQProperties;
import io.bhex.broker.server.domain.PushMessageType;
import io.bhex.broker.server.grpc.server.service.PushDataService;
import io.bhex.broker.server.message.producer.UserInfoProducer;
import io.bhex.broker.server.model.DataPushConfig;
import io.bhex.broker.server.model.DataPushMessage;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class OtcOrderConsumer {

    @Resource
    MQProperties mqProperties;

    @Resource
    private PushDataService pushDataService;

    @Autowired
    private ISequenceGenerator sequenceGenerator;

    private final static String OTC_ORDER_TOPIC_PREFIX = "OTC_ORDER_";

    private static final String NOTIFY_OTC_TRADE_MESSAGE_CONSUMER_GROUP = "_NOTIFY_OTC_TRADE_MESSAGE_CONSUMER_GROUP_RESEND";

    private static final Map<String, DefaultMQPushConsumer> consumerMap = new HashMap<>();

    public void bootOtcOrderConsumer(String topic, Long brokerId) {
        try {
            DefaultMQPushConsumer consumer = consumerMap.get(topic);
            if (consumer == null) {
                consumer = createConsumer(topic, brokerId);
                if (consumer != null) {
                    consumerMap.put(topic, consumer);
                    log.info("buildOtcOrderConsumer done. topic: {}", topic);
                } else {
                    log.error("buildOtcOrderConsumer error. topic: {}", topic);
                }
            }
        } catch (Exception e) {
            log.error(String.format("buildOtcOrderConsumer error. topic: %s", topic), e);
        }
    }

    private DefaultMQPushConsumer createConsumer(String topic, Long brokerId) {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(brokerId + NOTIFY_OTC_TRADE_MESSAGE_CONSUMER_GROUP);
        consumer.setConsumeThreadMax(20);
        consumer.setNamesrvAddr(mqProperties.getNameServers());
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.setMessageModel(MessageModel.CLUSTERING);
        try {
            consumer.subscribe(topic, "*");
            consumer.registerMessageListener((MessageListenerConcurrently) (list, consumeConcurrentlyContext) -> {
                try {
                    handleMessage(brokerId, list);
                } catch (Exception e) {
                    log.error("Push data consumer handle message error", e);
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

    public void handleMessage(Long brokerId, List<MessageExt> messages) {
        for (MessageExt message : messages) {
            String orderBody = new String(message.getBody());
            if (StringUtils.isEmpty(orderBody)) {
                return;
            }
            log.info("handleMessage otc order body {}", orderBody);
            try {
                pushDataService.pushOtcTradeInfoToMQ(brokerId, orderBody);
            } catch (Exception ex) {
                log.error("Push Otc Trade To MQ Fail brokerId {} body {} ex {}", brokerId, orderBody, ex);
            }

            Set<DataPushConfig> dataPushConfigSet = pushDataService.getOrgList();
            if (CollectionUtils.isEmpty(dataPushConfigSet)) {
                return;
            }
            DataPushConfig dataPushConfig
                    = dataPushConfigSet.stream().filter(config -> config.getOrgId().equals(brokerId)).findFirst().orElse(null);

            if (dataPushConfig == null) {
                return;
            }

            if (dataPushConfig.getOtcStatus() == null || dataPushConfig.getOtcStatus().equals(0)) {
                return;
            }

            DataPushMessage dataPushMessage
                    = pushDataService.createDataPushMessage(brokerId,
                    PushMessageType.OTC_TRADE,
                    orderBody
                    , String.valueOf(sequenceGenerator.getLong()));
            //异步进行推送
            pushDataService.pushServer(Arrays.asList(dataPushMessage));
        }
    }

    public String getOtcOrderTopic(Long brokerId) {
        return OTC_ORDER_TOPIC_PREFIX + brokerId;
    }
}
