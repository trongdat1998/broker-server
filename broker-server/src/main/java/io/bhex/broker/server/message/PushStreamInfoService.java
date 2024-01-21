package io.bhex.broker.server.message;

import io.bhex.base.mq.MessageProducer;
import io.bhex.base.mq.config.MQProperties;
import io.bhex.broker.server.message.consumer.OtcOrderConsumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.common.message.Message;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import javax.annotation.Resource;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author wangsc
 * @description 将经过grpc流的消息再推送给MQ
 * @date 2020-06-17 16:50
 */
@Component
@Slf4j
public class PushStreamInfoService {

    public final static String TRADE_PREFIX = "Trade";
    @Resource
    MQProperties mqProperties;
    @Resource
    OtcOrderConsumer otcOrderConsumer;
    @Resource(name = "mqStreamExecutor")
    ThreadPoolTaskExecutor mqStreamExecutor;

    private static final Map<String, MessageProducer> producerMap = new ConcurrentHashMap<>();

    public void pushNotifyToMq(String tags, byte[] bytes) {
        mqStreamExecutor.execute(() -> writeMessageToMq(RocketMQConstants.MQ_NOTIFY_BROKER_TOPIC, tags, bytes));
    }

    public void pushOtcOrderToMq(Long brokerId, String tags, byte[] bytes) {
        mqStreamExecutor.execute(() -> writeMessageToMq(otcOrderConsumer.getOtcOrderTopic(brokerId), tags, bytes));
    }

    public void pushTicketReceiveToMq(Long brokerId, String tags, byte[] bytes) {
        mqStreamExecutor.execute(() -> writeMessageToMq("BKR_TKT_" + brokerId, tags, bytes));
    }

    public void pushTradeDetailToMq(String tags, byte[] bytes) {
        mqStreamExecutor.execute(() -> writeMessageToMq(TRADE_PREFIX, tags, bytes));
    }

    private void writeMessageToMq(String topic, String tags, byte[] bytes) {
        MessageProducer producer = getProducerByTopic(topic);
        if (Objects.isNull(producer)) {
            log.error("Write Stream Info Message To MQ Error: producer null. topic => {}, tags => {}.", topic, tags);
            return;
        }
        Message msg = new Message(topic, bytes);
        msg.setTags(tags);
        producer.send(msg);
    }

    private MessageProducer getProducerByTopic(String topic) {
        try {
            MessageProducer producer = producerMap.get(topic);
            if (producer == null) {
                producer = createProducer(topic);
                producerMap.put(topic, producer);
                log.info("build streamInfoProducer done. topic: {}", topic);
            }
            return producer;
        } catch (Exception e) {
            log.error(String.format("build streamInfoProducer error. topic: %s", topic), e);
            return null;
        }
    }

    private MessageProducer createProducer(String topic) {
        Assert.notNull(mqProperties.getNameServers(), "name server address must be defined");
        // build a producer proxy
        MessageProducer sender = MessageProducer
                .newBuild(mqProperties.getNameServers(), topic)
                .setTimeout(mqProperties.getSendMsgTimeout())
                .setWithVipChannel(mqProperties.getVipChannelEnabled());
        sender.connect();
        log.info("[MQProducer] {} is ready!", sender.getClass().getSimpleName());
        return sender;
    }
}
