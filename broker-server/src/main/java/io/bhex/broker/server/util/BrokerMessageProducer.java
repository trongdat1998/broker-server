package io.bhex.broker.server.util;

import com.google.common.collect.ImmutableMap;

import com.google.common.collect.Maps;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;
import org.springframework.stereotype.Service;

import java.util.Map;

import io.bhex.base.mq.MessageProducer;
import io.bhex.base.mq.annotation.MQProducer;
import io.bhex.broker.common.util.JsonUtil;
import lombok.extern.slf4j.Slf4j;

@MQProducer(groupName = "trade-producer-group")
@Slf4j
@Service
public class BrokerMessageProducer extends MessageProducer {


    private static final Map<Integer, Integer> RETRY_LEVEL_MAP = ImmutableMap.<Integer, Integer>builder()
            .put(1, 2) // 5s
            .put(2, 3) // 10s
            .put(3, 4) // 30s
            .put(4, 5) // 1m
            .build();

    public <T> void sendMessage(String topic, String tag, T message) {
        this.sendMessage(topic, tag, message, Maps.newHashMap());

    }

    public <T> void sendMessage(String topic, String tag, T message, Map<String, String> properties) {
        int counter = 1;
        boolean success = true;
        do {
            success = this.sendMessage(topic, tag, message, RETRY_LEVEL_MAP.get(counter), properties);
            counter += 1;
        } while (!success && counter < 5);

    }

    public <T> boolean sendMessage(String topic, String tag, T message, int delayLevel, Map<String, String> properties) {

        String msgJson = JsonUtil.defaultGson().toJson(message);
        Message msg = new Message(topic, tag, msgJson.getBytes());

        properties.forEach((k, v) -> {
            msg.putUserProperty(k, v);
        });

        msg.setDelayTimeLevel(delayLevel);
        SendResult sendResult = send(msg, 5000L);
        if (sendResult == null) {
            log.error("sendMessage timeout! message:{}", msgJson);
            throw new IllegalStateException("Result of send message is null");
        }
        if (SendStatus.SEND_OK != sendResult.getSendStatus()
                && SendStatus.FLUSH_SLAVE_TIMEOUT != sendResult.getSendStatus()) {
            log.error("sendMessage exception! message:{},result={}",
                    msgJson,
                    JsonUtil.defaultGson().toJson(sendResult));
            return false;
        }
        return true;
    }
}
