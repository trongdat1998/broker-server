package io.bhex.broker.server.message.producer;

import io.bhex.base.mq.MessageProducer;
import io.bhex.base.mq.config.MQProperties;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import javax.annotation.Resource;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.message.producer
 * @Author: ming.xu
 * @CreateDate: 2019/12/19 4:54 PM
 * @Copyright（C）: 2019 BHEX Inc. All rights reserved.
 */
@Slf4j
@Component
public class UserInfoProducer {

    @Resource
    MQProperties mqProperties;

    /**
     * 'User_Register_' + orgId
     */
    private final static String USER_REGISTER_TOPIC_PREFIX = "User_Register_";

    /**
     * 'User_KYC_' + orgId
     */
    private final static String USER_KYC_TOPIC_PREFIX = "User_KYC_";


    /**
     * 'USER_OTC_TRADE_' + orgId
     */
    private final static String USER_OTC_TRADE_TOPIC_PREFIX = "USER_OTC_TRADE_";

    /**
     * 'User_Login' + orgId
     */
    private final static String USER_LOGIN_TOPIC_PREFIX = "User_Login_";

    /**
     * 'USER_SPOT_ORDER_NOTIFY_' + orgId
     */
    private final static String USER_SPOT_ORDER_NOTIFY_TOPIC_PREFIX = "USER_SPOT_ORDER_NOTIFY_";

    private static final Map<String, MessageProducer> producerMap = new ConcurrentHashMap<>();

    public void bootProducer(String topic) {
        try {
            MessageProducer producer = producerMap.get(topic);
            if (producer == null) {
                producer = createProducer(topic);
                if (producer != null) {
                    producerMap.put(topic, producer);
                    log.info("build UserInfoProducer done. topic: {}", topic);
                } else {
                    log.error("build UserInfoProducer error. topic: {}", topic);
                }
            }
        } catch (Exception e) {
            log.error(String.format("build UserInfoProducer error. topic: %s", topic), e);
        }
    }

    private MessageProducer createProducer(String topic) {
        Assert.notNull(mqProperties.getNameServers(), "name server address must be defined");

        String groupName = topic;
        // build a producer proxy
        MessageProducer sender = MessageProducer
                .newBuild(mqProperties.getNameServers(), groupName)
                .setTimeout(mqProperties.getSendMsgTimeout())
                .setWithVipChannel(mqProperties.getVipChannelEnabled());

        sender.connect();

        log.info("[MQProducer] {} is ready!", sender.getClass().getSimpleName());

        return sender;
    }

    public String getUserRegisterTopic(Long brokerId) {
        return USER_REGISTER_TOPIC_PREFIX + brokerId;
    }

    public String getUserKycTopicPrefix(Long brokerId) {
        return USER_KYC_TOPIC_PREFIX + brokerId;
    }

    public String getOtcTradeTopicPrefix(Long brokerId) {
        return USER_OTC_TRADE_TOPIC_PREFIX + brokerId;
    }

    public MessageProducer getProducerByTopic(String topic) {
        bootProducer(topic);
        MessageProducer producer = producerMap.get(topic);
        if (Objects.nonNull(producer)) {
            return producer;
        }
        return null;
    }
    public String getUserLoginTopic(Long brokerId) {
        return USER_LOGIN_TOPIC_PREFIX + brokerId;
    }

    public String getUserSpotOrderNotifyTopic(Long brokerId) {
        return USER_SPOT_ORDER_NOTIFY_TOPIC_PREFIX + brokerId;
    }
}
