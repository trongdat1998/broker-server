package io.bhex.broker.server.message.consumer;

import io.bhex.base.mq.config.MQProperties;
import io.bhex.broker.common.util.JsonUtil;
import io.bhex.broker.server.domain.staking.StakingConstant;
import io.bhex.broker.server.grpc.server.service.staking.StakingProductOrderService;
import io.bhex.broker.server.message.StakingSubscribeMessage;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.math.BigDecimal;
import java.util.List;

/**
 * 理财申购消息Consumer
 *
 * @author songxd
 * @date 2020-07-30
 */

@Slf4j
@Component
@Deprecated
public class StakingSubscribeConsumer {

    @Resource
    private MQProperties mqProperties;

    @Resource
    private StakingProductOrderService stakingProductOrderService;

    private DefaultMQPushConsumer consumer;

    private static final String CONSUMER_GROUP_POSTFIX = "SUBSCRIBE";

    @PostConstruct
    public void init() throws Exception {
        /*if (mqProperties.getNameServers() == null) {
            throw new IllegalArgumentException("rocketmq.nameSrvAddress 是必须的参数");
        }

        consumer = new DefaultMQPushConsumer(String.format(StakingConstant.STAKING_CONSUMER_GROUP_NAME, CONSUMER_GROUP_POSTFIX));
        try {
            consumer.setNamesrvAddr(mqProperties.getNameServers());
            consumer.setMessageModel(MessageModel.CLUSTERING);
            consumer.subscribe(StakingConstant.STAKING_MESSAGE_TOPIC, getTags());
            consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
            consumer.registerMessageListener(new StakingSubscribeConsumer.StakingSubscribeMessageListener());
            consumer.start();
        } catch (MQClientException e) {
            log.error(e.getMessage(), e);
            throw e;
        }*/
    }

    private String getTags() {
        return StakingConstant.STAKING_SUBSCRIBE_TAG;
    }

    /**
     * 理财申购消息Listener
     */
    private class StakingSubscribeMessageListener implements MessageListenerConcurrently {

        /**
         * Staking Product Subscribe Message Consume
         *
         * @param list
         * @param context
         * @return
         */
        @Override
        public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext context) {
            String messageJson;
            for (MessageExt messageExt : list) {
                log.info("staking subscribe handlerMessage| receive msgId: {}, topic : {}, tags : {}", messageExt.getMsgId(), messageExt.getTopic(), messageExt.getTags());

                // Message fromJson
                StakingSubscribeMessage stakingSubscribeMessage = null;
                try {
                    byte[] bytes = messageExt.getBody();
                    messageJson = new String(bytes);
                    stakingSubscribeMessage = JsonUtil.defaultGson().fromJson(messageJson, StakingSubscribeMessage.class);
                } catch (Exception e) {
                    log.error("StakingSubscribeConsumer handleMessage message fromJson error, messageId:" + messageExt.getMsgId(), e);
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }

                // validation
                if (stakingSubscribeMessage == null
                        || stakingSubscribeMessage.getOrgId().equals(0)
                        || stakingSubscribeMessage.getUserId().equals(0)
                        || stakingSubscribeMessage.getProductId().equals(0)
                        || stakingSubscribeMessage.getTransferId().equals(0)
                        || stakingSubscribeMessage.getLots() <= 0
                        || stakingSubscribeMessage.getAmount().compareTo(BigDecimal.ZERO) <= 0) {
                    log.info("StakingSubscribeConsumer illegal message, messageId:" + messageExt.getMsgId());
                    continue;
                }

                // handle message
                try {
                    // return stakingProductOrderService.processSubscribe(stakingSubscribeMessage);
                } catch (Exception e) {
                    log.error("StakingSubscribeConsumer handleMessage error, messageId:" + messageExt.getMsgId(), e);
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }
            }

            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        }
    }
}
