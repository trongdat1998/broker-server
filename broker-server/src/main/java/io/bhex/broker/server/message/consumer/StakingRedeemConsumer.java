package io.bhex.broker.server.message.consumer;

import io.bhex.base.mq.config.MQProperties;
import io.bhex.broker.common.util.JsonUtil;
import io.bhex.broker.server.domain.staking.StakingConstant;
import io.bhex.broker.server.grpc.server.service.staking.StakingProductCurrentStrategy;
import io.bhex.broker.server.message.StakingRedeemMessage;
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
 * 理财赎回消息Consumer
 *
 * @author songxd
 * @date 2020-07-30
 */

@Slf4j
@Component
@Deprecated
public class StakingRedeemConsumer {
    @Resource
    private MQProperties mqProperties;

    @Resource
    private StakingProductCurrentStrategy stakingProductCurrentStrategy;

    private DefaultMQPushConsumer consumer;

    private static final String CONSUMER_GROUP_POSTFIX = "REDEEM";

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
            consumer.registerMessageListener(new StakingRedeemConsumer.StakingRedeemMessageListener());
            consumer.start();
        } catch (MQClientException e) {
            log.error(e.getMessage(), e);
            throw e;
        }*/
    }

    private String getTags() {
        return StakingConstant.STAKING_REDEEM_TAG;
    }

    /**
     * 理财赎回消息Listener
     */
    private class StakingRedeemMessageListener implements MessageListenerConcurrently {

        /**
         * Staking Product Redeem Message Consume
         *
         * @param list
         * @param context
         * @return
         */
        @Override
        public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext context) {
            String messageJson;
            for (MessageExt messageExt : list) {
                log.info("staking redeem handlerMessage| receive msgId: {}, topic : {}, tags : {}", messageExt.getMsgId(), messageExt.getTopic(), messageExt.getTags());

                // Message fromJson
                StakingRedeemMessage stakingRedeemMessage = null;
                try {
                    byte[] bytes = messageExt.getBody();
                    messageJson = new String(bytes);
                    stakingRedeemMessage = JsonUtil.defaultGson().fromJson(messageJson, StakingRedeemMessage.class);
                } catch (Exception e) {
                    log.error("StakingRedeemConsumer handleMessage message fromJson error, messageId:" + messageExt.getMsgId(), e);
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }

                // validation
                if (stakingRedeemMessage == null
                        || stakingRedeemMessage.getOrgId().equals(0)
                        || stakingRedeemMessage.getUserId().equals(0)
                        || stakingRedeemMessage.getProductId().equals(0)
                        || stakingRedeemMessage.getTransferId().equals(0)
                        || stakingRedeemMessage.getAmount().compareTo(BigDecimal.ZERO) <= 0) {
                    log.info("StakingRedeemConsumer illegal message, messageId:" + messageExt.getMsgId());
                    continue;
                }
                // handle message
                try {
                    // return stakingProductCurrentStrategy.processRedeemMessage(stakingRedeemMessage);
                } catch (Exception e) {
                    log.error("StakingSubscribeConsumer handleMessage error, messageId:" + messageExt.getMsgId(), e);
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }
            }

            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        }
    }
}
