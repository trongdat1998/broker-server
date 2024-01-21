package io.bhex.broker.server.message.consumer;

import io.bhex.broker.server.grpc.server.service.TradeDetailAppPushService;
import io.bhex.broker.server.message.TopicConsumer;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Resource;

import io.bhex.base.account.Trade;
import io.bhex.base.mq.config.MQProperties;
import io.bhex.broker.server.message.MessageTradeDetail;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class TradeDetailConsumer {

    @Resource
    MQProperties mqProperties;

    @Resource
    private TradeDetailAppPushService tradeDetailAppPushService;

    public static final String TRADE_TOPIC = "Trade";

    //private static final String NOTIFY_TRADE_DETAIL_MESSAGE_CONSUMER_GROUP = "_NOTIFY_MINE_TRADE_DETAIL_MESSAGE_CONSUMER_GROUP_RESEND";

    private static final Map<String, DefaultMQPushConsumer> consumerMap = new HashMap<>();

    public void bootTradeDetailConsumer(String group, Long brokerId, TradeDetailHandler handler) {
        try {
            String consumerKey = brokerId + group;
            DefaultMQPushConsumer consumer = consumerMap.get(consumerKey);
            if (consumer == null) {
                consumer = createConsumer(group, brokerId, handler);
                if (consumer != null) {
                    consumerMap.put(consumerKey, consumer);
                    log.info("{} buildTradeDetailConsumer done. topic: {}", consumerKey, TRADE_TOPIC);
                } else {
                    log.error("{} buildTradeDetailConsumer error. topic: {}", consumerKey, TRADE_TOPIC);
                }
            }
        } catch (Exception e) {
            log.error(String.format("buildTradeDetailConsumer error. group: %s", brokerId + group), e);
        }
    }

    private DefaultMQPushConsumer createConsumer(String group, Long brokerId, TradeDetailHandler handler) {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(brokerId + group);
        consumer.setConsumeThreadMax(20);
        consumer.setNamesrvAddr(mqProperties.getNameServers());
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.setMessageModel(MessageModel.CLUSTERING);
//        consumer.setConsumeMessageBatchMaxSize(100);
        try {
            consumer.subscribe(TRADE_TOPIC, brokerId + "");
            consumer.registerMessageListener((MessageListenerConcurrently) (list, consumeConcurrentlyContext) -> {
                try {
                    handleMessage(list, handler, group);
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

    public interface TradeDetailHandler {
        boolean processTrade(MessageTradeDetail tradeDetail);
    }

    public void handleMessage(List<MessageExt> messages, TradeDetailHandler handler, String group) throws Exception {
        boolean result = Boolean.TRUE;
        for (MessageExt message : messages) {
            Trade trade;
            try {
                trade = getTrade(message);
            } catch (Exception e) {
                log.error("Trade parse error, messageId:" + message.getMsgId(), e);
                throw e;
            }
            if (trade == null) {
                log.warn("handleMessage get Trade null or empty. messageId: {}", message.getMsgId());
                continue;
            }
            MessageTradeDetail tradeDetail = MessageTradeDetail.messageBuild(trade);
            if (tradeDetail != null) {
                try {
                    result = handler.processTrade(tradeDetail);
                    //result = hobbitKpiStatisticsService.handleTradeData(tradeDetail, false);
                } catch (Throwable ex) {
                    log.error("group:{} message processing failed error", group, ex);
                    result = Boolean.TRUE;
                }
            }
        }
        if (!result) {
            throw new RuntimeException();
        }
    }

    public static Trade getTrade(MessageExt messageExt) throws Exception {
        try {
            return Trade.parser().parseFrom(messageExt.getBody());
        } catch (Exception e) {
            log.error("getTrade error", e);
            throw e;
        }
    }
}
