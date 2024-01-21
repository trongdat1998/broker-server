package io.bhex.broker.server.message.consumer;

import com.google.gson.Gson;

import io.bhex.broker.server.message.TopicConsumer;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Resource;

import io.bhex.base.match.Ticket;
import io.bhex.base.mq.config.MQProperties;
import io.bhex.base.proto.DecimalUtil;
import io.bhex.base.quote.Rate;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.server.domain.PushMessageType;
import io.bhex.broker.server.grpc.server.service.BasicService;
import io.bhex.broker.server.grpc.server.service.PushDataService;
import io.bhex.broker.server.grpc.server.service.po.UserTradeDetailMessage;
import io.bhex.broker.server.model.Account;
import io.bhex.broker.server.model.DataPushMessage;
import io.bhex.broker.server.model.Symbol;
import io.bhex.broker.server.primary.mapper.AccountMapper;
import io.bhex.broker.server.primary.mapper.SymbolMapper;
import lombok.extern.slf4j.Slf4j;

@Deprecated
@Slf4j
@Component
public class TicketReceiveConsumer {

    @Resource
    MQProperties mqProperties;

    @Resource
    private BasicService basicService;

    @Resource
    private PushDataService pushDataService;

    @Resource
    private SymbolMapper symbolMapper;

    @Resource
    private AccountMapper accountMapper;

    private static final String NOTIFY_TRADE_MESSAGE_CONSUMER_GROUP = "_NOTIFY_TRADE_MESSAGE_CONSUMER_GROUP_RESEND";

    private static final Map<String, DefaultMQPushConsumer> consumerMap = new HashMap<>();

    public void bootTicketConsumer(String topic, Long brokerId) {
        try {
            DefaultMQPushConsumer consumer = consumerMap.get(topic);
            if (consumer == null) {
                consumer = createConsumer(topic, brokerId);
                if (consumer != null) {
                    consumerMap.put(topic, consumer);
                    log.info("buildTicketConsumer done. topic: {}", topic);
                } else {
                    log.error("buildTicketConsumer error. topic: {}", topic);
                }
            }
        } catch (Exception e) {
            log.error(String.format("buildTicketConsumer error. topic: %s", topic), e);
        }
    }

    private DefaultMQPushConsumer createConsumer(String topic, Long brokerId) {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(brokerId + NOTIFY_TRADE_MESSAGE_CONSUMER_GROUP);
        consumer.setConsumeThreadMax(20);
        consumer.setNamesrvAddr(mqProperties.getNameServers());
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.setMessageModel(MessageModel.CLUSTERING);
        try {
            consumer.subscribe(topic, "*");
            consumer.registerMessageListener((MessageListenerConcurrently) (list, consumeConcurrentlyContext) -> {
                try {
                    handleMessage(list);
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

    public void handleMessage(List<MessageExt> messages) {
        for (MessageExt message : messages) {
            Ticket ticket = getTicketList(message);
            if (ticket == null) {
                log.warn("handleMessage get ticket list null or empty. messageId: {},ticketId {}", message.getMsgId(), ticket.getTicketId());
                continue;
            }
            log.info("handleMessage ticketId {} takerOrderId {} makerOrderId {}", ticket.getTicketId(), ticket.getTakerOrderId(), ticket.getMakerOrderId());
            Long orgId = Long.parseLong(message.getTopic().replace("BKR_TKT_", ""));
            List<DataPushMessage> dataPushMessages = buildMessage(orgId, ticket);
            if (CollectionUtils.isNotEmpty(dataPushMessages)) {
                //异步进行推送
                pushDataService.pushServer(dataPushMessages);
            }
        }
    }

    private Ticket getTicketList(MessageExt ext) {
        try {
            return Ticket.parseFrom(ext.getBody());
        } catch (Exception e) {
            log.error("getTicketList error, messageId:" + ext.getMsgId(), e);
        }
        return null;
    }

    private List<DataPushMessage> buildMessage(Long orgId, Ticket ticket) {
        List<DataPushMessage> dataPushMessages = new ArrayList<>();
        if (ticket.getTakerBrokerId() == ticket.getMakerBrokerId()) {
            UserTradeDetailMessage takerMessage = buildUserTradeDetailMessage(orgId, ticket.getTakerAccountId(), ticket, Boolean.FALSE);
            dataPushMessages.add(pushDataService.createDataPushMessage(orgId, PushMessageType.TRADE_SPOT, new Gson().toJson(takerMessage), ticket.getTicketId() + "0"));
            UserTradeDetailMessage makerMessage = buildUserTradeDetailMessage(orgId, ticket.getMakerAccountId(), ticket, Boolean.TRUE);
            dataPushMessages.add(pushDataService.createDataPushMessage(orgId, PushMessageType.TRADE_SPOT, new Gson().toJson(makerMessage), ticket.getTicketId() + "1"));
        } else {
            if (orgId.equals(ticket.getTakerBrokerId())) {
                UserTradeDetailMessage takerMessage = buildUserTradeDetailMessage(orgId, ticket.getTakerAccountId(), ticket, Boolean.FALSE);
                dataPushMessages.add(pushDataService.createDataPushMessage(orgId, PushMessageType.TRADE_SPOT, new Gson().toJson(takerMessage), ticket.getTicketId() + "0"));
            }
            if (orgId.equals(ticket.getMakerBrokerId())) {
                UserTradeDetailMessage makerMessage = buildUserTradeDetailMessage(orgId, ticket.getMakerAccountId(), ticket, Boolean.TRUE);
                dataPushMessages.add(pushDataService.createDataPushMessage(orgId, PushMessageType.TRADE_SPOT, new Gson().toJson(makerMessage), ticket.getTicketId() + "1"));
            }
        }
        return dataPushMessages;
    }


    private UserTradeDetailMessage buildUserTradeDetailMessage(Long orgId, Long accountId, Ticket ticket, Boolean isMaker) {
        Account takerAccount = accountMapper.getAccountByAccountId(ticket.getTakerAccountId());
        Account makerAccount = accountMapper.getAccountByAccountId(ticket.getMakerAccountId());
        Long takerUserId = takerAccount != null && takerAccount.getUserId() > 0 ? takerAccount.getUserId() : 0L;
        Long markerUserId = makerAccount != null && makerAccount.getUserId() > 0 ? makerAccount.getUserId() : 0L;
        if (takerUserId.equals(0) || markerUserId.equals(0)) {
            log.info("buildUserTradeDetailMessage orgId {} takerAccountId {} markerAccountId {} takerBrokerId {} makerBrokerId {} ", orgId, ticket.getTakerAccountId(), ticket.getMakerAccountId(),
                    ticket.getTakerBrokerId(), ticket.getMakerBrokerId());
        }

        Symbol symbol = this.symbolMapper.getOrgSymbol(orgId, ticket.getSymbolId());
        if (symbol == null) {
            log.error("Push data build user trade detail message fail not find symbol orgId {} symbolId {} orderId {}",
                    orgId, ticket.getSymbolId(), isMaker ? ticket.getMakerOrderId() : ticket.getTakerOrderId());
            throw new BrokerException(BrokerErrorCode.SYSTEM_ERROR);
        }

        Rate fxRate = basicService.getV3Rate(orgId, symbol.getQuoteTokenId());
        if (fxRate == null) {
            log.error("Push data build user trade detail message fail not find fxRate orgId {} symbolId {}  orderId {} baseTokenId {}",
                    orgId, ticket.getSymbolId(), isMaker ? ticket.getMakerOrderId() : ticket.getTakerOrderId(), symbol.getBaseTokenId());
            throw new BrokerException(BrokerErrorCode.EXCHANGE_RATE_NOT_EXIST);
        }

        String feeTokenId;
        BigDecimal usdFee;
        BigDecimal fee;
        if (isMaker) {
            feeTokenId = ticket.getMakerFeeAsset();
            fee = StringUtils.isNotEmpty(ticket.getMakerFee().getStr())
                    ? new BigDecimal(ticket.getMakerFee().getStr()).stripTrailingZeros().setScale(8, RoundingMode.DOWN)
                    : BigDecimal.ZERO.setScale(8, RoundingMode.DOWN);
            if (StringUtils.isNotEmpty(feeTokenId)) {
                Rate feeRate = basicService.getV3Rate(orgId, ticket.getMakerFeeAsset());
                if (feeRate != null && feeRate.getRatesMap().get("USD") != null) {
                    usdFee = new BigDecimal(StringUtils.isNotEmpty(ticket.getMakerFee().getStr()) ? ticket.getMakerFee().getStr() : "0")
                            .multiply(DecimalUtil.toBigDecimal(feeRate.getRatesMap().get("USD")))
                            .stripTrailingZeros()
                            .setScale(8, RoundingMode.DOWN);
                } else {
                    usdFee = BigDecimal.ZERO;
                }
            } else {
                usdFee = BigDecimal.ZERO;
            }
        } else {
            feeTokenId = ticket.getTakerFeeAsset();
            fee = StringUtils.isNotEmpty(ticket.getTakerFee().getStr())
                    ? new BigDecimal(ticket.getTakerFee().getStr()).stripTrailingZeros().setScale(8, RoundingMode.DOWN)
                    : BigDecimal.ZERO.setScale(8, RoundingMode.DOWN);
            if (StringUtils.isNotEmpty(feeTokenId)) {
                Rate feeRate = basicService.getV3Rate(orgId, ticket.getTakerFeeAsset());
                if (feeRate != null && feeRate.getRatesMap().get("USD") != null) {
                    usdFee = new BigDecimal(StringUtils.isNotEmpty(ticket.getTakerFee().getStr()) ? ticket.getTakerFee().getStr() : "0")
                            .multiply(DecimalUtil.toBigDecimal(feeRate.getRatesMap().get("USD")))
                            .stripTrailingZeros()
                            .setScale(8, RoundingMode.DOWN);
                } else {
                    usdFee = BigDecimal.ZERO;
                }
            } else {
                usdFee = BigDecimal.ZERO;
            }
        }

        BigDecimal usdAmount = new BigDecimal(ticket.getAmount().getStr())
                .multiply(DecimalUtil.toBigDecimal(fxRate.getRatesMap().get("USD")))
                .stripTrailingZeros()
                .setScale(8, RoundingMode.DOWN);
        Integer makerSide = ticket.getIsBuyerMaker() ? 0 : 1;
        Integer takerSide = !ticket.getIsBuyerMaker() ? 0 : 1;
        UserTradeDetailMessage tradeDetailMessage = UserTradeDetailMessage.builder()
                .accountId(accountId != null ? accountId : 0L)
                .userId(isMaker ? markerUserId : takerUserId)
                .quoteToken(StringUtils.isNotEmpty(symbol.getQuoteTokenId()) ? symbol.getQuoteTokenId() : "")
                .baseToken(StringUtils.isNotEmpty(symbol.getBaseTokenId()) ? symbol.getBaseTokenId() : "")
                .amount(StringUtils.isNotEmpty(ticket.getAmount().getStr())
                        ? new BigDecimal(ticket.getAmount().getStr()).stripTrailingZeros().setScale(8, RoundingMode.DOWN)
                        : BigDecimal.ZERO.setScale(8, RoundingMode.DOWN))
                .symbolId(StringUtils.isNotEmpty(ticket.getSymbolId()) ? ticket.getSymbolId() : "")
                .isMaker(isMaker ? 1 : 0)
                .orderId(isMaker ? ticket.getMakerOrderId() : ticket.getTakerOrderId())
                .price(StringUtils.isNotEmpty(ticket.getPrice().getStr())
                        ? new BigDecimal(ticket.getPrice().getStr()).stripTrailingZeros().setScale(8, RoundingMode.DOWN)
                        : BigDecimal.ZERO.setScale(8, RoundingMode.DOWN))
                .quantity(StringUtils.isNotEmpty(ticket.getQuantity().getStr())
                        ? new BigDecimal(ticket.getQuantity().getStr()).stripTrailingZeros().setScale(8, RoundingMode.DOWN)
                        : BigDecimal.ZERO.setScale(8, RoundingMode.DOWN))
                .feeToken(feeTokenId)
                .fee(fee)
                .usdFee(usdFee)
                .usdAmount(usdAmount)
                .createTime(ticket.getTime() > 0 ? ticket.getTime() : 0L)
                .side(isMaker ? makerSide : takerSide)
                .ticketId(ticket.getTicketId() > 0 ? ticket.getTicketId() : 0L)
                .targetOrderId(isMaker ? ticket.getTakerOrderId() : ticket.getMakerOrderId())
                .targetAccountId(isMaker ? ticket.getTakerAccountId() : ticket.getMakerAccountId())
                .targetUserId(isMaker ? takerUserId : markerUserId)
                .signature("")
                .build();
        return tradeDetailMessage;
    }
}
