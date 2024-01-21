package io.bhex.broker.server.message.service;

import io.bhex.base.common.CommonNotifyMessage;
import io.bhex.base.common.MessageType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

import java.util.Map;

@Slf4j
@Component
public class MessageHandlerFactory implements ApplicationContextAware {

    private Map<String, MessageHandler> messageHandlerMap;

    @Override
    public void setApplicationContext(ApplicationContext appContext) throws BeansException {
        messageHandlerMap = appContext.getBeansOfType(MessageHandler.class);
    }

    private MessageHandler getMessageHandler(MessageType messageType) {
        if (messageType == null) {
            throw new MessageHandleException("getMessageHandler error: messageType is NULL.");
        }

        if (messageHandlerMap == null) {
            throw new MessageHandleException("getMessageHandler error: messageHandlerMap not ready.");
        }

        MessageHandler messageHandler = messageHandlerMap.get(messageType.name());
        if (messageHandler == null) {
            throw new MessageHandleException(
                    "getMessageHandler error: can not find suitable instance for messageType:" + messageType.name());
        }

        return messageHandler;
    }

    public void handleMessage(CommonNotifyMessage notifyMessage) {
        MessageType messageType = notifyMessage.getType();

        try {
            MessageHandler messageHandler = getMessageHandler(messageType);
            messageHandler.handleMessage(notifyMessage);
        } catch (MessageHandleException e) {
            log.error(String.format("handleMessage for meesageType: %s error.", messageType), e);
        } catch (Throwable e) {
            log.error("handleMessage caught exception.", e);
        }
    }
}
