package io.bhex.broker.server.message.service;

import io.bhex.base.common.CommonNotifyMessage;

public interface MessageHandler {

    void handleMessage(CommonNotifyMessage notifyMessage);
}
