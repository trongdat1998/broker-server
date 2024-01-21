package io.bhex.broker.server.message.service;

public enum NotifyUserStrategy {
    // 短信形式通知
    MOBILE,
    // 邮件形式通知
    EMAIL,
    // 短信和邮件都通知
    BOTH,
    // 都不通知
    NONE
}
