package io.bhex.broker.server.message.service;

public class MessageHandleException extends RuntimeException {

    public MessageHandleException(String message) {
        super(message);
    }

    public MessageHandleException(String message, Throwable cause) {
        super(message, cause);
    }
}
