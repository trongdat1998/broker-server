package io.bhex.broker.server.grpc.server.service.kyc.tencent;

public class WebankKycException extends RuntimeException {

    private String bizCode;
    private String bizMsg;

    public WebankKycException(String bizCode, String bizMsg) {
        super(String.format("code: %s msg: %s", bizCode, bizMsg));
        this.bizCode = bizCode;
        this.bizMsg = bizMsg;
    }

    public WebankKycException(String message, Throwable cause) {
        super(message, cause);
    }

    public String getBizCode() {
        return bizCode;
    }

    public String getBizMsg() {
        return bizMsg;
    }
}
