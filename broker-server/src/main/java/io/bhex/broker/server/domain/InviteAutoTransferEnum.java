package io.bhex.broker.server.domain;

public enum InviteAutoTransferEnum {

    MANUAL(0),
    AUTO(1);

    private int code;

    InviteAutoTransferEnum(int code) {
        this.code = code;
    }

    public int code() {
        return this.code;
    }

    public static InviteAutoTransferEnum fromValue(int code) {
        for (InviteAutoTransferEnum transferEnum : InviteAutoTransferEnum.values()) {
            if (transferEnum.code() == code) {
                return transferEnum;
            }
        }
        return null;
    }

}
