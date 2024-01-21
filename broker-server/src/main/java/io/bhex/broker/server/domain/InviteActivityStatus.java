package io.bhex.broker.server.domain;

public enum InviteActivityStatus {

    OFFLINE(0, "下线"),
    ONLINE(1, "在线"),;

    InviteActivityStatus(int status, String desc) {
        this.status = status;
        this.desc = desc;
    }

    private int status;

    private String desc;

    public int getStatus() {
        return status;
    }

    public String getDesc() {
        return desc;
    }

}
