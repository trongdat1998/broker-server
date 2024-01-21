package io.bhex.broker.server.domain;

public enum AppChannel {
    UNKNOWN(""),
    OFFICIAL("official"),
    GOOGLEPLAY("googleplay"),
    ENTERPRISE("enterprise"),
    TESTFLIGHT("testflight"),
    APPSTORE("appstore");
    private final String value;
    AppChannel(String value) {
        this.value = value;
    }

    public String value() {
        return this.value;
    }

    public static AppChannel getByValue(String channel){
        if (channel == null || channel.equals("")) {
            return null;
        }
        for (AppChannel appChannel : AppChannel.values()) {
            if (appChannel.value.equals(channel)) {
                return appChannel;
            }
        }
        return null;
    }
    public static void main(String[] args) {
        System.out.println(AppChannel.GOOGLEPLAY.value);
    }
}
