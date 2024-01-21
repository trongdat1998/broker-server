package io.bhex.broker.server.push.bo;

import java.util.Objects;

/**
 * app设备推送索引
 */
public final class AppPushDeviceIndex{
    /**
     * 应用程序id
     */
    private final String appId;
    /**
     * app通道
     */
    private final String appChannel;
    /**
     * 三方通道
     */
    private final String thirdPushType;
    /**
     * 设备类型
     */
    private final String deviceType;

    /**
     * 国际化语言
     */
    private final String language;

    public AppPushDeviceIndex(String appId, String appChannel, String thirdPushType, String deviceType, String language) {
        this.appChannel = appChannel;
        this.thirdPushType = thirdPushType;
        this.appId = appId;
        this.deviceType = deviceType;
        this.language = language;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AppPushDeviceIndex)) return false;
        AppPushDeviceIndex that = (AppPushDeviceIndex) o;
        return Objects.equals(getAppId(), that.getAppId()) &&
                Objects.equals(getAppChannel(), that.getAppChannel()) &&
                Objects.equals(getThirdPushType(), that.getThirdPushType()) &&
                Objects.equals(getDeviceType(), that.getDeviceType()) &&
                Objects.equals(getLanguage(), that.getLanguage());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getAppId(), getAppChannel(), getThirdPushType(), getDeviceType(), getLanguage());
    }

    public String getAppId() {
        return appId;
    }

    public String getAppChannel() {
        return appChannel;
    }

    public String getThirdPushType() {
        return thirdPushType;
    }

    public String getDeviceType() {
        return deviceType;
    }

    public String getLanguage() {
        return language;
    }
}