package io.bhex.broker.server.domain;

public enum AdvertisementStatus {

    OFFLINE(0),
    ONLINE(1),;

    private int status;

    AdvertisementStatus(int status) {
        this.status = status;
    }

    public int getStatus() {
        return status;
    }

    public static AdvertisementStatus fromType(int status) {
        for (AdvertisementStatus at : AdvertisementStatus.values()) {
            if (at.getStatus() == status) {
                return at;
            }
        }
        return null;
    }

}
