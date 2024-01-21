package io.bhex.broker.server.domain;

public enum OtcThirdPartyId {

    BANXA(101), // banxa

    MOONPAY(102);

    private long id;

    OtcThirdPartyId(long id) {
        this.id = id;
    }

    public long getId() {
        return id;
    }

}
