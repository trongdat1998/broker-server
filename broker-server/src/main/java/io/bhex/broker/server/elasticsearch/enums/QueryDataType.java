package io.bhex.broker.server.elasticsearch.enums;

public enum QueryDataType {

    COIN(1),
    OPTION(2),
    FUTURES(3);

    private int type;

    QueryDataType(int type) {
        this.type = type;
    }

    public int type() {
        return type;
    }

}
