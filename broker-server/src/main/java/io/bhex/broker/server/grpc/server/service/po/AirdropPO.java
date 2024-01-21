package io.bhex.broker.server.grpc.server.service.po;

import lombok.Data;

@Data
public class AirdropPO {

    public static final Integer AIRDROP_TYPE_CANDY = 1;
    public static final Integer AIRDROP_TYPE_FORK = 2;

    public static final Integer USER_TYPE_ALL = 1;
    public static final Integer USER_TYPE_SPECIAL = 2;

}
