package io.bhex.broker.server.grpc.server.service.po;

import lombok.Data;

import java.util.List;

@Data
public class BanxaPrices {
    private String spot_price;
    private List<BanxaPrice> prices;
}

