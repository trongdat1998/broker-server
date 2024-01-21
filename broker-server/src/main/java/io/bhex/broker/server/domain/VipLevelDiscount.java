package io.bhex.broker.server.domain;

import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;

@Data
@Builder(builderClassName = "Builder", toBuilder = true)
public class VipLevelDiscount {

    private BigDecimal vipMakerDiscount;

    private BigDecimal vipTakerDiscount;

}
