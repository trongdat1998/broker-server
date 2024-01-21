package io.bhex.broker.server.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@NoArgsConstructor
@AllArgsConstructor
@Data
@Builder
public class StatisticsSymbolTradeFee {

    private String symbolId;
    private String feeTokenId;
    private BigDecimal feeTotal;

}
