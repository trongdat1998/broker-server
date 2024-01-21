package io.bhex.broker.server.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class SymbolTradeInfo {

    private String symbolId;
    private Integer side;
    private BigDecimal totalAmount;
    private BigDecimal totalQuantity;
    private BigDecimal avgPrice;

}
