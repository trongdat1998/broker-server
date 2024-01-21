package io.bhex.broker.server.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Builder
@AllArgsConstructor
@NoArgsConstructor
@Data
public class AssetOption {

    private BigDecimal optionUSDTTotal;

    private BigDecimal optionBTCTotal;

    private BigDecimal optionCoinBTCTotal;

    private BigDecimal optionCoinUSDTTotal;

}
