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
public class AssetFutures {

    private BigDecimal futuresCoinUSDTTotal;

    private BigDecimal futuresCoinBTCTotal;

}
