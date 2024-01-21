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
public class SellAbleValueDetail {

    private BigDecimal remainQuantity;//余额可卖张数

    private BigDecimal closeQuantity;//平仓期权的张数

    private BigDecimal sellAbleQuantity;//可卖期权数量
}
