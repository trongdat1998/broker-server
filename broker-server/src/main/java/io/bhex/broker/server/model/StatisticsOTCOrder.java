package io.bhex.broker.server.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.sql.Timestamp;

@NoArgsConstructor
@AllArgsConstructor
@Data
@Builder
public class StatisticsOTCOrder {

    private Long orderId;
    private Long orgId;
    private Long userId;
    private Long accountId;
    private Integer side;
    private String tokenId;
    private String currencyId;
    private BigDecimal price;
    private BigDecimal quantity;
    private BigDecimal amount;
    private BigDecimal fee;
    private Integer paymentType;
    private Integer status;
    private Timestamp transferDate;
    private Timestamp createDate;
    private Timestamp updateDate;

}
