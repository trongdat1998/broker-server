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
@Builder(builderClassName = "Builder", toBuilder = true)
public class StatisticsTradeDetail {

    private Long ticketId;
    private Long tradeId;
    private Long orgId;
    private Long orderId;
    private Long userId;
    private Long accountId;
    private String symbolId;
    private Integer orderType;
    private Integer side;
    private BigDecimal price;
    private BigDecimal quantity;
    private BigDecimal amount;
    private String feeToken;
    private BigDecimal fee;
    private String sysTokenId;
    private Integer isUseSysToken;
    private Integer isActuallyUsedSysToken;
    private Timestamp matchTime;
    private Long matchOrgId;
    private Long matchUserId;
    private Long matchAccountId;
    private Timestamp createdAt;
    private Timestamp updatedAt;

}
