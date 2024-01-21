package io.bhex.broker.server.grpc.server.service.po;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class UserTradeDetailMessage {

    private Long id;

    private Long userId;

    private Long accountId;

    private String symbolId;

    private String baseToken;

    private String quoteToken;

    private Long orderId;

    private BigDecimal quantity;

    private BigDecimal amount;

    private BigDecimal usdAmount;

    private BigDecimal price;

    private String feeToken;

    private BigDecimal fee;

    private BigDecimal usdFee;

    private Integer isMaker;

    private Long createTime;

    private String signature;

    /** 新增字段**/

    private Integer side;

    private Long ticketId;

    private Long targetOrderId;

    private Long targetAccountId;

    private Long targetUserId;
}
