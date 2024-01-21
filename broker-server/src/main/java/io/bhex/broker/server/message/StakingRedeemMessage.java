package io.bhex.broker.server.message;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.math.BigDecimal;

/**
 * 理财产品(活期)赎回
 *
 * @author songxd
 * @date 2020-07-30
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class StakingRedeemMessage implements Serializable {
    private Long orgId;
    private Long userId;
    private Long assetId;
    private Long productId;
    private Long transferId;
    private Long orgAccountId;
    private String tokenId;
    /**
     * 用户请求时间戳
     */
    private Long requestTime;
    /**
     * 赎回金额
     */
    private BigDecimal amount;
}
