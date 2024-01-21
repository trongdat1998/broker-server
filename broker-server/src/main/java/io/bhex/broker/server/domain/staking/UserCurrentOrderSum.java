package io.bhex.broker.server.domain.staking;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * 用户活期订单合计
 * @author songxd
 * @date 2020-09-12
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class UserCurrentOrderSum {
    private Long orgId;
    private Long productId;
    private Long userId;
    private BigDecimal amount;
}
