package io.bhex.broker.server.domain.staking;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.util.Map;

/**
 * 理财产品基类
 * @author songxd
 * @date
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class StakingSchema {
    private Long orgId;
    private Long userId;
    private Long userAccountId;
    private Long productId;

    private Integer lots;
    private BigDecimal amount;

    private Map<String,String> attrProperty;
}
