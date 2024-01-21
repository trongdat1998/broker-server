package io.bhex.broker.server.message;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Map;

/**
 * 理财产品申购Message
 *
 * @author songxd
 * @date 2020-07-30
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@Deprecated
public class StakingSubscribeMessage implements Serializable {
    private Long orgId;
    private Long userId;
    private Long userAccountId;
    private Long productId;
    private Long targetAccountId;
    private Integer productType;
    private Long transferId;
    private String tokenId;
    private Integer lots;
    private BigDecimal amount;
    private Long interestStartDate;
    /**
     * 请求时间
     */
    private Long requestTime;

    private Map<String,String> attrPrams;
}
