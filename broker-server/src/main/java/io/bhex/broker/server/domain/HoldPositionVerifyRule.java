package io.bhex.broker.server.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 持仓验证规则
 *
 * @author songxd
 * @date 2020-09-17
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class HoldPositionVerifyRule {
    /**
     * 持仓金额
     */
    private String positionVolume;

    /**
     * 持仓币种
     */
    private String positionToken;

    /**
     * 平均持仓金额
     */
    private String verifyAvgBalanceVolume;

    /**
     * 平均持仓币种
     */
    private String verifyAvgBalanceToken;

    /**
     * 平均持仓开始时间
     */
    private Long verifyAvgBalanceStartTime;

    /**
     * 平均持仓结束时间
     */
    private Long verifyAvgBalanceEndTime;
}
