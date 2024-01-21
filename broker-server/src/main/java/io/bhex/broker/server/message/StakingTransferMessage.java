package io.bhex.broker.server.message;

import io.bhex.broker.grpc.staking.StakingProductType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * 因理财项目刚上线，理财项目利息和持仓生息的利息都是计算好以后在后台由工作人员确认无误后再执行转账操作
 * @author songxd
 * @date 2020-08-03
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@Deprecated
public class StakingTransferMessage implements Serializable {
    private Long orgId;
    private Long productId;
    private Long rebateId;
    /**
     * 转账类型：定期、持仓
     */
    private StakingProductType stakingProductType;
}