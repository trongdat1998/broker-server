package io.bhex.broker.server.grpc.server.service.staking;

import lombok.*;
import org.springframework.context.ApplicationEvent;

/**
 * 因理财项目刚上线，理财项目利息和持仓生息的利息都是计算好以后在后台由工作人员确认无误后再执行转账操作
 * @author songxd
 * @date 2020-09-01
 */
@Data
@Builder
@EqualsAndHashCode(callSuper = true)
public class StakingTransferEvent extends ApplicationEvent {

    private static final long  serialVersionUID = 2067985978439028190L;

    public StakingTransferEvent(Long orgId, Long productId, Long rebateId, Integer rebateType, Integer productType){
        super(rebateId);
        this.orgId = orgId;
        this.productId = productId;
        this.rebateId = rebateId;
        this.rebateType = rebateType;
        this.productType = productType;
    }

    private Long orgId;
    private Long productId;
    private Long rebateId;
    private Integer rebateType;
    /**
     * 转账类型：定期、持仓
     */
    private Integer productType;
}
