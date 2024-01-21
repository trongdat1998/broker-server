package io.bhex.broker.server.grpc.server.service.staking;

import lombok.*;
import org.springframework.context.ApplicationEvent;

/**
 * 理财申购事件
 * @author sognxd
 * @date 2020-09-01
 */
@Data
@Builder
@EqualsAndHashCode(callSuper = true)
public class StakingSubscribeEvent extends ApplicationEvent {

    private static final long  serialVersionUID = 1168135582136357552L;

    public StakingSubscribeEvent(Long orgId, Long userId, Long productId, Long orderId,Long transferId, Integer productType) {
        super(orderId);
        this.orgId = orgId;
        this.userId = userId;
        this.productId = productId;
        this.orderId = orderId;
        this.transferId = transferId;
        this.productType = productType;
    }

    private Long orgId;
    private Long userId;
    private Long productId;
    private Long orderId;
    private Long transferId;
    private Integer productType;
}
