package io.bhex.broker.server.grpc.server.service.staking;

import lombok.*;
import org.springframework.context.ApplicationEvent;

import java.math.BigDecimal;

/**
 * 活期赎回Event
 *
 * @author songxd
 * @date 2020-09-08
 */
@Data
@Builder
@EqualsAndHashCode(callSuper = true)
public class StakingRedeemEvent extends ApplicationEvent {

    private static final long serialVersionUID = 2168135582136357552L;

    public StakingRedeemEvent(Long orgId, Long userId, Long productId, Long assetId, Long transferId
            , BigDecimal amounts) {
        super(assetId);
        this.orgId = orgId;
        this.userId = userId;
        this.productId = productId;
        this.assetId = assetId;
        this.transferId = transferId;
        this.amounts = amounts;
    }
    private Long orgId;
    private Long userId;
    private Long productId;
    private Long assetId;
    private Long transferId;
    private BigDecimal amounts;
}