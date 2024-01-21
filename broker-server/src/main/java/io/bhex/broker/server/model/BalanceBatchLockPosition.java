package io.bhex.broker.server.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Column;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import java.math.BigDecimal;

@NoArgsConstructor
@AllArgsConstructor
@Data
@Builder(builderClassName = "Builder", toBuilder = true)
@Table(name = "tb_balance_batch_lock_position")
public class BalanceBatchLockPosition {

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;
    private Long taskId;
    private Long orgId;
    private String clientOrderId;
    private Long lockId; // 默认为0
    @Column(name = "`desc`")
    private String desc;
    private Long userId;
    private Long accountId;
    private String tokenId;
    private BigDecimal amount; // 锁仓量
    private BigDecimal remainUnlockedAmount; // 剩余未解锁量
    private Integer toPositionLocked; // 是否转入locked position
    private Integer status;
    private String response;
    private Long created;
    private Long updated;

}
