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
@Table(name = "tb_balance_unlock_position")
public class BalanceUnlockPosition {
    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;
    private Long orgId;
    private String clientOrderId;
    private Long unlockId; // 默认为0, 客户端为了保持幂等性，需要生成的
    private Long lockId; // 如果fromPositionLocked==1, 此项需要传入冻结时候的lockId
    @Column(name = "`desc`")
    private String desc;
    private Long userId;
    private Long accountId;
    private String tokenId;
    private BigDecimal amount; // 解锁量
    private Integer fromPositionLocked; // 是否从 positionLocked 解锁
    private Integer status;
    private String response;
    private Long created;
    private Long updated;
}
