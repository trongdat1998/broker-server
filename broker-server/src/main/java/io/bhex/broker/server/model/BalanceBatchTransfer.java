package io.bhex.broker.server.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import java.math.BigDecimal;

@NoArgsConstructor
@AllArgsConstructor
@Data
@Builder(builderClassName = "Builder", toBuilder = true)
@Table(name = "tb_balance_batch_transfer")
public class BalanceBatchTransfer {

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;
    private Long taskId;
    private Long orgId;
    private String clientOrderId;
    private Integer type;
    private Long transferId;
    private Integer subject;
    private Integer subSubject;
    private Long sourceUserId;
    private Integer sourceAccountType;
    private Long sourceAccountId;
    private Long targetUserId;
    private Integer targetAccountType;
    private Long targetAccountId;
    private String tokenId;
    private BigDecimal amount;
    private Integer fromSourceLock;
    private Integer toTargetLock;
    private Integer status;
    private String response;
    private Long created;
    private Long updated;

}
