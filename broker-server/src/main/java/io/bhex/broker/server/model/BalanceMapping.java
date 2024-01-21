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
@Builder
@Table(name = "tb_balance_mapping")
public class BalanceMapping {

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;
    private Long orgId;
    private String clientOrderId;
    private Long sourceTransferId;
    private Integer sourceSubject;
    private Long sourceUserId;
    private Long sourceAccountId;
    private String sourceTokenId;
    private BigDecimal sourceAmount;
    private Integer fromSourceLock;
    private Integer toTargetLock;
    private Long targetTransferId;
    private Integer targetSubject;
    private Long targetUserId;
    private Long targetAccountId;
    private String targetTokenId;
    private BigDecimal targetAmount;
    private Integer fromTargetLock;
    private Integer toSourceLock;
    private Integer status;
    private String response;
    private Long created;
    private Long updated;

}
