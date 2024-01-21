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
@Table(name = "tb_user_transfer_record")
public class UserTransferRecord {

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;
    private Long orgId;
    private Long userId;
    private String clientOrderId;
    private Long transferId;
    private Integer subject;
    private Integer subSubject;
    private Long sourceUserId;
    private Long sourceAccountId;
    private Long targetUserId;
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
