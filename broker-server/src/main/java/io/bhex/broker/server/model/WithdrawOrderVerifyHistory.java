package io.bhex.broker.server.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import java.sql.Timestamp;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "tb_withdraw_order_verify_history")
public class WithdrawOrderVerifyHistory {
    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;
    private Long orgId;
    private Long userId;
    private Long accountId;
    private Long withdrawOrderId;

    private String adminUserName;
    private String remark;
    private Timestamp verifyTime;
    private Integer verifyStatus;
    private Integer failedReason;
    private String refuseReason;
}
