package io.bhex.broker.server.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Column;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;

@NoArgsConstructor
@AllArgsConstructor
@Data
@Builder
@Table(name = "tb_balance_batch_transfer_task")
public class BalanceBatchTransferTask {

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;
    private Long orgId;
    private String clientOrderId;
    private Integer type; // 0 transfer 1 air_drop
    @Column(name = "`desc`")
    private String desc; // 备注
    private Integer status;
    private Long created;
    private Long updated;

}
