package io.bhex.broker.server.model.auditflow;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;
import java.io.Serializable;

/**
 * 审批日志
 *
 * @author generator
 * @date 2021-01-12
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "tb_flow_audit_logs")
public class FlowAuditLogsPO implements Serializable {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer id;

    private Long orgId;

    private Integer flowConfigId;

    private Integer flowNodeId;

    private Integer level;

    private Integer recordId;

    /**
     * 审批人
     */
    private Long approver;

    /**
     * 审批人姓名
     */
    private String approverName;

    /**
     * 审批状态 0=拒绝 1=通过
     */
    private Integer approvalStatus;

    /**
     * 审批备注
     */
    private String approvalNote;

    /**
     * 创建时间
     */
    private Long createdAt;

    /**
     * 修改时间
     */
    private Long updatedAt;

    private static final long serialVersionUID = 1L;
}