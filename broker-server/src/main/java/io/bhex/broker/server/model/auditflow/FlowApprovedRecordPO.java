package io.bhex.broker.server.model.auditflow;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;

/**
 * 流程已审批记录
 *
 * @author songxd
 * @date 2021-01-13
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class FlowApprovedRecordPO {

    private Integer recordId;

    private Long orgId;

    /**
     * 流程ID
     */
    private Integer flowConfigId;

    /**
     * 业务记录ID
     */
    private Long bizId;

    /**
     * 业务类型:1=空投
     */
    private Integer bizType;

    /**
     * 业务标题
     */
    private String bizTitle;

    /**
     * 申请人
     */
    private Long applicant;

    /**
     * 申请人姓名
     */
    private String applicantName;

    /**
     * 申请时间
     */
    private Long applyDate;

    /**
     * 审批时间
     */
    private Long auditDate;

    /**
     * 审批级别
     */
    private Integer level;

    /**
     * 审批人
     */
    private Long approver;

    /**
     * 审批人姓名
     */
    private String approverName;

    /**
     * 审批状态 1=审批通过 2=审批拒绝
     */
    private Integer approvalStatus;

    /**
     * 审批备注
     */
    private String approvalNote;
}
