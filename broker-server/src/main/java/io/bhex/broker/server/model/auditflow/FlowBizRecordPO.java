package io.bhex.broker.server.model.auditflow;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;
import java.io.Serializable;

/**
 * 审批业务记录
 *
 * @author generator
 * @date 2021-01-12
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "tb_flow_biz_record")
public class FlowBizRecordPO implements Serializable {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer id;

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
     * 申请时间
     */
    private Long applyDate;

    /**
     * 申请人
     */
    private Long applicant;

    /**
     * 申请人姓名
     */
    private String applicantName;

    /**
     * 申请人Email
     */
    private String applicantEmail;

    /**
     * 申请人mobile
     */
    private String applicantMobile;

    /**
     * 当前所处审批节点
     */
    private Integer flowNodeId;

    /**
     * 当前审批级别
     */
    private Integer currentLevel;

    /**
     * 审批人
     */
    private Long approver;

    /**
     * 审批人姓名
     */
    private String approverName;

    /**
     * 状态 0=待审批 1=审批通过 2=审批拒绝
     */
    private Integer status;

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