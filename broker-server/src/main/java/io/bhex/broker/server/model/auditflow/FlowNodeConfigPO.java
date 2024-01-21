package io.bhex.broker.server.model.auditflow;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;
import java.io.Serializable;

/**
 * 审批流节点配置
 *
 * @author generator
 * @date 2021-01-12
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "tb_flow_node_config")
public class FlowNodeConfigPO implements Serializable {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer id;

    private Long orgId;

    private Integer flowConfigId;

    /**
     * 当前审批级别
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
     * 是否发送通知:1=是否 1=否
     */
    private Integer allowNotify;

    /**
     * 通知方式:0=邮件 1=短信 2=邮件+短信
     */
    private Integer notifyMode;

    /**
     * 是否允许越过:0=否 1=是
     */
    private Integer allowPass;

    /**
     * 状态 0=删除 1=正常
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

    /**
     * 审批人Email
     */
    private String approverEmail;

    /**
     * 审批人Phone
     */
    private String approverPhone;

    private String language;

    private static final long serialVersionUID = 1L;
}