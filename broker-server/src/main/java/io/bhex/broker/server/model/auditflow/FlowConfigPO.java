package io.bhex.broker.server.model.auditflow;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;
import java.io.Serializable;

/**
 * 审批流配置
 *
 * @author generator
 * @date
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "tb_flow_config")
public class FlowConfigPO implements Serializable {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer id;

    private Long orgId;

    /**
     * 业务类型:1=空投
     */
    private Integer bizType;

    /**
     * 流程名称
     */
    private String flowName;

    /**
     * 审批级别
     */
    private Integer levelCount;

    /**
     * 是否允许修改 0=否 1=是
     */
    private Integer allowModify;

    /**
     * 是否允许禁用:0=否 1=是
     */
    private Integer allowForbidden;

    /**
     * 状态 1=启用 0=禁用
     */
    private Integer status;

    /**
     * 创建人
     */
    private Long createUser;

    /**
     * 创建人姓名
     */
    private String createUserName;

    /**
     * 最后修改人
     */
    private Long modifyUser;

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