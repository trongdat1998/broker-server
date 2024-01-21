package io.bhex.broker.server.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * @author wangsc
 * @description org触发的业务推送
 * @date 2020-09-12 12:09
 */
@Data
@Builder(builderClassName = "Builder", toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "tb_org_business_push_record")
public class OrgBusinessPushRecord {
    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;
    private Long orgId;
    private String businessType;
    /**
     * 推送范围 1全部 0符合条件的用户
     */
    private Integer rangeType;
    /**
     * 业务通知的动态参数(json格式)
     */
    private String reqParam;
    /**
     * pushUrl的扩展参数(json格式)
     */
    private String urlParam;
    /**
     * 发送状态0-待执行 1-执行成功 2-执行异常
     */
    private Integer status;
    /**
     * 异常备注
     */
    private String remark;
    private Long sendCount;
    private Long created;
    private Long updated;
}
