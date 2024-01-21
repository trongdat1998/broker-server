package io.bhex.broker.server.grpc.server.service.auditflow;

import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.context.ApplicationEvent;

/**
 * flow audit end event
 *
 * @author songxd
 * @date 2021-01-13
 */
@Data
@Builder
@EqualsAndHashCode(callSuper = true)
public class FlowAuditEndEvent extends ApplicationEvent {

    private static final long serialVersionUID = 1168135582136357558L;

    public FlowAuditEndEvent(Long orgId, Long bizId, Integer bizType, Integer auditStatus) {
        super(bizId);
        this.orgId = orgId;
        this.bizId = bizId;
        this.bizType = bizType;
        this.auditStatus = auditStatus;
    }
    private Long orgId;
    /**
     * id
     */
    private Long bizId;
    /**
     * 业务类型 1=免费空投 2=持币空投
     */
    private Integer bizType;
    /**
     * 审核状态：1=通过
     */
    private Integer auditStatus;
}