package io.bhex.broker.server.grpc.server.service.auditflow;

import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.context.ApplicationEvent;

/**
 * flow audit event
 *
 * @author songxd
 * @date 2021-01-13
 */
@Data
@Builder
@EqualsAndHashCode(callSuper = true)
public class FlowAuditEvent extends ApplicationEvent {

    private static final long serialVersionUID = 1168135582136357566L;

    public FlowAuditEvent(Long orgId, Long bizId, Integer bizType, String title, Long applyDate
            , Long applicant, String applicantName, String applicantEmail) {
        super(bizId);
        this.orgId = orgId;
        this.bizId = bizId;
        this.bizType = bizType;
        this.title = title;
        this.applyDate = applyDate;
        this.applicant = applicant;
        this.applicantName = applicantName;
        this.applicantEmail = applicantEmail;
    }

    private Long orgId;
    private Long bizId;
    private Integer bizType;
    private String title;
    private Long applyDate;
    private Long applicant;
    private String applicantName;
    private String applicantEmail;
}