package io.bhex.broker.server.grpc.server.service.auditflow;

import io.bhex.broker.grpc.auditflow.*;
import io.bhex.broker.server.model.auditflow.FlowAuditLogsPO;
import io.bhex.broker.server.primary.mapper.auditflow.FlowAuditLogsMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import tk.mybatis.mapper.entity.Example;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;

/**
 * 流程：审批及审批日志
 *
 * @author songxd
 * @date 2021-01-13
 */
@Service
@Slf4j
public class FlowAuditLogService {
    @Resource
    private FlowAuditLogsMapper flowAuditLogsMapper;

    /**
     * get audit logs by recordId
     *
     * @param request
     * @return
     */
    public AdminFlowGetAuditLogListReply getAuditLogList(AdminFlowGetAuditLogListRequest request){
        AdminFlowGetAuditLogListReply.Builder builder = AdminFlowGetAuditLogListReply.newBuilder();
        Example example = new Example(FlowAuditLogsPO.class);
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("orgId", request.getOrgId());
        criteria.andEqualTo("flowConfigId", request.getFlowConfigId());
        criteria.andEqualTo("recordId", request.getRecordId());
        example.orderBy("id").desc();

        List<FlowAuditLogsPO> auditLogsList = flowAuditLogsMapper.selectByExample(example);

        if (CollectionUtils.isEmpty(auditLogsList)) {
            return builder.setCode(0).setMessage("success").addAllAuditLogs(new ArrayList<>()).build();
        }
        List<FlowAuditLog> replyAuditLogList = new ArrayList<>();

        auditLogsList.forEach(auditLog -> {
            replyAuditLogList.add(FlowAuditLog.newBuilder()
                    .setId(auditLog.getId())
                    .setOrgId(auditLog.getOrgId())
                    .setApprover(auditLog.getApprover())
                    .setApproverName(auditLog.getApproverName())
                    .setLevel(auditLog.getLevel())
                    .setApprovalStatus(auditLog.getApprovalStatus())
                    .setApprovalNote(auditLog.getApprovalNote())
                    .setAuditDate(auditLog.getCreatedAt())
                    .build());
                }
        );
        return builder.setCode(0).setMessage("success").addAllAuditLogs(replyAuditLogList).build();
    }
}
