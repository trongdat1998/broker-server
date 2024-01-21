package io.bhex.broker.server.grpc.server.service.auditflow;

import io.bhex.broker.grpc.auditflow.*;
import io.bhex.broker.server.model.auditflow.FlowApprovedRecordPO;
import io.bhex.broker.server.model.auditflow.FlowBizRecordPO;
import io.bhex.broker.server.primary.mapper.auditflow.FlowBizRecordMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.session.RowBounds;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import tk.mybatis.mapper.entity.Example;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;

/**
 * 流程：业务记录
 *
 * @author songxd
 * @date 2021-01-13
 */
@Service
@Slf4j
public class FlowBizRecordService {
    @Resource
    private FlowBizRecordMapper flowBizRecordMapper;

    private final static Integer DEFAULT_MAX_LIMIT = 100;

    /**
     * get audit record count
     *
     * @param orgId
     * @param flowConfigId
     * @return
     */
    public Integer getAuditRecordCount(Long orgId, Integer flowConfigId){
        return flowBizRecordMapper.getAuditCount(orgId, flowConfigId);
    }

    /**
     * get biz records
     *
     * @param request request
     * @return
     */
    public AdminFlowGetAuditRecordListReply getAuditRecordList(AdminFlowGetRecordListRequest request) {
        AdminFlowGetAuditRecordListReply.Builder builder = AdminFlowGetAuditRecordListReply.newBuilder();

        List<FlowBizRecordPO> bizRecordList = flowBizRecordMapper.listAuditRecord(request.getOrgId()
                , request.getBizType(), request.getUserId(), request.getStartRecordId()
                , request.getLimit() > DEFAULT_MAX_LIMIT ? DEFAULT_MAX_LIMIT : request.getLimit());
        if (CollectionUtils.isEmpty(bizRecordList)) {
            return builder.setCode(0).setMessage("success").addAllRecords(new ArrayList<>()).build();
        }
        List<FlowBizRecord> replyRecordList = new ArrayList<>();

        bizRecordList.forEach(bizRecord -> {
                    replyRecordList.add(FlowBizRecord.newBuilder()
                            .setId(bizRecord.getId())
                            .setOrgId(bizRecord.getOrgId())
                            .setFlowConfigId(bizRecord.getFlowConfigId())
                            .setBizId(bizRecord.getBizId())
                            .setBizType(bizRecord.getBizType())
                            .setBizTitle(bizRecord.getBizTitle())
                            .setApplicant(bizRecord.getApplicant())
                            .setApplicantName(bizRecord.getApplicantName())
                            .setApplyDate(bizRecord.getApplyDate())
                            .setCurrentLevel(bizRecord.getCurrentLevel())
                            .setApprover(bizRecord.getApprover())
                            .setApproverName(bizRecord.getApproverName())
                            .setStatus(bizRecord.getStatus())
                            .setCreatedAt(bizRecord.getCreatedAt())
                            .build());
                }
        );
        return builder.setCode(0).setMessage("success").addAllRecords(replyRecordList).build();
    }


    /**
     * get approved list
     *
     * @param request
     * @return
     */
    public AdminFlowGetApprovedRecordListReply getApprovedList(AdminFlowGetRecordListRequest request){
        AdminFlowGetApprovedRecordListReply.Builder builder = AdminFlowGetApprovedRecordListReply.newBuilder();

        List<FlowApprovedRecordPO> approvedRecordList = flowBizRecordMapper.listApprovedRecord(request.getOrgId()
                , request.getBizType(), request.getUserId(), request.getStartRecordId()
                , request.getLimit() > DEFAULT_MAX_LIMIT ? DEFAULT_MAX_LIMIT : request.getLimit());
        if (CollectionUtils.isEmpty(approvedRecordList)) {
            return builder.setCode(0).setMessage("success").addAllRecords(new ArrayList<>()).build();
        }
        List<FlowApprovedRecord> replyRecordList = new ArrayList<>();

        approvedRecordList.forEach(approvedRecord -> {
                    replyRecordList.add(FlowApprovedRecord.newBuilder()
                            .setRecordId(approvedRecord.getRecordId())
                            .setOrgId(approvedRecord.getOrgId())
                            .setFlowConfigId(approvedRecord.getFlowConfigId())
                            .setBizId(approvedRecord.getBizId())
                            .setBizType(approvedRecord.getBizType())
                            .setBizTitle(approvedRecord.getBizTitle())
                            .setApplicant(approvedRecord.getApplicant())
                            .setApplicantName(approvedRecord.getApplicantName())
                            .setApplyDate(approvedRecord.getApplyDate())
                            .setAuditDate(approvedRecord.getAuditDate())
                            .setLevel(approvedRecord.getLevel())
                            .setApprover(approvedRecord.getApprover())
                            .setApproverName(approvedRecord.getApproverName())
                            .setApprovalStatus(approvedRecord.getApprovalStatus())
                            .setApprovalNote(approvedRecord.getApprovalNote())
                            .build());
                }
        );
        return builder.setCode(0).setMessage("success").addAllRecords(replyRecordList).build();
    }
}
