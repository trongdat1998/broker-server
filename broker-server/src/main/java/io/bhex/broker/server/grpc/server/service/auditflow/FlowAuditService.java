package io.bhex.broker.server.grpc.server.service.auditflow;


import com.google.common.base.Strings;
import com.google.gson.JsonObject;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.grpc.auditflow.AdminFlowAuditReply;
import io.bhex.broker.grpc.auditflow.AdminFlowAuditRequest;
import io.bhex.broker.grpc.common.Header;
import io.bhex.broker.server.domain.NoticeBusinessType;
import io.bhex.broker.server.grpc.server.service.NoticeTemplateService;
import io.bhex.broker.server.model.auditflow.*;
import io.bhex.broker.server.primary.mapper.auditflow.FlowAuditLogsMapper;
import io.bhex.broker.server.primary.mapper.auditflow.FlowBizRecordMapper;
import io.bhex.broker.server.primary.mapper.auditflow.FlowConfigMapper;
import io.bhex.broker.server.primary.mapper.auditflow.FlowNodeConfigMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.core.task.TaskExecutor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.util.concurrent.CompletableFuture;

/**
 * audit process
 *
 * @author songxd
 * @date 2021-01-13
 */
@Service
@Slf4j
public class FlowAuditService {
    @Resource
    private FlowAuditLogsMapper flowAuditLogsMapper;

    @Autowired
    private ApplicationContext applicationContext;

    @Resource
    private FlowBizRecordMapper flowBizRecordMapper;

    @Resource
    private FlowConfigMapper flowConfigMapper;

    @Resource
    private FlowNodeConfigMapper flowNodeConfigMapper;

    @Resource(name = "asyncTaskExecutor")
    private TaskExecutor taskExecutor;

    @Resource
    private NoticeTemplateService noticeTemplateService;

    /**
     * audit process
     *
     * @param request
     * @return 1= not the current approver
     */
    @Transactional(rollbackFor = Exception.class)
    public AdminFlowAuditReply auditProcess(AdminFlowAuditRequest request) {
        AdminFlowAuditReply.Builder builder = AdminFlowAuditReply.newBuilder();

        long currentTimeMillis = System.currentTimeMillis();
        // 1. check current approver is current user
        FlowBizRecordPO bizRecordPo = checkBizRecord(request);
        boolean isCurrentApprover = bizRecordPo.getApprover().equals(request.getUserId());
        if (!isCurrentApprover) {
            return builder.setCode(1).setMessage("").build();
        }

        // 2. get next flow node
        FlowNodeConfigPO flowNextNode = getNextNode(bizRecordPo.getOrgId(), bizRecordPo.getFlowConfigId(), bizRecordPo.getFlowNodeId(), bizRecordPo.getCurrentLevel());

        try {
            if (flowNextNode == null || request.getAuditStatus() == 0) {
                // 3. the flow end, update biz record status by request.auditStatus
                flowBizRecordMapper.updateStatus(bizRecordPo.getOrgId(), bizRecordPo.getId(), request.getAuditStatus() == 0 ? 2 : 1, currentTimeMillis);

            } else {
                // 3. update flow node info of record (flow_node_id,current_level,approver,approver_name)
                flowBizRecordMapper.updateFlowNodeInfo(bizRecordPo.getOrgId(), bizRecordPo.getId(), flowNextNode.getId(), flowNextNode.getLevel()
                        , flowNextNode.getApprover(), flowNextNode.getApproverName(), currentTimeMillis);
            }

            // 4. insert audit logs
            flowAuditLogsMapper.insertSelective(FlowAuditLogsPO.builder()
                    .orgId(request.getOrgId())
                    .flowConfigId(bizRecordPo.getFlowConfigId())
                    .flowNodeId(bizRecordPo.getFlowNodeId())
                    .level(bizRecordPo.getCurrentLevel())
                    .recordId(bizRecordPo.getId())
                    .approver(request.getUserId())
                    .approverName(request.getUserName())
                    .approvalStatus(request.getAuditStatus())
                    .approvalNote(request.getAuditNote())
                    .createdAt(currentTimeMillis)
                    .updatedAt(currentTimeMillis)
                    .build());

            // 5. send audit end event
            if (flowNextNode == null || request.getAuditStatus() == 0) {
                // the flow end, send auditEndEvent
                CompletableFuture.runAsync(() ->
                        applicationContext.publishEvent(
                                FlowAuditEndEvent.builder()
                                        .orgId(bizRecordPo.getOrgId())
                                        .bizId(bizRecordPo.getBizId())
                                        .bizType(bizRecordPo.getBizType())
                                        .auditStatus(request.getAuditStatus() == 0 ? 2 : 1)
                                        .build()
                        ), taskExecutor);
                // send notice to applicant
                sendAuditFinishNotice(bizRecordPo);
            } else {
                if (flowNextNode.getAllowNotify().equals(1)) {
                    // send notify
                    sendNotice(flowNextNode, bizRecordPo.getBizType());
                }
            }

        } catch (Exception ex) {
            log.error("flow audit error:{}-{}", request.toString(), ex.getMessage());
            throw ex;
        }
        return builder.setCode(0).setMessage("").build();
    }

    /**
     * sync biz record to flow biz records
     *
     * @param syncEvent
     */
    public void syncAuditBizRecord(FlowAuditEvent syncEvent) {

        long currentTimeMillis = System.currentTimeMillis();

        // get flowConfig by bizType
        FlowConfigPO flowConfig = flowConfigMapper.selectOne(FlowConfigPO.builder().orgId(syncEvent.getOrgId()).bizType(syncEvent.getBizType()).build());
        if (flowConfig == null || flowConfig.getStatus() == 0) {
            log.error("flow sync biz record error, flow config is null or forbidden:{}", syncEvent.toString());
            // the flow not config, send auditEndEvent
            CompletableFuture.runAsync(() ->
                    applicationContext.publishEvent(
                            FlowAuditEndEvent.builder()
                                    .orgId(syncEvent.getOrgId())
                                    .bizId(syncEvent.getBizId())
                                    .bizType(syncEvent.getBizType())
                                    .auditStatus(1)
                                    .build()
                    ), taskExecutor);
            return;
        }
        // get first flow node
        FlowNodeConfigPO flowNode = flowNodeConfigMapper.getNextNodeConfig(flowConfig.getOrgId(), flowConfig.getId(), 0, 0);
        if (flowNode == null) {
            log.error("flow sync biz record error, flow node is null:{}", syncEvent.toString());
            throw new BrokerException(BrokerErrorCode.DB_RECORD_ERROR);
        } else {
            if (flowNode.getAllowNotify().equals(1)) {
                // send notify
                sendNotice(flowNode, syncEvent.getBizType());
            }
        }
        // insert biz record
        flowBizRecordMapper.insertSelective(FlowBizRecordPO.builder()
                .orgId(syncEvent.getOrgId())
                .flowConfigId(flowConfig.getId())
                .bizId(syncEvent.getBizId())
                .bizType(syncEvent.getBizType())
                .bizTitle(syncEvent.getTitle())
                .applyDate(syncEvent.getApplyDate())
                .applicant(syncEvent.getApplicant())
                .applicantName(syncEvent.getApplicantName())
                .applicantEmail(syncEvent.getApplicantEmail())
                .applicantMobile("")
                .flowNodeId(flowNode.getId())
                .currentLevel(flowNode.getLevel())
                .approver(flowNode.getApprover())
                .approverName(flowNode.getApproverName())
                .status(0)
                .createdAt(currentTimeMillis)
                .updatedAt(currentTimeMillis)
                .build());
    }

    /**
     * check current biz record approver
     *
     * @param request
     * @return
     */
    private FlowBizRecordPO checkBizRecord(AdminFlowAuditRequest request) {
        FlowBizRecordPO bizRecordPo = flowBizRecordMapper.selectOne(FlowBizRecordPO.builder().id(request.getRecordId()).orgId(request.getOrgId()).build());
        if (bizRecordPo == null || !bizRecordPo.getStatus().equals(0)) {
            log.warn("flow audit biz db record error: orgId->{},recordId->{}", request.getOrgId(), request.getRecordId());
            throw new BrokerException(BrokerErrorCode.DB_RECORD_ERROR);
        }
        return bizRecordPo;
    }

    /**
     * get next flow node
     *
     * @param orgId
     * @param flowConfigId
     * @param flowNodeId
     * @param level
     * @return
     */
    private FlowNodeConfigPO getNextNode(Long orgId, Integer flowConfigId, Integer flowNodeId, Integer level) {
        return flowNodeConfigMapper.getNextNodeConfig(orgId, flowConfigId, flowNodeId, level);
    }

    /**
     * send audit notice
     *
     * @param nodeConfig
     */
    private void sendNotice(FlowNodeConfigPO nodeConfig, Integer bizType) {
        if (nodeConfig.getAllowNotify() == 1) {
            boolean smsMode = nodeConfig.getNotifyMode() == 1 || nodeConfig.getNotifyMode() == 2;
            boolean emailMode = nodeConfig.getNotifyMode() == 0 || nodeConfig.getNotifyMode() == 2;

            String bizName = "";

            Header header = Header.newBuilder().setOrgId(0L).setUserId(0L).setLanguage(nodeConfig.getLanguage()).build();
            JsonObject jsonObject = new JsonObject();
            jsonObject.addProperty("UserName", "");
            jsonObject.addProperty("BizType", bizName);

            if (smsMode && !Strings.isNullOrEmpty(nodeConfig.getApproverPhone())) {
                //noticeTemplateService.sendSmsNoticeAsync(header, user.getUserId(), NoticeBusinessType.FIND_PASSWORD_SUCCESS,user.getNationalCode(), user.getMobile());
            }
            if (emailMode && !Strings.isNullOrEmpty(nodeConfig.getApproverEmail())) {
                noticeTemplateService.sendEmailNoticeAsync(header, 0L, NoticeBusinessType.BROKER_ADMIN_FLOW_AUDIT_NOTICE, nodeConfig.getApproverEmail(), jsonObject);
            }
        }
    }

    /**
     * send audit notice
     *
     * @param bizRecordPo
     */
    private void sendAuditFinishNotice(FlowBizRecordPO bizRecordPo) {

        if(!Strings.isNullOrEmpty(bizRecordPo.getApplicantEmail())) {

            FlowNodeConfigPO nodeConfig = flowNodeConfigMapper.selectOne(FlowNodeConfigPO.builder()
                    .orgId(bizRecordPo.getOrgId())
                    .flowConfigId(bizRecordPo.getFlowConfigId())
                    .id(bizRecordPo.getFlowNodeId())
                    .build());

            if(nodeConfig == null){
                return;
            }

            Header header = Header.newBuilder().setOrgId(0L).setUserId(0L).setLanguage(nodeConfig.getLanguage()).build();
            JsonObject jsonObject = new JsonObject();
            jsonObject.addProperty("UserName", "");
            jsonObject.addProperty("BizType", "");

            noticeTemplateService.sendEmailNoticeAsync(header, 0L, NoticeBusinessType.BROKER_ADMIN_FLOW_AUDIT_FINISH_NOTICE, bizRecordPo.getApplicantEmail(), jsonObject);
        }
    }
}
