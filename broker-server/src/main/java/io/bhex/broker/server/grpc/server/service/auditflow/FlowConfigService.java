package io.bhex.broker.server.grpc.server.service.auditflow;

import com.google.common.base.Strings;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.bhex.broker.grpc.auditflow.*;
import io.bhex.broker.server.model.auditflow.FlowConfigPO;
import io.bhex.broker.server.model.auditflow.FlowConstant;
import io.bhex.broker.server.model.auditflow.FlowNodeConfigPO;
import io.bhex.broker.server.primary.mapper.auditflow.FlowConfigMapper;
import io.bhex.broker.server.primary.mapper.auditflow.FlowNodeConfigMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.session.RowBounds;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;
import tk.mybatis.mapper.entity.Example;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;;
import java.util.concurrent.TimeUnit;

/**
 * 流程配置
 *
 * @author songxd
 * @date 2021-01-13
 */
@Service
@Slf4j
public class FlowConfigService {

    @Resource
    private FlowConfigMapper flowConfigMapper;

    @Resource
    private FlowNodeConfigMapper flowNodeConfigMapper;

    private final FlowBizRecordService flowBizRecordService;

    private final Cache<String, String> BIZ_TYPE_TEXT_CACHE = CacheBuilder
            .newBuilder()
            .expireAfterWrite(10L, TimeUnit.MINUTES)
            .build();

    public FlowConfigService(FlowBizRecordService flowBizRecordService) {
        this.flowBizRecordService = flowBizRecordService;
    }

    /**
     * save and update
     *
     * @param request request
     * @return code: 1=param error 2=biz type is exists 3=not allow modify 4= not allow forbidden 5=there are unprocessed records
     */
    @Transactional(rollbackFor = Exception.class)
    public AdminSaveFlowConfigReply saveFlowConfig(AdminSaveFlowConfigRequest request) {
        AdminSaveFlowConfigReply.Builder builder = AdminSaveFlowConfigReply.newBuilder();

        if (request.getBizType() == 0 ||
                request.getNodesCount() == 0 || request.getNodesCount() != request.getLevelCount()) {
            // param error
            return builder.setCode(1).setMessage("").setFlowConfigId(0).build();
        }

        long currentTimeMillis = System.currentTimeMillis();
        FlowConfigPO flowConfigPo;

        // check this biz type is exists
        Example flowConfigExample = new Example(FlowConfigPO.class);
        Example.Criteria criteria = flowConfigExample.createCriteria()
                .andEqualTo("orgId", request.getOrgId())
                .andEqualTo("bizType", request.getBizType());
        if (request.getId() != 0) {
            criteria.andEqualTo("id", request.getId());
        }
        FlowConfigPO flowConfig = flowConfigMapper.selectOneByExample(flowConfigExample);

        if (request.getId() == 0) {
            // check this biz type is exists
            if (flowConfig != null && flowConfig.getId() > 0) {
                // biz type exists
                log.warn("flow config is exists:orgId:{} bizType:{},flowId:{}", request.getOrgId(), request.getBizType(), flowConfig.getId());
                return builder.setCode(2).setMessage("").setFlowConfigId(0).build();
            }
        } else {
            // check is allow modify
            if (flowConfig.getAllowModify() == 0) {
                log.warn("flow config not allow modify:orgId:{} bizType:{},flowId:{}", request.getOrgId(), request.getBizType(), flowConfig.getId());
                return builder.setCode(3).setMessage("").setFlowConfigId(0).build();
            }
            // check is set forbidden
/*            if (flowConfig.getAllowForbidden() == 0 && request.getStatus() == 0) {
                log.warn("flow config not allow forbidden:orgId:{} bizType:{},flowId:{}", request.getOrgId(), request.getBizType(), flowConfig.getId());
                return builder.setCode(4).setMessage("").build();
            }*/
            // check is have not audit records
            Integer notAuditCount = flowBizRecordService.getAuditRecordCount(request.getOrgId(), request.getId());
            if (notAuditCount.compareTo(0) > 0) {
                log.warn("flow config have not process record:orgId:{} bizType:{},flowId:{}", request.getOrgId(), request.getBizType(), flowConfig.getId());
                return builder.setCode(5).setMessage("").setFlowConfigId(0).build();
            }
        }
        // insert or update flow config
        flowConfigPo = insertAndUpdateFlowConfig(request, currentTimeMillis);
        // insert or update flow node
        insertAndUpdateFlowNode(request.getNodesList(), flowConfigPo, currentTimeMillis);
        return builder.build();
    }

    /**
     * 获取流程列表
     *
     * @param request
     * @return
     */
    public AdminGetFlowConfigListReply getFlowConfigList(AdminGetFlowConfigListRequest request) {

        AdminGetFlowConfigListReply.Builder builder = AdminGetFlowConfigListReply.newBuilder();

        Example example = new Example(FlowConfigPO.class);
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("orgId", request.getOrgId());
        if (request.getStartFlowConfigId() > 0) {
            criteria.andLessThan("id", request.getStartFlowConfigId());
        }
        if (request.getBizType() > 0) {
            criteria.andEqualTo("bizType", request.getBizType());
        }
        example.orderBy("id").desc();

        List<FlowConfigPO> flowConfigPoList = flowConfigMapper.selectByExampleAndRowBounds(example, new RowBounds(0, request.getLimit()));
        if (CollectionUtils.isEmpty(flowConfigPoList)) {
            return builder.setCode(0)
                    .setMessage("success")
                    .addAllFlows(new ArrayList<>())
                    .build();
        }
        List<FlowConfig> flowConfigList = new ArrayList<>();

        flowConfigPoList.forEach(flowConfig -> {
                    flowConfigList.add(FlowConfig.newBuilder()
                            .setId(flowConfig.getId())
                            .setOrgId(flowConfig.getOrgId())
                            .setUserId(flowConfig.getCreateUser())
                            .setUserName(flowConfig.getCreateUserName())
                            .setFlowName(flowConfig.getFlowName())
                            .setBizType(flowConfig.getBizType())
                            .setBizName("")
                            .setLevelCount(flowConfig.getLevelCount())
                            .setAllowModify(flowConfig.getAllowModify())
                            .setAllowForbidden(flowConfig.getAllowForbidden())
                            .setCreatedAt(flowConfig.getCreatedAt())
                            .setStatus(flowConfig.getStatus())
                            .build());
                }
        );
        return builder.setCode(0).setMessage("success").addAllFlows(flowConfigList).build();
    }

    /**
     * get flow biz type list
     *
     * @param request
     * @return
     */
    @Deprecated
    public AdminGetFlowBizTypeListReply getFlowBizTypeList(AdminGetFlowBizTypeListRequest request) {

        AdminGetFlowBizTypeListReply.Builder builder = AdminGetFlowBizTypeListReply.newBuilder();
        List<AdminGetFlowBizTypeListReply.DictValue> dictValueList = new ArrayList<>();
        return builder.setCode(0).setMessage("success").addAllBizTypes(dictValueList).build();
    }

    /**
     * set flow forbidden status
     *
     * @param request
     * @return
     */
    public AdminSetFlowForbiddenReply setFlowForbidden(AdminSetFlowForbiddenRequest request) {
        AdminSetFlowForbiddenReply.Builder builder = AdminSetFlowForbiddenReply.newBuilder();

        Example flowConfigExample = new Example(FlowConfigPO.class);
        flowConfigExample.createCriteria()
                .andEqualTo("orgId", request.getOrgId())
                .andEqualTo("id", request.getFlowConfigId());
        FlowConfigPO flowConfig = flowConfigMapper.selectOneByExample(flowConfigExample);
        if (flowConfig == null) {
            return builder.setCode(1).setMessage("").build();
        }

        if (flowConfig.getAllowForbidden() == 0 && request.getForbiddenStatus() == 0) {
            return builder.setCode(2).setMessage("").build();
        }

        try {
            flowConfigMapper.updateForbiddenStatus(request.getOrgId(), request.getFlowConfigId()
                    , request.getUserId(), request.getForbiddenStatus(), System.currentTimeMillis());
        } catch (Exception ex) {
            log.error("flow config set forbidden status error:org->{},flowId->{},e->{}", request.getOrgId(), request.getFlowConfigId(), ex.getMessage());
            throw ex;
        }
        return builder.setCode(0).setMessage("").build();
    }

    /**
     * get flow config detail
     *
     * @param request
     * @return
     */
    public AdminGetFlowConfigDetailReply getFlowConfigDetail(AdminGetFlowConfigDetailRequest request) {

        AdminGetFlowConfigDetailReply.Builder builder = AdminGetFlowConfigDetailReply.newBuilder();

        Example flowConfigExample = new Example(FlowConfigPO.class);
        flowConfigExample.createCriteria()
                .andEqualTo("orgId", request.getOrgId())
                .andEqualTo("id", request.getFlowConfigId());

        FlowConfigPO flowConfigPo = flowConfigMapper.selectOneByExample(flowConfigExample);
        if (flowConfigPo == null) {
            return builder.setCode(1).build();
        }

        List<FlowNodeConfigPO> flowNodeConfigPoList = flowNodeConfigMapper.listFlowNodes(request.getOrgId(), request.getFlowConfigId());
        if (CollectionUtils.isEmpty(flowNodeConfigPoList)) {
            return builder.setCode(1).build();
        }

        List<FlowNode> nodesList = new ArrayList<>();
        flowNodeConfigPoList.forEach(node -> {
            nodesList.add(FlowNode.newBuilder()
                    .setLevel(node.getLevel())
                    .setApprover(node.getApprover())
                    .setApproverName(node.getApproverName())
                    .setAllowNotify(node.getAllowNotify())
                    .setNotifyMode(node.getNotifyMode())
                    .setAllowPass(node.getAllowPass())
                    .build());
        });

        return builder.setCode(0)
                .setConfig(AdminGetFlowConfigDetailReply.FlowConfigInfo.newBuilder()
                        .setId(flowConfigPo.getId())
                        .setOrgId(flowConfigPo.getOrgId())
                        .setFlowName(flowConfigPo.getFlowName())
                        .setBizType(flowConfigPo.getBizType())
                        .setLevelCount(flowConfigPo.getLevelCount())
                        .setAllowModify(flowConfigPo.getAllowModify())
                        .setAllowForbidden(flowConfigPo.getAllowForbidden())
                        .setStatus(flowConfigPo.getStatus())
                        .addAllNodes(nodesList)
                        .build())
                .build();

    }

    /**
     * save or update flow config
     *
     * @param request           request
     * @param currentTimeMillis currentTime
     * @return
     */
    private FlowConfigPO insertAndUpdateFlowConfig(AdminSaveFlowConfigRequest request, long currentTimeMillis) {
        FlowConfigPO flowConfigPo;
        if (request.getId() != 0) {
            flowConfigPo = FlowConfigPO.builder()
                    .id(request.getId())
                    .orgId(request.getOrgId())
                    .bizType(request.getBizType())
                    .flowName(request.getFlowName())
                    .levelCount(request.getLevelCount())
                    .allowModify(request.getAllowModify())
                    .allowForbidden(request.getAllowForbidden())
                    .modifyUser(request.getUserId())
                    .updatedAt(currentTimeMillis)
                    .status(request.getStatus())
                    .build();
            Example example = new Example(FlowConfigPO.class);
            example.createCriteria().andEqualTo("id", request.getId()).andEqualTo("orgId", flowConfigPo.getOrgId());
            flowConfigMapper.updateByExampleSelective(flowConfigPo, example);
        } else {
            flowConfigPo = FlowConfigPO.builder()
                    .orgId(request.getOrgId())
                    .bizType(request.getBizType())
                    .flowName(request.getFlowName())
                    .levelCount(request.getLevelCount())
                    .allowModify(request.getAllowModify())
                    .allowForbidden(request.getAllowForbidden())
                    .createUser(request.getUserId())
                    .createUserName(request.getUserName())
                    .modifyUser(request.getUserId())
                    .status(request.getStatus())
                    .createdAt(currentTimeMillis)
                    .updatedAt(currentTimeMillis)
                    .build();
            flowConfigMapper.insertSelective(flowConfigPo);
        }

        return flowConfigPo;
    }

    /**
     * save or update flow node config
     *
     * @param list
     * @param flowConfigPo
     * @param currentTimeMillis
     */
    private void insertAndUpdateFlowNode(List<FlowNode> list, FlowConfigPO flowConfigPo, long currentTimeMillis) {
        if (!CollectionUtils.isEmpty(list)) {
            flowNodeConfigMapper.updateStatus(flowConfigPo.getOrgId(), flowConfigPo.getId(), 0, currentTimeMillis);
            List<FlowNodeConfigPO> nodeList = new ArrayList<>();
            list.forEach(flowNode -> {
                nodeList.add(FlowNodeConfigPO.builder()
                        .orgId(flowConfigPo.getOrgId())
                        .flowConfigId(flowConfigPo.getId())
                        .level(flowNode.getLevel())
                        .approver(flowNode.getApprover())
                        .approverName(flowNode.getApproverName())
                        .allowNotify(flowNode.getAllowNotify())
                        .notifyMode(flowNode.getNotifyMode())
                        .allowPass(flowNode.getAllowPass())
                        .status(1)
                        .approverEmail(flowNode.getApproverEmail())
                        .approverPhone(flowNode.getApproverPhone())
                        .language(Strings.isNullOrEmpty(flowNode.getLanguage()) ? FlowConstant.DEFAULT_LANGUAGE : flowNode.getLanguage())
                        .createdAt(currentTimeMillis)
                        .updatedAt(currentTimeMillis)
                        .build());
            });
            flowNodeConfigMapper.insertList(nodeList);
        }
    }
}
