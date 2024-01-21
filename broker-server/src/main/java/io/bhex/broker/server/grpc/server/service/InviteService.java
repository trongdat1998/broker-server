package io.bhex.broker.server.grpc.server.service;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;

import com.alibaba.fastjson.JSON;
import com.github.pagehelper.PageHelper;

import io.bhex.broker.server.model.StatisticsRpcBrokerUserFee;
import io.bhex.broker.server.statistics.statistics.mapper.StatisticsRptBrokerUserFeeMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.ibatis.session.RowBounds;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.Resource;

import io.bhex.base.clear.BrokerUserFeeRequest;
import io.bhex.base.clear.BrokerUserFeeResponse;
import io.bhex.base.idgen.api.ISequenceGenerator;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.util.JsonUtil;
import io.bhex.broker.grpc.common.Header;
import io.bhex.broker.grpc.invite.AddInviteBlackListRequest;
import io.bhex.broker.grpc.invite.AddInviteBlackListResponse;
import io.bhex.broker.grpc.invite.BindInviteRelationRequest;
import io.bhex.broker.grpc.invite.BindInviteRelationResponse;
import io.bhex.broker.grpc.invite.CancelInviteRelationRequest;
import io.bhex.broker.grpc.invite.CancelInviteRelationResponse;
import io.bhex.broker.grpc.invite.DeleteInviteBlackListRequest;
import io.bhex.broker.grpc.invite.DeleteInviteBlackListResponse;
import io.bhex.broker.grpc.invite.ExecuteAdminGrantInviteBonusRequest;
import io.bhex.broker.grpc.invite.ExecuteAdminGrantInviteBonusResponse;
import io.bhex.broker.grpc.invite.GenerateAdminInviteBonusRecordRequest;
import io.bhex.broker.grpc.invite.GenerateAdminInviteBonusRecordResponse;
import io.bhex.broker.grpc.invite.GetAdminInviteBonusRecordRequest;
import io.bhex.broker.grpc.invite.GetAdminInviteBonusRecordResponse;
import io.bhex.broker.grpc.invite.GetDailyTaskListRequest;
import io.bhex.broker.grpc.invite.GetDailyTaskListResponse;
import io.bhex.broker.grpc.invite.GetInviteBlackListRequest;
import io.bhex.broker.grpc.invite.GetInviteBlackListResponse;
import io.bhex.broker.grpc.invite.GetInviteBonusRecordResponse;
import io.bhex.broker.grpc.invite.GetInviteCommonSettingResponse;
import io.bhex.broker.grpc.invite.GetInviteFeeBackActivityResponse;
import io.bhex.broker.grpc.invite.GetInviteInfoResponse;
import io.bhex.broker.grpc.invite.GetInviteStatisticsRecordListRequest;
import io.bhex.broker.grpc.invite.GetInviteStatisticsRecordListResponse;
import io.bhex.broker.grpc.invite.InitInviteFeeBackActivityResponse;
import io.bhex.broker.grpc.invite.InviteBlackUser;
import io.bhex.broker.grpc.invite.InviteCommonSetting;
import io.bhex.broker.grpc.invite.UpdateInviteCommonSettingRequest;
import io.bhex.broker.grpc.invite.UpdateInviteCommonSettingResponse;
import io.bhex.broker.grpc.invite.UpdateInviteFeeBackActivityResponse;
import io.bhex.broker.grpc.invite.UpdateInviteFeeBackLevelResponse;
import io.bhex.broker.grpc.invite.UpdateInviteFeebackAutoTransferResponse;
import io.bhex.broker.grpc.invite.UpdateInviteFeebackPeriodRequest;
import io.bhex.broker.grpc.invite.UpdateInviteFeebackPeriodResponse;
import io.bhex.broker.grpc.user.level.QueryMyLevelConfigResponse;
import io.bhex.broker.server.domain.BrokerServerConstants;
import io.bhex.broker.server.domain.CommonIni;
import io.bhex.broker.server.domain.InviteActivityStatus;
import io.bhex.broker.server.domain.InviteActivityType;
import io.bhex.broker.server.domain.InviteAutoTransferEnum;
import io.bhex.broker.server.domain.InviteBlackListStatus;
import io.bhex.broker.server.domain.InviteBonusRecordStatus;
import io.bhex.broker.server.domain.InviteType;
import io.bhex.broker.server.domain.InviteWechatConfig;
import io.bhex.broker.server.grpc.client.service.GrpcCommissionService;
import io.bhex.broker.server.model.Account;
import io.bhex.broker.server.model.AgentUser;
import io.bhex.broker.server.model.Broker;
import io.bhex.broker.server.model.InviteActivity;
import io.bhex.broker.server.model.InviteBlackList;
import io.bhex.broker.server.model.InviteBonusDetail;
import io.bhex.broker.server.model.InviteBonusRecord;
import io.bhex.broker.server.model.InviteDailyTask;
import io.bhex.broker.server.model.InviteDetail;
import io.bhex.broker.server.model.InviteInfo;
import io.bhex.broker.server.model.InviteLevel;
import io.bhex.broker.server.model.InviteRank;
import io.bhex.broker.server.model.InviteRelation;
import io.bhex.broker.server.model.InviteRelationHistory;
import io.bhex.broker.server.model.InviteStatisticsRecord;
import io.bhex.broker.server.model.User;
import io.bhex.broker.server.primary.mapper.AccountMapper;
import io.bhex.broker.server.primary.mapper.AgentUserMapper;
import io.bhex.broker.server.primary.mapper.CommonIniMapper;
import io.bhex.broker.server.primary.mapper.InviteActivityMapper;
import io.bhex.broker.server.primary.mapper.InviteBlackListMapper;
import io.bhex.broker.server.primary.mapper.InviteBonusRecordMapper;
import io.bhex.broker.server.primary.mapper.InviteDailyTaskMapper;
import io.bhex.broker.server.primary.mapper.InviteDetailMapper;
import io.bhex.broker.server.primary.mapper.InviteInfoMapper;
import io.bhex.broker.server.primary.mapper.InviteLevelMapper;
import io.bhex.broker.server.primary.mapper.InviteRelationHistoryMapper;
import io.bhex.broker.server.primary.mapper.InviteRelationMapper;
import io.bhex.broker.server.primary.mapper.InviteStatisticsRecordMapper;
import io.bhex.broker.server.primary.mapper.UserMapper;
import io.bhex.broker.server.util.BaseReqUtil;
import io.bhex.broker.server.util.BeanCopyUtils;
import io.bhex.broker.server.util.PageUtil;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import tk.mybatis.mapper.entity.Example;

@Slf4j
@Service
public class InviteService {

    public static final String INVITE_BLACK_LIST_USER_KEY = "invite_black_list_user:%s:%s";

    public static final String INVITE_TITLE_PIC_PC = "invite_title_pic_pc";
    public static final String INVITE_TITLE_PIC_APP = "invite_title_pic_app";
    public static final String INVITE_ACTIVITY_RULE_URL = "invite_activity_rule_url";
    public static final String INVITE_POSTER_TEMPLATE = "invite_poster_template";
    public static final String INVITE_BROKER_LOGO_URL = "invite_broker_logo_url";
    public static final String APP_DOWNLOAD_PAGE_TEXT = "app_download_page_text";
    public static final String BHOP_DOWNLOAD_IOS_URL = "bhop_download_ios_url";
    public static final String BHOP_DOWNLOAD_ANDROID_URL = "bhop_download_android_url";

    private static final List<String> INVITE_COMMON_INI_NAME_LIST = Lists.newArrayList(
            INVITE_TITLE_PIC_PC,
            INVITE_TITLE_PIC_APP,
            INVITE_ACTIVITY_RULE_URL,
            INVITE_POSTER_TEMPLATE,
            INVITE_BROKER_LOGO_URL,
            APP_DOWNLOAD_PAGE_TEXT,
            BHOP_DOWNLOAD_IOS_URL,
            BHOP_DOWNLOAD_ANDROID_URL,
            InviteWechatConfig.INVITE_SHARE_WX_TITLE,
            InviteWechatConfig.INVITE_SHARE_WX_CONTENT
    );

    @Resource
    private InviteInfoMapper inviteInfoMapper;

    @Resource
    private InviteRelationMapper inviteRelationMapper;
    @Resource
    private InviteRelationHistoryMapper inviteRelationHistoryMapper;

    @Resource
    private InviteBonusRecordMapper inviteBonusRecordMapper;

    @Resource
    private InviteLevelMapper inviteLevelMapper;

    @Resource
    private InviteBlackListMapper inviteBlackListMapper;

    @Resource
    private UserMapper userMapper;
    @Resource
    private AccountService accountService;

    @Resource
    private InviteActivityMapper inviteActivityMapper;

    @Resource
    private InviteDailyTaskMapper inviteDailyTaskMapper;
    @Resource(name = "stringRedisTemplate")
    private StringRedisTemplate redisTemplate;

    @Autowired
    private InviteTaskService inviteTaskService;

    @Autowired
    GrpcCommissionService grpcCommissionService;

    @Autowired
    private ISequenceGenerator sequenceGenerator;

    @Resource
    private InviteStatisticsRecordMapper inviteStatisticsRecordMapper;

    @Autowired
    private CommonIniService commonIniService;

    @Autowired
    private BrokerService brokerService;

    @Resource
    private InviteDetailMapper inviteDetailMapper;

    @Resource
    private CommonIniMapper commonIniMapper;

    @Resource
    private AccountMapper accountMapper;

    @Resource
    private AgentUserMapper agentUserMapper;

    @Resource
    private AgentUserService agentUserService;

    @Resource
    private UserLevelService userLevelService;

    @Resource
    private StatisticsRptBrokerUserFeeMapper statisticsRptBrokerUserFeeMapper;

    /**
     * 找邀请关系，只存在2级关系
     */
    public void findInviteRelation(long orgId, long inviteId, long userId, long accountId, String name) {
        log.info("Build invite relation,inviteId={},userId={},accountId={},name={}", inviteId, userId, accountId, name);
        // 构建直接邀请关系
        buildInviteRelation(orgId, inviteId, InviteType.DIRECT.getValue(), userId, accountId, name);
        try {
            log.info("start agentInviteUserHandle ");
            //如果是经纪人则创建经纪人关系 并且把邀请返佣的邀请关系改为无效即可
            AgentUser agentUser = this.agentUserMapper.queryAgentUserByUserId(orgId, inviteId);
            if (agentUser != null && agentUser.getIsAgent().equals(1)) {
                log.info("start agentInviteUserHandle agentUser {} ", JSON.toJSONString(agentUser));
                agentUserService.agentInviteUserHandle(orgId, userId, agentUser);
                this.inviteRelationMapper.updateInviteRelationStatusByUserId(userId);
            }
        } catch (Exception ex) {
            log.error("Create agent invite user orgId {} userId {} error {}", orgId, userId);
        }
        // 查询邀请人是否也是被邀请的人，找他的上一级
        User inviteUser = userMapper.getByUserId(inviteId);
        if (inviteUser.getInviteUserId() == null || inviteUser.getInviteUserId() <= 0) {
            return;
        }
        // 构建间接邀请关系
        buildInviteRelation(orgId, inviteUser.getInviteUserId(), InviteType.INDIRECT.getValue(), userId, accountId, name);
    }

    public void buildInviteRelation(long orgId, long inviteUserId, int inviteType, long userId, long accountId, String name) {
        // 查询邀请人是否存在邀请信息
        InviteInfo inviteInfo = inviteInfoMapper.getInviteInfoByUserId(inviteUserId);
        if (inviteInfo == null) {
            Long inviteAccountId = accountService.getAccountId(orgId, inviteUserId);
            inviteInfo = initUserInviteInfo(orgId, inviteUserId, inviteAccountId);
        }
        try {
            // 修改邀请总数
            inviteInfo.setInviteCount(inviteInfo.getInviteCount() + 1);
            if (inviteType == InviteType.DIRECT.getValue()) {
                inviteInfo.setInviteDirectVaildCount(inviteInfo.getInviteDirectVaildCount() + 1);
            } else {
                inviteInfo.setInviteIndirectVaildCount(inviteInfo.getInviteIndirectVaildCount() + 1);
            }
            inviteInfoMapper.updateByPrimaryKeySelective(inviteInfo);
        } catch (Exception ex) {
            log.warn("update inviteCount  orgId {} inviteUserId {} error {}", orgId, inviteUserId);
        }

        // 查询、插入邀请关系
        InviteRelation condition = InviteRelation.builder()
                .orgId(orgId)
                .userId(inviteUserId)
                .invitedId(userId)
                .invitedType(inviteType)
                .build();

        InviteRelation relation = inviteRelationMapper.selectOne(condition);
        if (relation == null) {
            relation = InviteRelation.builder()
                    .orgId(orgId)
                    .userId(inviteUserId)
                    .invitedId(userId)
                    .invitedAccountId(accountId)
                    .invitedType(inviteType)
                    .invitedName(name)
                    .invitedHobbitLeader(0)
                    .build();
            inviteRelationMapper.insertSelective(relation);
        }
        log.info(" user:{} invitee:{} inviteeName:{} inviteType:{}", inviteUserId, userId, name, inviteType);
    }

    /**
     * Y -> A -> B -> C -> D -> F -> G 删除B的邀请关系，要删除 Y->B A->B 以及 A->C、F、G，B本身的关系不变
     */
    @Transactional
    public CancelInviteRelationResponse cancelInviteRelation(CancelInviteRelationRequest request) {
        long orgId = request.getOrgId();
        long userId = request.getInvitedUserId();

        //delete releation to backup
        InviteRelation deleteCondition = InviteRelation.builder()
                .orgId(orgId)
                .invitedId(userId)
                .build();
        List<InviteRelation> deletedRelations = inviteRelationMapper.select(deleteCondition);
        if (CollectionUtils.isEmpty(deletedRelations)) {
            return CancelInviteRelationResponse.newBuilder().build();
        }
        long directInviteUserId = 0;
        long indrectInviteUserId = 0;
        for (InviteRelation relation : deletedRelations) {
            if (relation.getInvitedType() == InviteType.DIRECT.getValue()) {
                directInviteUserId = relation.getUserId();
            } else if (relation.getInvitedType() == InviteType.INDIRECT.getValue()) {
                indrectInviteUserId = relation.getUserId();
            }
            inviteRelationMapper.deleteByPrimaryKey(relation);
            InviteRelationHistory history = new InviteRelationHistory();
            BeanCopyUtils.copyPropertiesIgnoreNull(relation, history);
            inviteRelationHistoryMapper.insertSelective(history);
            userMapper.cancelInviteRelation(orgId, userId);
        }


        //B的直接邀请人
        InviteRelation deleteDirectCondition = InviteRelation.builder()
                .orgId(orgId)
                .userId(userId)
                .invitedType(InviteType.DIRECT.getValue())
                .build();
        List<InviteRelation> deletedDirectRelations = inviteRelationMapper.select(deleteDirectCondition);
        if (!CollectionUtils.isEmpty(deletedDirectRelations)) {
            // 搞掉 A->C、F、G
            Example example = new Example(InviteRelation.class);
            example.createCriteria().andEqualTo("orgId", orgId)
                    .andEqualTo("invitedType", InviteType.INDIRECT.getValue())
                    .andEqualTo("userId", directInviteUserId)
                    .andIn("invitedId", deletedDirectRelations.stream().map(r -> r.getInvitedId()).collect(Collectors.toList()));
            List<InviteRelation> list = inviteRelationMapper.selectByExample(example);

            for (InviteRelation relation : list) {
                inviteRelationMapper.deleteByPrimaryKey(relation);
                InviteRelationHistory history = new InviteRelationHistory();
                BeanCopyUtils.copyPropertiesIgnoreNull(relation, history);
                inviteRelationHistoryMapper.insertSelective(history);
                userMapper.cancelSecondLevelInviteRelation(orgId, relation.getInvitedId());
                log.info("delete indirect relation uid:{} relation:{}", relation.getUserId(), relation.getInvitedId());
            }
        }

        JsonObject directInviteUserJO = updateInviteInfo(orgId, directInviteUserId);
        log.info("directInviteUserJO:{}", directInviteUserJO);

        if (indrectInviteUserId > 0) {
            JsonObject indirectInviteUserJO = updateInviteInfo(orgId, indrectInviteUserId);
            log.info("indirectInviteUserJO:{}", indirectInviteUserJO);
        }

        return CancelInviteRelationResponse.newBuilder().build();
    }

    /**
     * Y -> A B -> C -> D -> F -> G B新绑定邀请关系A，要增加 Y->B A->B 以及 A->C、F、G，B本身的邀请关系不变
     */
    @Transactional
    public BindInviteRelationResponse bindInviteRelation(BindInviteRelationRequest request) {
        if (request.getInviteUserId() == request.getInvitedUserId()) {
            return BindInviteRelationResponse.newBuilder().setRet(1).setMsg("same.uid").build();
        }
        long orgId = request.getOrgId();
        User userA = userMapper.getByOrgAndUserId(orgId, request.getInviteUserId());
        User userB = userMapper.getByOrgAndUserId(orgId, request.getInvitedUserId());

        //判断A是是不是经纪人 如果A是有效的经纪人 并且B不是经纪人则创建经纪人的关系
        try {
            //如果是经纪人则创建经纪人关系 并且把邀请返佣的邀请关系改为无效即可
            AgentUser agentUser = this.agentUserMapper.queryAgentUserByUserId(orgId, userA.getUserId());
            if (agentUser != null && agentUser.getIsAgent().equals(1)) {
                agentUserService.agentInviteUserHandle(orgId, userB.getUserId(), agentUser);
            }
        } catch (Exception ex) {
            log.error("Create agent invite user orgId {} userId {} error {}", orgId, userB.getUserId());
        }

        List<InviteRelation> relations = inviteRelationMapper.getInviteRelationsByInvitedId(orgId, request.getInvitedUserId());
        if (!CollectionUtils.isEmpty(relations)) {
            return BindInviteRelationResponse.newBuilder().setRet(-1).build();
        }

        InviteRelation yaCondition = InviteRelation.builder()
                .orgId(orgId)
                .invitedType(InviteType.DIRECT.getValue())
                .invitedId(userA.getUserId())
                .build();
        InviteRelation yaRelation = inviteRelationMapper.selectOne(yaCondition);
        if (yaRelation != null) {
            //建立Y -> B间接关系
            buildInviteRelation(orgId, yaRelation.getUserId(), InviteType.INDIRECT.getValue(), userB.getUserId(),
                    accountService.getAccountId(orgId, userB.getUserId()),
                    StringUtils.isEmpty(userB.getEmail()) ? userB.getMobile() : userB.getEmail());
            log.info("create indirect relation Y:{} -> B:{}", yaRelation.getUserId(), userB.getUserId());
        }

        InviteRelation bDirectCondition = InviteRelation.builder()
                .orgId(orgId)
                .invitedType(InviteType.DIRECT.getValue())
                .userId(userB.getUserId())
                .build();
        List<InviteRelation> bDirectRelations = inviteRelationMapper.select(bDirectCondition);
        if (!CollectionUtils.isEmpty(bDirectRelations)) {
            for (InviteRelation relation : bDirectRelations) {
                //创建 A -> relation的间接邀请关系
                User user = userMapper.getByOrgAndUserId(orgId, relation.getInvitedId());
                buildInviteRelation(orgId, userA.getUserId(), InviteType.INDIRECT.getValue(), user.getUserId(),
                        accountService.getAccountId(orgId, user.getUserId()),
                        StringUtils.isEmpty(user.getEmail()) ? user.getMobile() : user.getEmail());

                User updateObj = User.builder()
                        .userId(user.getUserId())
                        .secondLevelInviteUserId(userA.getUserId())
                        .updated(System.currentTimeMillis())
                        .build();
                int rows = userMapper.updateRecord(updateObj);
                log.info("create indirect relation A:{} -> {}", userA.getUserId(), user.getUserId());

            }
        }

        //建立 A-> B直接关系
        buildInviteRelation(orgId, userA.getUserId(), InviteType.DIRECT.getValue(), userB.getUserId(),
                accountService.getAccountId(orgId, userB.getUserId()),
                StringUtils.isEmpty(userB.getEmail()) ? userB.getMobile() : userB.getEmail());
        User updateObj = User.builder()
                .userId(userB.getUserId())
                .inviteUserId(userA.getUserId())
                .secondLevelInviteUserId(yaRelation != null ? yaRelation.getUserId() : 0)
                .updated(System.currentTimeMillis())
                .build();
        int rows = userMapper.updateRecord(updateObj);
        log.info("create direct relation A:{} -> B:{}", userA.getUserId(), userB.getUserId());


        return BindInviteRelationResponse.newBuilder().build();
    }

    public JsonObject updateInviteInfo(Long orgId, Long userId) {
        JsonObject dataObj = new JsonObject();

        InviteInfo inviteInfo = inviteInfoMapper.getInviteInfoByUserId(userId);
        if (inviteInfo == null) {
            Long accountId = accountService.getAccountId(orgId, userId);
            inviteInfo = initUserInviteInfo(orgId, userId, accountId);
        }
        dataObj.addProperty("originInviteCount", inviteInfo.getInviteCount());
        dataObj.addProperty("originInviteDirectCount", inviteInfo.getInviteDirectVaildCount());
        dataObj.addProperty("originInviteInDirectCount", inviteInfo.getInviteIndirectVaildCount());

        // 查询用户邀请的人
        int inviteCount = inviteRelationMapper.selectCount(InviteRelation.builder()
                .orgId(orgId)
                .userId(userId)
                .build());
        int directCount = inviteRelationMapper.selectCount(InviteRelation.builder()
                .orgId(orgId)
                .userId(userId)
                .invitedType(InviteType.DIRECT.getValue())
                .build());
        int indirectCount = inviteRelationMapper.selectCount(InviteRelation.builder()
                .orgId(orgId)
                .userId(userId)
                .invitedType(InviteType.INDIRECT.getValue())
                .build());

        InviteInfo updateObj = new InviteInfo();
        updateObj.setId(inviteInfo.getId());
        updateObj.setInviteCount(inviteCount);
        updateObj.setInviteDirectVaildCount(directCount);
        updateObj.setInviteIndirectVaildCount(indirectCount);
        updateObj.setUpdatedAt(new Date());
        inviteInfoMapper.updateByPrimaryKeySelective(updateObj);

        dataObj.addProperty("updatedInviteCount", inviteCount);
        dataObj.addProperty("updatedInviteDirectCount", directCount);
        dataObj.addProperty("updatedInviteInDirectCount", indirectCount);
        return dataObj;
    }

    public InviteInfo initUserInviteInfo(Long orgId, Long userId, Long accountId) {
        InviteInfo inviteInfo = InviteInfo.builder()
                .orgId(orgId)
                .userId(userId)
                .accountId(accountId)
                .inviteCount(0)
                .inviteVaildCount(0)
                .inviteDirectVaildCount(0)
                .inviteIndirectVaildCount(0)
                .inviteHobbitLeaderCount(0)
                .inviteLevel(0)
                .bonusCoin(0D)
                .bonusPoint(0D)
                .directRate(0D)
                .indirectRate(0D)
                .createdAt(new Date())
                .hide(0)
                .build();
        inviteInfoMapper.insertSelective(inviteInfo);
        return inviteInfo;
    }

    /**
     * 获取用户邀请的相关信息
     */
    public GetInviteInfoResponse getInviteInfo(Header header) {
        User user = userMapper.getByUserId(header.getUserId());
        if (user == null) {
            return GetInviteInfoResponse.newBuilder().build();
        }
        InviteInfo inviteInfo = inviteInfoMapper.getInviteInfoByUserId(user.getUserId());
        if (inviteInfo == null) {
            inviteInfo = InviteInfo.builder()
                    .orgId(header.getOrgId())
                    .userId(header.getUserId())
                    .accountId(0L)
                    .inviteLevel(0)
                    .inviteCount(0)
                    .inviteVaildCount(0)
                    .inviteDirectVaildCount(0)
                    .inviteIndirectVaildCount(0)
                    .inviteHobbitLeaderCount(0)
                    .directRate(0D)
                    .indirectRate(0d)
                    .bonusPoint(0D)
                    .bonusPoint(0D)
                    .createdAt(new Date(System.currentTimeMillis()))
                    .updatedAt(new Date(System.currentTimeMillis()))
                    .hide(0)
                    .build();
        }
        return GetInviteInfoResponse.newBuilder()
                .setInviteCode(user.getInviteCode())
                .setInfo(this.convertInviteInfo(inviteInfo))
                .build();
    }


    /**
     * 获取用户返佣记录
     */
    public GetInviteBonusRecordResponse getInviteBonusRecord(Header header, Integer nextId, Integer beforeId, int limit) {
        InviteInfo inviteInfo = inviteInfoMapper.getInviteInfoByUserId(header.getUserId());

//        Example example = Example.builder(InviteBonusRecord.class)
//                .orderByDesc("id")
//                .build();
//        Example.Criteria criteria = example.createCriteria();
//        criteria.andEqualTo("userId", header.getUserId())
//                .andEqualTo("status", 1);
//        if (nextId != null && nextId >= 0) {
//            // nextId 代表是向后获取一页
//            criteria.andGreaterThan("id", nextId);
//        }
//        if (beforeId != null && beforeId > 0) {
//            criteria.andLessThan("id", beforeId);
//        }

        InviteBonusRecordMapper.QueryCondition condition = InviteBonusRecordMapper.QueryCondition.builder()
                .userId(header.getUserId())
                .status(InviteBonusRecordStatus.FINISHED.value())
                .nextId(nextId)
                .beforeId(beforeId)
                .startIndex(0)
                .limit(limit)
                .orderBy(" id desc")
                .build();

        List<InviteBonusRecord> recordList = inviteBonusRecordMapper.selectByCondition(condition);


        String bonusCoins = "0";
        String bonusPoints = "0";
        if (inviteInfo != null && inviteInfo.getBonusCoin() != null) {
            bonusCoins = inviteInfo.getBonusCoin().toString();
        }

        if (inviteInfo != null && inviteInfo.getBonusPoint() != null) {
            bonusPoints = inviteInfo.getBonusPoint().toString();
        }

        return GetInviteBonusRecordResponse.newBuilder()
                .setTotalBonusCoins(bonusCoins)
                .setTotalBonusPoints(bonusPoints)
                .addAllRecordList(this.convertBonusRecordList(recordList))
                .build();
    }


    public GetAdminInviteBonusRecordResponse getAdminInviteBonusRecord(GetAdminInviteBonusRecordRequest request) {
        Example example = Example.builder(InviteBonusRecord.class)

                .orderByDesc("bonusAmount")
                .build();
        PageHelper.startPage(request.getPage(), request.getLimit());

        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("orgId", request.getOrgId());

        if (request.getStatisticsTime() > 0) {
            criteria.andEqualTo("statisticsTime", request.getStatisticsTime());
        }

        if (request.getUserId() > 0) {
            criteria.andEqualTo("userId", request.getUserId());
        }

        if (StringUtils.isNotEmpty(request.getToken())) {
            criteria.andEqualTo("token", request.getToken());
        }


        List<InviteBonusRecord> recordList = inviteBonusRecordMapper.selectByExample(example);
        if (CollectionUtils.isEmpty(recordList)) {
            return GetAdminInviteBonusRecordResponse.getDefaultInstance();
        }

        return GetAdminInviteBonusRecordResponse.newBuilder()
                .addAllRecordList(this.convertBonusRecordList(recordList))
                .build();
    }

    public GenerateAdminInviteBonusRecordResponse generateAdminInviteBonusRecord(GenerateAdminInviteBonusRecordRequest request) {

        int result = inviteTaskService.generateAdminInviteBonusRecord(request.getOrgId(), request.getStatisticsTime());

        return GenerateAdminInviteBonusRecordResponse.newBuilder()
                .setRet(result)
                .build();
    }

    public ExecuteAdminGrantInviteBonusResponse executeAdminGrantInviteBonus(ExecuteAdminGrantInviteBonusRequest request) {
        int result = inviteTaskService.executeAdminGrantInviteBonus(request.getOrgId(), request.getStatisticsTime());
        return ExecuteAdminGrantInviteBonusResponse.newBuilder()
                .setRet(result)
                .build();
    }

    public GetInviteFeeBackActivityResponse getInviteFeeBackActivity(Header header) {
        InviteActivity activityCondition = InviteActivity.builder()
                .orgId(header.getOrgId())
                .type(InviteActivityType.GENERAL_TRADE_FEEBACK.getValue())
                .build();

        InviteActivity activity = inviteActivityMapper.selectOne(activityCondition);
        if (activity == null) {
            return GetInviteFeeBackActivityResponse.newBuilder().build();
        }

        List<InviteLevel> levelList = this.getInviteLevelList(activity.getId());

        return GetInviteFeeBackActivityResponse.newBuilder()
                .setActivity(this.convertActivity(activity))
                .addAllLevelList(this.convertLevelList(levelList))
                .build();
    }

    public InviteActivity getInviteFeeBackActivity(Long brokerId) {
        InviteActivity activityCondition = InviteActivity.builder()
                .orgId(brokerId)
                .type(InviteActivityType.GENERAL_TRADE_FEEBACK.getValue())
                .build();

        InviteActivity activity = inviteActivityMapper.selectOne(activityCondition);
        if (activity == null) {
            return null;
        }
        return activity;
    }

    public InviteActivity getInviteFeeBackActivityCache(Long brokerId) {
        String cacheKey = BrokerServerConstants.INVITE_FEEBACK_ACTIVITY_KEY + brokerId;
        String cacheString = redisTemplate.opsForValue().get(cacheKey);
        if (StringUtils.isNotBlank(cacheString)) {
            return JsonUtil.defaultGson().fromJson(cacheString, InviteActivity.class);
        }

        InviteActivity activity = this.getInviteFeeBackActivity(brokerId);
        if (activity == null) {
            return null;
        }

        redisTemplate.opsForValue().set(cacheKey, JsonUtil.defaultGson().toJson(activity), 60, TimeUnit.SECONDS);

        return activity;
    }

    public UpdateInviteFeeBackLevelResponse updateInviteFeeBackLevel(Header header,
                                                                     Long actId,
                                                                     Long levelId,
                                                                     Integer levelCondition,
                                                                     BigDecimal directRate,
                                                                     BigDecimal indirectRate) {
        InviteActivity activityCondition = InviteActivity.builder()
                .orgId(header.getOrgId())
                .id(actId)
                .type(InviteActivityType.GENERAL_TRADE_FEEBACK.getValue())
                .build();

        InviteActivity activity = inviteActivityMapper.selectOne(activityCondition);
        if (activity == null) {
            return UpdateInviteFeeBackLevelResponse.newBuilder()
                    .setRet(BrokerErrorCode.ACTIVITY_INVITE_ACTIVITY_NOT_EXIST.code())
                    .build();
        }

        InviteLevel levelCon = InviteLevel.builder()
                .id(levelId)
                .actId(actId)
                .build();

        InviteLevel level = inviteLevelMapper.selectOne(levelCon);
        if (level == null) {
            return UpdateInviteFeeBackLevelResponse.newBuilder()
                    .setRet(BrokerErrorCode.ACTIVITY_INVITE_LEVEL_NOT_EXIST.code())
                    .build();
        }

        level.setLevelCondition(levelCondition);
        level.setDirectRate(directRate);
        level.setIndirectRate(indirectRate);
        level.setUpdatedAt(new Date(System.currentTimeMillis()));
        inviteLevelMapper.updateByPrimaryKeySelective(level);

        // 清除等级的缓存
        redisTemplate.delete(BrokerServerConstants.INVITE_LEVEL_KEY + actId);

        return UpdateInviteFeeBackLevelResponse.getDefaultInstance();
    }

    public UpdateInviteFeeBackActivityResponse updateInviteFeeBackActivity(Header header,
                                                                           Long actId,
                                                                           Integer status,
                                                                           Integer coinStatus,
                                                                           Integer futuresStatus) {
        InviteActivity activityCondition = InviteActivity.builder()
                .orgId(header.getOrgId())
                .id(actId)
                .type(InviteActivityType.GENERAL_TRADE_FEEBACK.getValue())
                .build();

        InviteActivity activity = inviteActivityMapper.selectOne(activityCondition);
        if (activity == null) {
            return UpdateInviteFeeBackActivityResponse.newBuilder()
                    .setRet(BrokerErrorCode.ACTIVITY_INVITE_ACTIVITY_NOT_EXIST.code())
                    .build();
        }
        if (activity.getStatus().equals(status) && activity.getCoinStatus().equals(coinStatus) && activity.getFuturesStatus().equals(futuresStatus)) {
            return UpdateInviteFeeBackActivityResponse.getDefaultInstance();
        }

        if (!activity.getStatus().equals(status)) {
            activity.setStatus(status);
        }

        if (!activity.getCoinStatus().equals(coinStatus)) {
            activity.setCoinStatus(coinStatus);
        }

        if (!activity.getFuturesStatus().equals(futuresStatus)) {
            activity.setFuturesStatus(futuresStatus);
        }

        activity.setUpdatedAt(new Date(System.currentTimeMillis()));
        inviteActivityMapper.updateByPrimaryKeySelective(activity);

        //初始化微信邀请返佣配置
        initInviteWechatConfig(header.getOrgId());
        return UpdateInviteFeeBackActivityResponse.getDefaultInstance();
    }

    public InitInviteFeeBackActivityResponse initInviteFeeBackActivity(Long orgId) {
        InviteActivity activityCondition = InviteActivity.builder()
                .orgId(orgId)
                .type(InviteActivityType.GENERAL_TRADE_FEEBACK.getValue())
                .build();

        InviteActivity activity = inviteActivityMapper.selectOne(activityCondition);
        if (activity != null) {
            return InitInviteFeeBackActivityResponse.getDefaultInstance();
        }

        InviteActivity inviteActivity = InviteActivity.builder()
                .orgId(orgId)
                .type(InviteActivityType.GENERAL_TRADE_FEEBACK.getValue())
                .grantTokenId("")
                .amount(0d)
                .period(0L)
                .autoTransfer(InviteAutoTransferEnum.MANUAL.code())
                .status(InviteActivityStatus.OFFLINE.getStatus())
                .createdAt(new Date(System.currentTimeMillis()))
                .updatedAt(new Date(System.currentTimeMillis()))
                .build();

        inviteActivityMapper.insertSelective(inviteActivity);

        List<InviteLevel> levelList = InviteLevel.getInviteLevelTemplate(inviteActivity.getId());
        inviteLevelMapper.insertList(levelList);

        return InitInviteFeeBackActivityResponse.getDefaultInstance();
    }


    public UpdateInviteFeebackAutoTransferResponse updateInviteFeebackAutoTransfer(Long orgId, int autoTransfer) {
        InviteActivity activity = this.getInviteFeeBackActivity(orgId);
        if (activity == null) {
            return UpdateInviteFeebackAutoTransferResponse.newBuilder().setRet(-1).build();
        }

        InviteAutoTransferEnum autoTransferEnum = InviteAutoTransferEnum.fromValue(autoTransfer);
        if (autoTransferEnum == null) {
            return UpdateInviteFeebackAutoTransferResponse.newBuilder().setRet(-2).build();
        }

        if (activity.getAutoTransfer() == autoTransferEnum.code()) {
            return UpdateInviteFeebackAutoTransferResponse.getDefaultInstance();
        }

        activity.setAutoTransfer(autoTransfer);
        activity.setUpdatedAt(new Date(System.currentTimeMillis()));
        inviteActivityMapper.updateByPrimaryKeySelective(activity);

        // 删除缓存
        String cacheKey = BrokerServerConstants.INVITE_FEEBACK_ACTIVITY_KEY + orgId;
        redisTemplate.delete(cacheKey);

        return UpdateInviteFeebackAutoTransferResponse.getDefaultInstance();
    }

    public UpdateInviteFeebackPeriodResponse updateInviteFeebackPeriod(UpdateInviteFeebackPeriodRequest request) {
        InviteActivity activity = this.getInviteFeeBackActivity(request.getOrgId());
        if (activity == null) {
            return UpdateInviteFeebackPeriodResponse.newBuilder().setRet(-1).build();
        }

        activity.setPeriod(request.getPeriod());
        activity.setUpdatedAt(new Date(System.currentTimeMillis()));
        inviteActivityMapper.updateByPrimaryKeySelective(activity);

        // 删除缓存
        String cacheKey = BrokerServerConstants.INVITE_FEEBACK_ACTIVITY_KEY + request.getOrgId();
        redisTemplate.delete(cacheKey);

        return UpdateInviteFeebackPeriodResponse.getDefaultInstance();
    }

    public GetInviteBlackListResponse getInviteBlackList(GetInviteBlackListRequest request) {
        long userId = request.getUserId();
        if (userId > 0) {
            InviteBlackList blackUser = this.getInviteBlackListByUserId(request.getOrgId(), request.getUserId());
            if (blackUser == null) {
                return GetInviteBlackListResponse.getDefaultInstance();
            }
            return GetInviteBlackListResponse.newBuilder().addUsers(this.convertBlackUser(blackUser)).build();
        }


        Example example = Example.builder(InviteBlackList.class)
                .orderByDesc("id")
                .build();

        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("orgId", request.getOrgId())
                .andEqualTo("status", InviteBlackListStatus.NORMAL.getStatus());

        int startIndex = PageUtil.getStartIndex(request.getPage(), request.getLimit());

        List<InviteBlackList> list = inviteBlackListMapper.selectByExampleAndRowBounds(example, new RowBounds(startIndex, request.getLimit()));
        if (CollectionUtils.isEmpty(list)) {
            return GetInviteBlackListResponse.getDefaultInstance();
        }
        return GetInviteBlackListResponse.newBuilder()
                .addAllUsers(this.convertBlackUserList(list))
                .build();
    }

    public AddInviteBlackListResponse addInviteBlackList(AddInviteBlackListRequest request) {
        User user = userMapper.getByUserId(request.getUserId());
        if (user == null) {
            return AddInviteBlackListResponse.newBuilder().setRet(-1).build();
        }

        InviteBlackList dbBlackUser = this.getInviteBlackListByUserId(request.getOrgId(), request.getUserId());
        if (dbBlackUser != null) {
            return AddInviteBlackListResponse.newBuilder().setRet(-2).build();
        }

        String userContact = StringUtils.isNotBlank(user.getMobile())
                ? user.getNationalCode() + " " + user.getMobile()
                : user.getEmail();

        InviteBlackList blackUser = InviteBlackList.builder()
                .orgId(request.getOrgId())
                .userId(request.getUserId())
                .userContact(Strings.nullToEmpty(userContact))
                .status(InviteBlackListStatus.NORMAL.getStatus())
                .createdAt(new Date(System.currentTimeMillis()))
                .updatedAt(new Date(System.currentTimeMillis()))
                .build();

        inviteBlackListMapper.insert(blackUser);

        // 放入redis
        String key = String.format(INVITE_BLACK_LIST_USER_KEY, request.getOrgId(), request.getUserId());
        redisTemplate.opsForValue().set(key, "1");

        return AddInviteBlackListResponse.getDefaultInstance();
    }


    public DeleteInviteBlackListResponse deleteInviteBlackList(DeleteInviteBlackListRequest request) {
        InviteBlackList dbBlackUser = this.getInviteBlackListByUserId(request.getOrgId(), request.getUserId());
        if (dbBlackUser == null) {
            return DeleteInviteBlackListResponse.newBuilder().setRet(-1).build();
        }

        dbBlackUser.setStatus(InviteBlackListStatus.DELETED.getStatus());
        dbBlackUser.setUpdatedAt(new Date(System.currentTimeMillis()));
        inviteBlackListMapper.updateByPrimaryKeySelective(dbBlackUser);

        // 在redis里删掉
        String key = String.format(INVITE_BLACK_LIST_USER_KEY, request.getOrgId(), request.getUserId());
        redisTemplate.delete(key);

        return DeleteInviteBlackListResponse.getDefaultInstance();
    }


    public GetInviteStatisticsRecordListResponse getInviteStatisticsRecordList(GetInviteStatisticsRecordListRequest request) {
        Example example = Example.builder(InviteStatisticsRecord.class)
                .orderByDesc("id")
                .build();

        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("orgId", request.getOrgId());
        if (request.getStatisticsTime() > 0) {
            criteria.andEqualTo("statisticsTime", request.getStatisticsTime());
        }
        if (StringUtils.isNotBlank(request.getToken())) {
            criteria.andEqualTo("token", request.getToken());
        }

        int startIndex = PageUtil.getStartIndex(request.getPage(), request.getLimit());
        List<InviteStatisticsRecord> recordList = inviteStatisticsRecordMapper.selectByExampleAndRowBounds(example, new RowBounds(startIndex, request.getLimit()));
        if (CollectionUtils.isEmpty(recordList)) {
            return GetInviteStatisticsRecordListResponse.getDefaultInstance();
        }

        return GetInviteStatisticsRecordListResponse.newBuilder()
                .addAllRecords(this.convertStatisticsRecordList(recordList))
                .build();
    }

    public InviteBlackList getInviteBlackListByUserId(Long orgId, Long userId) {
        InviteBlackList condition = InviteBlackList.builder()
                .orgId(orgId)
                .userId(userId)
                .status(InviteBlackListStatus.NORMAL.getStatus())
                .build();

        List<InviteBlackList> list = inviteBlackListMapper.select(condition);
        if (CollectionUtils.isEmpty(list)) {
            return null;
        }
        return list.get(0);
    }

    public GetInviteCommonSettingResponse getInviteCommonSetting(Long orgId) {
        List<InviteCommonSetting> settingList = Lists.newArrayList();
        List<CommonIni> commonIniList = commonIniService.queryCommonIniByOrgIdAndIniNames(orgId, INVITE_COMMON_INI_NAME_LIST);
        if (!CollectionUtils.isEmpty(commonIniList)) {
            for (CommonIni commonIni : commonIniList) {
                InviteCommonSetting setting = InviteCommonSetting.newBuilder()
                        .setOrgId(commonIni.getOrgId())
                        .setKey(commonIni.getIniName())
                        .setValue(commonIni.getIniValue())
                        .setDesc(commonIni.getIniDesc())
                        .setLanguage(commonIni.getLanguage())
                        .build();
                settingList.add(setting);
            }
        }
        return GetInviteCommonSettingResponse.newBuilder()
                .addAllSettings(settingList)
                .build();
    }

    public UpdateInviteCommonSettingResponse updateInviteCommonSetting(UpdateInviteCommonSettingRequest request) {
        InviteCommonSetting setting = request.getSetting();

        CommonIni commonIni = CommonIni.builder()
                .orgId(setting.getOrgId())
                .iniName(setting.getKey())
                .language(setting.getLanguage())
                .iniDesc(setting.getDesc())
                .iniValue(setting.getValue())
                .build();
        int res = commonIniService.insertOrUpdateCommonIni(commonIni);
        return UpdateInviteCommonSettingResponse.newBuilder()
                .setRet(res)
                .build();
    }

    public boolean checkUserInInviteBlackList(Long orgId, Long userId) {
        String key = String.format(INVITE_BLACK_LIST_USER_KEY, orgId, userId);
        String cacheString = redisTemplate.opsForValue().get(key);
        if (StringUtils.isNotBlank(cacheString)) {
            // 在黑名单
            return true;
        }

        // 不在redis里， 那就再去库里瞅瞅
        InviteBlackList blackUser = this.getInviteBlackListByUserId(orgId, userId);
        if (blackUser == null) {
            // 库里也没有， 那就真是不在了
            return false;
        }
        // 在库里，放到redis中
        redisTemplate.opsForValue().set(key, "1");
        // 在黑名单
        return true;
    }

    /**
     * 邀请返佣记录生成任务
     */
    @Transactional(isolation = Isolation.READ_COMMITTED, propagation = Propagation.REQUIRED, rollbackFor = Throwable.class)
    public void createInviteBonusRecord(InviteActivity activity, long time) throws Exception {
        long startTime = System.currentTimeMillis();
        log.info(" generalInviteBackFeeTask start: time:{} activity:{}", time, JsonUtil.defaultGson().toJson(activity));

        //开关没打开直接返回不处理
        int maxHandleRelationSize = 500;
        int limit = 100;
        int index = 1;

        while (true) {
            List<InviteInfo> infoList = getInviteInfoByOrgId(activity.getOrgId(), index, limit);
            if (CollectionUtils.isEmpty(infoList)) {
                break;
            }

            for (InviteInfo inviteInfo : infoList) {
                // 查询用户当前统计日是否实行过
                InviteBonusRecord recordCondition = InviteBonusRecord.builder()
                        .userId(inviteInfo.getUserId())
                        .actType(activity.getType())
                        .statisticsTime(time)
                        .build();
                List<InviteBonusRecord> recordList = inviteBonusRecordMapper.select(recordCondition);
                if (!CollectionUtils.isEmpty(recordList)) {
                    log.info(" executeUserBackFeeTask finish : user already executed: brokerId:{}  user:{}  actType:{}",
                            activity.getOrgId(), inviteInfo.getUserId(), activity.getType());
                    continue;
                }

                // 查询用户邀请的人
                InviteRelation relationCondition = InviteRelation.builder()
                        .orgId(inviteInfo.getOrgId())
                        .userId(inviteInfo.getUserId())
                        .build();
                int inviteRecordSize = inviteRelationMapper.selectCount(relationCondition);
                List<InviteRelation> relationList = new ArrayList<>(inviteRecordSize);
                if (inviteRecordSize <= maxHandleRelationSize) {
                    relationList = inviteRelationMapper.select(relationCondition);
                } else {
                    int remain = inviteRecordSize % maxHandleRelationSize;
                    int count = remain == 0 ? (inviteRecordSize / maxHandleRelationSize) : (inviteRecordSize / maxHandleRelationSize + 1);
                    Long fromInviteId = 0L;
                    for (int i = 0; i < count; i++) {
                        List<InviteRelation> relations = inviteRelationMapper.getInviteRelationByUserId(inviteInfo.getOrgId(), inviteInfo.getUserId(), fromInviteId, maxHandleRelationSize);
                        relationList.addAll(relations);
                        if (relations.size() > 0) {
                            fromInviteId = relations.get(relations.size() - 1).getId();
                        }
                    }
                }

                if (CollectionUtils.isEmpty(relationList)) {
                    log.info(" executeUserBackFeeTask finish : user no inviteed: brokerId:{}  user:{}  actType:{}",
                            activity.getOrgId(), inviteInfo.getUserId(), activity.getType());
                    continue;
                }
                log.info(" executeUserBackFeeTask userId:{} has {} invited record", inviteInfo.getUserId(), relationList.size());

                //用户ID对应的关系数据
                Map<Long, InviteRelation> relationMap = new HashMap<>();
                //accountId 对应的uid
                Map<Long, Long> accountUserMap = new HashMap<>();
                //被邀请人币币accountId集合
                List<Long> invitedUserAccountList = Lists.newArrayList();
                //被邀请人的UID集合
                List<Long> invitedUserList = Lists.newArrayList();
                //过期用户集合
                Set<Long> overdueUserList = new HashSet<>();

                int inviteCount = relationList.size();
                int validCount = 0;
                int directCount = 0;
                int indirectCount = 0;
                for (InviteRelation inviteRelation : relationList) {
                    // 如果是有效的则记录有效人数
                    if (inviteRelation.getInvitedStatus().equals(1)) {
                        validCount++;
                    }
                    // 记录用户的直接、间接邀请人数
                    if (inviteRelation.getInvitedType().equals(InviteType.DIRECT.getValue())) {
                        directCount++;
                    } else {
                        indirectCount++;
                    }
                    relationMap.put(inviteRelation.getInvitedId(), inviteRelation);
                    // period == 0 说明邀请设置期限是永久有效的，   邀请关系建立时间 + 过期期限 小于现在的时间， 就是过期的， 放起来
                    if (activity.getPeriod() > 0
                            && (inviteRelation.getCreatedAt().getTime() + activity.getPeriod()) < System.currentTimeMillis()) {
                        overdueUserList.add(inviteRelation.getInvitedId());
                    }
                    accountUserMap.put(inviteRelation.getInvitedAccountId(), inviteRelation.getInvitedId());

                    //vip等级不返佣是指 停止给上级邀请人返佣无论直接还是间接，查的是交易人的等级
                    QueryMyLevelConfigResponse vipLevelConfig = userLevelService.queryMyLevelConfig(inviteRelation.getOrgId(),
                            inviteRelation.getInvitedId(), false, false);
                    if (vipLevelConfig != null && !vipLevelConfig.getInviteBonusStatus()) {
                        log.info("orgId:{} userId:{} invitedId:{} vip level not InviteBonus", inviteRelation.getOrgId(),
                                inviteRelation.getUserId(), inviteRelation.getInvitedId());
                        continue;
                    }

                    //如果该邀请用户是经纪人则不做返佣处理
                    AgentUser agentUser = this.agentUserMapper.queryAgentUserByUserId(inviteRelation.getOrgId(), inviteRelation.getInvitedId());
                    if (agentUser == null || agentUser.getStatus().equals(0)) {
                        invitedUserAccountList.add(inviteRelation.getInvitedAccountId());
                        invitedUserList.add(inviteRelation.getInvitedId());
                    }
                }
                invitedUserAccountList = invitedUserAccountList.stream().distinct().collect(Collectors.toList());
                invitedUserList = invitedUserList.stream().distinct().collect(Collectors.toList());
                // 哪怕用户没有返佣， 也更新一下邀请人数
                inviteInfo.setInviteCount(inviteCount);
                inviteInfo.setInviteDirectVaildCount(directCount);
                inviteInfo.setInviteIndirectVaildCount(indirectCount);

                // 修改邀请信息
                inviteInfoMapper.updateByPrimaryKeySelective(inviteInfo);

                //获取币币手续费数据
                List<UserFeeRecord> userFeeRecordList = Lists.newArrayList();
                if (activity.getCoinStatus().equals(1)) {
                    if (org.apache.commons.collections4.CollectionUtils.isNotEmpty(invitedUserAccountList)) {
                        List<UserFeeRecord> coinFeeList = getInvitedUserFeeRecordList(inviteInfo.getUserId(), invitedUserAccountList, time, 1, activity.getOrgId());
                        userFeeRecordList.addAll(coinFeeList);
                    }
                }

                //获取期货手续费数据
                if (activity.getFuturesStatus().equals(1) ) {
                    if (org.apache.commons.collections4.CollectionUtils.isNotEmpty(invitedUserList)) {
                        List<Account> futuresAccount = accountMapper.queryFuturesAccountList(invitedUserList);
                        futuresAccount.forEach(s -> {
                            accountUserMap.put(s.getAccountId(), s.getUserId());
                        });
                        List<Long> futuresAccountId = futuresAccount.stream().map(Account::getAccountId).collect(Collectors.toList());
                        if (org.apache.commons.collections4.CollectionUtils.isNotEmpty(futuresAccountId)) {
                            List<UserFeeRecord> futuresFeeList = getInvitedUserFeeRecordList(inviteInfo.getUserId(), futuresAccountId, time, 2, activity.getOrgId());
                            userFeeRecordList.addAll(futuresFeeList);
                        }
                    }
                }

                if (CollectionUtils.isEmpty(userFeeRecordList)) {
                    log.info(" executeUserBackFeeTask finish : inviteed no trade : brokerId:{}  user:{}  actType:{} validCount:{}",
                            activity.getOrgId(), inviteInfo.getUserId(), activity.getType(), validCount);
                    continue;
                }

                Map<Long, InviteRelation> updateInviteRelationMap = Maps.newHashMap();
                // 这里再次统计一下用户的有效邀请人数量
                for (UserFeeRecord userFeeRecord : userFeeRecordList) {
                    Long userId = accountUserMap.get(userFeeRecord.getAccountId());
                    if (userId == null) {
                        log.info(" executeUserBackFeeTask compute valid user  error!   not find user !!"
                                + "feeAccountId:{}  inviteInfo-uid:{}", userFeeRecord.getAccountId(), inviteInfo.getUserId());
                        continue;
                    }
                    InviteRelation relation = relationMap.get(userId);
                    if (relation == null) {
                        log.info(" executeUserBackFeeTask compute valid user  error! : user fee has no relation!!  "
                                + "feeAccountId:{}  inviteInfo-uid:{}", userFeeRecord.getAccountId(), inviteInfo.getUserId());
                        continue;
                    }

                    // 原来邀请关系是无效邀请人，  这回有交易了，确认成为有效邀请人
                    if (relation.getInvitedStatus().equals(0)) {
                        validCount++;

                        // 设置为有效邀请人
                        relation.setInvitedStatus(1);
                        updateInviteRelationMap.put(relation.getInvitedAccountId(), relation);
                    }
                }
                // 这里是用总的邀请人数计算等级
                InviteLevel level = computeInviteLevel(activity.getId(), inviteCount);
                if (level == null) {
                    log.info(" executeUserBackFeeTask compute level error! : user no level!!  user:{} inviteCount:{} validCount:{}",
                            inviteInfo.getUserId(), inviteCount, validCount);
                    continue;
                }

                // 如果新的邀请等级大于了原来的邀请等级， 所有的邀请关系上的返佣比例都得修改
                BigDecimal newDirectRate = level.getDirectRate();
                BigDecimal newIndirectRate = level.getIndirectRate();

                List<InviteDetail> inviteDetailList = Lists.newArrayList();
                Map<String, Map<Integer, BigDecimal>> newBonusTokenMap = Maps.newHashMap();
                for (UserFeeRecord record : userFeeRecordList) {
                    Long userId = accountUserMap.get(record.getAccountId());
                    if (userId == null) {
                        log.info(" executeUserBackFeeTask compute valid user  error!   not find user !!"
                                + "feeAccountId:{}  inviteInfo-uid:{}", record.getAccountId(), inviteInfo.getUserId());
                        continue;
                    }
                    InviteRelation relation = relationMap.get(userId);
                    if (relation == null) {
                        log.info(" executeUserBackFeeTask compute back fee error! : user fee has no relation!!  feeAccountId:{}  inviteInfo-uid:{}",
                                record.getAccountId(), inviteInfo.getUserId());
                        continue;
                    }

                    // 过期用户， 他的手续费不反
                    if (overdueUserList.contains(userId)) {
                        log.info(" executeUserBackFeeTask compute back fee continue: out of period:: orgId:{}  time:{} account:{} ", activity.getOrgId(), time, record.getAccountId());
                        continue;
                    }

                    // 根据被邀请人类型， 选取返佣比率
                    BigDecimal rate = relation.getRate();
                    // 用户未设置费率时， 邀请等级晋升后    使用新的费率
                    if (relation.getInvitedType().equals(InviteType.DIRECT.getValue())) {
                        rate = newDirectRate;
                    } else {
                        rate = newIndirectRate;
                    }

                    // 因为费率的变动，所以需要更新
                    relation.setRate(rate);
                    updateInviteRelationMap.put(relation.getInvitedAccountId(), relation);

                    // 计算获取数量
                    List<FeeRecord> feeRecordList = record.getRecordList();
                    if (CollectionUtils.isEmpty(feeRecordList)) {
                        continue;
                    }

                    for (FeeRecord feeRecord : feeRecordList) {
                        BigDecimal amount = feeRecord.getAmount().multiply(rate).setScale(8, RoundingMode.DOWN);
                        if (amount.compareTo(BigDecimal.ZERO) <= 0) {
                            continue;
                        }
                        Map<Integer, BigDecimal> bigDecimalMap = newBonusTokenMap.get(feeRecord.getToken());
                        if (bigDecimalMap == null) {
                            bigDecimalMap = Maps.newHashMap();
                        }
                        BigDecimal totalAmount = bigDecimalMap.get(record.getType());
                        if (totalAmount == null) {
                            totalAmount = BigDecimal.ZERO;
                        }
                        totalAmount = totalAmount.add(amount);
                        bigDecimalMap.put(record.getType(), totalAmount);
                        newBonusTokenMap.put(feeRecord.getToken(), bigDecimalMap);
                        if ("BHEX_CARD".equalsIgnoreCase(feeRecord.getToken())
                                || "BHEX_UCARD".equalsIgnoreCase(feeRecord.getToken())
                                || Lists.newArrayList(AccountService.TEST_TOKENS).contains(feeRecord.getToken())) {
                            continue;
                        }
                        InviteDetail inviteDetail = new InviteDetail();
                        inviteDetail.setOrgId(activity.getOrgId());
                        inviteDetail.setUserId(inviteInfo.getUserId());
                        inviteDetail.setInviteUserId(relation.getInvitedId());
                        inviteDetail.setGetAmount(amount);
                        inviteDetail.setStatisticsTime(time);
                        inviteDetail.setType(record.getType());
                        inviteDetail.setStatus(1);
                        inviteDetail.setTokenId(feeRecord.getToken());
                        inviteDetail.setCreatedAt(new Date());
                        inviteDetail.setRate(rate);
                        inviteDetail.setAmount(feeRecord.getAmount());
                        inviteDetailList.add(inviteDetail);
                    }
                }

                // 统计用户返佣记录，以及转账记录
                List<InviteBonusRecord> bonusRecordList = Lists.newArrayList();
                for (Map.Entry<String, Map<Integer, BigDecimal>> entry : newBonusTokenMap.entrySet()) {
                    String token = entry.getKey();
                    Map<Integer, BigDecimal> newMap = entry.getValue();
                    for (Map.Entry<Integer, BigDecimal> decimalMap : newMap.entrySet()) {
                        Integer type = decimalMap.getKey();
                        //点卡不进行返佣
                        if ("BHEX_CARD".equalsIgnoreCase(token) || "BHEX_UCARD".equalsIgnoreCase(token)
                                || Lists.newArrayList(AccountService.TEST_TOKENS).contains(token)) {
                            continue;
                        }
                        BigDecimal amount = decimalMap.getValue();
                        if (amount == null) {
                            continue;
                        }
                        // 保留8位小数， 若小于0 ， 则跳过
                        amount = amount.setScale(8, RoundingMode.DOWN);
                        if (amount.compareTo(BigDecimal.ZERO) <= 0) {
                            continue;
                        }
                        // 构建转账记录
                        InviteBonusRecord record = InviteBonusRecord.builder()
                                .orgId(activity.getOrgId())
                                .userId(inviteInfo.getUserId())
                                .transferId(sequenceGenerator.getLong())
                                .actType(activity.getType())
                                .token(token)
                                .bonusAmount(amount.doubleValue())
                                .statisticsTime(time)
                                .status(InviteBonusRecordStatus.WAITING.value())
                                .createdAt(new Date(System.currentTimeMillis()))
                                .updatedAt(new Date(System.currentTimeMillis()))
                                .type(type)
                                .build();
                        bonusRecordList.add(record);
                    }
                }

                inviteInfo.setInviteCount(inviteCount);
                inviteInfo.setInviteDirectVaildCount(directCount);
                inviteInfo.setInviteIndirectVaildCount(indirectCount);
                inviteInfo.setInviteLevel(level.getLevel());
                inviteInfo.setDirectRate(newDirectRate.doubleValue());
                inviteInfo.setIndirectRate(newIndirectRate.doubleValue());
                inviteInfo.setUpdatedAt(new Date(System.currentTimeMillis()));

                // 修改邀请信息
                inviteInfoMapper.updateByPrimaryKeySelective(inviteInfo);

                // 修改邀请关系中变为有效邀请人的记录
                if (!CollectionUtils.isEmpty(updateInviteRelationMap)) {
                    for (InviteRelation relation : updateInviteRelationMap.values()) {
                        relation.setUpdateAt(new Date(System.currentTimeMillis()));
                        inviteRelationMapper.updateByPrimaryKeySelective(relation);
                    }
                }

                if (checkUserInInviteBlackList(inviteInfo.getOrgId(), inviteInfo.getUserId())) {
                    log.warn("orgId:{} userId:{} in invite black list", inviteInfo.getOrgId(), inviteInfo.getUserId());
                    continue;
                }

                //存储邀请返佣明细数据 批量存储
                if (org.apache.commons.collections4.CollectionUtils.isNotEmpty(inviteDetailList)) {
                    inviteDetailMapper.insertList(inviteDetailList);
                }

                // 插入记录
                if (!CollectionUtils.isEmpty(bonusRecordList)) {
                    inviteBonusRecordMapper.insertList(bonusRecordList);
                    log.info(" executeUserBackFeeTask user:{} level:{} record:{}", inviteInfo.getUserId(), JsonUtil.defaultGson().toJson(level), JsonUtil.defaultGson().toJson(bonusRecordList));
                }
            }
            index++;
        }

        if (this.inviteDailyTaskMapper.updateInviteDailyTaskChangeStatusToSuccess(activity.getOrgId(), time) != 1) {
            log.error(" updateInviteDailyTaskChangeStatusToSuccess fail orgId:{} time:{}", activity.getOrgId(), time);
        }
        log.info(" generalInviteBackFeeTask finished: cost:{} time:{} activity:{}",
                (System.currentTimeMillis() - startTime), time, JsonUtil.defaultGson().toJson(activity));
    }

    /**
     * 测试返佣数据是否正常生成
     */
    @Transactional(isolation = Isolation.READ_COMMITTED, propagation = Propagation.REQUIRED, rollbackFor = Throwable.class)
    public void testCreateInviteBonusRecord(InviteActivity activity, long time) throws Exception {
        long startTime = System.currentTimeMillis();
        //开关没打开直接返回不处理
        if (!activity.getCoinStatus().equals(1) && !activity.getFuturesStatus().equals(1)) {
            return;
        }
        int maxHandleRelationSize = 500;
        int limit = 100;
        int index = 1;
        while (true) {
            List<InviteInfo> infoList = getInviteInfoByOrgId(activity.getOrgId(), index, limit);
            if (CollectionUtils.isEmpty(infoList)) {
                break;
            }

            for (InviteInfo inviteInfo : infoList) {
                // 查询用户邀请的人
                InviteRelation relationCondition = InviteRelation.builder()
                        .orgId(inviteInfo.getOrgId())
                        .userId(inviteInfo.getUserId())
                        .build();
                int inviteRecordSize = inviteRelationMapper.selectCount(relationCondition);
                List<InviteRelation> relationList = new ArrayList<>(inviteRecordSize);
                if (inviteRecordSize <= maxHandleRelationSize) {
                    relationList = inviteRelationMapper.select(relationCondition);
                } else {
                    int remain = inviteRecordSize % maxHandleRelationSize;
                    int count = remain == 0 ? (inviteRecordSize / maxHandleRelationSize) : (inviteRecordSize / maxHandleRelationSize + 1);
                    Long fromInviteId = 0L;
                    for (int i = 0; i < count; i++) {
                        List<InviteRelation> relations = inviteRelationMapper.getInviteRelationByUserId(inviteInfo.getOrgId(), inviteInfo.getUserId(), fromInviteId, maxHandleRelationSize);
                        relationList.addAll(relations);
                        if (relations.size() > 0) {
                            fromInviteId = relations.get(relations.size() - 1).getId();
                        }
                    }
                }
                if (CollectionUtils.isEmpty(relationList)) {
                    log.info(" executeUserBackFeeTask finish : user no inviteed: brokerId:{}  user:{}  actType:{}",
                            activity.getOrgId(), inviteInfo.getUserId(), activity.getType());
                    continue;
                }

                //用户ID对应的关系数据
                Map<Long, InviteRelation> relationMap = new HashMap<>();
                //accountId 对应的uid
                Map<Long, Long> accountUserMap = new HashMap<>();
                //被邀请人币币accountId集合
                List<Long> invitedUserAccountList = Lists.newArrayList();
                //被邀请人的UID集合
                List<Long> invitedUserList = Lists.newArrayList();
                //过期用户集合
                Set<Long> overdueUserList = new HashSet<>();

                int inviteCount = relationList.size();
                int validCount = 0;
                int directCount = 0;
                int indirectCount = 0;
                for (InviteRelation inviteRelation : relationList) {
                    // 如果是有效的则记录有效人数
                    if (inviteRelation.getInvitedStatus().equals(1)) {
                        validCount++;
                    }
                    // 记录用户的直接、间接邀请人数
                    if (inviteRelation.getInvitedType().equals(InviteType.DIRECT.getValue())) {
                        directCount++;
                    } else {
                        indirectCount++;
                    }
                    relationMap.put(inviteRelation.getInvitedId(), inviteRelation);
                    // period == 0 说明邀请设置期限是永久有效的，   邀请关系建立时间 + 过期期限 小于现在的时间， 就是过期的， 放起来
                    if (activity.getPeriod() > 0
                            && (inviteRelation.getCreatedAt().getTime() + activity.getPeriod()) < System.currentTimeMillis()) {
                        overdueUserList.add(inviteRelation.getInvitedId());
                    }
                    accountUserMap.put(inviteRelation.getInvitedAccountId(), inviteRelation.getInvitedId());
                    invitedUserAccountList.add(inviteRelation.getInvitedAccountId());
                    invitedUserList.add(inviteRelation.getInvitedId());
                }
                invitedUserAccountList = invitedUserAccountList.stream().distinct().collect(Collectors.toList());
                invitedUserList = invitedUserList.stream().distinct().collect(Collectors.toList());
                // 哪怕用户没有返佣， 也更新一下邀请人数
                inviteInfo.setInviteCount(inviteCount);
                inviteInfo.setInviteDirectVaildCount(directCount);
                inviteInfo.setInviteIndirectVaildCount(indirectCount);

                //获取币币手续费数据
                List<UserFeeRecord> userFeeRecordList = Lists.newArrayList();
                if (activity.getCoinStatus().equals(1)) {
                    if (org.apache.commons.collections4.CollectionUtils.isNotEmpty(invitedUserAccountList)) {
                        List<UserFeeRecord> coinFeeList = getInvitedUserFeeRecordList(inviteInfo.getUserId(), invitedUserAccountList, time, 1, activity.getOrgId());
                        userFeeRecordList.addAll(coinFeeList);
                    }
                }

                //获取期货手续费数据
                if (activity.getFuturesStatus().equals(1)) {
                    if (org.apache.commons.collections4.CollectionUtils.isNotEmpty(invitedUserList)) {
                        List<Account> futuresAccount = accountMapper.queryFuturesAccountList(invitedUserList);
                        futuresAccount.forEach(s -> {
                            accountUserMap.put(s.getAccountId(), s.getUserId());
                        });
                        List<Long> futuresAccountId = futuresAccount.stream().map(Account::getAccountId).collect(Collectors.toList());
                        if (org.apache.commons.collections4.CollectionUtils.isNotEmpty(futuresAccountId)) {
                            List<UserFeeRecord> futuresFeeList = getInvitedUserFeeRecordList(inviteInfo.getUserId(), futuresAccountId, time, 2, activity.getOrgId());
                            userFeeRecordList.addAll(futuresFeeList);
                        }
                    }
                }

                if (CollectionUtils.isEmpty(userFeeRecordList)) {
                    log.info(" executeUserBackFeeTask finish : inviteed no trade : brokerId:{}  user:{}  actType:{}",
                            activity.getOrgId(), inviteInfo.getUserId(), activity.getType());
                    continue;
                }

                Map<Long, InviteRelation> updateInviteRelationMap = Maps.newHashMap();
                // 这里再次统计一下用户的有效邀请人数量
                for (UserFeeRecord userFeeRecord : userFeeRecordList) {
                    Long userId = accountUserMap.get(userFeeRecord.getAccountId());
                    if (userId == null) {
                        log.info(" executeUserBackFeeTask compute valid user  error!   not find user !!"
                                + "feeAccountId:{}  inviteInfo-uid:{}", userFeeRecord.getAccountId(), inviteInfo.getUserId());
                        continue;
                    }
                    InviteRelation relation = relationMap.get(userId);
                    if (relation == null) {
                        log.info(" executeUserBackFeeTask compute valid user  error! : user fee has no relation!!  "
                                + "feeAccountId:{}  inviteInfo-uid:{}", userFeeRecord.getAccountId(), inviteInfo.getUserId());
                        continue;
                    }

                    // 原来邀请关系是无效邀请人，  这回有交易了，确认成为有效邀请人
                    if (relation.getInvitedStatus().equals(0)) {
                        validCount++;

                        // 设置为有效邀请人
                        relation.setInvitedStatus(1);
                        updateInviteRelationMap.put(relation.getInvitedAccountId(), relation);
                    }
                }
                // 这里是用总的邀请人数计算等级
                InviteLevel level = computeInviteLevel(activity.getId(), inviteCount);
                if (level == null) {
                    log.info(" executeUserBackFeeTask compute level error! : user no level!!  user:{} inviteCount:{} validCount:{}",
                            inviteInfo.getUserId(), inviteCount, validCount);
                    continue;
                }

                // 如果新的邀请等级大于了原来的邀请等级， 所有的邀请关系上的返佣比例都得修改
                boolean changeRate = false;
                BigDecimal newDirectRate = level.getDirectRate();
                BigDecimal newIndirectRate = level.getIndirectRate();
                if (level.getLevel() > inviteInfo.getInviteLevel()) {
                    changeRate = true;
                }

                List<InviteDetail> inviteDetailList = Lists.newArrayList();
                Map<String, Map<Integer, BigDecimal>> newBonusTokenMap = Maps.newHashMap();
                for (UserFeeRecord record : userFeeRecordList) {
                    Long userId = accountUserMap.get(record.getAccountId());
                    if (userId == null) {
                        log.info(" executeUserBackFeeTask compute valid user  error!   not find user !!"
                                + "feeAccountId:{}  inviteInfo-uid:{}", record.getAccountId(), inviteInfo.getUserId());
                        continue;
                    }
                    InviteRelation relation = relationMap.get(userId);
                    if (relation == null) {
                        log.info(" executeUserBackFeeTask compute back fee error! : user fee has no relation!!  feeAccountId:{}  inviteInfo-uid:{}",
                                record.getAccountId(), inviteInfo.getUserId());
                        continue;
                    }

                    // 过期用户， 他的手续费不反
                    if (overdueUserList.contains(userId)) {
                        log.info(" executeUserBackFeeTask compute back fee continue: out of period:: orgId:{}  time:{} account:{} ", activity.getOrgId(), time, record.getAccountId());
                        continue;
                    }

                    // 根据被邀请人类型， 选取返佣比率
                    BigDecimal rate = relation.getRate();
                    // 用户未设置费率时， 邀请等级晋升后    使用新的费率
                    if (rate.compareTo(BigDecimal.ZERO) <= 0 || changeRate) {
                        if (relation.getInvitedType().equals(InviteType.DIRECT.getValue())) {
                            rate = newDirectRate;
                        } else {
                            rate = newIndirectRate;
                        }

                        // 因为费率的变动，所以需要更新
                        relation.setRate(rate);
                        updateInviteRelationMap.put(relation.getInvitedAccountId(), relation);
                    }

                    // 计算获取数量
                    List<FeeRecord> feeRecordList = record.getRecordList();
                    if (CollectionUtils.isEmpty(feeRecordList)) {
                        continue;
                    }

                    for (FeeRecord feeRecord : feeRecordList) {
                        BigDecimal amount = feeRecord.getAmount().multiply(rate).setScale(8, RoundingMode.DOWN);
                        if (amount.compareTo(BigDecimal.ZERO) <= 0) {
                            continue;
                        }
                        Map<Integer, BigDecimal> bigDecimalMap = newBonusTokenMap.get(feeRecord.getToken());
                        if (bigDecimalMap == null) {
                            bigDecimalMap = Maps.newHashMap();
                        }
                        BigDecimal totalAmount = bigDecimalMap.get(record.getType());
                        if (totalAmount == null) {
                            totalAmount = BigDecimal.ZERO;
                        }
                        totalAmount = totalAmount.add(amount);
                        bigDecimalMap.put(record.getType(), totalAmount);
                        newBonusTokenMap.put(feeRecord.getToken(), bigDecimalMap);

                        InviteDetail inviteDetail = new InviteDetail();
                        inviteDetail.setOrgId(activity.getOrgId());
                        inviteDetail.setUserId(inviteInfo.getUserId());
                        inviteDetail.setInviteUserId(relation.getInvitedId());
                        inviteDetail.setGetAmount(amount);
                        inviteDetail.setStatisticsTime(time);
                        inviteDetail.setType(record.getType());
                        inviteDetail.setStatus(1);
                        inviteDetail.setTokenId(feeRecord.getToken());
                        inviteDetail.setCreatedAt(new Date());
                        inviteDetail.setRate(rate);
                        inviteDetail.setAmount(feeRecord.getAmount());
                        inviteDetailList.add(inviteDetail);
                    }
                }

                if (checkUserInInviteBlackList(inviteInfo.getOrgId(), inviteInfo.getUserId())) {
                    log.warn("orgId:{} userId:{} in invite black list", inviteInfo.getOrgId(), inviteInfo.getUserId());
                    continue;
                }

                //存储邀请返佣明细数据 批量存储
                if (org.apache.commons.collections4.CollectionUtils.isNotEmpty(inviteDetailList)) {
                    inviteDetailMapper.insertList(inviteDetailList);
                }
            }
            index++;
        }

        if (this.inviteDailyTaskMapper.updateInviteDailyTaskChangeStatusToSuccess(activity.getOrgId(), time) != 1) {
            log.error(" updateInviteDailyTaskChangeStatusToSuccess fail orgId:{} time:{}", activity.getOrgId(), time);
        }
        log.info(" generalInviteBackFeeTask finished: cost:{} time:{} activity:{}",
                (System.currentTimeMillis() - startTime), time, JsonUtil.defaultGson().toJson(activity));
    }

    private List<InviteInfo> getInviteInfoByOrgId(Long orgId, int startPage, int limit) {
        InviteInfo infoCondition = InviteInfo.builder()
                .orgId(orgId)
                .build();

        List<InviteInfo> infoList = inviteInfoMapper.selectByRowBounds(infoCondition, new RowBounds(PageUtil.getStartIndex(startPage, limit), limit));
        if (CollectionUtils.isEmpty(infoList)) {
            return null;
        }
        return infoList;
    }

    private List<UserFeeRecord> getInvitedUserFeeRecordList(Long fromId, List<Long> invitedAccountList, long time, Integer type, Long orgId) {
        Map<Long, UserFeeRecord> recordMap = Maps.newHashMap();
        String date = null;
        try {
            date = DateFormatUtils.format(DateUtils.parseDate(time + "", "yyyyMMdd"), "yyyy-MM-dd");
        } catch (ParseException e) {
            log.error("getInvitedUserFeeRecordList exception:", e);
        }

        List<List<Long>> partAccountList = Lists.partition(invitedAccountList, 1000);
        for (List<Long> accountList : partAccountList) {
            List<StatisticsRpcBrokerUserFee> list = statisticsRptBrokerUserFeeMapper.queryBrokerUserFeeByAccountIds(accountList, date);
            if (CollectionUtils.isEmpty(list)) {
                continue;
            }
            for (StatisticsRpcBrokerUserFee brokerUserFee : list) {
                UserFeeRecord userFeeRecord = recordMap.get(brokerUserFee.getAccountId());
                if (userFeeRecord == null) {
                    userFeeRecord = new UserFeeRecord();
                    userFeeRecord.setAccountId(brokerUserFee.getAccountId());
                }
                userFeeRecord.setType(type);
                userFeeRecord.getRecordList().add(new FeeRecord(brokerUserFee.getTokenId(), brokerUserFee.getTokenFee()));
                recordMap.put(brokerUserFee.getAccountId(), userFeeRecord);
            }

            log.info("getInvitedUserFeeRecordList fromId:{}  content:{}", fromId, list);
        }
        return Lists.newArrayList(recordMap.values());
    }

    private InviteLevel computeInviteLevel(long actId, int vaildInvited) {
        List<InviteLevel> levelList = getInviteLevelList(actId);
        if (CollectionUtils.isEmpty(levelList)) {
            return null;
        }

        InviteLevel curLevel = null;
        for (InviteLevel level : levelList) {
            // 如果邀请数量大于等级条件，则直接跳至下一个等级进行判断,
            // 如果邀请数量小于等于等级条件，则直接使用该等级
            if (vaildInvited >= level.getLevelCondition()) {
                curLevel = level;
            }
        }

        return curLevel;
    }

    public List<InviteLevel> getInviteLevelList(long actId) {
        // TODO 测试时去掉缓存
        redisTemplate.delete(BrokerServerConstants.INVITE_LEVEL_KEY + actId);

        String cacheString = redisTemplate.opsForValue().get(BrokerServerConstants.INVITE_LEVEL_KEY + actId);
        if (StringUtils.isNotBlank(cacheString)) {
            return JsonUtil.defaultGson().fromJson(cacheString, new TypeToken<List<InviteLevel>>() {
            }.getType());
        }

        Example example = Example.builder(InviteLevel.class)
                .orderByAsc("level")
                .build();

        example.createCriteria().andEqualTo("actId", actId);

        List<InviteLevel> levelList = inviteLevelMapper.selectByExample(example);
        if (CollectionUtils.isEmpty(levelList)) {
            return null;
        }

        // 放入缓存
        redisTemplate.opsForValue().set(BrokerServerConstants.INVITE_LEVEL_KEY + actId, JsonUtil.defaultGson().toJson(levelList));

        return levelList;
    }


    public GetDailyTaskListResponse getDailyTaskList(GetDailyTaskListRequest request) {
        Example example = Example.builder(InviteDailyTask.class)
                .orderByDesc("statisticsTime")
                .build();

        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("orgId", request.getOrgId());
        if (request.getStartTime() > 0) {
            criteria.andGreaterThan("statisticsTime", request.getStartTime());
        }

        if (request.getEndTime() > 0) {
            criteria.andLessThanOrEqualTo("statisticsTime", request.getEndTime());
        }

        int startIndex = PageUtil.getStartIndex(request.getPage(), request.getLimit());
        List<InviteDailyTask> taskList = inviteDailyTaskMapper.selectByExampleAndRowBounds(example, new RowBounds(startIndex, request.getLimit()));
        if (CollectionUtils.isEmpty(taskList)) {
            return GetDailyTaskListResponse.getDefaultInstance();
        }
        return GetDailyTaskListResponse.newBuilder()
                .addAllTasks(this.convertDailyTaskList(taskList))
                .build();
    }

    public InviteDailyTask getInviteDailyTask(Long orgId, Long time) {
        InviteDailyTask condition = InviteDailyTask.builder()
                .orgId(orgId)
                .statisticsTime(time)
                .build();
        return inviteDailyTaskMapper.selectOne(condition);
    }

    public io.bhex.broker.grpc.invite.InviteInfo convertInviteInfo(InviteInfo inviteInfo) {
        return io.bhex.broker.grpc.invite.InviteInfo.newBuilder()
                .setUserId(inviteInfo.getUserId())
                .setInviteCount(inviteInfo.getInviteCount())
                //.setInviteVaildCount(inviteInfo.getInviteVaildCount())
                .setInviteDirectVaildCount(inviteInfo.getInviteDirectVaildCount())
                .setInviteIndirectVaildCount(inviteInfo.getInviteIndirectVaildCount())
                .setInviteHobbitLeaderCount(inviteInfo.getInviteHobbitLeaderCount())
                .setInviteLevel(inviteInfo.getInviteLevel())
                .setDirectRate(inviteInfo.getDirectRate() == null ? "0" : inviteInfo.getDirectRate().toString())
                .setIndirectRate(inviteInfo.getIndirectRate() == null ? "0" : inviteInfo.getIndirectRate().toString())
                .setBonusCoin(inviteInfo.getBonusCoin() == null ? "0" : inviteInfo.getBonusCoin().toString())
                .setBonusPoint(inviteInfo.getBonusPoint() == null ? "0" : inviteInfo.getBonusPoint().toString())
                .setHide(inviteInfo.getHide() == null ? 0 : inviteInfo.getHide())
                .build();
    }

    public List<io.bhex.broker.grpc.invite.InviteRank> convertRankList(List<InviteRank> rankList) {
        List<io.bhex.broker.grpc.invite.InviteRank> rList = Lists.newArrayList();
        if (CollectionUtils.isEmpty(rankList)) {
            return rList;
        }

        for (io.bhex.broker.server.model.InviteRank rank : rankList) {
            rList.add(this.convertRank(rank));
        }

        return rList;
    }

    public io.bhex.broker.grpc.invite.InviteRank convertRank(InviteRank rank) {
        return io.bhex.broker.grpc.invite.InviteRank.newBuilder()
                .setUserId(rank.getUserId())
                .setUserName(rank.getUserName())
                .setMonth(rank.getMonth())
                .setType(rank.getType())
                .setAmount(rank.getAmount().toString())
                .build();
    }

    public List<io.bhex.broker.grpc.invite.InviteRelation> convertRelationList(List<InviteRelation> relationList) {
        List<io.bhex.broker.grpc.invite.InviteRelation> list = Lists.newArrayList();
        if (CollectionUtils.isEmpty(relationList)) {
            return list;
        }
        for (InviteRelation relation : relationList) {
            list.add(this.convertRelation(relation));
        }
        return list;
    }

    public io.bhex.broker.grpc.invite.InviteRelation convertRelation(InviteRelation relation) {
        return io.bhex.broker.grpc.invite.InviteRelation.newBuilder()
                .setId(relation.getId())
                .setUserId(relation.getUserId())
                .setInvitedId(relation.getInvitedId())
                .setInvitedName(relation.getInvitedName())
                .setInvitedType(relation.getInvitedType())
                .setInvitedStatus(relation.getInvitedStatus())
                .setContributeCoin(relation.getContributeCoin().toString())
                .setContributePoint(relation.getContributePoint().toString())
                .setCreatedAt(relation.getCreatedAt().getTime())
                .build();
    }

    public List<io.bhex.broker.grpc.invite.InviteBonusDetail> convertBonusDetailList(List<InviteBonusDetail> detailList) {
        List<io.bhex.broker.grpc.invite.InviteBonusDetail> list = Lists.newArrayList();
        if (CollectionUtils.isEmpty(detailList)) {
            return list;
        }

        for (InviteBonusDetail detail : detailList) {
            list.add(this.convertBonusDetail(detail));
        }

        return list;
    }

    public io.bhex.broker.grpc.invite.InviteBonusDetail convertBonusDetail(InviteBonusDetail detail) {
        return io.bhex.broker.grpc.invite.InviteBonusDetail.newBuilder()
                .setId(detail.getId())
                .setUserId(detail.getUserId())
                .setInvitedId(detail.getInvitedId())
                .setInvitedName(detail.getInvitedName())
                .setStatisticsTime(detail.getStatisticsTime())
                .setContributeCoin(detail.getContributeCoin().toString())
                .setContributePoint(detail.getContributePoint().toString())
                .build();
    }

    public List<io.bhex.broker.grpc.invite.InviteBonusRecord> convertBonusRecordList(List<InviteBonusRecord> recordList) {
        List<io.bhex.broker.grpc.invite.InviteBonusRecord> list = Lists.newArrayList();
        if (CollectionUtils.isEmpty(recordList)) {
            return list;
        }
        for (InviteBonusRecord record : recordList) {
            list.add(this.convertBonusRecord(record));
        }
        return list;
    }

    public io.bhex.broker.grpc.invite.InviteBonusRecord convertBonusRecord(InviteBonusRecord record) {
        return io.bhex.broker.grpc.invite.InviteBonusRecord.newBuilder()
                .setId(record.getId())
                .setUserId(record.getUserId())
                .setToken(record.getToken())
                .setBonusAmount(record.getBonusAmount() == null ? "0" : record.getBonusAmount().toString())
                .setStatisticsTime(record.getStatisticsTime())
                .setStatus(record.getStatus())
                .build();
    }

    public List<io.bhex.broker.grpc.invite.InviteDailyTask> convertDailyTaskList(List<InviteDailyTask> taskList) {
        List<io.bhex.broker.grpc.invite.InviteDailyTask> list = Lists.newArrayList();
        if (CollectionUtils.isEmpty(taskList)) {
            return list;
        }
        for (InviteDailyTask task : taskList) {
            list.add(this.convertDailyTask(task));
        }
        return list;
    }

    public io.bhex.broker.grpc.invite.InviteDailyTask convertDailyTask(InviteDailyTask task) {
        return io.bhex.broker.grpc.invite.InviteDailyTask.newBuilder()
                .setId(task.getId())
                .setStatisticsTime(task.getStatisticsTime())
                .setTotalAmount(task.getTotalAmount() == null ? "0" : task.getTotalAmount().toString())
                .setChangeStatus(task.getChangeStatus() == null ? -1 : task.getChangeStatus())
                .setGrantStatus(task.getGrantStatus())
                .build();
    }

    public List<io.bhex.broker.grpc.invite.InviteLevel> convertLevelList(List<InviteLevel> list) {
        if (CollectionUtils.isEmpty(list)) {
            return Lists.newArrayList();
        }
        List<io.bhex.broker.grpc.invite.InviteLevel> resList = Lists.newArrayList();
        for (InviteLevel level : list) {
            resList.add(this.convertLevel(level));
        }
        return resList;
    }

    public io.bhex.broker.grpc.invite.InviteLevel convertLevel(InviteLevel level) {
        return io.bhex.broker.grpc.invite.InviteLevel.newBuilder()
                .setId(level.getId())
                .setActId(level.getActId())
                .setLevel(level.getLevel())
                .setLevelTag(level.getLevelTag())
                .setTokenCondition(level.getTokenCondition() == null ? "0" : level.getTokenCondition().toString())
                .setLevelCondition(level.getLevelCondition() == null ? "0" : level.getLevelCondition().toString())
                .setDirectRate(level.getDirectRate() == null ? "0" : level.getDirectRate().toString())
                .setIndirectRate(level.getIndirectRate() == null ? "0" : level.getIndirectRate().toString())
                .setTokenDirectRate(level.getTokenDirectRate() == null ? "0" : level.getTokenDirectRate().toString())
                .build();
    }

    public io.bhex.broker.grpc.invite.InviteActivity convertActivity(InviteActivity activity) {
        return io.bhex.broker.grpc.invite.InviteActivity.newBuilder()
                .setId(activity.getId())
                .setOrgId(activity.getOrgId())
                .setType(activity.getType())
                .setStatus(activity.getStatus())
                .setAutoTransfer(activity.getAutoTransfer())
                .setCoinStatus(activity.getCoinStatus())
                .setFuturesStatus(activity.getFuturesStatus())
                .setPeriod(activity.getPeriod())
                .build();
    }

    public List<InviteBlackUser> convertBlackUserList(List<InviteBlackList> lists) {
        List<InviteBlackUser> resultList = Lists.newArrayList();
        if (CollectionUtils.isEmpty(lists)) {
            return resultList;
        }
        for (InviteBlackList black : lists) {
            resultList.add(this.convertBlackUser(black));
        }
        return resultList;
    }

    public InviteBlackUser convertBlackUser(InviteBlackList black) {
        return InviteBlackUser.newBuilder()
                .setId(black.getId())
                .setOrgId(black.getOrgId())
                .setUserId(black.getUserId())
                .setUserContact(black.getUserContact())
                .setCreatedAt(black.getCreatedAt() != null ? black.getCreatedAt().getTime() : System.currentTimeMillis())
                .build();
    }

    public List<io.bhex.broker.grpc.invite.InviteStatisticsRecord> convertStatisticsRecordList(List<InviteStatisticsRecord> recordList) {
        if (CollectionUtils.isEmpty(recordList)) {
            return Lists.newArrayList();
        }
        List<io.bhex.broker.grpc.invite.InviteStatisticsRecord> list = Lists.newArrayList();
        for (InviteStatisticsRecord record : recordList) {
            list.add(this.convertStatisticsRecord(record));
        }
        return list;
    }

    public io.bhex.broker.grpc.invite.InviteStatisticsRecord convertStatisticsRecord(InviteStatisticsRecord record) {
        return io.bhex.broker.grpc.invite.InviteStatisticsRecord.newBuilder()
                .setOrgId(record.getOrgId())
                .setStatisticsTime(record.getStatisticsTime())
                .setToken(record.getToken())
                .setAmount(record.getAmount().toString())
                .setTransferAmount(record.getTransferAmount() <= record.getAmount()
                        ? record.getTransferAmount().toString() : record.getAmount().toString())
                .setStatus(record.getStatus())
                .setCreatedAt(record.getCreatedAt() != null ? record.getCreatedAt().getTime() : System.currentTimeMillis())
                .setUpdatedAt(record.getUpdatedAt() != null ? record.getUpdatedAt().getTime() : System.currentTimeMillis())
                .build();
    }

    public void initInviteWechatConfig(Long orgId) {
        List<CommonIni> commonInis = commonIniMapper.getByOrgIdAndInitName(orgId, "invite_share_url");
        if (org.apache.commons.collections4.CollectionUtils.isNotEmpty(commonInis)) {
            return;
        }
        List<CommonIni> commonIniList = createBrokerInviteWechatConfig(orgId);
        if (commonIniList.size() > 0) {
            commonIniList.forEach(commonIni -> {
                commonIniService.insertOrUpdateCommonIni(commonIni);
            });
        }
    }

    private List<CommonIni> createBrokerInviteWechatConfig(Long orgId) {
        Broker broker = this.brokerService.getBrokerById(orgId);
        if (broker == null) {
            return new ArrayList<>();
        }

        List<CommonIni> commonIniList = new ArrayList<>();
        commonIniList.add(createCommonIni(orgId, InviteWechatConfig.INVITE_SHARE, InviteWechatConfig.INVITE_SHARE_URL_DESC, String.format(InviteWechatConfig.INVITE_SHARE_URL, Splitter.on(",").omitEmptyStrings().splitToList(broker.getApiDomain()).get(0), "%s", "%s", "%s"), InviteWechatConfig.LANGUAGE_CN));
        commonIniList.add(createCommonIni(orgId, InviteWechatConfig.INVITE_SHARE, InviteWechatConfig.INVITE_SHARE_URL_DESC, String.format(InviteWechatConfig.INVITE_SHARE_URL, Splitter.on(",").omitEmptyStrings().splitToList(broker.getApiDomain()).get(0), "%s", "%s", "%s"), InviteWechatConfig.LANGUAGE_US));
        commonIniList.add(createCommonIni(orgId, InviteWechatConfig.INVITE_SHARE_WX_TITLE, InviteWechatConfig.INVITE_SHARE_WX_TITLE_DESC, broker.getBrokerName(), InviteWechatConfig.LANGUAGE_CN));
        commonIniList.add(createCommonIni(orgId, InviteWechatConfig.INVITE_SHARE_WX_TITLE, InviteWechatConfig.INVITE_SHARE_WX_TITLE_DESC, broker.getBrokerName(), InviteWechatConfig.LANGUAGE_US));
        commonIniList.add(createCommonIni(orgId, InviteWechatConfig.INVITE_SHARE_WX_CONTENT, InviteWechatConfig.INVITE_SHARE_WX_CONTENT_DESC, String.format(InviteWechatConfig.INVITE_SHARE_WX_CONTENT_CN, broker.getBrokerName()), InviteWechatConfig.LANGUAGE_CN));
        commonIniList.add(createCommonIni(orgId, InviteWechatConfig.INVITE_SHARE_WX_CONTENT, InviteWechatConfig.INVITE_SHARE_WX_CONTENT_DESC, String.format(InviteWechatConfig.INVITE_SHARE_WX_CONTENT_US, broker.getBrokerName()), InviteWechatConfig.LANGUAGE_US));
        return commonIniList;
    }

    private CommonIni createCommonIni(Long orgId, String name, String desc, String value, String language) {
        return CommonIni
                .builder()
                .orgId(orgId)
                .iniDesc(desc)
                .iniName(name)
                .iniValue(value)
                .language(language)
                .created(new Date().getTime())
                .updated(new Date().getTime())
                .build();
    }

    @Data
    class UserFeeRecord {
        private Long userId;

        private Long accountId;

        private Integer type;

        List<FeeRecord> recordList = Lists.newArrayList();
    }

    @Data
    class FeeRecord {
        private String token;

        private BigDecimal amount;

        FeeRecord(String token, BigDecimal amount) {
            this.token = token;
            this.amount = amount;
        }

        FeeRecord() {
        }
    }
}
