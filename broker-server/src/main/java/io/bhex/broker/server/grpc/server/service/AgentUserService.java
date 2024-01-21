package io.bhex.broker.server.grpc.server.service;


import com.google.common.base.Strings;
import com.google.common.collect.Maps;

import com.alibaba.fastjson.JSON;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.ibatis.session.RowBounds;
import org.joda.time.DateTime;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation.Resource;

import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.grpc.agent.AddAgentUserRequest;
import io.bhex.broker.grpc.agent.AddAgentUserResponse;
import io.bhex.broker.grpc.agent.AgentBrokerInfo;
import io.bhex.broker.grpc.agent.AgentInfoRequest;
import io.bhex.broker.grpc.agent.AgentInfoResponse;
import io.bhex.broker.grpc.agent.AgentUserInfo;
import io.bhex.broker.grpc.agent.AgentUserUpgradeRequest;
import io.bhex.broker.grpc.agent.AgentUserUpgradeResponse;
import io.bhex.broker.grpc.agent.CancelAgentUserResponse;
import io.bhex.broker.grpc.agent.QueryAgentBrokerRequest;
import io.bhex.broker.grpc.agent.QueryAgentBrokerResponse;
import io.bhex.broker.grpc.agent.QueryAgentCommissionListResponse;
import io.bhex.broker.grpc.agent.QueryAgentUserRequest;
import io.bhex.broker.grpc.agent.QueryAgentUserResponse;
import io.bhex.broker.grpc.agent.QueryBrokerAgentListRequest;
import io.bhex.broker.grpc.agent.QueryBrokerAgentListResponse;
import io.bhex.broker.grpc.agent.QueryBrokerUserListRequest;
import io.bhex.broker.grpc.agent.QueryBrokerUserListResponse;
import io.bhex.broker.grpc.agent.RebindAgentUserResponse;
import io.bhex.broker.grpc.agent.UpdateBrokerRequest;
import io.bhex.broker.grpc.agent.UpdateBrokerResponse;
import io.bhex.broker.grpc.common.BasicRet;
import io.bhex.broker.server.grpc.server.service.statistics.StatisticsAgentService;
import io.bhex.broker.server.model.Account;
import io.bhex.broker.server.model.AgentCommission;
import io.bhex.broker.server.model.AgentConfig;
import io.bhex.broker.server.model.AgentUser;
import io.bhex.broker.server.model.Country;
import io.bhex.broker.server.model.InviteRelation;
import io.bhex.broker.server.model.User;
import io.bhex.broker.server.model.UserVerify;
import io.bhex.broker.server.primary.mapper.AccountMapper;
import io.bhex.broker.server.primary.mapper.AgentConfigMapper;
import io.bhex.broker.server.primary.mapper.AgentUserMapper;
import io.bhex.broker.server.primary.mapper.CountryMapper;
import io.bhex.broker.server.primary.mapper.InviteRelationMapper;
import io.bhex.broker.server.primary.mapper.UserMapper;
import io.bhex.broker.server.primary.mapper.UserVerifyMapper;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import tk.mybatis.mapper.entity.Example;


@Slf4j
@Service
public class AgentUserService {

    private enum AgentType {
        USER(0),
        AGENT(1),
        ALL(2);

        @Getter
        private Integer agentType;

        AgentType(Integer agentType) {
            this.agentType = agentType;
        }
    }

    @Resource
    private UserMapper userMapper;

    @Resource
    private AccountMapper accountMapper;

    @Resource
    private CountryMapper countryMapper;

    @Resource
    private AgentUserMapper agentUserMapper;

    @Resource
    private UserVerifyMapper userVerifyMapper;

    @Resource
    private BasicService basicService;

    @Resource
    private InviteRelationMapper inviteRelationMapper;

    @Resource
    private StatisticsAgentService statisticsAgentService;

    @Resource
    private AgentConfigMapper agentConfigMapper;


    /**
     * 券商后台升级1级经纪人
     *
     * @param request
     * @return
     */
    @Transactional(isolation = Isolation.READ_COMMITTED, propagation = Propagation.REQUIRED, rollbackFor = Throwable.class)
    public AddAgentUserResponse addMasterAgentUser(AddAgentUserRequest request) {
        AgentUser agentInfo = this.agentUserMapper.queryAgentUserByUserId(request.getOrgId(), request.getUserId());
        User user = this.userMapper.getByUserId(request.getUserId());
        if (user == null) {
            throw new BrokerException(BrokerErrorCode.USER_NOT_EXIST);
        }
        Account account = accountMapper.getMainAccount(request.getOrgId(), request.getUserId());
        if (account == null) {
            throw new BrokerException(BrokerErrorCode.ACCOUNT_NOT_EXIST);
        }
        AgentConfig agentConfig = getAgentConfig(request.getOrgId());
        AgentUser agentUser = AgentUser
                .builder()
                .orgId(request.getOrgId())
                .userId(request.getUserId())
                .accountId(account.getAccountId())
                .superiorUserId((agentInfo != null && agentInfo.getSuperiorUserId() > 0) ? agentInfo.getSuperiorUserId() : 0L)
                .p1((agentInfo != null && agentInfo.getP1() > 0) ? agentInfo.getP1() : request.getUserId())
                .rate(StringUtils.isNotEmpty(request.getCoinDefaultRate()) ? new BigDecimal(request.getCoinDefaultRate()) : BigDecimal.ZERO)
                .childrenDefaultRate(StringUtils.isNotEmpty(request.getCoinChildrenDefaultRate()) ? new BigDecimal(request.getCoinChildrenDefaultRate()) : BigDecimal.ZERO)
                .contractRate(StringUtils.isNotEmpty(request.getContractDefaultRate()) ? new BigDecimal(request.getContractDefaultRate()) : BigDecimal.ZERO)
                .contractChildrenDefaultRate(StringUtils.isNotEmpty(request.getContractChildrenDefaultRate()) ? new BigDecimal(request.getContractChildrenDefaultRate()) : BigDecimal.ZERO)
                .level((agentInfo != null && agentInfo.getLevel() > 0) ? agentInfo.getLevel() : 1)
                .agentName(StringUtils.isNotEmpty(request.getAgentName()) ? request.getAgentName() : "")
                .leader(StringUtils.isNotEmpty(request.getLeader()) ? request.getLeader() : "")
                .leaderMobile(StringUtils.isNotEmpty(request.getMobile()) ? request.getMobile() : "")
                .mark(StringUtils.isNotEmpty(request.getMark()) ? request.getMark() : "")
                .created(new Date())
                .isAbs(agentConfig != null ? 1 : 0)
                .build();

        if (agentUser.getLevel().equals(1)) {
            agentUser.setRate(StringUtils.isNotEmpty(request.getCoinDefaultRate()) ? new BigDecimal(request.getCoinDefaultRate()) : BigDecimal.ZERO);
            agentUser.setChildrenDefaultRate(StringUtils.isNotEmpty(request.getCoinDefaultRate()) ? new BigDecimal(request.getCoinDefaultRate()) : BigDecimal.ZERO);
            agentUser.setContractRate(StringUtils.isNotEmpty(request.getContractDefaultRate()) ? new BigDecimal(request.getContractDefaultRate()) : BigDecimal.ZERO);
            agentUser.setContractChildrenDefaultRate(StringUtils.isNotEmpty(request.getContractDefaultRate()) ? new BigDecimal(request.getContractDefaultRate()) : BigDecimal.ZERO);
        }

        //配置的数据
        if (agentInfo == null) {
            BigDecimal coinChildrenDefaultRate = new BigDecimal(request.getCoinDefaultRate());
            BigDecimal contractChildrenDefaultRate = new BigDecimal(request.getContractDefaultRate());
            if (agentConfig != null) {
                if (coinChildrenDefaultRate.compareTo(agentConfig.getCoinMaxRate()) > 0) {
                    return AddAgentUserResponse.newBuilder().setBasicRet(BasicRet.newBuilder().setCode(4).setMsg("agent.exceeded.maximum.limit.coin").build()).build();
                }
                if (contractChildrenDefaultRate.compareTo(agentConfig.getContractMaxRate()) > 0) {
                    return AddAgentUserResponse.newBuilder().setBasicRet(BasicRet.newBuilder().setCode(5).setMsg("agent.exceeded.maximum.limit.contract").build()).build();
                }
            }
            agentUser.setIsAgent(1);
            agentUser.setIsDel(0);
            agentUser.setStatus(1);
            agentUser.setCreated(new Date());
            agentUser.setUpdated(new Date());
            agentUser.setMobile(user.getMobile());
            agentUser.setEmail(user.getEmail());
            agentUser.setRegisterTime(new Date(user.getCreated()));
            this.agentUserMapper.insertSelective(agentUser);
        } else {
            if (agentUser.getLevel() > 1) {
                BigDecimal coinChildrenDefaultRate = new BigDecimal(request.getCoinChildrenDefaultRate());
                BigDecimal contractChildrenDefaultRate = new BigDecimal(request.getContractChildrenDefaultRate());
                log.info("userId {} coinChildrenDefaultRate {} contractChildrenDefaultRate {}", request.getUserId(), coinChildrenDefaultRate.toPlainString(), contractChildrenDefaultRate.toPlainString());
                //如果绝对值模式超出上级的配置上限 提示
                AgentUser superiorAgent = this.agentUserMapper.queryAgentUserByUserId(request.getOrgId(), agentUser.getSuperiorUserId());
                if (agentConfig != null) {
                    if (coinChildrenDefaultRate.compareTo(agentConfig.getCoinMaxRate()) > 0) {
                        return AddAgentUserResponse.newBuilder().setBasicRet(BasicRet.newBuilder().setCode(4).setMsg("agent.exceeded.maximum.limit.coin").build()).build();
                    }
                    if (contractChildrenDefaultRate.compareTo(agentConfig.getContractMaxRate()) > 0) {
                        return AddAgentUserResponse.newBuilder().setBasicRet(BasicRet.newBuilder().setCode(5).setMsg("agent.exceeded.maximum.limit.contract").build()).build();
                    }
                    if (coinChildrenDefaultRate.compareTo(superiorAgent.getChildrenDefaultRate()) > 0) {
                        return AddAgentUserResponse.newBuilder().setBasicRet(BasicRet.newBuilder().setCode(2).setMsg("agent.coin.cannot.exceed.maximum.limit").build()).build();
                    }
                    if (contractChildrenDefaultRate.compareTo(superiorAgent.getContractChildrenDefaultRate()) > 0) {
                        return AddAgentUserResponse.newBuilder().setBasicRet(BasicRet.newBuilder().setCode(3).setMsg("agent.contract.cannot.exceed.maximum.limit").build()).build();
                    }
                    String sqlLevel = String.format("%s = %s", "p" + agentUser.getLevel(), agentUser.getUserId());
                    //如果费率调高 则下级不受影响直接更新即可
                    if (coinChildrenDefaultRate.compareTo(agentInfo.getChildrenDefaultRate()) >= 0) {
                        this.agentUserMapper.updateChildrenRateBySuperiorUserId(request.getOrgId(), request.getUserId(), new BigDecimal(request.getCoinChildrenDefaultRate()));
                    } else {
                        //如果费率调低 则下级所有节点费率无效 需要重新配置
                        log.info("cleanAgentCoinRateByUserId userId {} sqlLevel {}", agentUser.getUserId(), sqlLevel);
                        this.agentUserMapper.cleanAgentCoinRateByUserId(request.getOrgId(), sqlLevel, request.getUserId());
                    }
                    //如果费率调高 则下级不受影响直接更新即可
                    if (contractChildrenDefaultRate.compareTo(agentInfo.getContractChildrenDefaultRate()) >= 0) {
                        this.agentUserMapper.updateContractChildrenRateBySuperiorUserId(request.getOrgId(), request.getUserId(), new BigDecimal(request.getContractChildrenDefaultRate()));
                    } else {
                        //如果费率调低 则下级所有节点费率无效 需要重新配置
                        log.info("cleanAgentContractRateByUserId userId {} sqlLevel {}", agentUser.getUserId(), sqlLevel);
                        this.agentUserMapper.cleanAgentContractRateByUserId(request.getOrgId(), sqlLevel, request.getUserId());
                    }
                } else {
                    if (StringUtils.isNotEmpty(request.getCoinChildrenDefaultRate()) && new BigDecimal(request.getCoinChildrenDefaultRate()).compareTo(BigDecimal.ZERO) > 0) {
                        this.agentUserMapper.updateChildrenRateBySuperiorUserId(request.getOrgId(), request.getUserId(), new BigDecimal(request.getCoinChildrenDefaultRate()));
                    }
                    if (StringUtils.isNotEmpty(request.getContractChildrenDefaultRate()) && new BigDecimal(request.getContractChildrenDefaultRate()).compareTo(BigDecimal.ZERO) > 0) {
                        this.agentUserMapper.updateContractChildrenRateBySuperiorUserId(request.getOrgId(), request.getUserId(), new BigDecimal(request.getContractChildrenDefaultRate()));
                    }
                }
            } else if (agentInfo.getLevel().equals(1)) {
                BigDecimal coinChildrenDefaultRate = new BigDecimal(request.getCoinDefaultRate());
                BigDecimal contractChildrenDefaultRate = new BigDecimal(request.getContractDefaultRate());
                if (agentConfig != null) {
                    if (coinChildrenDefaultRate.compareTo(agentConfig.getCoinMaxRate()) > 0) {
                        return AddAgentUserResponse.newBuilder().setBasicRet(BasicRet.newBuilder().setCode(4).setMsg("agent.exceeded.maximum.limit.coin").build()).build();
                    }
                    if (contractChildrenDefaultRate.compareTo(agentConfig.getContractMaxRate()) > 0) {
                        return AddAgentUserResponse.newBuilder().setBasicRet(BasicRet.newBuilder().setCode(5).setMsg("agent.exceeded.maximum.limit.contract").build()).build();
                    }
                    String sqlLevel = String.format("%s = %s", "p" + agentUser.getLevel(), agentUser.getUserId());
                    //如果费率调高 则下级不受影响直接更新即可
                    if (coinChildrenDefaultRate.compareTo(agentInfo.getChildrenDefaultRate()) >= 0) {
                        this.agentUserMapper.updateChildrenRateBySuperiorUserId(request.getOrgId(), request.getUserId(), coinChildrenDefaultRate);
                    } else {
                        log.info("cleanAgentCoinRateByUserId userId {} sqlLevel {} coinChildrenDefaultRate {} getChildrenDefaultRate {} ",
                                agentUser.getUserId(), sqlLevel, coinChildrenDefaultRate, agentInfo.getChildrenDefaultCoinFee());
                        //如果费率调低 则下级所有节点费率无效 需要重新配置
                        this.agentUserMapper.cleanAgentCoinRateByUserId(request.getOrgId(), sqlLevel, request.getUserId());
                    }
                    //如果费率调高 则下级不受影响直接更新即可
                    if (contractChildrenDefaultRate.compareTo(agentInfo.getContractChildrenDefaultRate()) >= 0) {
                        this.agentUserMapper.updateContractChildrenRateBySuperiorUserId(request.getOrgId(), request.getUserId(), contractChildrenDefaultRate);
                    } else {
                        log.info("cleanAgentContractRateByUserId userId {} sqlLevel {} contractChildrenDefaultRate {} getContractChildrenDefaultRate {} ",
                                agentUser.getUserId(), sqlLevel, contractChildrenDefaultRate, agentInfo.getContractChildrenDefaultRate());
                        //如果费率调低 则下级所有节点费率无效 需要重新配置
                        this.agentUserMapper.cleanAgentContractRateByUserId(request.getOrgId(), "p" + agentUser.getLevel(), request.getUserId());
                    }
                } else {
                    if (StringUtils.isNotEmpty(request.getCoinDefaultRate()) && new BigDecimal(request.getCoinDefaultRate()).compareTo(BigDecimal.ZERO) > 0) {
                        this.agentUserMapper.updateChildrenRateBySuperiorUserId(request.getOrgId(), request.getUserId(), new BigDecimal(request.getCoinDefaultRate()));
                    }
                    if (StringUtils.isNotEmpty(request.getContractDefaultRate()) && new BigDecimal(request.getContractDefaultRate()).compareTo(BigDecimal.ZERO) > 0) {
                        this.agentUserMapper.updateContractChildrenRateBySuperiorUserId(request.getOrgId(), request.getUserId(), new BigDecimal(request.getContractDefaultRate()));
                    }
                }
            }
            agentUser.setId(agentInfo.getId());
            agentUser.setUpdated(new Date());
            this.agentUserMapper.updateByPrimaryKeySelective(agentUser);
        }

        //升级完经纪人 迁移直线邀请数据
        try {
            moveUserInvitationData(request.getOrgId(), request.getUserId());
        } catch (Exception ex) {
            log.info("moveUserInvitationData error orgId {} userId {}", request.getOrgId(), request.getUserId());
        }
        return AddAgentUserResponse.getDefaultInstance();
    }

    /**
     * 经纪人邀请下级处理
     *
     * @param orgId
     * @param userId
     * @param agentUser
     */
    public void agentInviteUserHandle(Long orgId, Long userId, AgentUser agentUser) {
        try {
            AgentUser agentInfo = this.agentUserMapper.queryAgentUserByUserId(orgId, userId);
            //如果是经纪人下面的普通用户 并且目前非经纪人身份 并且下面没有子用户 则删掉重新建立关系即可
            if (agentInfo != null) {
                int number = countAllSubmember(agentInfo);
                if (number == 0 || number == 1) {
                    //删除当前经纪人
                    agentUserMapper.deleteByPrimaryKey(agentInfo.getId());
                } else {
                    log.info("agent user exist orgId {} userId {}", orgId, userId);
                    throw new BrokerException(BrokerErrorCode.SYSTEM_ERROR);
                }
            }

            User user = this.userMapper.getByUserId(userId);
            if (user == null) {
                log.info("user not exist orgId {} userId {}", orgId, userId);
                throw new BrokerException(BrokerErrorCode.USER_NOT_EXIST);
            }

            Account account = accountMapper.getMainAccount(orgId, userId);
            if (account == null) {
                log.info("account not exist orgId {} userId {}", orgId, userId);
                throw new BrokerException(BrokerErrorCode.ACCOUNT_NOT_EXIST);
            }

            int level = agentUser.getLevel();
            log.info("agentUser userId {} agentUserId {} level {}", userId, agentUser.getUserId(), level);
            if (level == 11) {
                throw new BrokerException(BrokerErrorCode.SYSTEM_ERROR);
            }

            Example example = new Example(AgentConfig.class);
            Example.Criteria criteria = example.createCriteria();
            criteria.andEqualTo("orgId", orgId);
            AgentConfig agentConfig = this.agentConfigMapper.selectOneByExample(example);
            AgentUser newAgentUser = AgentUser
                    .builder()
                    .orgId(orgId)
                    .userId(userId)
                    .accountId(account.getAccountId())
                    .superiorUserId(agentUser.getUserId())
                    .p1(level == 1 ? agentUser.getUserId() : agentUser.getP1())
                    .p2(level == 2 ? agentUser.getUserId() : agentUser.getP2())
                    .p3(level == 3 ? agentUser.getUserId() : agentUser.getP3())
                    .p4(level == 4 ? agentUser.getUserId() : agentUser.getP4())
                    .p5(level == 5 ? agentUser.getUserId() : agentUser.getP5())
                    .p6(level == 6 ? agentUser.getUserId() : agentUser.getP6())
                    .p7(level == 7 ? agentUser.getUserId() : agentUser.getP7())
                    .p8(level == 8 ? agentUser.getUserId() : agentUser.getP8())
                    .p9(level == 9 ? agentUser.getUserId() : agentUser.getP9())
                    .p10(level == 10 ? agentUser.getUserId() : agentUser.getP10())
                    .level(agentUser.getLevel() + 1)
                    .isAgent(0)
                    .status(agentUser.getStatus())
                    .isDel(0)
                    .mobile(StringUtils.isNotEmpty(user.getMobile()) ? user.getMobile() : "")
                    .email(StringUtils.isNotEmpty(user.getEmail()) ? user.getEmail() : "")
                    .registerTime(user.getCreated() != null ? new Date(user.getCreated()) : new Date())
                    .created(new Date())
                    .updated(new Date())
                    .isAbs(agentConfig != null ? 1 : 0)
                    .build();
            log.info("agentInviteUserHandle newAgentUser {} ", JSON.toJSONString(newAgentUser));
            this.agentUserMapper.insertSelective(newAgentUser);
            //更新当前被邀请用户级别
            int userLevel = agentUser.getLevel() + 1;
            if (userLevel <= 10) {
                String sqlLevel = String.format("%s = %s", "p" + userLevel, userId);
                this.agentUserMapper.updateUserLevelByUserId(orgId, userId, sqlLevel);
            }
            log.info("Create agent invite user success orgId {} userId {} agentUserId {}", orgId, userId, agentUser.getUserId());
        } catch (Exception ex) {
            log.error("agentInviteUserHandle fail orgId {} userId {} inviteUserId {} error {}", orgId, userId, agentUser.getUserId(), ex);
        }
    }

    /**
     * 券商后台查询经纪人列表
     *
     * @param request
     * @return
     */
    public QueryBrokerAgentListResponse queryBrokerAgentList(QueryBrokerAgentListRequest request) {
        int start = (request.getPage() - 1) * request.getLimit();
        List<AgentUser> agentUserList
                = this.agentUserMapper.queryBrokerAgentList(request.getOrgId(), request.getUserId(), request.getAgentName(), start, request.getLimit());
//        if (CollectionUtils.isEmpty(agentUserList)) {
//            AgentUser agentUser = agentUserMapper.queryAgentUserGetOne(request.getOrgId());
//            if (agentUser == null) {
//                //默认开启绝对值模式
//                agentConfigMapper.insert(AgentConfig
//                        .builder()
//                        .orgId(request.getOrgId())
//                        .build());
//            }
//            return QueryBrokerAgentListResponse.getDefaultInstance();
//        }
        return QueryBrokerAgentListResponse.newBuilder().addAllAgentInfo(buildBrokerAgentList(agentUserList)).build();
    }

    /**
     * 券商后台经纪人下面用户列表
     *
     * @param request
     * @return
     */
    public QueryBrokerUserListResponse queryBrokerUserList(QueryBrokerUserListRequest request) {
        int start = (request.getPage() - 1) * request.getLimit();
        List<AgentUser> agentUserList = this.agentUserMapper
                .queryBrokerUserList(request.getOrgId(), request.getUserId(), request.getEmail(), request.getMobile(), request.getAgentName(), start, request.getLimit());

        return QueryBrokerUserListResponse.newBuilder().addAllUserInfo(buildUserInfoList(agentUserList)).build();
    }

    /**
     * 券商后台查询经纪人列表 数据构建
     *
     * @param agentUserList
     * @return
     */
    private List<QueryBrokerAgentListResponse.AgentInfo> buildBrokerAgentList(List<AgentUser> agentUserList) {
        List<QueryBrokerAgentListResponse.AgentInfo> agentInfoList = new ArrayList<>();
        if (CollectionUtils.isEmpty(agentUserList)) {
            return new ArrayList<>();
        }

        agentUserList.forEach(agentUser -> {
            AgentUser superiorAgent = this.agentUserMapper.queryAgentUserByUserId(agentUser.getOrgId(), agentUser.getSuperiorUserId());
            Integer userCount = countSubmember(agentUser);
            if (userCount >= 1) {
                userCount = userCount - 1;
            }
            agentInfoList.add(QueryBrokerAgentListResponse.AgentInfo
                    .newBuilder()
                    .setId(agentUser.getId())
                    .setAgentName(agentUser.getAgentName())
                    .setUserId(agentUser.getUserId())
                    .setMobile(agentUser.getMobile())
                    .setEmail(agentUser.getEmail())
                    .setLevel(agentUser.getLevel())
                    .setLeader(agentUser.getLeader())
                    .setLeaderMobile(agentUser.getLeaderMobile())
                    .setSuperiorName(superiorAgent != null ? superiorAgent.getAgentName() : "")
                    .setPeopleNumber(String.valueOf(userCount))
                    .setMark(agentUser.getMark())
                    .setCoinRate(agentUser.getRate().stripTrailingZeros().toPlainString())
                    .setContractRate(agentUser.getContractRate().stripTrailingZeros().toPlainString())
                    .setLowLevelCoinRate(agentUser.getChildrenDefaultRate().stripTrailingZeros().toPlainString())
                    .setLowLevelContractRate(agentUser.getContractChildrenDefaultRate().stripTrailingZeros().toPlainString())
                    .setTime(new DateTime(agentUser.getCreated().getTime()).toString("yyyy-MM-dd HH:mm:ss"))
                    .setStatus(String.valueOf(agentUser.getStatus()))
                    .build());
        });
        return agentInfoList;
    }

    private List<QueryBrokerUserListResponse.UserInfo> buildUserInfoList(List<AgentUser> agentUserList) {
        List<QueryBrokerUserListResponse.UserInfo> userInfoList = new ArrayList<>();
        if (CollectionUtils.isEmpty(agentUserList)) {
            return new ArrayList<>();
        }

        agentUserList.forEach(agentUser -> {
            //获取用户的kyc真实姓名 和国籍
            //获取父节点数据

            UserVerify userVerify = this.userVerifyMapper.getKycPassUserByUserId(agentUser.getUserId());
            AgentUser superiorAgent = this.agentUserMapper.queryAgentUserByUserId(agentUser.getOrgId(), agentUser.getSuperiorUserId());

            String countryName = "";
            if (userVerify != null) {
                Country country = countryMapper.queryCountryById(userVerify.getNationality());
                if (country != null && StringUtils.isNotEmpty(country.getCountryName())) {
                    countryName = country.getCountryName();
                }
            }

            String firstName = "";
            String secondName = "";
            if (userVerify != null) {
                UserVerify newUserVerify = UserVerify.decrypt(userVerify);
                firstName = StringUtils.isNotEmpty(newUserVerify.getFirstName()) ? newUserVerify.getFirstName() : "";
                secondName = StringUtils.isNotEmpty(newUserVerify.getSecondName()) ? newUserVerify.getSecondName() : "";
            }
            userInfoList.add(QueryBrokerUserListResponse.UserInfo
                    .newBuilder()
                    .setMobile(agentUser.getMobile())
                    .setEmail(agentUser.getEmail())
                    .setAgentName(agentUser.getAgentName())
                    .setUserId(agentUser.getUserId())
                    .setUserName(firstName + secondName)
                    .setCountry(countryName)
                    .setAgentName(superiorAgent != null ? superiorAgent.getAgentName() : "")
                    .setAgentUserId(superiorAgent != null ? String.valueOf(superiorAgent.getUserId()) : "")
                    .setAgentLevel(superiorAgent != null ? String.valueOf(superiorAgent.getLevel()) : "")
                    .setRegisterTime(new DateTime(agentUser.getRegisterTime().getTime()).toString("yyyy-MM-dd HH:mm:ss"))
                    .setStatus(String.valueOf(agentUser.getStatus()))
                    .build());
        });

        return userInfoList;
    }

    /**
     * 查询下一级的全部用户信息 1.判断当前用户是否为经纪人 2.如果level为11 没有下一级 3.查询当前用户等级，并查询所有此等级的父id为此uid的用户
     */
    public QueryAgentUserResponse queryAgentUser(QueryAgentUserRequest request) {
        AgentUser agentUser = agentUserMapper.queryAgentUserByUserId(request.getHeader().getOrgId(), request.getHeader().getUserId());
        if (Objects.nonNull(agentUser) && agentUser.getIsDel().equals(0) && agentUser.getIsAgent().equals(1)) {
            List<AgentUser> submemberList;
            if (StringUtils.isNotEmpty(request.getUserContact())) {
                submemberList = seachAgentUser(request.getUserContact(), agentUser, 0);
            } else {
                submemberList = querySubmember(agentUser, AgentType.ALL, null, request.getFromId(), request.getEndId(), request.getLimit());
            }
            List<AgentUserInfo> agentUserInfoList = submemberList.stream()
                    .map(m -> toAgentUserInfo(m, request.getHeader().getLanguage()))
                    .collect(Collectors.toList());
            return QueryAgentUserResponse.newBuilder()
                    .addAllUserInfo(agentUserInfoList)
                    .build();
        }
        return QueryAgentUserResponse.getDefaultInstance();
    }

    private List<AgentUser> seachAgentUser(String userContact, AgentUser parentAgentUser, int isAgent) {
        return seachAgentUser(null, userContact, null, parentAgentUser, isAgent);
    }

    private List<AgentUser> seachAgentUser(Long userId, String agentName, AgentUser parentAgentUser, int isAgent) {
        return seachAgentUser(userId, null, agentName, parentAgentUser, isAgent);
    }

    private List<AgentUser> seachAgentUser(Long userId, String userContact, String agentName, AgentUser parentAgentUser, int isAgent) {
        Long orgId = parentAgentUser.getOrgId();
        Example example = Example.builder(AgentUser.class).build();
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("orgId", orgId);
        criteria.andEqualTo("superiorUserId", parentAgentUser.getUserId());
        criteria.andEqualTo("isDel", 0);
        criteria.andEqualTo("status", 1);
        if (isAgent == 1) {
            criteria.andEqualTo("isAgent", isAgent);
        }
        if (Objects.nonNull(userId) && userId > 0) {
            criteria.andEqualTo("userId", userId);
        }
        if (StringUtils.isNotEmpty(userContact)) {
            // 默认userId为0，这样条件未命中时则查询不到用户
            userId = 0L;
            // userContact 为 uid、邮箱、手机号
            if (userContact.indexOf("@") > -1) {
                // 通过邮箱查
                User user = userMapper.getByEmail(orgId, userContact);
                if (Objects.nonNull(user)) {
                    userId = user.getUserId();
                }
            } else {
                Long contactUid = Long.parseLong(userContact);
                // 通过uid查
                AgentUser agentUser = agentUserMapper.queryAgentUserByUserId(orgId, contactUid);
                if (Objects.nonNull(agentUser)) {
                    userId = agentUser.getUserId();
                } else {
                    // 通过手机号查
                    User user = userMapper.getByMobileAndOrgId(orgId, userContact);
                    if (Objects.nonNull(user)) {
                        userId = user.getUserId();
                    }
                }
            }
            criteria.andEqualTo("userId", userId);
        }
        if (StringUtils.isNotEmpty(agentName)) {
            criteria.andEqualTo("agentName", agentName);
        }
        List<AgentUser> agentUsers = agentUserMapper.selectByExample(example);
        return agentUsers;
    }

    private AgentUserInfo toAgentUserInfo(AgentUser agentUser, String language) {
        UserVerify userVerify = this.userVerifyMapper.getKycPassUserByUserId(agentUser.getUserId());
        String countryName = "";
        String userName = "";
        if (userVerify != null) {
            UserVerify newUserVerify = UserVerify.decrypt(userVerify);
            if (StringUtils.isNotEmpty(newUserVerify.getFirstName())) {
                userName = newUserVerify.getFirstName();
            }
            if (StringUtils.isNotEmpty(newUserVerify.getSecondName())) {
                userName = userName + newUserVerify.getSecondName();
            }
            countryName = Strings.nullToEmpty(basicService.getCountryName(userVerify.getNationality(), language));
        }
        return AgentUserInfo.newBuilder()
                .setId(agentUser.getId())
                .setUserId(agentUser.getUserId())
                .setUserName(userName)
                .setAgentName(Strings.nullToEmpty(agentUser.getAgentName()))
                .setMobile(Strings.nullToEmpty(agentUser.getMobile()))
                .setEmail(Strings.nullToEmpty(agentUser.getEmail()))
                .setRegisterTime(agentUser.getRegisterTime().getTime())
                .setCountry(countryName)
                .setIsAgent(agentUser.getIsAgent())
                .build();
    }

    private List<AgentUser> querySubmember(AgentUser agentUser, AgentType agentType, Long userId, Long fromId, Long endId, Integer limit) {
        Example example = Example.builder(AgentUser.class).orderByDesc("id").build();
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("orgId", agentUser.getOrgId());
        criteria.andEqualTo("superiorUserId", agentUser.getUserId());
        criteria.andEqualTo("isDel", 0);
        criteria.andEqualTo("status", 1);

        // 如果指定用户id，就按照用户id进行查找
        if (Objects.nonNull(userId) && userId > 0) {
            criteria.andEqualTo("userId", userId);
        } else {
            if (Objects.nonNull(fromId) && fromId > 0) {
                criteria.andLessThan("id", fromId);
            } else if (Objects.nonNull(endId) && endId > 0) {
                criteria.andGreaterThan("id", endId);
            }
            // agentType 为 all时 无需过滤
            if (agentType.equals(AgentType.AGENT)) {
                criteria.andEqualTo("isAgent", 1);
            } else if (agentType.equals(AgentType.USER)) {
                criteria.andEqualTo("isAgent", 0);
            }
            criteria.andNotEqualTo("userId", agentUser.getUserId());
            log.info("querySubmember superiorUserId {}", agentUser.getUserId());
        }
        // 翻页信息
        if (Objects.isNull(limit) || limit > 100) {
            limit = 100;
        }
        return agentUserMapper.selectByExampleAndRowBounds(example, new RowBounds(0, limit));
    }

    /**
     * 用户升级为经纪人 1.判断当前用户是否为经纪人 2.当前用户是否为升级用户的上级经纪人 3.同步当前经纪人上级信息给当前用户 4.全部上级用户"发展会员数" +1
     */
    public AgentUserUpgradeResponse agentUserUpgrade(AgentUserUpgradeRequest request) {
        Long agentUserId = request.getHeader().getUserId();
        AgentUser agentUser = agentUserMapper.queryAgentUserByUserId(request.getHeader().getOrgId(), agentUserId);
        if (request.getHeader().getUserId() == request.getUserId()) {
            throw new BrokerException(BrokerErrorCode.OAUTH_REQUEST_HAS_NO_PERMISSION);
        }
        if (Objects.nonNull(agentUser) && agentUser.getIsDel().equals(0) && agentUser.getIsAgent().equals(1)) {
            AgentUser memberUser = agentUserMapper.queryAgentUserByUserId(request.getHeader().getOrgId(), request.getUserId());
            if (Objects.isNull(memberUser)) {
                throw new BrokerException(BrokerErrorCode.USER_NOT_EXIST);
            }
            if (memberUser.getLevel() > 10) {
                throw new BrokerException(BrokerErrorCode.AGENT_LEVEL_MAX_LIMIT);
            }
            if (agentUserId.equals(memberUser.getSuperiorUserId())) {
                BigDecimal rate = StringUtils.isNotEmpty(request.getRate()) ? new BigDecimal(request.getRate()) : memberUser.getRate();
                if (rate.compareTo(BigDecimal.ZERO) < 0) {
                    rate = BigDecimal.ZERO;
                }
                BigDecimal contractRate = StringUtils.isNotEmpty(request.getContractRate()) ? new BigDecimal(request.getContractRate()) : memberUser.getContractRate();
                if (contractRate.compareTo(BigDecimal.ZERO) < 0) {
                    contractRate = BigDecimal.ZERO;
                }
                AgentUser superiorAgent = this.agentUserMapper.queryAgentUserByUserId(request.getHeader().getOrgId(), memberUser.getSuperiorUserId());
                //如果绝对值模式超出上级的配置上限 提示
                AgentConfig agentConfig = getAgentConfig(request.getHeader().getOrgId());
                if (agentConfig != null) {
                    if (rate.compareTo(agentConfig.getCoinMaxRate()) > 0) {
                        throw new BrokerException(BrokerErrorCode.CANNOT_EXCEED_MAXIMUM_LIMIT);
                    }
                    if (contractRate.compareTo(agentConfig.getContractMaxRate()) > 0) {
                        throw new BrokerException(BrokerErrorCode.CANNOT_EXCEED_MAXIMUM_LIMIT);
                    }

                    if (rate.compareTo(superiorAgent.getChildrenDefaultRate()) > 0) {
                        throw new BrokerException(BrokerErrorCode.CANNOT_EXCEED_MAXIMUM_LIMIT);
                    }
                    if (contractRate.compareTo(superiorAgent.getContractChildrenDefaultRate()) > 0) {
                        throw new BrokerException(BrokerErrorCode.CANNOT_EXCEED_MAXIMUM_LIMIT);
                    }

                    //如果费率调低则不允许
                    if (rate.compareTo(memberUser.getChildrenDefaultRate()) < 0) {
                        throw new BrokerException(BrokerErrorCode.AGENT_CONTACT_EXCHANGE_CUSTOMER);
                    }
                    //如果费率调低则不允许
                    if (contractRate.compareTo(memberUser.getContractChildrenDefaultRate()) < 0) {
                        throw new BrokerException(BrokerErrorCode.AGENT_CONTACT_EXCHANGE_CUSTOMER);
                    }
                }
                memberUser.setAgentName(request.getAgentName());
                memberUser.setLeader(request.getLeader());
                memberUser.setRate(superiorAgent.getChildrenDefaultRate());
                memberUser.setContractRate(superiorAgent.getContractChildrenDefaultRate());
                memberUser.setChildrenDefaultRate(rate);
                memberUser.setContractChildrenDefaultRate(contractRate);
                memberUser.setLeaderMobile(request.getLeaderMobile());
                memberUser.setMark(request.getMark());
                memberUser.setIsAgent(1);
                agentUserMapper.updateByPrimaryKey(memberUser);
                if (rate.compareTo(BigDecimal.ZERO) > 0) {
                    if (memberUser.getContractRate().compareTo(rate) != 0) {
                        this.agentUserMapper.updateChildrenRateBySuperiorUserId(request.getHeader().getOrgId(), request.getUserId(), rate);
                    }
                }
                if (contractRate.compareTo(BigDecimal.ZERO) > 0) {
                    if (memberUser.getContractRate().compareTo(contractRate) != 0) {
                        this.agentUserMapper.updateContractChildrenRateBySuperiorUserId(request.getHeader().getOrgId(), request.getUserId(), contractRate);
                    }
                }

                //升级完经纪人 迁移直线邀请数据
                try {
                    moveUserInvitationData(memberUser.getOrgId(), memberUser.getUserId());
                } catch (Exception ex) {
                    log.info("moveUserInvitationData error orgId {} userId {}", request.getHeader().getOrgId(), request.getUserId());
                }

                return AgentUserUpgradeResponse.getDefaultInstance();
            }
        }
        throw new BrokerException(BrokerErrorCode.OAUTH_REQUEST_HAS_NO_PERMISSION);
    }

    /**
     * 经纪人基础信息
     */
    public AgentInfoResponse agentInfo(AgentInfoRequest request) {
        AgentInfoResponse.Builder builder = AgentInfoResponse.newBuilder();
        Long agentUserId = request.getHeader().getUserId();
        AgentUser agentUser = agentUserMapper.queryAgentUserByUserId(request.getHeader().getOrgId(), agentUserId);
        if (Objects.nonNull(agentUser) && agentUser.getIsDel().equals(0) && agentUser.getIsAgent().equals(1)) {
            builder.setCoinRate(agentUser.getRate().toPlainString());
            builder.setChildrenDefaultRate(agentUser.getChildrenDefaultRate().toPlainString());
            builder.setContractRate(agentUser.getContractRate().toPlainString());
            builder.setContractChildrenDefaultRate(agentUser.getContractChildrenDefaultRate().toPlainString());
            builder.setIsAbs(agentUser.getIsAbs());
            return builder.build();
        }
        throw new BrokerException(BrokerErrorCode.OAUTH_REQUEST_HAS_NO_PERMISSION);
    }

    /**
     * 查询下一级的全部用户信息 1.判断当前用户是否为经纪人 2.如果level为11 没有下一级 3.查询当前用户等级，并查询所有此等级的父id为此uid的经纪人
     */
    public QueryAgentBrokerResponse queryAgentBroker(QueryAgentBrokerRequest request) {
        AgentUser agentUser = agentUserMapper.queryAgentUserByUserId(request.getHeader().getOrgId(), request.getHeader().getUserId());
        if (Objects.nonNull(agentUser) && agentUser.getIsDel().equals(0) && agentUser.getIsAgent().equals(1)) {
            Integer allSubMemberCount = countSubAgentMember(agentUser);
            List<AgentUser> submemberList;
            if (Objects.nonNull(request.getUserId()) && request.getUserId() > 0 || StringUtils.isNotEmpty(request.getUserName())) {
                submemberList = seachAgentUser(request.getUserId(), request.getUserName(), agentUser, 1);
            } else {
                submemberList = querySubmember(agentUser, AgentType.AGENT, null, request.getFromId(), request.getEndId(), request.getLimit());
            }
            List<AgentBrokerInfo> agentUserInfoList = submemberList.stream()
                    .map(m -> toAgentBrokerInfo(m, agentUser.getUserId(), agentUser.getAgentName()))
                    .collect(Collectors.toList());
            return QueryAgentBrokerResponse.newBuilder()
                    .addAllBrokerInfo(agentUserInfoList)
                    .setAllSubMemberCount(allSubMemberCount)
                    .build();
        }
        return QueryAgentBrokerResponse.getDefaultInstance();
    }

    private AgentBrokerInfo toAgentBrokerInfo(AgentUser agentUser, Long parentAgentUserId, String parentAgentUserName) {
        Integer countSubmember = countSubmember(agentUser);
        if (countSubmember >= 1) {
            countSubmember = countSubmember - 1;
        }
        return AgentBrokerInfo.newBuilder()
                .setId(agentUser.getId())
                .setAgentUserId(agentUser.getUserId())
                .setAgentName(agentUser.getAgentName())
                .setLevel(agentUser.getLevel())
                .setLeader(agentUser.getLeader())
                .setLeaderMobile(agentUser.getLeaderMobile())
                .setRate(agentUser.getChildrenDefaultRate().toPlainString())
                .setContractRate(agentUser.getContractChildrenDefaultRate().toPlainString())
                .setParentAgentUserId(parentAgentUserId)
                .setParentAgentName(parentAgentUserName)
                .setMark(agentUser.getMark())
                .setSubMemberCount(countSubmember)
                .setMobile(agentUser.getMobile())
                .setEmail(agentUser.getEmail())
                .setRegisterTime(agentUser.getRegisterTime().getTime())
                .build();
    }

    /**
     * 更新经纪人信息 1.判断当前用户是否为经纪人 2.当前用户是否为升级用户的上级经纪人
     */
    public UpdateBrokerResponse updateBroker(UpdateBrokerRequest request) {
        Long agentUserId = request.getHeader().getUserId();
        AgentUser agentUser = agentUserMapper.queryAgentUserByUserId(request.getHeader().getOrgId(), agentUserId);
        if (request.getHeader().getUserId() == request.getUserId()) {
            throw new BrokerException(BrokerErrorCode.OAUTH_REQUEST_HAS_NO_PERMISSION);
        }
        if (Objects.nonNull(agentUser) && agentUser.getIsDel().equals(0) && agentUser.getIsAgent().equals(1)) {
            AgentUser memberUser = agentUserMapper.queryAgentUserByUserId(request.getHeader().getOrgId(), request.getUserId());
            if (Objects.isNull(memberUser)) {
                throw new BrokerException(BrokerErrorCode.USER_NOT_EXIST);
            }
            if (agentUserId.equals(memberUser.getSuperiorUserId())) {
                AgentUser superiorAgent = this.agentUserMapper.queryAgentUserByUserId(request.getHeader().getOrgId(), memberUser.getSuperiorUserId());
                BigDecimal rate = StringUtils.isNotEmpty(request.getRate()) ? new BigDecimal(request.getRate()) : memberUser.getRate();
                if (rate.compareTo(BigDecimal.ZERO) < 0) {
                    rate = BigDecimal.ZERO;
                }
                BigDecimal contractRate = StringUtils.isNotEmpty(request.getContractRate()) ? new BigDecimal(request.getContractRate()) : memberUser.getContractRate();
                if (contractRate.compareTo(BigDecimal.ZERO) < 0) {
                    contractRate = BigDecimal.ZERO;
                }
                //如果绝对值模式超出上级的配置上限 提示
                AgentConfig agentConfig = getAgentConfig(request.getHeader().getOrgId());
                if (agentConfig != null) {
                    if (rate.compareTo(agentConfig.getCoinMaxRate()) > 0) {
                        throw new BrokerException(BrokerErrorCode.CANNOT_EXCEED_MAXIMUM_LIMIT);
                    }
                    if (contractRate.compareTo(agentConfig.getContractMaxRate()) > 0) {
                        throw new BrokerException(BrokerErrorCode.CANNOT_EXCEED_MAXIMUM_LIMIT);
                    }
                    if (rate.compareTo(superiorAgent.getChildrenDefaultRate()) > 0) {
                        throw new BrokerException(BrokerErrorCode.CANNOT_EXCEED_MAXIMUM_LIMIT);
                    }
                    if (contractRate.compareTo(superiorAgent.getContractChildrenDefaultRate()) > 0) {
                        throw new BrokerException(BrokerErrorCode.CANNOT_EXCEED_MAXIMUM_LIMIT);
                    }
                    //如果费率调低则不允许
                    if (rate.compareTo(memberUser.getChildrenDefaultRate()) < 0) {
                        throw new BrokerException(BrokerErrorCode.AGENT_CONTACT_EXCHANGE_CUSTOMER);
                    }
                    //如果费率调低则不允许
                    if (contractRate.compareTo(memberUser.getContractChildrenDefaultRate()) < 0) {
                        throw new BrokerException(BrokerErrorCode.AGENT_CONTACT_EXCHANGE_CUSTOMER);
                    }
                }

                memberUser.setAgentName(request.getAgentName());
                memberUser.setLeader(request.getLeader());
                memberUser.setRate(superiorAgent.getChildrenDefaultRate());
                memberUser.setContractRate(superiorAgent.getContractChildrenDefaultRate());
                memberUser.setChildrenDefaultRate(rate);
                memberUser.setContractChildrenDefaultRate(contractRate);
                memberUser.setLeaderMobile(request.getLeaderMobile());
                memberUser.setMark(request.getMark());
                agentUserMapper.updateByPrimaryKey(memberUser);

                if (rate.compareTo(BigDecimal.ZERO) > 0) {
                    if (memberUser.getContractRate().compareTo(rate) != 0) {
                        this.agentUserMapper.updateChildrenRateBySuperiorUserId(request.getHeader().getOrgId(), request.getUserId(), rate);
                    }
                }

                if (contractRate.compareTo(BigDecimal.ZERO) > 0) {
                    if (memberUser.getContractRate().compareTo(contractRate) != 0) {
                        this.agentUserMapper.updateContractChildrenRateBySuperiorUserId(request.getHeader().getOrgId(), request.getUserId(), contractRate);
                    }
                }
                return UpdateBrokerResponse.getDefaultInstance();
            }
        }
        throw new BrokerException(BrokerErrorCode.OAUTH_REQUEST_HAS_NO_PERMISSION);
    }

    public void historyInvitationHandel(Long orgId, Integer level) {
        List<AgentUser> agentUserList = this.agentUserMapper.queryAgentUserListByLevel(orgId, level);
        if (CollectionUtils.isEmpty(agentUserList)) {
            return;
        }
        agentUserList.forEach(agentUser -> {
            List<InviteRelation> inviteRelationList
                    = inviteRelationMapper.getInviteRelationsByUserId(agentUser.getOrgId(), agentUser.getUserId());

            if (CollectionUtils.isEmpty(inviteRelationList)) {
                return;
            }

            inviteRelationList.forEach(inviteRelation -> {
                AgentUser agentInfo = this.agentUserMapper.queryAgentUserByUserId(inviteRelation.getOrgId(), inviteRelation.getInvitedId());
                if (agentInfo == null) {
                    try {
                        agentInviteUserHandle(orgId, inviteRelation.getInvitedId(), agentUser);
                        this.inviteRelationMapper.updateInviteRelationStatusByUserId(inviteRelation.getInvitedId());
                    } catch (Exception ex) {
                        log.info("historyInvitationHandel fail orgId {} userId {} ex {}", orgId, inviteRelation.getInvitedId(), ex);
                    }
                }
            });
        });
    }

    public void moveUserInvitationData(Long orgId, Long userId) {
        try {
            AgentUser agentUser = this.agentUserMapper.queryAgentUserByUserId(orgId, userId);
            if (agentUser == null || agentUser.getIsAgent().equals(0)) {
                return;
            }
            List<InviteRelation> inviteRelationList
                    = inviteRelationMapper.getInviteRelationsByUserId(agentUser.getOrgId(), agentUser.getUserId());

            if (CollectionUtils.isEmpty(inviteRelationList)) {
                return;
            }

            inviteRelationList.forEach(inviteRelation -> {
                AgentUser agentInfo = this.agentUserMapper.queryAgentUserByUserId(inviteRelation.getOrgId(), inviteRelation.getInvitedId());
                if (agentInfo == null) {
                    try {
                        agentInviteUserHandle(orgId, inviteRelation.getInvitedId(), agentUser);
                        this.inviteRelationMapper.updateInviteRelationStatusByUserId(inviteRelation.getInvitedId());
                    } catch (Exception ex) {
                        log.info("historyInvitationHandel fail orgId {} userId {} ex {}", orgId, inviteRelation.getInvitedId(), ex);
                    }
                }
            });
        } catch (Exception ex) {
            log.info("moveUserInvitationData fail orgId {} userId {}", orgId, userId);
        }
    }

    @Transactional(isolation = Isolation.READ_COMMITTED, propagation = Propagation.REQUIRED, rollbackFor = Throwable.class)
    public CancelAgentUserResponse cancelAgentUser(Long orgId, Long userId) {
        AgentUser agentInfo = this.agentUserMapper.queryAgentUserByUserId(orgId, userId);
        if (agentInfo == null || agentInfo.getIsAgent().equals(0) || agentInfo.getStatus().equals(0)) {
            return CancelAgentUserResponse.newBuilder().setBasicRet(BasicRet.newBuilder().setCode(1).setMsg("agent.cancel.user.status.not.allow").build()).build();
        }

        try {
            int userLevel = agentInfo.getLevel();
            String sqlLevel = String.format("%s = %s", "p" + userLevel, userId);
            //解绑经纪人 则下面所有节点都变成无效 并且只有无效的状态下才可以去重新分配
            //无效的经纪人关系 则不进行经纪人的返佣 普通返佣则恢复正常
            this.agentUserMapper.cancelAgentUserByLevel(orgId, sqlLevel);
            return CancelAgentUserResponse.newBuilder().setBasicRet(BasicRet.newBuilder().setCode(0).setMsg("").build()).build();
        } catch (Exception ex) {
            log.info("cancelAgentUser fail orgId {} userId {} error {}", orgId, userId, ex);
            return CancelAgentUserResponse.newBuilder().setBasicRet(BasicRet.newBuilder().setCode(1).setMsg("agent.cancel.user.not.fail").build()).build();
        }
    }

    @Transactional(isolation = Isolation.READ_COMMITTED, propagation = Propagation.REQUIRED, rollbackFor = Throwable.class)
    public RebindAgentUserResponse rebindAgentUser(Long orgId, Long userId, Long targetUserId) {
        AgentUser agentInfo = this.agentUserMapper.queryAgentUserByUserId(orgId, userId);
        if (agentInfo == null) {
            return RebindAgentUserResponse.newBuilder().setBasicRet(BasicRet.newBuilder().setCode(1).setMsg("agent.user.not.find").build()).build();
        }

        if (agentInfo.getStatus().equals(1) || agentInfo.getIsAgent().equals(0)) {
            return RebindAgentUserResponse.newBuilder().setBasicRet(BasicRet.newBuilder().setCode(1).setMsg("agent.status.not.modify.rebind").build()).build();
        }

        AgentUser targetAgentInfo = this.agentUserMapper.queryAgentUserByUserId(orgId, targetUserId);
        if (targetAgentInfo == null || targetAgentInfo.getIsAgent().equals(0)) {
            return RebindAgentUserResponse.newBuilder().setBasicRet(BasicRet.newBuilder().setCode(1).setMsg("target.agent.user.status.not.allow.rebind").build()).build();
        }

        String sqlLevel = String.format("%s = %s", "p" + agentInfo.getLevel(), userId);
        //如果UID相同则只能唤醒操作
        if (userId.equals(targetUserId)) {
            //唤醒节点操作
            this.agentUserMapper.wakeUpAgentUserByLevel(orgId, sqlLevel);
            return RebindAgentUserResponse.newBuilder().setBasicRet(BasicRet.newBuilder().setCode(0).build()).build();
        }

        if (agentInfo.getLevel() <= targetAgentInfo.getLevel()) {
            return RebindAgentUserResponse.newBuilder().setBasicRet(BasicRet.newBuilder().setCode(1).setMsg("rebind.level.error").build()).build();
        }

        List<AgentUser> agentUserList = this.agentUserMapper.lockAgentUserByLevel(orgId, sqlLevel);

        if (CollectionUtils.isEmpty(agentUserList)) {
            return RebindAgentUserResponse.newBuilder().setBasicRet(BasicRet.newBuilder().setCode(1).setMsg("agent.user.list.is.null").build()).build();
        }

        try {
            agentUserList.forEach(user -> {
                LinkedHashMap<Integer, Long> linkedHashMap = Maps.newLinkedHashMap();
                int pLevel = agentInfo.getLevel();
                int cLevel = user.getLevel();

                if (pLevel == cLevel) {
                    linkedHashMap.put(pLevel, getLevelUser(user, "p" + pLevel));
                } else {
                    for (int i = pLevel; i <= cLevel; i++) {
                        linkedHashMap.put(i, getLevelUser(user, "p" + i));
                    }
                }
                user.setP1(targetAgentInfo.getP1());
                user.setP2(targetAgentInfo.getP2());
                user.setP3(targetAgentInfo.getP3());
                user.setP4(targetAgentInfo.getP4());
                user.setP5(targetAgentInfo.getP5());
                user.setP6(targetAgentInfo.getP6());
                user.setP7(targetAgentInfo.getP7());
                user.setP8(targetAgentInfo.getP8());
                user.setP9(targetAgentInfo.getP9());
                user.setP10(targetAgentInfo.getP10());
                int targetLevel = targetAgentInfo.getLevel() + 1;
                AgentUser agentUser = buildMoveAgentUser(user, targetLevel, linkedHashMap);
                if (agentUser != null) {
                    this.agentUserMapper.updateByPrimaryKey(agentUser);
                }
            });
            return RebindAgentUserResponse.newBuilder().setBasicRet(BasicRet.newBuilder().setCode(0).build()).build();
        } catch (Exception ex) {
            log.info("rebindAgentUser fail orgId {} userId {} targetUserId {} error {}", orgId, userId, targetUserId, ex);
            return RebindAgentUserResponse.newBuilder().setBasicRet(BasicRet.newBuilder().setCode(1).setMsg("agent.rebind.fail").build()).build();
        }
    }

    public AgentUser buildMoveAgentUser(AgentUser agentUser, int level, LinkedHashMap<Integer, Long> linkedHashMap) {
        int agentLevel = 0;
        for (int i = level; i < 12; i++) {
            if (linkedHashMap.size() == 0) {
                continue;
            }
            String userLevel = "p" + i;
            Integer key = linkedHashMap.keySet().stream().findFirst().orElse(0);
            Long value = linkedHashMap.get(key);
            switch (userLevel) {
                case "p1":
                    agentLevel = 1;
                    agentUser.setP1(value != null && value > 0 ? value : agentUser.getUserId());
                    break;
                case "p2":
                    agentLevel = 2;
                    agentUser.setP2(value != null && value > 0 ? value : agentUser.getUserId());
                    break;
                case "p3":
                    agentLevel = 3;
                    agentUser.setP3(value != null && value > 0 ? value : agentUser.getUserId());
                    break;
                case "p4":
                    agentLevel = 4;
                    agentUser.setP4(value != null && value > 0 ? value : agentUser.getUserId());
                    break;
                case "p5":
                    agentLevel = 5;
                    agentUser.setP5(value != null && value > 0 ? value : agentUser.getUserId());
                    break;
                case "p6":
                    agentLevel = 6;
                    agentUser.setP6(value != null && value > 0 ? value : agentUser.getUserId());
                    break;
                case "p7":
                    agentLevel = 7;
                    agentUser.setP7(value != null && value > 0 ? value : agentUser.getUserId());
                    break;
                case "p8":
                    agentLevel = 8;
                    agentUser.setP8(value != null && value > 0 ? value : agentUser.getUserId());
                    break;
                case "p9":
                    agentLevel = 9;
                    agentUser.setP9(value != null && value > 0 ? value : agentUser.getUserId());
                    break;
                case "p10":
                    agentLevel = 10;
                    agentUser.setP10(value != null && value > 0 ? value : agentUser.getUserId());
                    break;
                case "p11":
                    agentLevel = 11;
                    break;
            }
            linkedHashMap.remove(key);
        }
        agentUser.setLevel(agentLevel);
        agentUser.setStatus(1);
        if (agentLevel == 1) {
            agentUser.setSuperiorUserId(0L);
        } else if (agentLevel == 11) {
            agentUser.setSuperiorUserId(getLevelUser(agentUser, "p10"));
        } else {
            agentUser.setSuperiorUserId(getLevelUser(agentUser, "p" + (agentLevel - 1)));
        }

        if (agentUser.getSuperiorUserId() != null && agentUser.getSuperiorUserId() > 0) {
            //获取上级的分佣比例和配置 计算本身应该获得的默认分佣比例 进行更新
            AgentUser superiorUser = this.agentUserMapper.queryAgentUserByUserId(agentUser.getOrgId(), agentUser.getSuperiorUserId());
            if (superiorUser == null) {
                //coin
                if (superiorUser.getChildrenDefaultRate() != null && superiorUser.getChildrenDefaultRate().compareTo(BigDecimal.ZERO) > 0) {
                    BigDecimal childrenDefaultRate = superiorUser.getRate().multiply(superiorUser.getChildrenDefaultRate()).setScale(8, RoundingMode.DOWN);
                    agentUser.setRate(childrenDefaultRate);
                }

                //contract
                if (superiorUser.getContractChildrenDefaultRate() != null && superiorUser.getContractChildrenDefaultRate().compareTo(BigDecimal.ZERO) > 0) {
                    BigDecimal contractChildrenDefaultRate = superiorUser.getContractRate().multiply(superiorUser.getContractChildrenDefaultRate()).setScale(8, RoundingMode.DOWN);
                    agentUser.setContractRate(contractChildrenDefaultRate);
                }
            }
        }
        return agentUser;
    }

    public long getLevelUser(AgentUser agentUser, String level) {
        switch (level) {
            case "p1":
                return agentUser.getP1();
            case "p2":
                return agentUser.getP2();
            case "p3":
                return agentUser.getP3();
            case "p4":
                return agentUser.getP4();
            case "p5":
                return agentUser.getP5();
            case "p6":
                return agentUser.getP6();
            case "p7":
                return agentUser.getP7();
            case "p8":
                return agentUser.getP8();
            case "p9":
                return agentUser.getP9();
            case "p10":
                return agentUser.getP10();
        }
        return 0;
    }

    /**
     * 经纪人分佣明细列表
     *
     * @param orgId
     * @param userId
     * @param targetUserId
     * @param tokenId
     * @param fromId
     * @param endId
     * @param limit
     * @param isAdmin
     * @param startTime
     * @param endTime
     * @return
     */
    public QueryAgentCommissionListResponse queryAgentCommissionList(Long orgId,
                                                                     Long userId,
                                                                     Long targetUserId,
                                                                     String tokenId,
                                                                     Integer fromId,
                                                                     Integer endId,
                                                                     Integer limit,
                                                                     Integer isAdmin,
                                                                     String startTime,
                                                                     String endTime) {
        if (orgId == null || orgId.equals(0l)) {
            return QueryAgentCommissionListResponse.getDefaultInstance();
        }

        //校验查询用户权限
        if ((Objects.nonNull(userId) && userId > 0) && Objects.nonNull(targetUserId) && targetUserId > 0) {
            AgentUser superiorAgentUser = agentUserMapper.queryAgentUserByUserId(orgId, userId);
            long superiorUserId = getLevelUser(superiorAgentUser, "p" + superiorAgentUser.getLevel());
            AgentUser agentUser = agentUserMapper.queryAgentUserByUserId(orgId, targetUserId);
            long levelUserId = getLevelUser(agentUser, "p" + superiorAgentUser.getLevel());
            if (superiorUserId != levelUserId) {
                return QueryAgentCommissionListResponse.getDefaultInstance();
            }
        }

        Long user_id = null;

        if (Objects.nonNull(targetUserId) && targetUserId > 0) {
            user_id = targetUserId;
        } else if (Objects.nonNull(isAdmin) && isAdmin.equals(0)) {
            user_id = userId;
        }

        List<AgentCommission> agentCommissionList
                = statisticsAgentService.queryStatisticsAgentCommissionList(orgId, user_id, tokenId, startTime, endTime, fromId, endId, limit);

        if (CollectionUtils.isEmpty(agentCommissionList)) {
            return QueryAgentCommissionListResponse.getDefaultInstance();
        }

        List<QueryAgentCommissionListResponse.AgentCommission> list = new ArrayList<>();

        agentCommissionList.forEach(agentCommission -> {
            AgentUser agentUser = agentUserMapper.queryAgentUserByUserId(orgId, targetUserId);
            AgentUser superiorAgentUser = new AgentUser();
            if (agentUser != null && agentUser.getSuperiorUserId() > 0) {
                superiorAgentUser = agentUserMapper.queryAgentUserByUserId(orgId, agentUser.getSuperiorUserId());
            }

            list.add(QueryAgentCommissionListResponse.AgentCommission
                    .newBuilder()
                    .setId(agentCommission.getId())
                    .setTime(new DateTime(agentCommission.getDt().getTime()).toString("yyyy-MM-dd"))
                    .setBrokerId(agentCommission.getBrokerId())
                    .setAgentName(agentUser != null ? agentUser.getAgentName() : "")
                    .setAgentFee(agentCommission.getAgentFee().stripTrailingZeros().toPlainString())
                    .setSuperiorName((superiorAgentUser != null && StringUtils.isNotEmpty(superiorAgentUser.getAgentName())) ? superiorAgentUser.getAgentName() : "")
                    .setSuperiorUserId((superiorAgentUser != null && superiorAgentUser.getUserId() != null) ? superiorAgentUser.getUserId() : 0L)
                    .setTokenId(agentCommission.getTokenId())
                    .setUserId(agentCommission.getUserId())
                    .build());
        });
        return QueryAgentCommissionListResponse.newBuilder().addAllCommission(list).build();
    }

    public void cleanAgentRate(Long orgId) {
        //清理历史的rate 更新是否是绝对值状态
        this.agentUserMapper.cleanAgentAllRateByOrgId(orgId);
        //默认开启绝对值模式
        agentConfigMapper.insert(AgentConfig
                .builder()
                .orgId(orgId)
                .coinMaxRate(new BigDecimal("0.002"))
                .contractMaxRate(new BigDecimal("0.002"))
                .build());
    }

    private AgentConfig getAgentConfig(Long orgId) {
        Example example = new Example(AgentConfig.class);
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("orgId", orgId);
        return this.agentConfigMapper.selectOneByExample(example);
    }

    private Integer countSubmember(AgentUser agentUser) {
        if (agentUser.getLevel() > 10 || agentUser.getLevel().equals(0)) {
            return 0;
        }
        String sqlLevel = String.format("%s = %s", "p" + agentUser.getLevel(), agentUser.getUserId());
        return this.agentUserMapper.queryUserCountByLevel(agentUser.getOrgId(), sqlLevel);
    }

    private Integer countAllSubmember(AgentUser agentUser) {
        if (agentUser.getLevel() > 10 || agentUser.getLevel().equals(0)) {
            return 0;
        }
        String sqlLevel = String.format("%s = %s", "p" + agentUser.getLevel(), agentUser.getUserId());
        return this.agentUserMapper.queryAllUserCountByLevel(agentUser.getOrgId(), sqlLevel);
    }

    private Integer countSubAgentMember(AgentUser agentUser) {
        //查这个用户直属下级代理数
        return this.agentUserMapper.queryAgentCountByLevel(agentUser.getOrgId(), agentUser.getUserId());
    }
}