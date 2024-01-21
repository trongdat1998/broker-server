package io.bhex.broker.server.grpc.server.service;

import com.github.pagehelper.PageHelper;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.bhex.base.idgen.api.ISequenceGenerator;
import io.bhex.base.margin.DeleteInterestByLevelRequest;
import io.bhex.base.margin.InterestConfig;
import io.bhex.base.margin.SetLevelInterestRequest;
import io.bhex.base.proto.BaseRequest;
import io.bhex.base.proto.DecimalUtil;
import io.bhex.broker.common.util.JsonUtil;
import io.bhex.broker.grpc.user.level.*;
import io.bhex.broker.server.domain.AccountType;
import io.bhex.broker.server.domain.BaseConfigConstants;
import io.bhex.broker.server.domain.StatisticsTradeAmountData;
import io.bhex.broker.server.domain.SwitchStatus;
import io.bhex.broker.server.grpc.client.service.GrpcMarginService;
import io.bhex.broker.server.domain.*;
import io.bhex.broker.server.model.BaseConfigInfo;
import io.bhex.broker.server.model.Broker;
import io.bhex.broker.server.model.UserLevel;
import io.bhex.broker.server.model.UserLevelConfig;
import io.bhex.broker.server.primary.mapper.BrokerMapper;
import io.bhex.broker.server.primary.mapper.UserLevelConfigMapper;
import io.bhex.broker.server.primary.mapper.UserLevelMapper;
import io.bhex.broker.server.statistics.statistics.mapper.StatisticsUserLevelDataMapper;
import io.bhex.broker.server.util.BeanCopyUtils;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.DateTime;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;
import tk.mybatis.mapper.entity.Example;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
@Service
public class UserLevelService {

    @Resource
    private UserLevelMapper userLevelMapper;
    @Resource
    private UserLevelConfigMapper userLevelConfigMapper;
    @Resource
    private StatisticsUserLevelDataMapper statisticsUserLevelDataMapper;
    @Resource
    private UserLevelScheduleService userLevelScheduleService;
    @Resource
    private BrokerMapper brokerMapper;
    @Resource
    private AccountService accountService;
    @Resource
    private BaseBizConfigService baseBizConfigService;
    @Resource
    private ISequenceGenerator sequenceGenerator;
    @Resource
    GrpcMarginService grpcMarginService;

    private static List<UserLevelConfig> availableConfigs = Lists.newArrayList();
    private static ImmutableMap<Long, HaveTokenConfig> haveTokenConfigMap = ImmutableMap.of();

    public void addBaseLevelConfig(long orgId) {
        List<UserLevelConfig> orgConfigs = userLevelConfigMapper.listOrgConfigs(orgId);
        if (CollectionUtils.isEmpty(orgConfigs)) {
            orgConfigs = Lists.newArrayList();
        }
        if (orgConfigs.stream().noneMatch(c -> c.getIsBaseLevel() == 1)) {
            UserLevelConfig levelConfig = new UserLevelConfig();
            levelConfig.setLevelPosition(sequenceGenerator.getLong());
            levelConfig.setOrgId(orgId);
            levelConfig.setLevelValue("[{\"language\":\"en_US\",\"levelName\":\"Lv.0\"},{\"language\":\"zh_CN\",\"levelName\":\"Lv.0\"}]");
            levelConfig.setLevelIcon("https://static.nucleex.com/static/levels/b-0.png");
            levelConfig.setStatus(UserLevelConfig.OK_STATUS);
            levelConfig.setCreated(System.currentTimeMillis());
            levelConfig.setUpdated(System.currentTimeMillis());
            levelConfig.setIsBaseLevel(1);
            levelConfig.setStatDate(0L);
            userLevelConfigMapper.insertSelective(levelConfig);
        }
    }

    @Transactional
    public UserLevelConfigResponse userLevelConfig(UserLevelConfigRequest request) {
        long levelConfigId = request.getUserLevelConfig().getLevelConfigId();
        long orgId = request.getUserLevelConfig().getOrgId();
        addBaseLevelConfig(orgId);
        Broker broker = brokerMapper.getByOrgId(orgId);
//        if (!broker.getFunctionsMap().getOrDefault("userLevel", false)) {
//            return UserLevelConfigResponse.newBuilder().setCode(1).build();
//        }

        boolean changeConfigCondition = true;
        UserLevelConfig dbConfig = null;
        if (levelConfigId > 0) {
            dbConfig = userLevelConfigMapper.getConfig(orgId, levelConfigId);
            if (dbConfig == null) {
                return UserLevelConfigResponse.newBuilder().build();
            }
            if (dbConfig.getLevelCondition().equals(request.getUserLevelConfig().getLevelCondition())
                    && dbConfig.getStatus() != UserLevelConfig.DELETED_STATUS) {
                changeConfigCondition = false;
            }
        }

        int configStatus = request.getPreview() ? UserLevelConfig.PREVIEW_STATUS : UserLevelConfig.OK_STATUS;
        UserLevelConfigObj configObj = request.getUserLevelConfig();
        UserLevelConfig levelConfig = new UserLevelConfig();
        if (dbConfig != null && dbConfig.getIsBaseLevel() == 1) { //基础级别只能修改icon和名字
            BeanCopyUtils.copyPropertiesIgnoreNull(dbConfig, levelConfig);
            configStatus = UserLevelConfig.OK_STATUS;
            levelConfig.setLevelValue(configObj.getLocaleDetail());
            levelConfig.setLevelIcon(configObj.getLevelIcon());
        } else {
            BeanCopyUtils.copyPropertiesIgnoreNull(configObj, levelConfig);
            levelConfig.setWithdrawUpperLimitInBtc(new BigDecimal(configObj.getWithdrawUpperLimitInBTC()));
            levelConfig.setCancelOtc24hWithdrawLimit(configObj.getCancelOtc24HWithdrawLimit() ? 1 : 0);
            levelConfig.setInviteBonusStatus(configObj.getInviteBonusStatus() ? 1 : 0);
            levelConfig.setSpotBuyMakerDiscount(new BigDecimal(configObj.getSpotBuyMakerDiscount()));
            levelConfig.setSpotBuyTakerDiscount(new BigDecimal(configObj.getSpotBuyTakerDiscount()));
            levelConfig.setSpotSellMakerDiscount(new BigDecimal(configObj.getSpotSellMakerDiscount()));
            levelConfig.setSpotSellTakerDiscount(new BigDecimal(configObj.getSpotSellTakerDiscount()));
            levelConfig.setOptionBuyMakerDiscount(new BigDecimal(configObj.getOptionBuyMakerDiscount()));
            levelConfig.setOptionBuyTakerDiscount(new BigDecimal(configObj.getOptionBuyTakerDiscount()));
            levelConfig.setOptionSellMakerDiscount(new BigDecimal(configObj.getOptionSellMakerDiscount()));
            levelConfig.setOptionSellTakerDiscount(new BigDecimal(configObj.getOptionSellTakerDiscount()));
            levelConfig.setContractBuyMakerDiscount(new BigDecimal(configObj.getContractBuyMakerDiscount()));
            levelConfig.setContractBuyTakerDiscount(new BigDecimal(configObj.getContractBuyTakerDiscount()));
            levelConfig.setContractSellMakerDiscount(new BigDecimal(configObj.getContractSellMakerDiscount()));
            levelConfig.setContractSellTakerDiscount(new BigDecimal(configObj.getContractSellTakerDiscount()));
            levelConfig.setStatus(configStatus);
            levelConfig.setLevelValue(configObj.getLocaleDetail());
            levelConfig.setStatDate(Long.parseLong(new DateTime().toString("yyyyMMddHH")));
        }

        levelConfig.setUpdated(System.currentTimeMillis());
        if (levelConfigId > 0) {
            levelConfig.setLeaderStatus(dbConfig.getLeaderStatus());
            levelConfig.setId(levelConfigId);
            userLevelConfigMapper.updateByPrimaryKeySelective(levelConfig);
            //userLevelMapper.updateLevelUserStatus(orgId, levelConfig.getId(), configStatus);
            if (changeConfigCondition) {
                userLevelScheduleService.refreshUserLevelData(broker, levelConfig, UUID.randomUUID().toString());
            }
        } else {
            levelConfig.setLevelPosition(sequenceGenerator.getLong());
            levelConfig.setIsBaseLevel(0);
            levelConfig.setCreated(System.currentTimeMillis());
            levelConfig.setLeaderStatus(0);
            userLevelConfigMapper.insertSelective(levelConfig);
            userLevelScheduleService.refreshUserLevelData(broker, levelConfig, UUID.randomUUID().toString());
        }
        if (dbConfig == null || dbConfig.getIsBaseLevel() == 0) {
            BigDecimal freeInterest = new BigDecimal("-1");
            SetLevelInterestRequest interestRequest = SetLevelInterestRequest.newBuilder()
                    .setBaseRequest(BaseRequest.newBuilder().setOrganizationId(orgId).build())
                    .addAllInterestConfig(request.getInterestConfigList().stream()
                            .map(interest -> InterestConfig.newBuilder()
                                    .setTokenId(interest.getTokenId())
                                    .setInterest(new BigDecimal(interest.getInterest()).compareTo(freeInterest) <= 0 ? DecimalUtil.fromBigDecimal(freeInterest)
                                            : DecimalUtil.fromBigDecimal(new BigDecimal(interest.getInterest()).divide(new BigDecimal(86400 / interest.getInterestPeriod()), 18, RoundingMode.DOWN)))
                                    .setInterestPeriod(interest.getInterestPeriod())
                                    .setCalculationPeriod(interest.getCalculationPeriod())
                                    .setSettlementPeriod(interest.getSettlementPeriod())
                                    .setLevelConfigId(levelConfigId)
                                    .setShowInterest(DecimalUtil.fromBigDecimal(new BigDecimal(interest.getInterest())))
                                    .build()).collect(Collectors.toList()))
                    .build();
            grpcMarginService.setLevelInterest(interestRequest);
        }
        int count = userLevelMapper.countLevelUsers(orgId, levelConfig.getId(), configStatus);
        return UserLevelConfigResponse.newBuilder().setLevelConfigId(levelConfig.getId()).setUserCount(count).build();
    }


    public List<UserLevelConfigObj> listUserLevelConfigs(ListUserLevelConfigsRequest request) {
        Example example = Example.builder(UserLevelConfig.class).orderByAsc("levelPosition").build();
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("orgId", request.getOrgId())
                .andGreaterThan("status", 0);
        if (request.getLevelConfigId() > 0) {
            criteria.andEqualTo("id", request.getLevelConfigId());
        }

        List<UserLevelConfig> configs = userLevelConfigMapper.selectByExample(example);

        if (CollectionUtils.isEmpty(configs)) {
            return Lists.newArrayList();
        }
        List<UserLevelConfigObj> objs = configs.stream()
                .map(c -> parseConfigObj(c))
                .collect(Collectors.toList());
        return objs;
    }

    @Transactional
    public DeleteUserLevelConfigResponse deleteUserLevelConfig(DeleteUserLevelConfigRequest request) {
        int row = userLevelConfigMapper.deleteConfig(request.getOrgId(), request.getLevelConfigId(), System.currentTimeMillis());
        userLevelMapper.deleteUserLevel(request.getOrgId(), request.getLevelConfigId(), System.currentTimeMillis());
        if (row > 0) {
            DeleteInterestByLevelRequest deleteInterestByLevelRequest = DeleteInterestByLevelRequest.newBuilder()
                    .setBaseRequest(BaseRequest.newBuilder().setOrganizationId(request.getOrgId()).build())
                    .setLevelConfigId(request.getLevelConfigId())
                    .build();
            grpcMarginService.deleteInterestByLevel(deleteInterestByLevelRequest);
        }
        return DeleteUserLevelConfigResponse.newBuilder().build();
    }

    public QueryLevelConfigUsersResponse queryLevelConfigUsers(QueryLevelConfigUsersRequest request) {
        Example example = Example.builder(UserLevel.class).orderByAsc("userId").build().selectProperties("userId");
        example.createCriteria().andEqualTo("orgId", request.getOrgId())
                .andEqualTo("levelConfigId", request.getLevelConfigId())
                .andGreaterThan("userId", request.getLastId())
                .andIn("status", request.getQueryWhiteList() ? Lists.newArrayList(3) : Lists.newArrayList(1, 3));
        PageHelper.startPage(1, request.getPageSize());

        List<UserLevel> userLevels = userLevelMapper.selectByExample(example);
        if (CollectionUtils.isEmpty(userLevels)) {
            return QueryLevelConfigUsersResponse.newBuilder().build();
        }
        List<Long> users = userLevels.stream()
                .map(s -> s.getUserId())
                .collect(Collectors.toList());
        return QueryLevelConfigUsersResponse.newBuilder().addAllUserId(users).build();
    }

    /**
     * @param request
     * @return
     */
    @Transactional
    public DeleteWhiteListUsersResponse deleteWhiteListUsers(DeleteWhiteListUsersRequest request) {
        List<Long> users = request.getUserIdList();
        if (CollectionUtils.isEmpty(users)) {
            return DeleteWhiteListUsersResponse.newBuilder().build();
        }
        Example example = new Example(UserLevel.class);
        example.createCriteria().andEqualTo("orgId", request.getOrgId())
                .andEqualTo("levelConfigId", request.getLevelConfigId())
                .andIn("userId", users);
        userLevelMapper.updateByExampleSelective(UserLevel.builder().status(0).build(), example);
        return DeleteWhiteListUsersResponse.newBuilder().build();
    }

    @Transactional
    public AddWhiteListUsersResponse addWhiteListUsers(AddWhiteListUsersRequest request) {
        if (!request.getIsAddition()) {
            userLevelMapper.deleteWhiteListUsers(request.getOrgId(), request.getLevelConfigId());
        }

        List<Long> users = request.getUserIdList().stream().distinct().collect(Collectors.toList());
        if (CollectionUtils.isEmpty(users)) {
            return AddWhiteListUsersResponse.newBuilder().build();
        }

        Example example = new Example(UserLevel.class);
        example.createCriteria().andEqualTo("orgId", request.getOrgId())
                .andEqualTo("levelConfigId", request.getLevelConfigId())
                .andIn("userId", users);
        List<UserLevel> existedUserLevels = userLevelMapper.selectByExample(example);
        if (existedUserLevels == null) {
            existedUserLevels = Lists.newArrayList();
        }
        for (Long userId : users) {
            Optional<UserLevel> userLevelOptional = existedUserLevels.stream().filter(u -> u.getUserId().equals(userId)).findAny();
            if (userLevelOptional.isPresent()) {
                UserLevel userLevel = userLevelOptional.get();
                userLevel.setStatus(UserLevel.WHITE_LIST_STATUS);
                userLevel.setUpdated(System.currentTimeMillis());
                userLevelMapper.updateByPrimaryKey(userLevel);
            } else {
                UserLevel userLevel = UserLevel.builder()
                        .levelConfigId(request.getLevelConfigId())
                        .orgId(request.getOrgId())
                        .userId(userId)
                        .levelData("{}")
                        .status(UserLevel.WHITE_LIST_STATUS)
                        .created(System.currentTimeMillis())
                        .updated(System.currentTimeMillis())
                        .build();
                userLevelMapper.insertSelective(userLevel);
            }
        }
        return AddWhiteListUsersResponse.newBuilder().build();
    }


    public QueryMyLevelConfigResponse queryMyLevelConfig(QueryMyLevelConfigRequest request) {
        QueryMyLevelConfigResponse response = queryMyLevelConfig(request.getHeader().getOrgId(),
                request.getHeader().getUserId(), request.getWithTradeData(), request.getWithTradeFee());
        if (response == null) {
            return QueryMyLevelConfigResponse.newBuilder().build();
        }
        return response;
    }


    public QueryMyLevelConfigResponse queryMyLevelConfig(long orgId, long userId, boolean withTradeData, boolean withTradeFeeData) {
        QueryMyLevelConfigResponse.Builder levelConfigBuilder = QueryMyLevelConfigResponse.newBuilder();
        SwitchStatus switchStatus = baseBizConfigService.getConfigSwitchStatus(orgId,
                BaseConfigConstants.USER_LEVEL_CONFIG_GROUP, "open.switch");

        List<UserLevel> userLevels = switchStatus.isOpen() ? userLevelMapper.queryMyAvailableConfigs(orgId, userId) : Lists.newArrayList();
        List<UserLevelConfig> myAvailableConfigs;
        if (CollectionUtils.isEmpty(userLevels)) { //如果没有找到等级用户，都放到默认等级中
            myAvailableConfigs = availableConfigs.stream()
                    .filter(s -> s.getOrgId() == orgId)
                    .filter(s -> s.getIsBaseLevel() == 1)
                    .collect(Collectors.toList());
            if (!CollectionUtils.isEmpty(myAvailableConfigs)) {
                levelConfigBuilder.addAllLevelConfigId(Lists.newArrayList(myAvailableConfigs.get(0).getId()));
            }
        } else {
            List<Long> myConfigs = userLevels.stream()
                    .map(s -> s.getLevelConfigId())
                    .collect(Collectors.toList());
            myAvailableConfigs = availableConfigs.stream()
                    .filter(s -> s.getOrgId() == orgId)
                    .filter(s -> myConfigs.contains(s.getId()))
                    .sorted(Comparator.comparing(UserLevelConfig::getLevelPosition).reversed())
                    .collect(Collectors.toList());
            levelConfigBuilder.addLevelConfigId(myAvailableConfigs.get(0).getId());
        }


        BigDecimal spotBuyMakerDiscount = BigDecimal.ONE;
        BigDecimal spotBuyTakerDiscount = BigDecimal.ONE;
        BigDecimal spotSellMakerDiscount = BigDecimal.ONE;
        BigDecimal spotSellTakerDiscount = BigDecimal.ONE;
        BigDecimal optionBuyMakerDiscount = BigDecimal.ONE;
        BigDecimal optionBuyTakerDiscount = BigDecimal.ONE;
        BigDecimal optionSellMakerDiscount = BigDecimal.ONE;
        BigDecimal optionSellTakerDiscount = BigDecimal.ONE;
        BigDecimal contractBuyMakerDiscount = BigDecimal.ONE;
        BigDecimal contractBuyTakerDiscount = BigDecimal.ONE;
        BigDecimal contractSellMakerDiscount = BigDecimal.ONE;
        BigDecimal contractSellTakerDiscount = BigDecimal.ONE;
        BigDecimal withdrawUpperLimitInBtc = BigDecimal.ZERO;
        Integer cancelOtc24hWithdrawLimit = 0; //1-是 0-否
        Integer inviteBonusStatus = 1;

        //取对用户来说最有利的配置
        for (UserLevelConfig c : myAvailableConfigs) {
            if (c.getSpotBuyMakerDiscount().compareTo(spotBuyMakerDiscount) < 0) {
                spotBuyMakerDiscount = calculateDiscount(orgId, userId, AccountType.MAIN, c.getSpotBuyMakerDiscount());
            }
            if (c.getSpotBuyTakerDiscount().compareTo(spotBuyTakerDiscount) < 0) {
                spotBuyTakerDiscount = calculateDiscount(orgId, userId, AccountType.MAIN, c.getSpotBuyTakerDiscount());
            }
            if (c.getSpotSellMakerDiscount().compareTo(spotSellMakerDiscount) < 0) {
                spotSellMakerDiscount = calculateDiscount(orgId, userId, AccountType.MAIN, c.getSpotSellMakerDiscount());
            }
            if (c.getSpotSellTakerDiscount().compareTo(spotSellTakerDiscount) < 0) {
                spotSellTakerDiscount = calculateDiscount(orgId, userId, AccountType.MAIN, c.getSpotSellTakerDiscount());
            }

            if (c.getOptionBuyMakerDiscount().compareTo(optionBuyMakerDiscount) < 0) {
                optionBuyMakerDiscount = calculateDiscount(orgId, userId, AccountType.OPTION, c.getOptionBuyMakerDiscount());
            }
            if (c.getOptionBuyTakerDiscount().compareTo(optionBuyTakerDiscount) < 0) {
                optionBuyTakerDiscount = calculateDiscount(orgId, userId, AccountType.OPTION, c.getOptionBuyTakerDiscount());
            }
            if (c.getOptionSellMakerDiscount().compareTo(optionSellMakerDiscount) < 0) {
                optionSellMakerDiscount = calculateDiscount(orgId, userId, AccountType.OPTION, c.getOptionSellMakerDiscount());
            }
            if (c.getOptionSellTakerDiscount().compareTo(optionSellTakerDiscount) < 0) {
                optionSellTakerDiscount = calculateDiscount(orgId, userId, AccountType.OPTION, c.getOptionSellTakerDiscount());
            }

            if (c.getContractBuyMakerDiscount().compareTo(contractBuyMakerDiscount) < 0) {
                contractBuyMakerDiscount = calculateDiscount(orgId, userId, AccountType.FUTURES, c.getContractBuyMakerDiscount());
            }
            if (c.getContractBuyTakerDiscount().compareTo(contractBuyTakerDiscount) < 0) {
                contractBuyTakerDiscount = calculateDiscount(orgId, userId, AccountType.FUTURES, c.getContractBuyTakerDiscount());
            }
            if (c.getContractSellMakerDiscount().compareTo(contractSellMakerDiscount) < 0) {
                contractSellMakerDiscount = calculateDiscount(orgId, userId, AccountType.FUTURES, c.getContractSellMakerDiscount());
            }
            if (c.getContractSellTakerDiscount().compareTo(contractSellTakerDiscount) < 0) {
                contractSellTakerDiscount = calculateDiscount(orgId, userId, AccountType.FUTURES, c.getContractSellTakerDiscount());
            }

            if (cancelOtc24hWithdrawLimit == 0 && c.getCancelOtc24hWithdrawLimit() == 1) {
                cancelOtc24hWithdrawLimit = 1;
            }
            if (inviteBonusStatus == 1 && c.getInviteBonusStatus() == 0) {
                inviteBonusStatus = 0;
            }
            if (withdrawUpperLimitInBtc.compareTo(c.getWithdrawUpperLimitInBtc()) < 0) {
                withdrawUpperLimitInBtc = c.getWithdrawUpperLimitInBtc();
            }
        }


        levelConfigBuilder.setCancelOtc24HWithdrawLimit(cancelOtc24hWithdrawLimit == 1);
        levelConfigBuilder.setInviteBonusStatus(inviteBonusStatus == 1);
        levelConfigBuilder.setWithdrawUpperLimitInBTC(withdrawUpperLimitInBtc.stripTrailingZeros().toPlainString());

        levelConfigBuilder.setSpotBuyMakerDiscount(spotBuyMakerDiscount.stripTrailingZeros().toPlainString());
        levelConfigBuilder.setSpotBuyTakerDiscount(spotBuyTakerDiscount.stripTrailingZeros().toPlainString());
        levelConfigBuilder.setSpotSellMakerDiscount(spotSellMakerDiscount.stripTrailingZeros().toPlainString());
        levelConfigBuilder.setSpotSellTakerDiscount(spotSellTakerDiscount.stripTrailingZeros().toPlainString());

        levelConfigBuilder.setOptionBuyMakerDiscount(optionBuyMakerDiscount.stripTrailingZeros().toPlainString());
        levelConfigBuilder.setOptionBuyTakerDiscount(optionBuyTakerDiscount.stripTrailingZeros().toPlainString());
        levelConfigBuilder.setOptionSellMakerDiscount(optionSellMakerDiscount.stripTrailingZeros().toPlainString());
        levelConfigBuilder.setOptionSellTakerDiscount(optionSellTakerDiscount.stripTrailingZeros().toPlainString());

        levelConfigBuilder.setContractBuyMakerDiscount(contractBuyMakerDiscount.stripTrailingZeros().toPlainString());
        levelConfigBuilder.setContractBuyTakerDiscount(contractBuyTakerDiscount.stripTrailingZeros().toPlainString());
        levelConfigBuilder.setContractSellMakerDiscount(contractSellMakerDiscount.stripTrailingZeros().toPlainString());
        levelConfigBuilder.setContractSellTakerDiscount(contractSellTakerDiscount.stripTrailingZeros().toPlainString());

        Map<String, String> levelData = Maps.newHashMap();
        for (UserLevel l : userLevels) {
            Map<String, String> lm = JsonUtil.defaultGson().fromJson(l.getLevelData(), Map.class);
            levelData.putAll(lm);
        }
        levelConfigBuilder.putAllLevelData(levelData);

        if (withTradeData) {
            String startDate = getDateStr(System.currentTimeMillis() - 30 * 24 * 3600_000L);
            String endDate = getDateStr(System.currentTimeMillis());
            List<StatisticsTradeAmountData> amountList = statisticsUserLevelDataMapper.tradeAmountBtcByUser(orgId, userId + "", startDate, endDate);
            if (!CollectionUtils.isEmpty(amountList)) {
                for (StatisticsTradeAmountData amountData : amountList) {
                    String amountStr = amountData.getAmount().setScale(4, RoundingMode.DOWN).stripTrailingZeros().toPlainString();
                    if (amountData.getTradeType() == 1) {
                        levelConfigBuilder.putMonthlyTradeAmountInBtc(AccountType.MAIN.value(), amountStr);
                    } else if (amountData.getTradeType() == 2) {
                        levelConfigBuilder.putMonthlyTradeAmountInBtc(AccountType.OPTION.value(), amountStr);
                    } else if (amountData.getTradeType() == 3) {
                        levelConfigBuilder.putMonthlyTradeAmountInBtc(AccountType.FUTURES.value(), amountStr);
                    }
                }
            }
        }

        if (withTradeFeeData) { //暂时只有管理后台用的到
            List<StatisticsTradeAmountData> amountList = statisticsUserLevelDataMapper.tradeFeeUsdtByUser(orgId, userId + "");
            if (!CollectionUtils.isEmpty(amountList)) {
                for (StatisticsTradeAmountData amountData : amountList) {
                    String amountStr = amountData.getAmount().setScale(4, RoundingMode.DOWN).stripTrailingZeros().toPlainString();
                    if (amountData.getTradeType() == 1) {
                        levelConfigBuilder.putTradeFeeInUsdt(AccountType.MAIN.value(), amountStr);
                    } else if (amountData.getTradeType() == 2) {
                        levelConfigBuilder.putTradeFeeInUsdt(AccountType.OPTION.value(), amountStr);
                    } else if (amountData.getTradeType() == 3) {
                        levelConfigBuilder.putTradeFeeInUsdt(AccountType.FUTURES.value(), amountStr);
                    }
                }
            }
        }

        return levelConfigBuilder.build();
    }

    public BigDecimal calculateDiscount(long orgId, long userId, AccountType accountType, BigDecimal orginalDiscount) {
        if (orginalDiscount.compareTo(BigDecimal.ZERO) <= 0) { //负折扣没有折上折
            return orginalDiscount;
        }
        if (accountType == AccountType.OPTION) {
            return orginalDiscount;
        }
        HaveTokenConfig haveTokenDiscount = haveTokenConfigMap.get(orgId);
        if (haveTokenDiscount == null || haveTokenDiscount.getDiscount().compareTo(BigDecimal.ONE) >= 0) {
            return orginalDiscount;
        }
        if (accountType == AccountType.MAIN && haveTokenDiscount.getSpotSwitch()) {
            if (satisfyHaveTokenNumber(orgId, userId, haveTokenDiscount.getTokenId(), haveTokenDiscount.getNumber())) {
                return orginalDiscount.multiply(haveTokenDiscount.getDiscount()).setScale(2, RoundingMode.UP);
            }
        } else if (accountType == AccountType.FUTURES && haveTokenDiscount.getContractSwitch()) {
            if (satisfyHaveTokenNumber(orgId, userId, haveTokenDiscount.getTokenId(), haveTokenDiscount.getNumber())) {
                return orginalDiscount.multiply(haveTokenDiscount.getDiscount()).setScale(2, RoundingMode.UP);
            }
        } else if (accountType == AccountType.MARGIN && haveTokenDiscount.getMarginSwitch()) {
            if (satisfyHaveTokenNumber(orgId, userId, haveTokenDiscount.getTokenId(), haveTokenDiscount.getNumber())) {
                return orginalDiscount.multiply(haveTokenDiscount.getDiscount()).setScale(2, RoundingMode.UP);
            }
        }
        return orginalDiscount;

    }

    private static final Cache<String, Boolean> HAVE_TOKEN_CACHE =
            CacheBuilder.newBuilder().expireAfterWrite(24, TimeUnit.HOURS).build();

    //判断持币数量是否满足条件
    private boolean satisfyHaveTokenNumber(long orgId, long userId, String tokenId, BigDecimal minTokenNumber) {
        String key = orgId + "-" + userId + "-" + tokenId + "-" + minTokenNumber.stripTrailingZeros().toPlainString();
        Boolean satisfyObj = HAVE_TOKEN_CACHE.getIfPresent(key);
        if (satisfyObj != null) {
            return satisfyObj;
        }

        List<StatisticsUserLevelData> list = statisticsUserLevelDataMapper.listBalanceAmountByUsers(orgId, tokenId, 1,
                Lists.newArrayList(userId), minTokenNumber, BigDecimal.ZERO);
        boolean satisfy = !CollectionUtils.isEmpty(list);
        HAVE_TOKEN_CACHE.put(key, satisfy);
        return satisfy;
    }

    private String getDateStr(long time) {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        String str = format.format(new Date(time));
        return str;
    }

    public List<UserLevelConfigObj> listAllUserLevelConfigs(ListAllUserLevelConfigsRequest request) {
        SwitchStatus switchStatus = baseBizConfigService.getConfigSwitchStatus(request.getHeader().getOrgId(),
                BaseConfigConstants.USER_LEVEL_CONFIG_GROUP, "open.switch");
        if (CollectionUtils.isEmpty(availableConfigs)) {
            return Lists.newArrayList();
        }
        List<UserLevelConfigObj> objs = availableConfigs.stream()
                .filter(c -> c.getOrgId().equals(request.getHeader().getOrgId()))
                .map(c -> parseConfigObj(c))
                .collect(Collectors.toList());
        if (!switchStatus.isOpen() && !CollectionUtils.isEmpty(objs)) { //如果未开放 只展示基础等级
            objs = objs.stream()
                    .filter(c -> c.getIsBaseLevel() == 1)
                    .collect(Collectors.toList());
        }
        return objs;
    }

    public UserLevelConfigObj parseConfigObj(UserLevelConfig levelConfig) {
        UserLevelConfigObj.Builder builder = UserLevelConfigObj.newBuilder();
        BeanCopyUtils.copyPropertiesIgnoreNull(levelConfig, builder);
        builder.setLevelConfigId(levelConfig.getId());
        builder.setLocaleDetail(levelConfig.getLevelValue());
        builder.setCancelOtc24HWithdrawLimit(levelConfig.getCancelOtc24hWithdrawLimit() == 1);
        builder.setInviteBonusStatus(levelConfig.getInviteBonusStatus() == 1);
        builder.setWithdrawUpperLimitInBTC(levelConfig.getWithdrawUpperLimitInBtc().stripTrailingZeros().toPlainString());
        builder.setSpotBuyMakerDiscount(levelConfig.getSpotBuyMakerDiscount().stripTrailingZeros().toPlainString());
        builder.setSpotBuyTakerDiscount(levelConfig.getSpotBuyTakerDiscount().stripTrailingZeros().toPlainString());
        builder.setSpotSellMakerDiscount(levelConfig.getSpotSellMakerDiscount().stripTrailingZeros().toPlainString());
        builder.setSpotSellTakerDiscount(levelConfig.getSpotSellTakerDiscount().stripTrailingZeros().toPlainString());

        builder.setOptionBuyMakerDiscount(levelConfig.getOptionBuyMakerDiscount().stripTrailingZeros().toPlainString());
        builder.setOptionBuyTakerDiscount(levelConfig.getOptionBuyTakerDiscount().stripTrailingZeros().toPlainString());
        builder.setOptionSellMakerDiscount(levelConfig.getOptionSellMakerDiscount().stripTrailingZeros().toPlainString());
        builder.setOptionSellTakerDiscount(levelConfig.getOptionSellTakerDiscount().stripTrailingZeros().toPlainString());

        builder.setContractBuyMakerDiscount(levelConfig.getContractBuyMakerDiscount().stripTrailingZeros().toPlainString());
        builder.setContractBuyTakerDiscount(levelConfig.getContractBuyTakerDiscount().stripTrailingZeros().toPlainString());
        builder.setContractSellMakerDiscount(levelConfig.getContractSellMakerDiscount().stripTrailingZeros().toPlainString());
        builder.setContractSellTakerDiscount(levelConfig.getContractSellTakerDiscount().stripTrailingZeros().toPlainString());
        return builder.build();
    }


    @PostConstruct
    @Scheduled(cron = "0/8 * * * * ?")
    public void loadAvailableConfigs() {
        availableConfigs = userLevelConfigMapper.listAvailableConfigs();
        List<BaseConfigInfo> configs = baseBizConfigService.getConfigsByGroupsAndKey(0,
                Lists.newArrayList(BaseConfigConstants.USER_LEVEL_CONFIG_GROUP), "have.token.config");

        Map<Long, HaveTokenConfig> configMap = Maps.newHashMap();
        if (!CollectionUtils.isEmpty(configs)) {
            for (BaseConfigInfo configInfo : configs) {
                HaveTokenConfig haveTokenConfig = JsonUtil.defaultGson().fromJson(configInfo.getConfValue(), HaveTokenConfig.class);
                if (haveTokenConfig != null && haveTokenConfig.getStatus() == 1) {
                    configMap.put(configInfo.getOrgId(), haveTokenConfig);
                }
            }
        }
        haveTokenConfigMap = ImmutableMap.copyOf(configMap);
    }

    //每天10点清空持币缓存
    @Scheduled(cron = "5 12 2 * * ?")
    public void clearHaveTokenCache() {
        HAVE_TOKEN_CACHE.cleanUp();
    }


    @Data
    private static class HaveTokenConfig {
        private String tokenId;
        private BigDecimal number;
        private BigDecimal discount;
        private Boolean spotSwitch = false;
        private Boolean contractSwitch = false;
        private Boolean marginSwitch = false;
        private Integer status;
    }
}
