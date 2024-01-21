package io.bhex.broker.server.grpc.server.service;


import com.github.pagehelper.PageHelper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.reflect.TypeToken;
import io.bhex.broker.common.util.JsonUtil;
import io.bhex.broker.server.domain.*;
import io.bhex.broker.server.model.*;
import io.bhex.broker.server.primary.mapper.*;
import io.bhex.broker.server.statistics.statistics.mapper.StatisticsUserLevelDataMapper;
import io.bhex.broker.server.util.RedisLockUtils;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.DateTime;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import tk.mybatis.mapper.entity.Example;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * @Description:用户等级服务
 * @Date: 2020/4/27 下午4:17
 * @Author: liwei
 * @Copyright（C）: 2018 BlueHelix Inc. All rights reserved.
 */
@Slf4j
@Service
public class UserLevelScheduleService {
    @Resource
    private BrokerMapper brokerMapper;
    @Resource
    private StatisticsUserLevelDataMapper statisticsUserLevelDataMapper;
    @Resource
    private UserVerifyMapper userVerifyMapper;
    @Resource
    private UserMapper userMapper;
    @Resource
    private UserLevelMapper userLevelMapper;
    @Resource
    private AccountMapper accountMapper;
    @Resource
    private BaseBizConfigService baseBizConfigService;
    @Resource
    private UserLevelConfigMapper userLevelConfigMapper;
    @Resource(name = "stringRedisTemplate")
    private StringRedisTemplate redisTemplate;

    public static final Integer PAGE_SIZE = 500;

    @Scheduled(cron = "25 10 * * * ?")
    public void refreshUserLevelData() {
        List<Broker> brokerList = brokerMapper.selectAll();
        if (CollectionUtils.isEmpty(brokerList)) {
            return;
        }
        Collections.shuffle(brokerList);
        for (Broker broker : brokerList) {
            if (broker.getStatus() != 1) {
                continue;
            }
            Map<String, Boolean> functionMap = broker.getFunctionsMap();
            if (functionMap == null || !functionMap.getOrDefault("userLevel", false)) {
                // continue;
            }
            refreshUserLevelData(broker);
        }
    }

    public void refreshUserLevelData(long orgId, long levelConfigId) {
        Broker broker = brokerMapper.getByOrgId(orgId);
        Map<String, Boolean> functionMap = broker.getFunctionsMap();
        if (functionMap == null || !functionMap.getOrDefault("userLevel", false)) {
            //return;
        }
        if (levelConfigId > 0) {
            refreshUserLevelData(broker, userLevelConfigMapper.selectByPrimaryKey(levelConfigId), UUID.randomUUID().toString());
        } else {
            refreshUserLevelData(broker);
        }
    }

    public void refreshUserLevelData(Broker broker) {
        Example example = Example.builder(UserLevelConfig.class).build();
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("orgId", broker.getOrgId()).andEqualTo("status", 1);

        List<UserLevelConfig> userLevelConfigs = userLevelConfigMapper.selectByExample(example);
        if (!CollectionUtils.isEmpty(userLevelConfigs)) {
            Collections.shuffle(userLevelConfigs);
            for (UserLevelConfig levelConfig : userLevelConfigs) {
                if (levelConfig.getIsBaseLevel() == 1) {
                    continue;
                }
                if (levelConfig.getStatDate() == Long.parseLong(new DateTime().toString("yyyyMMddHH"))) { //当前这一小时已执行过了
                    log.info("has executed {}", levelConfig.getId());
                    continue;
                }
                String lockKey = String.format(BrokerLockKeys.USER_LEVER_LOCK_KEY, broker.getOrgId().toString(), levelConfig.getId().toString());
                boolean lock = RedisLockUtils.tryLock(redisTemplate, lockKey, BrokerLockKeys.USER_LEVER_LOCK_KEY_EXPIRE);
                if (!lock) {
                    log.info("not get lock org:{} config:{}", levelConfig.getOrgId(), levelConfig.getId());
                    continue;
                }
                try {
                    String uuid = UUID.randomUUID().toString();
                    log.info("start refreshUserLevelData");
                    refreshUserLevelData(broker, levelConfig, uuid);
                    levelConfig.setUpdated(System.currentTimeMillis());
                    levelConfig.setStatDate(Long.parseLong(new DateTime().toString("yyyyMMddHH")));
                    userLevelConfigMapper.updateByPrimaryKeySelective(levelConfig);
                } catch (Exception e) {
                    log.error("refreshUserLevelData error, continue next broker", e);
                } finally {
                    RedisLockUtils.releaseLock(redisTemplate, lockKey);
                }
            }
        }
    }

    private interface DataFetcher {
        List<StatisticsUserLevelData> fetchData(Broker broker, Condition condition, List<StatisticsUserLevelData> preStepDataList);
    }

    private interface BasicDataFetcher {
        List<StatisticsUserLevelData> fetchData(Broker broker, Condition condition, Long lastId, int pageSize);
    }


    public void refreshUserLevelData(Broker broker, UserLevelConfig levelConfig, String uuid) {
        long t0 = System.currentTimeMillis();
        if (levelConfig.getIsBaseLevel() == 1) {
            return;
        }
        if (levelConfig.getStatus() == 0) {
            deleteUnQualifiedUser(broker.getOrgId(), levelConfig.getId(), t0);
            return;
        }
        List<List<Condition>> conditions = JsonUtil.defaultGson().fromJson(levelConfig.getLevelCondition(),
                new TypeToken<List<List<Condition>>>() {}.getType());

        if (levelConfig.getLeaderStatus() == 1) {
            Condition condition = new Condition();
            condition.setKey("hobbitLeader");
            conditions.add(Lists.newArrayList(condition));
        }

        Set<String> conditionKeys = new HashSet<>();
        conditions.forEach(c -> {
            c.forEach(cc -> {
                conditionKeys.add(cc.getKey());
            });
        });

        for (List<Condition> list : conditions) {
            matchedLevelCondition(broker, levelConfig, list, conditionKeys, uuid);
        }

        deleteUnQualifiedUser(broker.getOrgId(), levelConfig.getId(), t0);
        log.info("uuid:{} broker:{} config:{} condition:{} consume:{}ms", uuid, broker.getOrgId(), levelConfig.getId(), levelConfig.getLevelCondition(), System.currentTimeMillis() - t0);
    }

    private BasicDataFetcher basicDataFetcher(String key) {
        switch (key) {
            case "bindMobile": return new UserBindMobileBasicDataFetcher();
            case "balanceAmount": return new BalanceAmountBasicDataFetcher(1);
            case "7dBalanceAmount": return new BalanceAmountBasicDataFetcher(7);
            case "30dBalanceAmount": return new BalanceAmountBasicDataFetcher(30);
            case "kycLevel": return new KycLevelBasicDataFetcher();
            case "spotUserFee": return new SpotUserFeeBasicDataFetcher();
            case "contractUserFee": return new ContractUserFeeBasicDataFetcher();
            case "30dSpotTradeAmountBtc": return new TradeAmountBasicDataFetcher(30, 1);
            case "30dContractTradeAmountBtc": return new TradeAmountBasicDataFetcher(30, 3);
        }
        return null;
    }

    private DataFetcher listDataFetcherByUsers(String key) {
        switch (key) {
            case "bindMobile": return new UserBindMobileDataFetcher();
            case "balanceAmount": return new BalanceAmountDataFetcher(1);
            case "7dBalanceAmount": return new BalanceAmountDataFetcher(7);
            case "30dBalanceAmount": return new BalanceAmountDataFetcher(30);
            case "kycLevel": return new KycLevelDataFetcher();
            case "spotUserFee": return new SpotUserFeeDataFetcher();
            case "contractUserFee": return new ContractUserFeeDataFetcher();
            case "30dSpotTradeAmountBtc": return new TradeAmountDataFetcher(30, 1);
            case "30dContractTradeAmountBtc": return new TradeAmountDataFetcher(30, 3);
        }
        return null;
    }

    //以第一个指标为基础，不断的向下找 一层一层的过滤，过滤到最后就是满足条件的用户
    public void matchedLevelCondition(Broker broker, UserLevelConfig levelConfig, List<Condition> conditions, Set<String> conditionKeys, String uuid) {
        log.info("uuid:{} start matchedLevelCondition id:{} condition:{}", uuid, levelConfig.getId(), conditions);
        if (CollectionUtils.isEmpty(conditions)) {
            return;
        }
        String mainKey = conditions.get(0).getKey();
        BasicDataFetcher basicDataFetcher = basicDataFetcher(mainKey);
        long lastId = 0;
        for (;;) {
            Map<Long, Map<String, String>> matchedUserInfo = Maps.newHashMap();
            long t0 = System.currentTimeMillis();
            List<StatisticsUserLevelData> userLevelDataList = basicDataFetcher.fetchData(broker, conditions.get(0), lastId, PAGE_SIZE);
            if (CollectionUtils.isEmpty(userLevelDataList)) {
                break;
            }
            List<StatisticsUserLevelData> fullDataList = fillBasicUserLevelData(broker.getOrgId(), userLevelDataList);
            fullDataList.forEach(d -> {
                Map<String, String> valueMap = matchedUserInfo.getOrDefault(d.getUserId(), Maps.newHashMap());
                valueMap.put(conditions.get(0).getKey(), d.getValue() != null ? d.getValue().stripTrailingZeros().toPlainString() : "");
                matchedUserInfo.put(d.getUserId(), valueMap);
            });

            int mainListSize = userLevelDataList.size();
            lastId = userLevelDataList.get(userLevelDataList.size() - 1).getId();
            log.info("uuid:{} origin key:{} size:{} lastId:{}", uuid, conditions.get(0).getKey(), mainListSize, lastId);

            userLevelDataList = fullDataList;
            for (int i = 1; i < conditions.size(); i++) {
                Condition condition = conditions.get(i);
                DataFetcher usersFetcher = listDataFetcherByUsers(condition.getKey());
                userLevelDataList = usersFetcher.fetchData(broker, condition, userLevelDataList);
                if (CollectionUtils.isEmpty(userLevelDataList)) {
                    break;
                }
                userLevelDataList = mixUserLevelData(fullDataList, userLevelDataList);
                log.info("uuid:{} key:{} fullsize:{} size:{}", uuid, condition.getKey(), fullDataList.size(), userLevelDataList.size());
                userLevelDataList.forEach(d -> {
                    Map<String, String> valueMap = matchedUserInfo.getOrDefault(d.getUserId(), Maps.newHashMap());
                    valueMap.put(condition.getKey(), d.getValue() != null ? d.getValue().stripTrailingZeros().toPlainString() : "");
                    matchedUserInfo.put(d.getUserId(), valueMap);
                });
            }

            if (CollectionUtils.isEmpty(userLevelDataList)) {
                continue;
            }

            Set<Long> sets = matchedUserInfo.keySet().stream().collect(Collectors.toSet());
            Set<Long> leftUserIds = userLevelDataList.stream().map(d -> d.getUserId()).collect(Collectors.toSet());
            sets.forEach(u -> {
                if (!leftUserIds.contains(u)) {
                    matchedUserInfo.remove(u);
                }
            });

            log.info("uuid:{} consume time : {} ms", uuid, System.currentTimeMillis() - t0);
            log.info("uuid:{} levelConfig:{} m:{}", uuid, levelConfig.getId(), matchedUserInfo);

            saveUserLevel(broker.getOrgId(), levelConfig, matchedUserInfo, conditionKeys);

            if (mainListSize < PAGE_SIZE) {
                break;
            }
        }
    }

    private String getDateStr(long time) {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        String str = format.format(new Date(time));
        return str;
    }

    private void saveUserLevel(long orgId, UserLevelConfig levelConfig, Map<Long, Map<String, String>> matchedUserInfo, Set<String> conditionKeys) {
        if (CollectionUtils.isEmpty(matchedUserInfo)) {
            return;
        }
        Set<Long> matchedUsers = matchedUserInfo.keySet();
        Example example = new Example(UserLevel.class);
        example.createCriteria().andEqualTo("orgId", orgId)
                .andEqualTo("levelConfigId", levelConfig.getId())
                .andIn("userId", matchedUsers);
        List<UserLevel> existedUserLevels = userLevelMapper.selectByExample(example);
        if (existedUserLevels == null) {
            existedUserLevels = Lists.newArrayList();
        }

        for (UserLevel userLevel : existedUserLevels) {
            Map<String, String> levelDataMap = JsonUtil.defaultGson().fromJson(userLevel.getLevelData(),
                    new TypeToken<Map<String, String>>() {}.getType());
            levelDataMap.putAll(matchedUserInfo.get(userLevel.getUserId()));

            List<String> deleteKeys = levelDataMap.keySet().stream()
                    .filter(k -> !conditionKeys.contains(k)).collect(Collectors.toList());
            deleteKeys.forEach(k -> levelDataMap.remove(k));
            userLevel.setLevelData(JsonUtil.defaultGson().toJson(levelDataMap));
            userLevel.setUpdated(System.currentTimeMillis());
            userLevel.setStatus(userLevel.getStatus() != UserLevel.WHITE_LIST_STATUS ? levelConfig.getStatus() : UserLevel.WHITE_LIST_STATUS); //白名单状态的用户状态不可变更
            userLevelMapper.updateByPrimaryKeySelective(userLevel);
        }

        List<UserLevel> userLevels = existedUserLevels;
        Predicate<Long> filterFunc = u -> {
            return userLevels.stream().map(d -> d.getUserId())
                    .noneMatch(userid -> userid.equals(u));
        };
        List<Long> insertedUsers = matchedUsers.stream()
                .filter(filterFunc)
                .collect(Collectors.toList());

        long now = System.currentTimeMillis();
        List<UserLevel> insertedUserLevels = insertedUsers.stream().map(userId -> {
            UserLevel userLevel = UserLevel.builder()
                    .levelConfigId(levelConfig.getId())
                    .orgId(orgId)
                    .userId(userId)
                    .levelData(JsonUtil.defaultGson().toJson(matchedUserInfo.get(userId)))
                    .status(1)
                    .created(now)
                    .updated(now)
                    .build();
            return userLevel;
        }).collect(Collectors.toList());
        if (!CollectionUtils.isEmpty(insertedUserLevels)) {
            userLevelMapper.insertList(insertedUserLevels);
        }
    }

    //删除原有不合格的用户等级
    public void deleteUnQualifiedUser(long orgId, long levelConfigId, long time) {
        Example example = new Example(UserLevel.class);
        example.createCriteria().andEqualTo("orgId", orgId)
                .andEqualTo("levelConfigId", levelConfigId)
                .andEqualTo("status", 1)
                .andLessThan("updated", time);

        UserLevel ul = new UserLevel();
        ul.setStatus(0);
        ul.setUpdated(System.currentTimeMillis());

        int rows = userLevelMapper.updateByExampleSelective(ul, example);
        log.info("levelConfigId:{} deleteUnQualifiedUser rows : {}", levelConfigId, rows);
    }

    @Data
    public static class Condition {
        private String key;
        private BigDecimal minValue = BigDecimal.ZERO;
        private BigDecimal maxValue = BigDecimal.ZERO;
        private Integer value;
        private String tokenId;
    }



    private class UserBindMobileDataFetcher implements DataFetcher {
        //bindMobile|true
        public List<StatisticsUserLevelData> fetchData(Broker broker, Condition condition, List<StatisticsUserLevelData> preStepDataList) {
            List<Long> brokerUsers = preStepDataList.stream()
                    .filter(d -> d.getUserId() != null)
                    .map(d -> d.getUserId()).distinct()
                    .collect(Collectors.toList());
            Example example = Example.builder(User.class)
                    .build()
                    .selectProperties("userId");
            example.createCriteria()
                    .andEqualTo("orgId", broker.getOrgId())
                    .andEqualTo("userStatus", UserStatusEnum.ENABLE.getValue())
                    .andIn("userId", brokerUsers);
            List<User> users = userMapper.selectByExample(example);
            if (CollectionUtils.isEmpty(users)) {
                return Lists.newArrayList();
            }
            List<StatisticsUserLevelData> list = users.stream()
                    .map(s -> {
                        StatisticsUserLevelData data = new StatisticsUserLevelData();
                        data.setUserId(s.getUserId());
                        data.setId(s.getUserId());
                        data.setValue(BigDecimal.ONE);
                        return data;
                    })
                    .collect(Collectors.toList());
            return !CollectionUtils.isEmpty(list) ? list : Lists.newArrayList();
        }
    }

    private class UserBindMobileBasicDataFetcher implements BasicDataFetcher {
        //bindMobile|true
        public List<StatisticsUserLevelData> fetchData(Broker broker, Condition condition, Long lastId, int pageSize) {
            Example example = Example.builder(User.class)
                    .orderByAsc("userId")
                    .build()
                    .selectProperties("userId", "mobile");
            PageHelper.startPage(1, pageSize);
            Example.Criteria criteria = example.createCriteria();
            criteria.andEqualTo("orgId", broker.getOrgId());
            criteria.andEqualTo("userStatus", UserStatusEnum.ENABLE.getValue());
            criteria.andNotEqualTo("mobile", "");
            criteria.andGreaterThan("userId", lastId);
            List<User> users = userMapper.selectByExample(example);
            if (CollectionUtils.isEmpty(users)) {
                return Lists.newArrayList();
            }
            List<StatisticsUserLevelData> list = users.stream()
                    .map(s -> {
                        StatisticsUserLevelData data = new StatisticsUserLevelData();
                        data.setUserId(s.getUserId());
                        data.setId(s.getUserId());
                        data.setValue(BigDecimal.ONE);
                        return data;
                    })
                    .collect(Collectors.toList());
            return !CollectionUtils.isEmpty(list) ? list : Lists.newArrayList();
        }
    }

    private class KycLevelDataFetcher implements DataFetcher {
        //kycLevel|2
        public List<StatisticsUserLevelData> fetchData(Broker broker, Condition condition, List<StatisticsUserLevelData> preStepDataList) {
            List<Long> brokerUsers = preStepDataList.stream()
                    .filter(d -> d.getUserId() != null)
                    .map(d -> d.getUserId()).distinct()
                    .collect(Collectors.toList());
            Example example = Example.builder(UserVerify.class)
                    .build()
                    .selectProperties("userId", "kycLevel", "verifyStatus");
            example.createCriteria()
                    .andEqualTo("orgId", broker.getOrgId())
                    .andIn("userId", brokerUsers)
                    .andGreaterThanOrEqualTo("kycLevel", condition.getValue()*10);
            List<UserVerify> verifies = userVerifyMapper.selectByExample(example);
            if (CollectionUtils.isEmpty(verifies)) {
                return Lists.newArrayList();
            }

            List<StatisticsUserLevelData> list = verifies.stream()
                    .filter(d -> (d.getVerifyStatus() == UserVerifyStatus.PASSED.value() || d.getKycLevel()/10 > condition.getValue()))
                    .map(s -> {
                        StatisticsUserLevelData data = new StatisticsUserLevelData();
                        data.setUserId(s.getUserId());
                        data.setValue(new BigDecimal(s.getVerifyStatus() == UserVerifyStatus.PASSED.value() ? s.getKycLevel()/10 : s.getKycLevel()/10-1));
                        return data;
                    })
                    .collect(Collectors.toList());
            return !CollectionUtils.isEmpty(list) ? list : Lists.newArrayList();
        }
    }

    private class KycLevelBasicDataFetcher implements BasicDataFetcher {
        //kycLevel|2
        public List<StatisticsUserLevelData> fetchData(Broker broker, Condition condition, Long lastId, int pageSize) {
            Example example = Example.builder(UserVerify.class)
                    .orderByAsc("id")
                    .build()
                    .selectProperties("id", "userId", "kycLevel", "verifyStatus");
            PageHelper.startPage(1, pageSize);
            example.createCriteria()
                    .andEqualTo("orgId", broker.getOrgId())
                    .andGreaterThan("id", lastId)
                    .andGreaterThanOrEqualTo("kycLevel", condition.getValue()*10);
            List<UserVerify> verifies = userVerifyMapper.selectByExample(example);
            if (CollectionUtils.isEmpty(verifies)) {
                return Lists.newArrayList();
            }
            List<StatisticsUserLevelData> list = verifies.stream()
                    .filter(d -> (d.getVerifyStatus() == UserVerifyStatus.PASSED.value() || d.getKycLevel()/10 > condition.getValue()))
                    .map(s -> {
                        StatisticsUserLevelData data = new StatisticsUserLevelData();
                        data.setUserId(s.getUserId());
                        data.setId(s.getId());
                        data.setValue(new BigDecimal(s.getVerifyStatus() == UserVerifyStatus.PASSED.value() ? s.getKycLevel()/10 : s.getKycLevel()/10-1));
                        return data;
                    })
                    .collect(Collectors.toList());
            return !CollectionUtils.isEmpty(list) ? list : Lists.newArrayList();
        }
    }

    private class BalanceAmountDataFetcher implements DataFetcher {
        private int dayNum;

        public BalanceAmountDataFetcher(int dayNum) {
            this.dayNum = dayNum;
        }
        @Override
        public List<StatisticsUserLevelData> fetchData(Broker broker, Condition condition, List<StatisticsUserLevelData> preStepDataList) {
            List<Long> brokerUsers = preStepDataList.stream()
                    .filter(d -> d.getUserId() != null)
                    .map(d -> d.getUserId()).distinct()
                    .collect(Collectors.toList());
            List<StatisticsUserLevelData> list = statisticsUserLevelDataMapper.listBalanceAmountByUsers(broker.getOrgId(),
                    condition.getTokenId(), dayNum, brokerUsers, condition.getMinValue(), condition.getMaxValue());
            return !CollectionUtils.isEmpty(list) ? list : Lists.newArrayList();
        }
    }

    private class BalanceAmountBasicDataFetcher implements BasicDataFetcher {
        private int dayNum;
        public BalanceAmountBasicDataFetcher(int dayNum) {
            this.dayNum = dayNum;
        }
        @Override
        public List<StatisticsUserLevelData> fetchData(Broker broker, Condition condition, Long lastId, int pageSize) {
            List<StatisticsUserLevelData> list = statisticsUserLevelDataMapper.listBalanceAmount(broker.getOrgId(),
                    condition.getTokenId(), dayNum, lastId, PAGE_SIZE, condition.getMinValue(), condition.getMaxValue());
            return !CollectionUtils.isEmpty(list) ? list : Lists.newArrayList();
        }
    }

    private class SpotUserFeeDataFetcher implements DataFetcher {
        //spotUserFee|5000|10000 [5000,10000)
        public List<StatisticsUserLevelData> fetchData(Broker broker, Condition condition, List<StatisticsUserLevelData> preStepDataList) {
            List<Long> accountIds = preStepDataList.stream()
                    .filter(d -> d.getSpotAccountId() != null)
                    .map(d -> d.getSpotAccountId()).distinct()
                    .collect(Collectors.toList());
            List<StatisticsUserLevelData> list = statisticsUserLevelDataMapper.listSpotUserFeeByUsers(broker.getOrgId(),
                    accountIds, condition.getMinValue(), condition.getMaxValue());
            return !CollectionUtils.isEmpty(list) ? list : Lists.newArrayList();
        }
    }

    private class SpotUserFeeBasicDataFetcher implements BasicDataFetcher {
        //spotUserFee|5000|10000 [5000,10000)
        public List<StatisticsUserLevelData> fetchData(Broker broker, Condition condition, Long lastId, int pageSize) {
            List<StatisticsUserLevelData> list = statisticsUserLevelDataMapper.listSpotUserFees(broker.getOrgId(),
                    Long.parseLong(lastId.toString()), PAGE_SIZE, condition.getMinValue(), condition.getMaxValue());
            return !CollectionUtils.isEmpty(list) ? list : Lists.newArrayList();
        }
    }

    private class ContractUserFeeDataFetcher implements DataFetcher {
        //contractUserFee|5000|10000 [5000,10000)
        public List<StatisticsUserLevelData> fetchData(Broker broker, Condition condition, List<StatisticsUserLevelData> preStepDataList) {
            List<Long> accountIds = preStepDataList.stream()
                    .map(d -> d.getContractAccountId()).distinct()
                    .collect(Collectors.toList());
            List<StatisticsUserLevelData> list = statisticsUserLevelDataMapper.listContractUserFeeByUsers(broker.getOrgId(),
                    accountIds, condition.getMinValue(), condition.getMaxValue());
            return !CollectionUtils.isEmpty(list) ? list : Lists.newArrayList();
        }
    }

    private class ContractUserFeeBasicDataFetcher implements BasicDataFetcher {
        //contractUserFee|5000|10000 [5000,10000)
        public List<StatisticsUserLevelData> fetchData(Broker broker, Condition condition, Long lastId, int pageSize) {
            List<StatisticsUserLevelData> list = statisticsUserLevelDataMapper.listContractUserFees(broker.getOrgId(),
                    Long.parseLong(lastId.toString()), PAGE_SIZE, condition.getMinValue(), condition.getMaxValue());
            return !CollectionUtils.isEmpty(list) ? list : Lists.newArrayList();
        }
    }

    private class TradeAmountDataFetcher implements DataFetcher {
        private int dayNum;
        private int tradeType;
        public TradeAmountDataFetcher(int dayNum, int tradeType) {
            this.dayNum = dayNum;
            this.tradeType = tradeType;
        }
        @Override
        public List<StatisticsUserLevelData> fetchData(Broker broker, Condition condition, List<StatisticsUserLevelData> preStepDataList) {
            List<Long> userIds = preStepDataList.stream()
                    .map(d -> d.getUserId())
                    .filter(a -> a != null)
                    .collect(Collectors.toList());
            String startDate = getDateStr(System.currentTimeMillis() - dayNum*24*3600_000L);
            String endDate = getDateStr(System.currentTimeMillis());
            List<StatisticsUserLevelData> list = statisticsUserLevelDataMapper.listTradeAmountBtcByUsers(broker.getOrgId(),
                    tradeType, userIds, condition.getMinValue(), condition.getMaxValue(),
                    startDate, endDate);
            return !CollectionUtils.isEmpty(list) ? list : Lists.newArrayList();
        }
    }

    private class TradeAmountBasicDataFetcher implements BasicDataFetcher {
        private int dayNum;
        private int tradeType;
        public TradeAmountBasicDataFetcher(int dayNum, int tradeType) {
            this.dayNum = dayNum;
            this.tradeType = tradeType;
        }
        @Override
        public List<StatisticsUserLevelData> fetchData(Broker broker, Condition condition, Long lastId, int pageSize) {
            String startDate = getDateStr(System.currentTimeMillis() - dayNum*24*3600_000L);
            String endDate = getDateStr(System.currentTimeMillis());
            List<StatisticsUserLevelData> list = statisticsUserLevelDataMapper.listTradeAmountBtc(broker.getOrgId(),
                    tradeType, lastId, PAGE_SIZE, condition.getMinValue(), condition.getMaxValue(),
                    startDate, endDate);
            return !CollectionUtils.isEmpty(list) ? list : Lists.newArrayList();
        }
    }

    /**
     * @param brokerId
     * @param list
     */
    private List<StatisticsUserLevelData> fillBasicUserLevelData(long brokerId, List<StatisticsUserLevelData> list) {
        if (CollectionUtils.isEmpty(list)) {
            return Lists.newArrayList();
        }

        List<Long> userIds = list.stream()
                .filter(s -> s.getUserId() != null)
                .map(s -> s.getUserId())
                .collect(Collectors.toList());
        List<Long> spotAccountIds = list.stream()
                .filter(s -> s.getSpotAccountId() != null)
                .map(s -> s.getSpotAccountId())
                .collect(Collectors.toList());
        List<Long> contractAccountIds = list.stream()
                .filter(s -> s.getContractAccountId() != null)
                .map(s -> s.getContractAccountId())
                .collect(Collectors.toList());
        List<Long> optionAccountIds = list.stream()
                .filter(s -> s.getContractAccountId() != null)
                .map(s -> s.getContractAccountId())
                .collect(Collectors.toList());

        Example example = Example.builder(Account.class)
                .build()
                .selectProperties("accountId", "userId", "accountType");
        Example.Criteria criteria = example.createCriteria()
                .andEqualTo("orgId", brokerId);
        if (!CollectionUtils.isEmpty(userIds)) {
            criteria.andIn("userId", userIds);
        } else if (!CollectionUtils.isEmpty(spotAccountIds)) {
            criteria.andIn("accountId", spotAccountIds);
        } else if (!CollectionUtils.isEmpty(contractAccountIds)) {
            criteria.andIn("accountId", contractAccountIds);
        } else if (!CollectionUtils.isEmpty(optionAccountIds)) {
            criteria.andIn("accountId", optionAccountIds);
        }

        List<Account> accounts = accountMapper.selectByExample(example);
        if (CollectionUtils.isEmpty(list)) { //不会发生除非数据都是错误或假的
            return Lists.newArrayList();
        }

        List<StatisticsUserLevelData> userDataList = Lists.newArrayList();
        Map<Long, List<Account>> accountMap = accounts.stream()
                .collect(Collectors.groupingBy(Account::getUserId));
        for (Long userId : accountMap.keySet()) {
            StatisticsUserLevelData userLevelData = new StatisticsUserLevelData();
            userLevelData.setUserId(userId);
            for (Account account : accountMap.get(userId)) {
                int type = account.getAccountType();
                if (type == AccountType.MAIN.value()) {
                    userLevelData.setSpotAccountId(account.getAccountId());
                } else if (type == AccountType.OPTION.value()) {
                    userLevelData.setOptionAccountId(account.getAccountId());
                } else if (type == AccountType.FUTURES.value()) {
                    userLevelData.setContractAccountId(account.getAccountId());
                }
            }
            for (StatisticsUserLevelData d : list) {
                if (d.getUserId() != null && d.getUserId().equals(userLevelData.getUserId())) {
                    userLevelData.setValue(d.getValue());
                    break;
                }
                if (d.getOptionAccountId() != null && d.getOptionAccountId().equals(userLevelData.getOptionAccountId())) {
                    userLevelData.setValue(d.getValue());
                    break;
                }
                if (d.getSpotAccountId() != null && d.getSpotAccountId().equals(userLevelData.getSpotAccountId())) {
                    userLevelData.setValue(d.getValue());
                    break;
                }
                if (d.getContractAccountId() != null && d.getContractAccountId().equals(userLevelData.getContractAccountId())) {
                    userLevelData.setValue(d.getValue());
                    break;
                }
            }
            userDataList.add(userLevelData);
        }

        if (CollectionUtils.isEmpty(userIds)) {
            log.info("no userid try again to fill userid");
            fillBasicUserLevelData(brokerId, userDataList);
        }
        return userDataList;
    }

    private List<StatisticsUserLevelData> mixUserLevelData(List<StatisticsUserLevelData> orginList, List<StatisticsUserLevelData> dataList) {
        if (CollectionUtils.isEmpty(orginList) || CollectionUtils.isEmpty(dataList)) {
            return Lists.newArrayList();
        }

        Map<Long, StatisticsUserLevelData> originMap = new HashMap<>();
        orginList.forEach(d -> {
            originMap.put(d.getUserId(), d);
            originMap.put(d.getSpotAccountId(), d);
            originMap.put(d.getContractAccountId(), d);
            originMap.put(d.getOptionAccountId(), d);
        });


        for (StatisticsUserLevelData data : dataList) {
            StatisticsUserLevelData originData = null;
            if (data.getUserId() != null) {
                originData = originMap.get(data.getUserId());
            } else if (data.getSpotAccountId() != null) {
                originData = originMap.get(data.getSpotAccountId());
            } else if (data.getContractAccountId() != null) {
                originData = originMap.get(data.getContractAccountId());
            } else if (data.getOptionAccountId() != null) {
                originData = originMap.get(data.getOptionAccountId());
            }
            if (originData == null) {
                continue;
            }
            data.setUserId(originData.getUserId());
            data.setSpotAccountId(originData.getSpotAccountId());
            data.setContractAccountId(originData.getContractAccountId());
            data.setOptionAccountId(originData.getOptionAccountId());
        }
        return dataList;
    }
}
