package io.bhex.broker.server.grpc.server.service.activity;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.bhex.broker.common.util.JsonUtil;
import io.bhex.broker.server.domain.CommonIni;
import io.bhex.broker.server.grpc.server.service.CommonIniService;
import io.bhex.broker.server.grpc.server.service.UserService;
import io.bhex.broker.server.primary.mapper.ActivityDoyenCardUnlockLogMapper;
import io.bhex.broker.server.model.ActivityDoyenCardUnlockLog;
import io.bhex.broker.server.model.ActivitySendCardLogDaily;
import io.bhex.broker.server.model.User;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.stereotype.Service;
import tk.mybatis.mapper.entity.Example;

import javax.annotation.Resource;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.stream.Collectors;

@Service
@Slf4j
public class RiskControlService {

    @Resource(name = "activityRepositoryService")
    private ActivityRepositoryService activityRepositoryService;

    @Resource
    private UserService userService;

    @Resource
    private CommonIniService commonIniService;

    @Resource
    private ActivityDoyenCardUnlockLogMapper activityDoyenCardUnlockLogMapper;

    private int pageSize = 50;

    public SendDoyenCardCheckResult sendDoyenCardCheck(Long userId, List<Long> followerIds) {

        //检查同一用户的follower的ip+域名的每天是否超过发放限制
        Map<Long, String> followerIdentities = buildUserIdentity(followerIds);
        //筛选出小于限制的卡券发放记录
        Map<Long, ActivitySendCardLogDaily> finalLogMap = getValidLogMap(userId, followerIdentities);

        SendDoyenCardCheckResult sdccr = new SendDoyenCardCheckResult();
        sdccr.setValidUserIds(Lists.newArrayList(finalLogMap.keySet()));
        sdccr.setDailyLogs(Lists.newArrayList(Sets.newHashSet(finalLogMap.values())));

        return sdccr;

    }

    public void updateRiskRecord(Collection<ActivitySendCardLogDaily> logs) {

        boolean success = true;

        Timestamp now = Timestamp.from(new Date().toInstant());
        for (ActivitySendCardLogDaily ld : logs) {
            int cardNumber = ld.getCardNumber().intValue();
            if (cardNumber == 0) {
                ld.setCardNumber(1);
                success &= activityRepositoryService.saveSendCardLogDaily(ld);
            } else {

                ActivitySendCardLogDaily tmp = ActivitySendCardLogDaily.builder()
                        .id(ld.getId())
                        .cardNumber(cardNumber + 1)
                        .updateTime(now)
                        .version(ld.getVersion() + 1)
                        .build();

                success &= activityRepositoryService.updateSendCardLogDaily(ld.getVersion(), tmp);
            }
        }

        if (!success) {
            throw new IllegalStateException("Save ActivitySendCardLogDaily fail...,param=" + JsonUtil.defaultGson().toJson(logs));
        }

    }


    private Map<Long, String> buildUserIdentity(List<Long> userIds) {
        List<User> total = Lists.newArrayList();

        List<List<Long>> multiList = Lists.partition(userIds, pageSize);
        for (List<Long> list : multiList) {
            List<User> tmp = userService.getUsers(list);
            total.addAll(tmp);
        }

        List<Pair<Long, String>> userIdentities = total.stream().map(i -> {
            String identity = null;

            if (StringUtils.isNotEmpty(i.getMobile())) {
                identity = i.getMobile();
                return Pair.of(i.getUserId(), identity);
            }

            String email = StringUtils.defaultIfBlank(i.getEmail(), "");
            String ip = StringUtils.defaultIfBlank(i.getIp(), "");

            String emailDomain = StringUtils.substringAfter(email, "@");
            identity = ip + "|" + emailDomain;
            return Pair.of(i.getUserId(), identity);
        }).collect(Collectors.toList());

        return userIdentities.stream().collect(Collectors.toMap(i -> i.getKey(), i -> i.getValue()));
    }

    private Map<Long, ActivitySendCardLogDaily> getValidLogMap(Long userId, Map<Long, String> followerIdentities) {

        int dailyLimit = 3;
        CommonIni commonIni = commonIniService.getCommonIni(9001L, "sencVoucherLimitDaily");
        if (commonIni != null && !Strings.isNullOrEmpty(commonIni.getIniValue())) {

            try {
                dailyLimit = Integer.parseInt(commonIni.getIniValue());

            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }

        }

        final int baseLimit = dailyLimit;

        Date date = atStartOfDay();
        Timestamp now = Timestamp.from(new Date().toInstant());

        List<ActivitySendCardLogDaily> logDailies = activityRepositoryService.listSendCardDailyLog(userId, Lists.newArrayList(followerIdentities.values()), date);

        Map<Long, ActivitySendCardLogDaily> defaultResult = Maps.newHashMap();
        followerIdentities.forEach((k, v) -> {

            ActivitySendCardLogDaily logDaily = ActivitySendCardLogDaily.builder()
                    .identity(v)
                    .userId(userId)
                    .cardNumber(0)
                    .sendDay(date.getTime())
                    .createTime(now)
                    .updateTime(now)
                    .version(1)
                    .build();

            defaultResult.put(k, logDaily);
        });

        if (Objects.isNull(logDailies) || logDailies.isEmpty()) {
            return defaultResult;
        }

        //筛选出未超过限制的记录
        Map<Long, ActivitySendCardLogDaily> result = Maps.newHashMap();

        Map<String, ActivitySendCardLogDaily> logMap = logDailies.stream().collect(Collectors.toMap(i -> i.getIdentity(), i -> i));

        followerIdentities.forEach((k, v) -> {


            ActivitySendCardLogDaily logDaily = logMap.get(v);
            if (Objects.isNull(logDaily)) {
                logDaily = defaultResult.get(k);
                result.put(k, logDaily);
            } else if (logDaily.getCardNumber().intValue() < baseLimit) {
                result.put(k, logDaily);
            }
        });

        return result;
    }

    private Date atStartOfDay() {
        LocalDateTime ldt = LocalDate.now().atStartOfDay();
        TimeZone tz = TimeZone.getTimeZone("UTC");
        ZoneId zoneId = tz.toZoneId();
        ZonedDateTime zdt = ldt.atZone(zoneId);
        return Date.from(zdt.toInstant());
    }


    public List<Long> filterValidFollowIdsForUnlockDoyenCard(Long userId, List<Long> followerIds) {

        Example exp = new Example(ActivityDoyenCardUnlockLog.class);
        exp.createCriteria()
                .andEqualTo("userId", userId)
                .andIn("followerId", followerIds);

        List<ActivityDoyenCardUnlockLog> list = activityDoyenCardUnlockLogMapper.selectByExample(exp);
        List<Long> excludeIds = list.stream().map(i -> i.getFollowerId()).collect(Collectors.toList());

        Set<Long> baseIdSet = Sets.newHashSet(followerIds);
        Set<Long> excludeIdSet = Sets.newHashSet(excludeIds);
        List<Long> validIds = Lists.newArrayList(Sets.difference(baseIdSet, excludeIdSet));
        return validIds;
    }

    public void saveDoyenCardUnlockLog(Long userId, Set<Long> depositUserIds) {

        Timestamp now = Timestamp.from(new Date().toInstant());

        depositUserIds.forEach(i -> {

            ActivityDoyenCardUnlockLog adcl = ActivityDoyenCardUnlockLog
                    .builder()
                    .userId(userId)
                    .followerId(i)
                    .createTime(now)
                    .build();

            int row = activityDoyenCardUnlockLogMapper.insertSelective(adcl);
            if (row != 1) {
                throw new IllegalStateException("Save doyenCard unlock log faile,userId=" + userId + ",followerId=" + i);
            }
        });
    }


    @Data
    public static class SendDoyenCardCheckResult {

        private List<Long> validUserIds;

        private List<ActivitySendCardLogDaily> dailyLogs;

        public boolean passed() {
            if (Objects.isNull(this.validUserIds) || validUserIds.isEmpty()) {
                return false;
            }

            return true;
        }

    }
}
