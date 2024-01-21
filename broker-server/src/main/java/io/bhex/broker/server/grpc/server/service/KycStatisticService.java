package io.bhex.broker.server.grpc.server.service;

import io.bhex.broker.server.domain.BrokerLockKeys;
import io.bhex.broker.server.domain.UserVerifyStatus;
import io.bhex.broker.server.model.*;
import io.bhex.broker.server.primary.mapper.*;
import io.bhex.broker.server.util.RedisLockUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import javax.annotation.Resource;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @Description:
 * @Date: 2018/12/13 下午3:13
 * @Author: liwei
 * @Copyright（C）: 2018 BlueHelix Inc. All rights reserved.
 */
@Slf4j
@Service
public class KycStatisticService {


    @Resource
    private UserMapper userMapper;

    @Resource
    private UserKycApplyMapper userKycApplyMapper;
    @Resource
    private KycStatisticMapper kycStatisticMapper;

    @Resource(name = "stringRedisTemplate")
    private StringRedisTemplate redisTemplate;

    public static final int LIMIT = 1000;

    @Scheduled(cron = "30 4/5  * * * ?")
    public void loadKycData() {
        String lockKey = BrokerLockKeys.KYC_STATISTIC_KEY;
        boolean lock = RedisLockUtils.tryLock(redisTemplate, lockKey, BrokerLockKeys.KYC_STATISTIC_KEY_EXPIRE);
        if (!lock) {
            return;
        }
        try {
            for (int i = 0; i < 10; i++) {
                Long lastVerifyId = kycStatisticMapper.getLastVerifyHistoryId();
                log.info("load kyc data{}", lastVerifyId);
                List<UserKycApply> histories = userKycApplyMapper.getUserVerifyHistories(lastVerifyId, LIMIT);
                if (CollectionUtils.isEmpty(histories)) {
                    log.info("no kyc data");
                    return;
                }
                List<KycStatistic> statistics = getStatistics(histories);
                saveStatistics(statistics);
                if (histories.size() < LIMIT) {
                    return;
                }
            }
        } finally {
            RedisLockUtils.releaseLock(redisTemplate, lockKey);
        }
    }

    private String getRegPlatform(User user) {
        String platform = user.getPlatform();
        if ("PC".equals(platform)) {
            return "PC";
        }
        if ("MOBILE".equals(platform)) {
            String header = user.getAppBaseHeader();
            if (StringUtils.isEmpty(header)) {
                return "ANDROID";
            }
            return header.toUpperCase().contains("IOS") ? "IOS" : "ANDROID";
        }
        return "PC";
    }

    private List<KycStatistic> getStatistics(List<UserKycApply> histories) {
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        List<KycStatistic> statistics = new ArrayList<>(histories.size());
        for (UserKycApply history : histories) {
            KycStatistic kycStatistic = getStatistic(history, dateFormat);
            if (kycStatistic != null) {
                statistics.add(kycStatistic);
            }
        }
        return statistics;
    }

    public KycStatistic getStatistic(UserKycApply history, DateFormat dateFormat) {
        try {
            User user = userMapper.getByUserId(history.getUserId());
            String platform = getRegPlatform(user);
            KycStatistic statistic = new KycStatistic();
            statistic.setOrgId(user.getOrgId());
            statistic.setStatisticDate(dateFormat.format(history.getUpdated()));
            if (history.getVerifyStatus() == UserVerifyStatus.PASSED.value()) {
                statistic.incrPassedKycNumber(1);
                if (platform.equals("PC")) {
                    statistic.incrPcPassedKycNumber(1);
                } else if (platform.equals("ANDROID")) {
                    statistic.incrAndroidPassedKycNumber(1);
                } else if (platform.equals("IOS")) {
                    statistic.incrIosPassedKycNumber(1);
                }
            } else if (history.getVerifyStatus() == UserVerifyStatus.REFUSED.value()) {
                statistic.incrRejectKycNumber(1);
                if (platform.equals("PC")) {
                    statistic.incrPcRejectKycNumber(1);
                } else if (platform.equals("ANDROID")) {
                    statistic.incrAndroidRejectKycNumber(1);
                } else if (platform.equals("IOS")) {
                    statistic.incrIosRejectKycNumber(1);
                }
            }
            statistic.setLastVerifyHistoryId(history.getUpdated());
            return statistic;
        } catch (Exception e) {
            log.warn("get kyc statistic error:{}", history.getUserId());
        }
        return null;

    }

    private void saveStatistics(List<KycStatistic> statistics) {
        Map<Long, Map<String, List<KycStatistic>>> groups = statistics.stream()
                .collect(Collectors.groupingBy(KycStatistic::getOrgId, Collectors.groupingBy(KycStatistic::getStatisticDate)));
        groups.forEach((orgId, statMap) -> {
            statMap.forEach((statisticDate, list) -> {
                list = list.stream().sorted(Comparator.comparing(KycStatistic::getLastVerifyHistoryId)).collect(Collectors.toList());
                log.info("orgId:{} statisticDate:{} length:{}", orgId, statisticDate, list.size());
                KycStatistic incrStat = new KycStatistic();
                for (KycStatistic statistic : list) {
                    incrStat = add(incrStat, statistic);
                }
                saveStatistic(incrStat, orgId, statisticDate);
            });
        });

    }

    private void saveStatistic(KycStatistic incrStat, Long orgId, String statisticDate) {
        KycStatistic dailyStat = add(kycStatisticMapper.getDailyKycStatisticByDate(orgId, statisticDate), incrStat);
        if (dailyStat.getId() == null) {
            dailyStat.setCreated(new Timestamp(System.currentTimeMillis()));
            dailyStat.setUpdated(new Timestamp(System.currentTimeMillis()));
            dailyStat.setOrgId(orgId);
            dailyStat.setAggregate(0);
            kycStatisticMapper.insertSelective(dailyStat);
        } else {
            int count = kycStatisticMapper.updateByPrimaryKeySelective(dailyStat);
        }

        KycStatistic totalStat = add(kycStatisticMapper.getAggregateKycStatistic(orgId), incrStat);
        totalStat.setStatisticDate(statisticDate);
        kycStatisticMapper.updateByPrimaryKeySelective(totalStat);
    }


    private KycStatistic add(KycStatistic orginalStat, KycStatistic incrStat) {
        if (orginalStat == null) {
            return incrStat;
        }
        KycStatistic s = new KycStatistic();
        s.setStatisticDate(incrStat.getStatisticDate());
        s.setId(orginalStat.getId());
        s.setLastVerifyHistoryId(incrStat.getLastVerifyHistoryId());
        s.setUpdated(new Timestamp(System.currentTimeMillis()));
        s.setPassedKycNumber(orginalStat.getPassedKycNumber() + incrStat.getPassedKycNumber());
        s.setPcPassedKycNumber(orginalStat.getPcPassedKycNumber() + incrStat.getPcPassedKycNumber());
        s.setAndroidPassedKycNumber(orginalStat.getAndroidPassedKycNumber() + incrStat.getAndroidPassedKycNumber());
        s.setIosPassedKycNumber(orginalStat.getIosPassedKycNumber() + incrStat.getIosPassedKycNumber());
        s.setRejectKycNumber(orginalStat.getRejectKycNumber() + incrStat.getRejectKycNumber());
        s.setPcRejectKycNumber(orginalStat.getPcRejectKycNumber() + incrStat.getPcRejectKycNumber());
        s.setAndroidRejectKycNumber(orginalStat.getAndroidRejectKycNumber() + incrStat.getAndroidRejectKycNumber());
        s.setIosRejectKycNumber(orginalStat.getIosRejectKycNumber() + incrStat.getIosRejectKycNumber());
        return s;
    }


    public KycStatistic getAggregateKycStatistic(Long orgId) {
        return kycStatisticMapper.getAggregateKycStatistic(orgId);
    }

    public List<KycStatistic> getDailyKycStatistics(Long orgId, Long startDate, Long endDate) {
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        return kycStatisticMapper.getDailyKycStatistics(orgId, dateFormat.format(startDate), dateFormat.format(endDate));
    }
}
