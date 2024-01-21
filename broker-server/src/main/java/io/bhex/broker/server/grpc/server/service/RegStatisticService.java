package io.bhex.broker.server.grpc.server.service;

import com.google.common.collect.Lists;
import io.bhex.broker.server.domain.BrokerLockKeys;
import io.bhex.broker.server.primary.mapper.InviteRelationMapper;
import io.bhex.broker.server.primary.mapper.RegStatisticMapper;
import io.bhex.broker.server.primary.mapper.UserMapper;
import io.bhex.broker.server.model.InviteRelation;
import io.bhex.broker.server.model.RegStatistic;
import io.bhex.broker.server.model.User;
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
import java.util.*;
import java.util.stream.Collectors;

/**
 * @Description:
 * @Date: 2018/12/13 下午3:13
 * @Author: liwei
 * @Copyright（C）: 2018 BlueHelix Inc. All rights reserved.
 */
@Slf4j
@Service
public class RegStatisticService {


    @Resource
    private UserMapper userMapper;

    @Resource
    private InviteRelationMapper inviteRelationMapper;

    @Resource
    private RegStatisticMapper regStatisticMapper;

    @Resource(name = "stringRedisTemplate")
    private StringRedisTemplate redisTemplate;

    @Scheduled(cron = "15 1/10  * * * ?")
    public void loadRegData() {
        String lockKey = BrokerLockKeys.REG_STATISTIC_KEY;
        boolean lock = RedisLockUtils.tryLock(redisTemplate, lockKey, BrokerLockKeys.REG_STATISTIC_KEY_EXPIRE);
        if (!lock) {
            return;
        }
        try {
            for (int i = 0; i < 10; i++) {
                Long lastId = regStatisticMapper.getLastId();
                log.info("load data{}", lastId);
                List<User> users = userMapper.getUsers(lastId, 1000);
                if (CollectionUtils.isEmpty(users)) {
                    log.info("no data");
                    return;
                }
                List<RegStatistic> statistics = getStatistics(users);
                saveStatistics(statistics);
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

    private List<RegStatistic> getStatistics(List<User> users) {
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        List<RegStatistic> statistics = new ArrayList<>(users.size());
        for (User user : users) {
            String platform = getRegPlatform(user);
            RegStatistic statistic = new RegStatistic();
            statistic.setOrgId(user.getOrgId());
            statistic.setStatisticDate(dateFormat.format(user.getCreated()));
            statistic.incrRegNumber(1);
            if (platform.equals("PC")) {
                statistic.incrPcRegNumber(1);
            } else if (platform.equals("ANDROID")) {
                statistic.incrAndroidRegNumber(1);
            } else if (platform.equals("IOS")) {
                statistic.incrIosRegNumber(1);
            }

            if (user.getInviteUserId() == null || user.getInviteUserId() == 0) {
                statistic.incrNotInvitedRegNumber(1);
                if (platform.equals("PC")) {
                    statistic.incrPcNotInvitedRegNumber(1);
                } else if (platform.equals("ANDROID")) {
                    statistic.incrAndroidNotInvitedRegNumber(1);
                } else if (platform.equals("IOS")) {
                    statistic.incrIosNotInvitedRegNumber(1);
                }
            } else {
                List<InviteRelation> inviteRelations = inviteRelationMapper.getInviteRelationsByInvitedId(user.getOrgId(), user.getUserId());
                if (CollectionUtils.isEmpty(inviteRelations)) { //如果找不到邀请关系给个 1：直接
                    inviteRelations = Lists.newArrayList(InviteRelation.builder().invitedType(1).build());
                }
                for (InviteRelation relation : inviteRelations) {
                    //  `invited_type` int(2) NOT NULL COMMENT '被邀请类型  1：直接  2：间接',
                    if (relation.getInvitedType() == 1) {
                        statistic.incrDirectInviteRegNumber(1);
                        statistic.incrInviteRegNumber(1);
                        if (platform.equals("PC")) {
                            statistic.incrPcDirectInviteRegNumber(1);
                            statistic.incrPcInviteRegNumber(1);
                        } else if (platform.equals("ANDROID")) {
                            statistic.incrAndroidDirectInviteRegNumber(1);
                            statistic.incrAndroidInviteRegNumber(1);
                        } else if (platform.equals("IOS")) {
                            statistic.incrIosDirectInviteRegNumber(1);
                            statistic.incrIosInviteRegNumber(1);
                        }
                    } else if (relation.getInvitedType() == 2) {
                        statistic.incrIndirectInviteRegNumber(1);
                        if (platform.equals("PC")) {
                            statistic.incrPcIndirectInviteRegNumber(1);
                        } else if (platform.equals("ANDROID")) {
                            statistic.incrAndroidIndirectInviteRegNumber(1);
                        } else if (platform.equals("IOS")) {
                            statistic.incrIosIndirectInviteRegNumber(1);
                        }
                    }
                }
            }

            statistic.setLastId(user.getId());
            statistics.add(statistic);
        }
        return statistics;
    }

    private void saveStatistics(List<RegStatistic> statistics) {
        Map<Long, Map<String, List<RegStatistic>>> groups = statistics.stream()
                .collect(Collectors.groupingBy(RegStatistic::getOrgId, Collectors.groupingBy(RegStatistic::getStatisticDate)));
        groups.forEach((orgId, statMap) -> {
            statMap.forEach((statisticDate, list) -> {
                list = list.stream().sorted(Comparator.comparing(RegStatistic::getLastId)).collect(Collectors.toList());
                log.info("orgId:{} statisticDate:{} length:{}", orgId, statisticDate, list.size());
                RegStatistic incrStat = new RegStatistic();
                for (RegStatistic statistic : list) {
                    incrStat = add(incrStat, statistic);
                }
                saveStatistic(incrStat, orgId, statisticDate);
            });
        });

    }

    private void saveStatistic(RegStatistic incrStat, Long orgId, String statisticDate) {
        RegStatistic dailyStat = add(regStatisticMapper.getDailyRegStatisticByDate(orgId, statisticDate), incrStat);
        if (dailyStat.getId() == null) {
            dailyStat.setCreated(new Timestamp(System.currentTimeMillis()));
            dailyStat.setUpdated(new Timestamp(System.currentTimeMillis()));
            dailyStat.setOrgId(orgId);
            dailyStat.setAggregate(0);
            regStatisticMapper.insertSelective(dailyStat);
        } else {
            regStatisticMapper.updateByPrimaryKeySelective(dailyStat);
        }

        RegStatistic totalStat = add(regStatisticMapper.getAggregateRegStatistic(orgId), incrStat);
        totalStat.setStatisticDate(statisticDate);
        regStatisticMapper.updateByPrimaryKeySelective(totalStat);
    }


    private RegStatistic add(RegStatistic orginalStat, RegStatistic incrStat) {
        if (orginalStat == null) {
            return incrStat;
        }
        RegStatistic s = new RegStatistic();
        s.setStatisticDate(incrStat.getStatisticDate());
        s.setId(orginalStat.getId());
        s.setLastId(incrStat.getLastId());
        s.setUpdated(new Timestamp(System.currentTimeMillis()));
        s.setRegNumber(orginalStat.getRegNumber() + incrStat.getRegNumber());
        s.setPcRegNumber(orginalStat.getPcRegNumber() + incrStat.getPcRegNumber());
        s.setAndroidRegNumber(orginalStat.getAndroidRegNumber() + incrStat.getAndroidRegNumber());
        s.setIosRegNumber(orginalStat.getIosRegNumber() + incrStat.getIosRegNumber());
        s.setNotInvitedRegNumber(orginalStat.getNotInvitedRegNumber() + incrStat.getNotInvitedRegNumber());
        s.setPcNotInvitedRegNumber(orginalStat.getPcNotInvitedRegNumber() + incrStat.getPcNotInvitedRegNumber());
        s.setAndroidNotInvitedRegNumber(orginalStat.getAndroidNotInvitedRegNumber() + incrStat.getAndroidNotInvitedRegNumber());
        s.setIosNotInvitedRegNumber(orginalStat.getIosNotInvitedRegNumber() + incrStat.getIosNotInvitedRegNumber());
        s.setInviteRegNumber(orginalStat.getInviteRegNumber() + incrStat.getInviteRegNumber());
        s.setPcInviteRegNumber(orginalStat.getPcInviteRegNumber() + incrStat.getPcInviteRegNumber());
        s.setAndroidInviteRegNumber(orginalStat.getAndroidInviteRegNumber() + incrStat.getAndroidInviteRegNumber());
        s.setIosInviteRegNumber(orginalStat.getIosInviteRegNumber() + incrStat.getIosInviteRegNumber());
        s.setDirectInviteRegNumber(orginalStat.getDirectInviteRegNumber() + incrStat.getDirectInviteRegNumber());
        s.setPcDirectInviteRegNumber(orginalStat.getPcDirectInviteRegNumber() + incrStat.getPcDirectInviteRegNumber());
        s.setAndroidDirectInviteRegNumber(orginalStat.getAndroidDirectInviteRegNumber() + incrStat.getAndroidDirectInviteRegNumber());
        s.setIosDirectInviteRegNumber(orginalStat.getIosDirectInviteRegNumber() + incrStat.getIosDirectInviteRegNumber());
        s.setIndirectInviteRegNumber(orginalStat.getIndirectInviteRegNumber() + incrStat.getIndirectInviteRegNumber());
        s.setPcIndirectInviteRegNumber(orginalStat.getPcIndirectInviteRegNumber() + incrStat.getPcIndirectInviteRegNumber());
        s.setAndroidIndirectInviteRegNumber(orginalStat.getAndroidIndirectInviteRegNumber() + incrStat.getAndroidIndirectInviteRegNumber());
        s.setIosIndirectInviteRegNumber(orginalStat.getIosIndirectInviteRegNumber() + incrStat.getIosIndirectInviteRegNumber());
        s.setValidDirectInviteRegNumber(orginalStat.getValidDirectInviteRegNumber() + incrStat.getValidDirectInviteRegNumber());
        s.setPcValidDirectInviteRegNumber(orginalStat.getPcValidDirectInviteRegNumber() + incrStat.getPcValidDirectInviteRegNumber());
        s.setAndroidValidDirectInviteRegNumber(orginalStat.getAndroidValidDirectInviteRegNumber() + incrStat.getAndroidValidDirectInviteRegNumber());
        s.setIosValidDirectInviteRegNumber(orginalStat.getIosValidDirectInviteRegNumber() + incrStat.getIosValidDirectInviteRegNumber());
        s.setValidIndirectInviteRegNumber(orginalStat.getValidIndirectInviteRegNumber() + incrStat.getValidIndirectInviteRegNumber());
        s.setPcValidIndirectInviteRegNumber(orginalStat.getPcValidIndirectInviteRegNumber() + incrStat.getPcValidIndirectInviteRegNumber());
        s.setAndroidValidIndirectInviteRegNumber(orginalStat.getAndroidValidIndirectInviteRegNumber() + incrStat.getAndroidValidIndirectInviteRegNumber());
        s.setIosValidIndirectInviteRegNumber(orginalStat.getIosValidIndirectInviteRegNumber() + incrStat.getIosValidIndirectInviteRegNumber());
        return s;
    }


    /**
     * 更新有效邀请数据，实时更新
     *
     * @param relation
     */
    public void updateValidInvitedStatistic(InviteRelation relation) {
        User user = userMapper.getByUserId(relation.getInvitedId());
        RegStatistic incrStat = new RegStatistic();
        String platform = user.getPlatform();


        //  `invited_type` int(2) NOT NULL COMMENT '被邀请类型  1：直接  2：间接',
        if (relation.getInvitedType() == 1) {
            incrStat.incrValidDirectInviteRegNumber(1);

            if (platform.equals("PC")) {
                incrStat.incrPcValidDirectInviteRegNumber(1);
            } else if (platform.equals("ANDROID")) {
                incrStat.incrAndroidValidDirectInviteRegNumber(1);
            } else if (platform.equals("IOS")) {
                incrStat.incrIosValidDirectInviteRegNumber(1);
            }
        } else if (relation.getInvitedType() == 2) {
            incrStat.incrValidIndirectInviteRegNumber(1);
            if (platform.equals("PC")) {
                incrStat.incrPcValidIndirectInviteRegNumber(1);
            } else if (platform.equals("ANDROID")) {
                incrStat.incrAndroidValidIndirectInviteRegNumber(1);
            } else if (platform.equals("IOS")) {
                incrStat.incrIosValidIndirectInviteRegNumber(1);
            }
        }
        String regDate = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
        incrStat.setStatisticDate(regDate);
        saveStatistic(incrStat, user.getOrgId(), regDate);
    }

    public RegStatistic getAggregateRegStatistic(Long orgId) {
        return regStatisticMapper.getAggregateRegStatistic(orgId);
    }

    public List<RegStatistic> getDailyRegStatistics(Long orgId, Long startDate, Long endDate) {
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        return regStatisticMapper.getDailyRegStatistics(orgId, dateFormat.format(startDate), dateFormat.format(endDate));
    }
}
