package io.bhex.broker.server.grpc.server.service;


import io.bhex.broker.server.domain.LoginStatisticType;
import io.bhex.broker.server.primary.mapper.LoginLogMapper;
import io.bhex.broker.server.primary.mapper.LoginStatisticCountMapper;
import io.bhex.broker.server.primary.mapper.LoginStatisticMapper;
import io.bhex.broker.server.model.LoginLog;
import io.bhex.broker.server.model.LoginStatistic;
import io.bhex.broker.server.model.LoginStatisticCount;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
@Slf4j
public class StatisticService {

    private static final String PLATFORM_PC = "PC";

    private static final String PLATFORM_MOBILE = "MOBILE";

    private static final String ANDROID = "Android";

    private static final String IOS = "iOS";

    @Autowired
    private LoginLogMapper loginLogMapper;

    @Autowired
    private LoginStatisticMapper loginStatisticMapper;

    @Autowired
    private LoginStatisticCountMapper loginStatisticCountMapper;

//    @Scheduled(cron = "0 0 1 * * ?")
    public void analysisLog() {
        DateTime nowTime = new DateTime().minusDays(1);
        DateTime start = nowTime.withTimeAtStartOfDay();
        DateTime end = nowTime.millisOfDay().withMaximumValue();
        String time = nowTime.toString("yyyy-MM-dd");

        //登录详情
        handelLoginStatisticTask(time, start.getMillis(), end.getMillis());

        //汇总login count
        handelLoginStatisticCountTask(time);
    }


    public void handelLoginStatisticCountTask(String time) {
        List<LoginStatistic> loginStatistics
                = loginStatisticMapper.queryLoginStatisticListByTime(time);
        if (CollectionUtils.isEmpty(loginStatistics)) {
            log.info("Statistic count fail time {}", time);
            return;
        }

        //org ID 分组
        Map<Long, List<LoginStatistic>> resultList
                = loginStatistics.stream().collect(Collectors.groupingBy(LoginStatistic::getOrgId));

        resultList.keySet().forEach(s -> {
            List<LoginStatistic> orgLoginList = resultList.get(s);
            if (CollectionUtils.isEmpty(orgLoginList)) {
                return;
            }

            //汇总
            Long pcCount = orgLoginList.stream().filter(l -> l.getType().equals(LoginStatisticType.PC.value())).count();
            Long androidCount = orgLoginList.stream().filter(l -> l.getType().equals(LoginStatisticType.ANDROID.value())).count();
            Long iosCount = orgLoginList.stream().filter(l -> l.getType().equals(LoginStatisticType.IOS.value())).count();
            LoginStatisticCount loginStatisticCount = new LoginStatisticCount();
            loginStatisticCount.setOrgId(s);
            loginStatisticCount.setLoginCount(orgLoginList.size());
            loginStatisticCount.setPcLoginCount(pcCount > 0L ? Integer.parseInt(String.valueOf(pcCount)) : 0);
            loginStatisticCount.setAndroidCount(androidCount > 0L ? Integer.parseInt(String.valueOf(androidCount)) : 0);
            loginStatisticCount.setIosCount(iosCount > 0L ? Integer.parseInt(String.valueOf(iosCount)) : 0);
            loginStatisticCount.setCreated(new Date());
            loginStatisticCount.setStatisticDate(time);

            Long id = this.loginStatisticCountMapper.queryIdLoginStatisticCountByTime(time, s);

            if (id != null && id > 0) {
                loginStatisticCount.setId(id);
                this.loginStatisticCountMapper.updateByPrimaryKeySelective(loginStatisticCount);
            } else {
                this.loginStatisticCountMapper.insertSelective(loginStatisticCount);
            }
        });
    }


    public void handelLoginStatisticTask(String time, Long start, Long end) {
        List<LoginStatistic> loginStatistics = new ArrayList<>();
        if (this.loginStatisticMapper.countLoginStatisticByTime(time) > 0) {
            log.info("Task has been executed");
            return;
        }

        List<LoginLog> loginLogs = loginLogMapper.queryOneDayLoginLogList(start, end);
        if (CollectionUtils.isEmpty(loginLogs)) {
            log.info("data {} login list is null", time);
        }

        loginLogs.forEach(l -> {
            if (StringUtils.isEmpty(l.getPlatform())) {
                return;
            }

            if (l.getPlatform().equalsIgnoreCase(PLATFORM_PC)) {
                loginStatistics.add(buildLoginStatistic(l.getOrgId(), l.getUserId(), LoginStatisticType.PC.value(), time));
            } else if (l.getPlatform().equalsIgnoreCase(PLATFORM_MOBILE)) {
                if (StringUtils.isEmpty(l.getUserAgent())) {
                    return;
                }
                if (l.getUserAgent().contains(ANDROID)) {
                    loginStatistics.add(buildLoginStatistic(l.getOrgId(), l.getUserId(), LoginStatisticType.ANDROID.value(), time));
                } else if (l.getPlatform().contains(IOS)) {
                    loginStatistics.add(buildLoginStatistic(l.getOrgId(), l.getUserId(), LoginStatisticType.IOS.value(), time));
                }
            }
        });

        if (loginStatistics.size() > 0) {
            loginStatistics.forEach(s -> {
                loginStatisticMapper.insertSelective(s);
            });
        }
    }

    private LoginStatistic buildLoginStatistic(Long orgId, Long userId, Integer type, String time) {
        return LoginStatistic
                .builder()
                .userId(userId)
                .statisticDate(time)
                .created(new Date())
                .orgId(orgId)
                .updated(new Date())
                .type(type)
                .build();
    }

    public static void main(String[] args) {
        DateTime nowTime = new DateTime().minusDays(1);
        System.out.println(nowTime.toString("yyyy-MM-dd HH:mm:ss"));
        DateTime start = nowTime.withTimeAtStartOfDay();
        DateTime end = nowTime.millisOfDay().withMaximumValue();
        System.out.println(start.getMillis());
        System.out.println(end.getMillis());
    }

    //TODO 查询接口两个 一个查汇总 一个查当天数据列表分页
}
