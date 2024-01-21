package io.bhex.broker.server.push.service;

import io.bhex.broker.server.model.AdminPushTaskStatistics;
import io.bhex.broker.server.primary.mapper.AdminPushTaskStatisticsMapper;
import io.bhex.broker.server.push.bo.AppPushStatisticsSimple;
import org.springframework.stereotype.Service;
import tk.mybatis.mapper.entity.Example;

import javax.annotation.Resource;

/**
 * 券商推送任务当次执行效果统计表
 * author: wangshouchao
 * Date: 2020/07/25 06:25:33
 */
@Service
public class AdminPushTaskStatisticsService {

    @Resource
    private AdminPushTaskStatisticsMapper adminPushTaskStatisticsMapper;

    public void insert(AdminPushTaskStatistics adminPushTaskStatistics) {
        long currentTimeMillis = System.currentTimeMillis();
        adminPushTaskStatistics.setCreated(currentTimeMillis);
        adminPushTaskStatistics.setUpdated(currentTimeMillis);
        adminPushTaskStatisticsMapper.insertSelective(adminPushTaskStatistics);
    }

    public AdminPushTaskStatistics query(Long taskId, Long taskRound) {
        Example example = new Example(AdminPushTaskStatistics.class);
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("taskId", taskId);
        criteria.andEqualTo("taskRound", taskRound);
        return adminPushTaskStatisticsMapper.selectOneByExample(example);
    }

    /**
     * 查询最新的统计记录
     * @param taskId
     * @return
     */
    public AdminPushTaskStatistics queryRecently(Long taskId) {
        Example example = new Example(AdminPushTaskStatistics.class);
        example.orderBy("taskRound").desc();
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("taskId", taskId);
        return adminPushTaskStatisticsMapper.selectOneByExample(example);
    }

    public int queryCount(Long taskId, Long taskRound) {
        Example example = new Example(AdminPushTaskStatistics.class);
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("taskId", taskId);
        criteria.andEqualTo("taskRound", taskRound);
        return adminPushTaskStatisticsMapper.selectCountByExample(example);
    }

    public int updateTaskStatisticsNum(AppPushStatisticsSimple statisticsSimple) {
        AdminPushTaskStatistics adminPushTaskStatistics = new AdminPushTaskStatistics();
        adminPushTaskStatistics.setTaskId(statisticsSimple.getTaskId());
        adminPushTaskStatistics.setTaskRound(statisticsSimple.getTaskRound());
        adminPushTaskStatistics.setUpdated(System.currentTimeMillis());
        adminPushTaskStatistics.setCancelCount(statisticsSimple.getCancelCount().get());
        adminPushTaskStatistics.setClickCount(statisticsSimple.getClickCount().get());
        adminPushTaskStatistics.setDeliveryCount(statisticsSimple.getDeliveryCount().get());
        adminPushTaskStatistics.setEffectiveCount(statisticsSimple.getEffectiveCount().get());
        adminPushTaskStatistics.setUninstallCount(statisticsSimple.getUninstallCount().get());
        adminPushTaskStatistics.setUnsubscribeCount(statisticsSimple.getUnsubscribeCount().get());
        return adminPushTaskStatisticsMapper.updateTaskStatisticsNum(adminPushTaskStatistics);
    }

}