package io.bhex.broker.server.push.service;

import io.bhex.broker.server.model.AdminPushTask;
import io.bhex.broker.server.primary.mapper.AdminPushTaskMapper;
import io.bhex.broker.server.push.bo.CycleTypeEnum;
import io.bhex.broker.server.push.bo.PushTaskStatusEnum;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.ibatis.session.RowBounds;
import org.springframework.stereotype.Service;
import tk.mybatis.mapper.entity.Example;

import javax.annotation.Resource;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * TbAdminPushTaskServiceImpl
 * <p>
 * author: wangshouchao
 * Date: 2020/07/25 06:21:44
 */
@Slf4j
@Service
public class AdminPushTaskService {

    public final Long DAY_MILLIS = 24 * 60 * 60 * 1000L;

    public final Long WEEK_MILLIS = 7 * 24 * 60 * 60 * 1000L;


    @Resource
    private AdminPushTaskMapper adminPushTaskMapper;

    public int submit(AdminPushTask adminPushTask) {
        return adminPushTaskMapper.insertSelective(adminPushTask);
    }

    /**
     *  更新任务
     * @param adminPushTask
     * @return
     */
    public int update(AdminPushTask adminPushTask){
        return adminPushTaskMapper.updateByPrimaryKeySelective(adminPushTask);
    }

    /**
     * 更新pushing状态(正在进行中的状态)
     */
    public int updateAdminPushTaskPushingStatus(AdminPushTask adminPushTask, Long curTime, String text) {
        AdminPushTask updatePushTask = new AdminPushTask();
        updatePushTask.setUpdated(curTime);
        updatePushTask.setExecuteTime(curTime);
        updatePushTask.setStatus(PushTaskStatusEnum.PUSHING.getStatus());
        return updatePushTaskStatus(adminPushTask, text, updatePushTask);
    }

    /**
     * 更新单次执行已成状态
     */
    public int updateAdminPushTaskFinishStatus(AdminPushTask adminPushTask, String text) {
        Long curTime = System.currentTimeMillis();
        AdminPushTask updatePushTask = new AdminPushTask();
        updatePushTask.setUpdated(curTime);
        updatePushTask.setStatus(PushTaskStatusEnum.FINISH.getStatus());
        return updatePushTaskStatus(adminPushTask, text, updatePushTask);
    }

    /**
     * 更新取消状态
     */
    public int updateAdminPushTaskCancelStatus(AdminPushTask adminPushTask, String text) {
        Long curTime = System.currentTimeMillis();
        AdminPushTask updatePushTask = new AdminPushTask();
        updatePushTask.setUpdated(curTime);
        updatePushTask.setStatus(PushTaskStatusEnum.CANCEL.getStatus());
        return updatePushTaskStatus(adminPushTask, text, updatePushTask);
    }

    /**
     * 更新周期已成状态
     */
    public int updateAdminPushTaskPeriodFinishStatus(AdminPushTask adminPushTask, String text) {
        Long curTime = System.currentTimeMillis();
        AdminPushTask updatePushTask = new AdminPushTask();
        updatePushTask.setUpdated(curTime);
        updatePushTask.setStatus(PushTaskStatusEnum.PERIOD_FINISH.getStatus());
        //计算下一周期的执行时间和执行的次数
        if (CycleTypeEnum.DAY.getType() == adminPushTask.getCycleType()) {
            Long actionTime = adminPushTask.getActionTime();
            Long nextActionTime = ((curTime - actionTime) / DAY_MILLIS + 1) * DAY_MILLIS + actionTime;
            updatePushTask.setActionTime(nextActionTime);
        } else if (CycleTypeEnum.WEEK.getType() == adminPushTask.getCycleType()) {
            Long actionTime = adminPushTask.getActionTime();
            Long nextActionTime = ((curTime - actionTime) / WEEK_MILLIS + 1) * WEEK_MILLIS + actionTime;
            updatePushTask.setActionTime(nextActionTime);
        } else {
            //保护循环类型不存在时，默认按照日添加下一个执行时间,避免无限的循环
            log.warn("unknown cycle type!taskId:{},cycleType:{}", adminPushTask.getTaskId(), adminPushTask.getCycleType());
            Long actionTime = adminPushTask.getActionTime();
            Long nextActionTime = ((curTime - actionTime) / DAY_MILLIS + 1) * DAY_MILLIS + actionTime;
            updatePushTask.setActionTime(nextActionTime);
        }
        return updatePushTaskStatus(adminPushTask, text, updatePushTask);
    }

    private int updatePushTaskStatus(AdminPushTask adminPushTask, String text, AdminPushTask updatePushTask) {
        if (StringUtils.isNoneBlank(text)) {
            updatePushTask.setRemark(text);
        }
        Example example = new Example(AdminPushTask.class);
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("taskId", adminPushTask.getTaskId());
        criteria.andEqualTo("updated", adminPushTask.getUpdated());
        return adminPushTaskMapper.updateByExampleSelective(updatePushTask, example);
    }


    /**
     * 获取近期30分内需要执行的任务
     */
    public List<AdminPushTask> queryRecentlyPushTasks() {
        //任务数理论上不多,查询一次性任务<=30分钟内即将开始的任务,查询周期任务<=30分钟内即将开始(包含所有已经超过执行时间的)
        //超过当前执行时间1小时内的，都将再次执行，超过执行时间1小时外的，都选择不执行(执行状态调整为超时未执行),周期任务拨动下一个周期执行时间
        Example example = new Example(AdminPushTask.class);
        example.excludeProperties("userIds");
        Example.Criteria criteria = example.createCriteria();
        criteria.andBetween("status", "0", "2");
        long halfHour = 30 * 60 * 1000;
        criteria.andLessThanOrEqualTo("actionTime", System.currentTimeMillis() + halfHour);
        return adminPushTaskMapper.selectByExample(example);
    }

    /**
     * 查询utc时间0点创建的task数
     * 因为created不准备做索引,调整为使用redis的计数来控制每日创建数
     */
    @Deprecated
    public int queryCreateTaskCountByUtcTodayTime(Long orgId) {
        Long utcDayTime = Instant.now().getEpochSecond() / 86400 * 86400 * 1000;
        Example example = new Example(AdminPushTask.class);
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("orgId", orgId);
        criteria.andGreaterThanOrEqualTo("created", utcDayTime);
        return adminPushTaskMapper.selectCountByExample(example);
    }

    /**
     * 管理端查询
     */
    public List<AdminPushTask> queryPushTasks(Long orgId, Long startTime, Long endTime, Long startId, Long endId, Integer limit) {
        Example example = new Example(AdminPushTask.class);
        example.excludeProperties("userIds");
        example.orderBy("updated").desc();
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("orgId", orgId);
        if (startTime != null && startTime > 0) {
            criteria.andGreaterThanOrEqualTo("updated", startTime);
        }
        if (endTime != null && endTime > 0) {
            criteria.andLessThanOrEqualTo("updated", endTime);
        }
        if (startId != null && startId > 0) {
            criteria.andGreaterThanOrEqualTo("taskId", startId);
        }
        if (endId != null && endId > 0) {
            criteria.andLessThanOrEqualTo("taskId", endId);
        }
        limit = Math.min(limit, 100);
        return adminPushTaskMapper.selectByExampleAndRowBounds(example, new RowBounds(0, limit));
    }

    /**
     *  查询完整的task
     */
    public AdminPushTask query(Long orgId, Long taskId) {
        Example example = new Example(AdminPushTask.class);
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("taskId", taskId);
        criteria.andEqualTo("orgId", orgId);
        return adminPushTaskMapper.selectOneByExample(example);
    }

    /**
     * 在程序即将执行时查询userIds
     */
    public List<Long> queryUserIdsByTaskId(Long taskId) {
        Example example = new Example(AdminPushTask.class);
        example.selectProperties("userIds", "rangeType");
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("taskId", taskId);
        AdminPushTask adminPushTask = adminPushTaskMapper.selectOneByExample(example);
        String userIds = adminPushTask.getUserIds();
        if(StringUtils.isNotBlank(userIds)){
            try {
                if (userIds.contains(",")) {
                    return Arrays.stream(userIds.split(",")).map(Long::parseLong).collect(Collectors.toList());
                } else {
                    return Collections.singletonList(Long.parseLong(userIds));
                }
            } catch (Exception e) {
                log.error("userIds handle error!{}", taskId, e);
                return new ArrayList<>();
            }
        }else {
            return new ArrayList<>();
        }
    }
}