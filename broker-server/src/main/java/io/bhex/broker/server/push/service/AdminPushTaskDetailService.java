package io.bhex.broker.server.push.service;

import io.bhex.broker.server.model.AdminPushTaskDetail;
import io.bhex.broker.server.primary.mapper.AdminPushTaskDetailMapper;
import io.bhex.broker.server.push.bo.AdminPushDetailStatisticsSimple;
import org.apache.ibatis.session.RowBounds;
import org.springframework.stereotype.Service;
import tk.mybatis.mapper.entity.Example;

import javax.annotation.Resource;
import java.util.List;

/**
 * 推送任务分组推送详情表(每组最多100个下发token)
 * ^_^ 待定
 * author: wangshouchao
 * Date: 2020/07/25 06:23:37
 */
@Service
public class AdminPushTaskDetailService {

    @Resource
    private AdminPushTaskDetailMapper adminPushTaskDetailMapper;

    public int insert(AdminPushTaskDetail adminPushTaskDetail) {
        return adminPushTaskDetailMapper.insertSelective(adminPushTaskDetail);
    }

    public List<AdminPushTaskDetail> queryPushTaskDetailByTask(Long taskId, Long taskRound) {
        Example example = Example.builder(AdminPushTaskDetail.class).build();
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("taskId", taskId);
        criteria.andEqualTo("taskRound", taskRound);
        return adminPushTaskDetailMapper.selectByExample(example);
    }

    public List<AdminPushTaskDetail> queryAdminPushTaskSendDetail(Long orgId, Long taskId, Long taskRound, Long startTime, Integer limit) {
        Example example = Example.builder(AdminPushTaskDetail.class).build();
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("orgId", orgId);
        criteria.andEqualTo("taskId", taskId);
        if (taskRound > 0) {
            criteria.andEqualTo("taskRound", taskRound);
        }
        if (startTime > 0) {
            criteria.andGreaterThanOrEqualTo("created", startTime);
        }
        return adminPushTaskDetailMapper.selectByExampleAndRowBounds(example, new RowBounds(1, limit));
    }

    public int updatePushDetailStatisticsNum(AdminPushDetailStatisticsSimple detailStatisticsSimple) {
        AdminPushTaskDetail adminPushTaskDetail = new AdminPushTaskDetail();
        adminPushTaskDetail.setReqOrderId(detailStatisticsSimple.getReqOrderId());
        adminPushTaskDetail.setUpdated(System.currentTimeMillis());
        adminPushTaskDetail.setClickCount(detailStatisticsSimple.getClickCount().get());
        adminPushTaskDetail.setDeliveryCount(detailStatisticsSimple.getDeliveryCount().get());
        return adminPushTaskDetailMapper.updateDetailStatisticsNum(adminPushTaskDetail);
    }
}