package io.bhex.broker.server.push.service;

import io.bhex.broker.server.model.AppPushDeviceHistory;
import io.bhex.broker.server.primary.mapper.AppPushDeviceHistoryMapper;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;

/**
 * 用户推送设备历史表
 * author: wangshouchao
 * Date: 2020/07/25 06:26:29
 */
@Service
public class AppPushDeviceHistoryService {

    @Resource
    private AppPushDeviceHistoryMapper appPushDeviceHistoryMapper;

    public void submit(AppPushDeviceHistory appPushDeviceHistory) {
    }

    public void removeById(Long id) {
    }

    public void removeByIds(List<Long> ids) {
    }


}