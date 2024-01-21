package io.bhex.broker.server.grpc.server.service;


import com.google.common.base.Strings;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.github.pagehelper.PageHelper;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Resource;

import io.bhex.broker.grpc.bwlist.EditUserBlackWhiteConfigRequest;
import io.bhex.broker.grpc.bwlist.GetUserBlackWhiteConfigRequest;
import io.bhex.broker.grpc.bwlist.UserBlackWhiteConfig;
import io.bhex.broker.grpc.bwlist.UserBlackWhiteListType;
import io.bhex.broker.grpc.bwlist.UserBlackWhiteType;
import io.bhex.broker.server.domain.UserBlackWhiteListConfig;
import io.bhex.broker.server.primary.mapper.UserBlackWhiteListConfigMapper;
import io.bhex.broker.server.util.BeanCopyUtils;
import lombok.extern.slf4j.Slf4j;
import tk.mybatis.mapper.entity.Example;

/**
 * @Description:
 * @Date: 2019/8/23 下午4:08
 * @Author: liwei
 * @Copyright（C）: 2018 BlueHelix Inc. All rights reserved.
 */

@Slf4j
@Service
public class UserBlackWhiteListConfigService {

    @Resource
    private UserBlackWhiteListConfigMapper userBlackWhiteListConfigMapper;

    public List<UserBlackWhiteListConfig> getBlackWhiteConfigs(Long orgId, Integer listType, Integer bwType, int pageSize, long fromId, long userId) {
        long now = System.currentTimeMillis();
        Example example = Example.builder(UserBlackWhiteListConfig.class)
                .orderByDesc("id")
                .build();
        PageHelper.startPage(0, pageSize);
        Example.Criteria criteria = example.createCriteria()
                .andEqualTo("orgId", orgId)
                .andEqualTo("status", 1)
                //.andEqualTo("listType", listType)
                //.andEqualTo("bwType", bwType)
                .andLessThanOrEqualTo("startTime", now)
                .andGreaterThanOrEqualTo("endTime", now);
        if (listType > 0) {
            criteria.andEqualTo("listType", listType);
        }
        if (bwType > 0) {
            criteria.andEqualTo("bwType", bwType);
        }
        if (userId > 0) {
            criteria.andEqualTo("userId", userId);
        }
        if (fromId > 0) {
            criteria.andLessThan("id", fromId);
        }
        List<UserBlackWhiteListConfig> configs = userBlackWhiteListConfigMapper.selectByExample(example);
        return configs;
    }

    public UserBlackWhiteListConfig getBlackWhiteConfig(Long orgId, Long userId, Integer listType, Integer bwType) {
        UserBlackWhiteListConfig config = userBlackWhiteListConfigMapper.getOneConfig(orgId, userId,
                listType, bwType, System.currentTimeMillis());
        return config;
    }

    public UserBlackWhiteConfig getBlackWhiteConfig(GetUserBlackWhiteConfigRequest request) {
        UserBlackWhiteListConfig config = getBlackWhiteConfig(request.getHeader().getOrgId(), request.getUserId(),
                request.getListTypeValue(), request.getBwTypeValue());
        if (config == null) {
            return UserBlackWhiteConfig.newBuilder().build();
        }
        UserBlackWhiteConfig.Builder builder = UserBlackWhiteConfig.newBuilder();
        BeanCopyUtils.copyPropertiesIgnoreNull(config, builder);
        builder.setBwTypeValue(config.getBwType());
        builder.setListTypeValue(config.getListType());
        return builder.build();
    }

    public boolean inWithdrawWhiteBlackList(Long orgId, String tokenId, Long userId, UserBlackWhiteType blackWhiteType) {
        UserBlackWhiteListConfig config = getBlackWhiteConfig(orgId, userId,
                UserBlackWhiteListType.WITHDRAW_BLACK_WHITE_LIST_TYPE_VALUE,
                blackWhiteType.getNumber());
        if (config == null) {
            return false;
        }

        String configValue = config.getExtraInfo();
        if (StringUtils.isEmpty(configValue)) {
            return true;
        }

        JSONObject configJO = JSONObject.parseObject(configValue);
        if (configJO.getBoolean("global")) {
            //全局，针对全币种
            return true;
        }

        JSONArray array = configJO.getJSONArray("tokens");
        for (int i = 0; i < array.size(); i++) {
            String token = array.getString(i);
            if (token.equals(tokenId)) {
                return true;
            }
        }
        return false;
    }

    @Transactional
    public boolean edit(EditUserBlackWhiteConfigRequest request) {
        long now = System.currentTimeMillis();
        long startTime = request.getStartTime() > 0 ? request.getStartTime() : now;
        long endTime = request.getEndTime() > 0 ? request.getEndTime() : now + 100L * 366 * 84400_000;

        if (request.getFullVolumeUser()) { //如果请求是全量增加，则删除原有数据
            int deletedRows = userBlackWhiteListConfigMapper.deleteConfigs(request.getHeader().getOrgId(),
                    request.getListTypeValue(), request.getBwTypeValue(), System.currentTimeMillis());
            log.info("delete bw config:{} rows:{}", request, deletedRows);
        }

        List<Long> userIds = request.getUserIdsList();
        if (CollectionUtils.isEmpty(userIds)) {
            userIds = new ArrayList<>();
        }
        if (request.getUserId() > 0) {
            userIds.add(request.getUserId());
        }
        for (long userId : userIds) {
            UserBlackWhiteListConfig config = getBlackWhiteConfig(request.getHeader().getOrgId(), userId,
                    request.getListTypeValue(), request.getBwTypeValue());
            log.info("config:{}", config);
            if (config != null) {
                config.setStatus(request.getStatus());
                config.setReason(Strings.nullToEmpty(request.getReason()));
                config.setExtraInfo(Strings.nullToEmpty(request.getExtraInfo()));
                config.setStartTime(startTime);
                config.setEndTime(endTime);
                config.setUpdated(now);
                log.info("config:{}", config);
                int row = userBlackWhiteListConfigMapper.updateByPrimaryKey(config);
                log.info("affect row : {} id : {}", row, config.getId());
            } else {
//            if (config.getListType() != request.getListTypeValue()) {
//
//            }
                config = UserBlackWhiteListConfig.builder()
                        .orgId(request.getHeader().getOrgId())
                        .userId(userId)
                        .bwType(request.getBwTypeValue())
                        .listType(request.getListTypeValue())
                        .reason(Strings.nullToEmpty(request.getReason()))
                        .status(request.getStatus())
                        .extraInfo(Strings.nullToEmpty(request.getExtraInfo()))
                        .startTime(startTime)
                        .endTime(endTime)
                        .created(now)
                        .updated(now)
                        .build();
                userBlackWhiteListConfigMapper.insertSelective(config);
            }
        }
        return true;


    }

    public List<UserBlackWhiteListConfig> getUserConfigListByOrgId(Long orgId, Integer bwType, Integer listType) {
        Example example = new Example(UserBlackWhiteListConfig.class);
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("orgId", orgId);
        criteria.andEqualTo("bwType", bwType);
        criteria.andEqualTo("listType", listType);
        criteria.andEqualTo("status", 1);
        return userBlackWhiteListConfigMapper.selectByExample(example);
    }
}
