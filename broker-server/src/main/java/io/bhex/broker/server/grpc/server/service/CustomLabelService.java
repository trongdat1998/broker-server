package io.bhex.broker.server.grpc.server.service;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.grpc.common.Header;
import io.bhex.broker.grpc.customlabel.*;
import io.bhex.broker.server.model.CustomLabel;
import io.bhex.broker.server.model.CustomLabelUser;
import io.bhex.broker.server.model.User;
import io.bhex.broker.server.primary.mapper.CustomLabelMapper;
import io.bhex.broker.server.primary.mapper.CustomLabelUserMapper;
import io.bhex.broker.server.primary.mapper.UserMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tk.mybatis.mapper.entity.Example;

import javax.annotation.Resource;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.grpc.server.service
 * @Author: ming.xu
 * @CreateDate: 2019/12/13 9:45 PM
 * @Copyright（C）: 2019 BHEX Inc. All rights reserved.
 */
@Slf4j
@Service
public class CustomLabelService {

    @Resource
    private CustomLabelMapper customLabelMapper;
    @Resource
    private CustomLabelUserMapper customLabelUserMapper;

    @Resource
    private UserMapper userMapper;

    public GetBrokerCustomLabelReply getBrokerCustomLabel(GetBrokerCustomLabelRequest request) {
        List<CustomLabel> customLabelList = getCustomLabelByOrgId(request.getOrgId());
        List<GetBrokerCustomLabelReply.CustomLabelInfo> infoList = customLabelList.stream().map(cl ->
                GetBrokerCustomLabelReply.CustomLabelInfo.newBuilder()
                        .setId(cl.getId())
                        .setOrgId(cl.getOrgId())
                        .setLabelId(cl.getLabelId())
                        .setColorCode(cl.getColorCode())
                        .setLanguage(cl.getLanguage())
                        .setLabelValue(cl.getLabelValue())
                        .setLabelType(cl.getType())
                        .build()
        ).collect(Collectors.toList());

        return GetBrokerCustomLabelReply.newBuilder()
                .setRet(0)
                .addAllCustomLabelInfo(infoList)
                .build();
    }

    private List<CustomLabel> getCustomLabelByOrgId(Long orgId) {
        Example example = Example.builder(CustomLabel.class).build();
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("orgId", orgId);
        criteria.andEqualTo("status", 1);
        criteria.andEqualTo("isDel", 0);

        return customLabelMapper.selectByExample(example);
    }

    @Transactional
    public SaveUserCustomLabelReply saveUserCustomLabel(SaveUserCustomLabelRequest request) {
        Header header = request.getHeader();
        User user = userMapper.getByOrgAndUserId(header.getOrgId(), header.getUserId());
        if (user == null) {
            log.warn("org:{} UserId:{} not found", header.getOrgId(), header.getUserId());
            throw new BrokerException(BrokerErrorCode.PARAM_INVALID);
        }
        long labelId = request.getLabelId();
        CustomLabel customLabel = customLabelMapper.getOneByLabelId(header.getOrgId(), labelId);
        if (customLabel == null) {
            log.warn("org:{} labelId:{} not found", header.getOrgId(), labelId);
            throw new BrokerException(BrokerErrorCode.PARAM_INVALID);
        }
        CustomLabelUser customLabelUser = customLabelUserMapper.getByLabelAndUserId(header.getOrgId(), labelId, header.getUserId());
        if (customLabelUser == null) {
            customLabelUser = CustomLabelUser.builder()
                    .orgId(header.getOrgId())
                    .userId(header.getUserId())
                    .labelId(labelId)
                    .isDel(0)
                    .createdAt(System.currentTimeMillis())
                    .updatedAt(System.currentTimeMillis())
                    .build();
            customLabelUserMapper.insertSelective(customLabelUser);
        } else {
            customLabelUser.setIsDel(0);
            customLabelUser.setUpdatedAt(System.currentTimeMillis());
            customLabelUserMapper.updateByPrimaryKeySelective(customLabelUser);
        }
        List<Long> labels = user.getCustomLabelIdList();
        labels.add(labelId);
        String labelsStr = String.join(",", labels.stream().distinct().map(l -> l.toString()).collect(Collectors.toList()));
        user.setCustomLabelIds(labelsStr);
        userMapper.updateByPrimaryKeySelective(user);
        return SaveUserCustomLabelReply.newBuilder().build();
    }

    @Transactional
    public DelUserCustomLabelReply delUserCustomLabel(DelUserCustomLabelRequest request) {
        Header header = request.getHeader();
        User user = userMapper.getByOrgAndUserId(header.getOrgId(), header.getUserId());
        if (user == null) {
            log.warn("org:{} UserId:{} not found", header.getOrgId(), header.getUserId());
            throw new BrokerException(BrokerErrorCode.PARAM_INVALID);
        }
        long labelId = request.getLabelId();
        CustomLabel customLabel = customLabelMapper.getOneByLabelId(header.getOrgId(), labelId);
        if (customLabel == null) {
            log.warn("org:{} labelId:{} not found", header.getOrgId(), labelId);
            throw new BrokerException(BrokerErrorCode.PARAM_INVALID);
        }

        CustomLabelUser customLabelUser = customLabelUserMapper.getByLabelAndUserId(header.getOrgId(), labelId, header.getUserId());
        if (customLabelUser != null) {
            customLabelUser.setIsDel(1);
            customLabelUser.setUpdatedAt(System.currentTimeMillis());
            customLabelUserMapper.updateByPrimaryKeySelective(customLabelUser);

            List<Long> labels = user.getCustomLabelIdList();
            labels = labels.stream().filter(l -> !l.equals(labelId)).collect(Collectors.toList());
            String labelsStr = String.join(",", labels.stream().distinct().map(l -> l.toString()).collect(Collectors.toList()));
            user.setCustomLabelIds(labelsStr);
            userMapper.updateByPrimaryKeySelective(user);
        }

        return DelUserCustomLabelReply.newBuilder().build();
    }

}
