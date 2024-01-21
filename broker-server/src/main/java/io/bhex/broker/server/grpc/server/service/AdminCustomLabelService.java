package io.bhex.broker.server.grpc.server.service;

import com.google.api.client.util.Lists;
import io.bhex.base.idgen.api.ISequenceGenerator;
import io.bhex.broker.grpc.admin.*;
import io.bhex.broker.server.model.CustomLabel;
import io.bhex.broker.server.model.CustomLabelUser;
import io.bhex.broker.server.model.User;
import io.bhex.broker.server.primary.mapper.CustomLabelMapper;
import io.bhex.broker.server.primary.mapper.CustomLabelUserMapper;
import io.bhex.broker.server.primary.mapper.UserMapper;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;
import tk.mybatis.mapper.entity.Example;

import javax.annotation.Resource;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.grpc.server.service
 * @Author: ming.xu
 * @CreateDate: 2019/12/13 3:23 PM
 * @Copyright（C）: 2019 BHEX Inc. All rights reserved.
 */
@Slf4j
@Service
public class AdminCustomLabelService {

    private static final Integer CUSTOM_LABEL_LOCALE_REQUIRED = 50301;

    private static final Integer CUSTOM_LABEL_USER_ID_EMPTY = 50310;

    private static final Integer CUSTOM_LABEL_USER_ID_HAVE_NOT_EXIST = 50311;

    private static final Integer CUSTOM_LABEL_USER_ID_FORMAT_ERROR = 50312;

    @Resource
    private ISequenceGenerator sequenceGenerator;

    @Resource
    private CustomLabelMapper customLabelMapper;
    @Resource
    private CustomLabelUserMapper customLabelUserMapper;

    @Resource
    private UserMapper userMapper;

    public QueryCustomLabelReply queryCustomLabel(QueryCustomLabelRequest request) {
        List<CustomLabel> customLabels = queryCustomLabel(request.getOrgId(), request.getFromId(), request.getEndId(), request.getLimit(), request.getLabelType());
        Map<Long, List<CustomLabel>> customLabelLocalMap = customLabels.stream().collect(Collectors.groupingBy(CustomLabel::getLabelId));
        List<QueryCustomLabelReply.CustomLabelInfo> infoList = new ArrayList<>();
        customLabelLocalMap.forEach((k, v) -> {
            if (!CollectionUtils.isEmpty(v) && Objects.nonNull(v.get(0))) {
                List<QueryCustomLabelReply.LocaleDetail> localeDetailList = v.stream().map(cl ->
                        QueryCustomLabelReply.LocaleDetail.newBuilder()
                                .setLanguage(cl.getLanguage())
                                .setLabelValue(cl.getLabelValue())
                                .build()
                ).collect(Collectors.toList());

                CustomLabel customLabel = v.get(0);
                List<Long> userIds = customLabel.getType() == 1 ? customLabelUserMapper.getUserIdsByLabel(customLabel.getOrgId(), customLabel.getLabelId()) : Lists.newArrayList();
                String userIdsStr = CollectionUtils.isEmpty(userIds) ? "" : String.join(",", userIds.stream().map(u -> u + "").collect(Collectors.toList()));
                QueryCustomLabelReply.CustomLabelInfo info = QueryCustomLabelReply.CustomLabelInfo.newBuilder()
                        .setLabelId(customLabel.getLabelId())
                        .setOrgId(customLabel.getOrgId())
                        .setColorCode(customLabel.getColorCode())
                        .setUserCount(CollectionUtils.isEmpty(userIds) ? 0 : userIds.size())
                        .setUserIdsStr(userIdsStr)
                        .setStatus(customLabel.getStatus())
                        .setLabelType(customLabel.getType())
                        .setCreatedAt(customLabel.getCreatedAt())
                        .setUpdatedAt(customLabel.getUpdatedAt())
                        .addAllLocaleDetail(CollectionUtils.isEmpty(localeDetailList) ? Arrays.asList() : localeDetailList)
                        .build();
                infoList.add(info);
            }
        });
        return QueryCustomLabelReply.newBuilder()
                .addAllCustomLabelInfo(infoList)
                .build();
    }

    @Transactional
    public SaveCustomLabelReply saveCustomLabel(SaveCustomLabelRequest request) {
        List<SaveCustomLabelRequest.LocaleDetail> localeDetailList = request.getLocaleDetailList();
        if (CollectionUtils.isEmpty(localeDetailList)) {
            return SaveCustomLabelReply.newBuilder()
                    .setRet(CUSTOM_LABEL_LOCALE_REQUIRED)
                    .build();
        }
        Long labelId;
        if (Objects.nonNull(request.getLabelId()) && request.getLabelId() > 0L) {
            labelId = request.getLabelId();
            // 如果为更新，则先删除此label全部配置
            // 同时也会清除用户与此标签的关联关系
            delCustomLabel(request.getOrgId(), labelId);
        } else {
            labelId = sequenceGenerator.getLong();
        }
        List<Long> userIdList;
        // 区分用户标签和symbol标签
        if (request.getLabelType() == 1) {
            try {
                userIdList = processUserIdStr(request.getOrgId(), request.getUserIdsStr());
            } catch (UserIdStrFormatException e) {
                return SaveCustomLabelReply.newBuilder()
                        .setRet(e.getErrorCode())
                        .addAllErrorUserIds(e.wrongUserIdList)
                        .build();
            }
        } else {
            userIdList = Lists.newArrayList();
        }

        for (SaveCustomLabelRequest.LocaleDetail l : localeDetailList) {
            CustomLabel.CustomLabelBuilder builder = CustomLabel.builder()
                    .orgId(request.getOrgId())
                    .labelId(labelId)
                    .language(l.getLanguage())
                    .labelValue(l.getLabelValue())
                    .colorCode(request.getColorCode())
                    .userCount(userIdList.size())
                    //.userIdsStr(String.join(",", userIdList.stream().map(id -> id.toString()).collect(Collectors.toList())))
                    .status(1)
                    .isDel(0)
                    .type(request.getLabelType());
            CustomLabel existCL = customLabelMapper.selectByLabelIdAndLanguage(request.getOrgId(), labelId, l.getLanguage());
            if (Objects.nonNull(existCL)) {
                builder.id(existCL.getId());
                builder.createdAt(existCL.getCreatedAt());
                builder.updatedAt(System.currentTimeMillis());
                customLabelMapper.updateRecord(builder.build());
            } else {
                builder.createdAt(System.currentTimeMillis());
                builder.updatedAt(System.currentTimeMillis());
                customLabelMapper.insertSelective(builder.build());
            }
        }

        for (Long userId : userIdList) {
            CustomLabelUser customLabelUser = customLabelUserMapper.getByLabelAndUserId(request.getOrgId(), labelId, userId);
            if (customLabelUser == null) {
                customLabelUser = CustomLabelUser.builder()
                        .orgId(request.getOrgId())
                        .userId(userId)
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
        }

        // 上面已经将用户此标签的配置清除。
        // useridList 给用户添加标签id
        userIdList.forEach(uid -> {
            User user = userMapper.getByUserId(uid);
            List<Long> customLabelIdList = user.getCustomLabelIdList();
            customLabelIdList.add(labelId);
            user.setCustomLabelIdList(customLabelIdList);
            userMapper.updateRecord(user);
        });

        return SaveCustomLabelReply.newBuilder()
                .setRet(0)
                .setLabelId(labelId)
                .build();
    }

    private List<Long> processUserIdStr(Long orgId, String userIdStr) {
        if (StringUtils.isEmpty(userIdStr)) {
            throw new UserIdStrFormatException(CUSTOM_LABEL_USER_ID_EMPTY, Arrays.asList());
        }
        try {
            // 去空格、换行、中文逗号转英文逗号
            userIdStr = userIdStr.trim();
            userIdStr = userIdStr.replaceAll(" ", "");
            userIdStr = userIdStr.replaceAll("\n\r", "");
            userIdStr = userIdStr.replaceAll("\r", "");
            userIdStr = userIdStr.replaceAll("\n", "");
            userIdStr = userIdStr.replaceAll("，", ",");
            userIdStr = userIdStr.replaceAll(";", ",");
            userIdStr = userIdStr.replaceAll("；", ",");
            String[] userIdStrArr = userIdStr.split(",");
            Set<Long> uidList = new HashSet<>();
            List<Long> wrongUidList = new ArrayList<>();
            for (String uidStr : userIdStrArr) {
                if (uidStr.length() > 20) {
                    wrongUidList.add(Long.parseLong(uidStr));
                }
                if (StringUtils.isNotEmpty(uidStr)) {
                    uidList.add(Long.parseLong(uidStr));
                }
            }
            if (!CollectionUtils.isEmpty(wrongUidList)) {
                throw new UserIdStrFormatException(CUSTOM_LABEL_USER_ID_FORMAT_ERROR, wrongUidList);
            }
            Set<Long> existUserIdSet = existUserId(orgId, uidList);
            uidList.forEach(uid -> {
                if (!existUserIdSet.contains(uid)) {
                    wrongUidList.add(uid);
                }
            });
            if (!CollectionUtils.isEmpty(wrongUidList)) {
                throw new UserIdStrFormatException(CUSTOM_LABEL_USER_ID_HAVE_NOT_EXIST, wrongUidList);
            }
            return new ArrayList<>(existUserIdSet);
        } catch (NumberFormatException e) {
            throw new UserIdStrFormatException(CUSTOM_LABEL_USER_ID_FORMAT_ERROR, Arrays.asList());
        }
    }

    private Set<Long> existUserId(Long orgId, Set<Long> uidList) {
        Set<Long> existUidList = new HashSet<>();
        Set<Long> tempList = new HashSet<>();
        for (Long uid : uidList) {
            tempList.add(uid);
            if (tempList.size() == 500) {
                existUidList.addAll(getUidList(orgId, tempList));
                tempList.clear();
            }
        }
        if (!CollectionUtils.isEmpty(tempList)) {
            existUidList.addAll(getUidList(orgId, tempList));
        }
        return existUidList;
    }

    private Set<Long> getUidList(Long orgId, Set<Long> uidList) {
        Example example = Example.builder(User.class).build();
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("orgId", orgId);
        criteria.andIn("userId", uidList);
        List<User> users = userMapper.selectByExample(example);
        return users.stream().map(User::getUserId).collect(Collectors.toSet());
    }

    public class UserIdStrFormatException extends RuntimeException {

        @Getter
        private Integer errorCode;

        @Getter
        private List<Long> wrongUserIdList;

        public UserIdStrFormatException(Integer errorCode, List<Long> wrongUserIdList) {
            this.errorCode = errorCode;
            this.wrongUserIdList = wrongUserIdList;
        }

    }

    public DelCustomLabelReply delCustomLabel(DelCustomLabelRequest request) {
        delCustomLabel(request.getOrgId(), request.getLabelId());
        return DelCustomLabelReply.newBuilder().setRet(0).build();
    }

    private void delCustomLabel(Long orgId, Long labelId) {
        CustomLabel customLabel = customLabelMapper.getOneByLabelId(orgId, labelId);
        if (Objects.nonNull(customLabel)) {
            List<Long> labelUsers = customLabelUserMapper.getUserIdsByLabel(orgId, labelId);

            // 清除标签
            customLabelMapper.delCustomLabel(orgId, labelId, System.currentTimeMillis());
            customLabelUserMapper.deleteLabelUser(orgId, labelId);

            // 清除关联此标签的用户设置
            if (!CollectionUtils.isEmpty(labelUsers)) {
                for (Long uid : labelUsers) {
                    User user = userMapper.getByUserId(uid);
                    List<Long> customLabelIdList = user.getCustomLabelIdList();
                    customLabelIdList.remove(labelId);
                    user.setCustomLabelIdList(customLabelIdList);
                    userMapper.updateRecord(user);
                }
            }
        }
    }

    private List<CustomLabel> queryCustomLabel(Long orgId, Long fromId, Long endId, Integer limit, Integer type) {
        // 翻页信息
        if (Objects.isNull(limit) || limit > 100) {
            limit = 100;
        }
        Example example = Example.builder(CustomLabel.class).orderByAsc("labelId").build();
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("orgId", orgId);
        criteria.andEqualTo("status", 1);
        criteria.andEqualTo("isDel", 0);
        if (Objects.nonNull(fromId) && fromId > 0) {
            List<Long> labelIds = customLabelMapper.pageableByFromId(orgId, fromId, limit);
            if (CollectionUtils.isEmpty(labelIds)) {
                return Arrays.asList();
            }
            criteria.andIn("labelId", labelIds);
        } else if (Objects.nonNull(endId) && endId > 0) {
            List<Long> labelIds = customLabelMapper.pageableByEndId(orgId, endId, limit);
            if (CollectionUtils.isEmpty(labelIds)) {
                return Arrays.asList();
            }
            criteria.andIn("labelId", labelIds);
        }
        criteria.andEqualTo("type", type);
        return customLabelMapper.selectByExample(example);
    }
}
