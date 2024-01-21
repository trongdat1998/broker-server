package io.bhex.broker.server.grpc.server.service;

import com.github.pagehelper.PageHelper;
import com.google.common.base.Strings;
import io.bhex.base.common.CancelBaseTokenConfigRequest;
import io.bhex.base.common.EditBaseTokenConfigsRequest;
import io.bhex.base.common.SwtichStatus;
import io.bhex.broker.server.model.BaseTokenConfigInfo;
import io.bhex.broker.server.primary.mapper.BaseTokenConfigInfoMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tk.mybatis.mapper.entity.Example;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Slf4j
@Service
public class BaseTokenConfigService extends BaseConfigServiceAbstract {

    @Resource
    private BaseTokenConfigInfoMapper tokenConfigInfoMapper;

    public BaseTokenConfigInfo getOneTokenConfig(long orgId, String token, String group, String key, String language) {
        return getOneTokenConfig(orgId, token, group, key, language, true);
    }

    public BaseTokenConfigInfo getOneTokenConfig(long orgId, String token, String group, String key, String language, boolean available) {
        Example example = Example.builder(BaseTokenConfigInfo.class).build();
        Example.Criteria criteria = example.createCriteria()
                .andEqualTo("orgId", orgId)
                .andEqualTo("confGroup", group);
        if (StringUtils.isNotEmpty(key)) {
            criteria.andEqualTo("confKey", key);
        }
        if (StringUtils.isNotEmpty(language)) {
            criteria.andEqualTo("language", language);
        }
        if (StringUtils.isNotEmpty(token)) {
            criteria.andEqualTo("token", token);
        }
        if (available) {
            criteria.andEqualTo("status", 1);
        }

        BaseTokenConfigInfo baseConfigInfo = tokenConfigInfoMapper.selectOneByExample(example);
        if (baseConfigInfo == null) {
            return null;
        }
        long start = baseConfigInfo.getNewStartTime(), end = baseConfigInfo.getNewEndTime();
        if (start == 0 && end == 0) {
            return baseConfigInfo;
        }

        long now = System.currentTimeMillis();
        if (isEffective(now, baseConfigInfo.getNewStartTime(), baseConfigInfo.getNewEndTime())) {
            baseConfigInfo.setConfValue(baseConfigInfo.getNewConfValue());
            baseConfigInfo.setExtraValue(baseConfigInfo.getNewExtraValue());
        } else  if (baseConfigInfo.getCreated().equals(baseConfigInfo.getUpdated())) {
            //并且是新加的，此时认为不可用
            baseConfigInfo.setIsOpen(false);
        }
        return baseConfigInfo;
    }

    @Transactional
    public boolean editTokenConfigs(EditBaseTokenConfigsRequest request) {
        List<EditBaseTokenConfigsRequest.Config> configs = request.getConfigList();
        for (EditBaseTokenConfigsRequest.Config config : configs) {
            long now = System.currentTimeMillis();
            String confValue = config.getValue();
            if (config.getSwitchStatus() != SwtichStatus.SWITCH_NOT_SWITCH) {
                confValue = config.getSwitchStatus() == SwtichStatus.SWITCH_OPEN ? "true" : "false";
            }
            BaseTokenConfigInfo existed = getOneTokenConfig(config.getOrgId(), config.getToken(),
                    config.getGroup(), config.getKey(), config.getLanguage(), false);
            boolean hasEffectiveTime = config.getStartTime() > 0 || config.getEndTime() > 0;
            if (existed == null) {
                BaseTokenConfigInfo configInfo = BaseTokenConfigInfo.builder()
                        .orgId(config.getOrgId())
                        .confGroup(config.getGroup())
                        .confKey(config.getKey())
                        .confValue(confValue)
                        .extraValue(config.getExtraValue())
                        .adminUserName(config.getAdminUserName())
                        .created(now)
                        .updated(now)
                        .newConfValue(hasEffectiveTime ? confValue : "")
                        .newExtraValue(hasEffectiveTime ? config.getExtraValue() : "")
                        .newStartTime(config.getStartTime())
                        .newEndTime(config.getEndTime())
                        .language(config.getLanguage())
                        .token(Strings.nullToEmpty(config.getToken()))
                        .status(1)
                        .build();
                tokenConfigInfoMapper.insertSelective(configInfo);
            } else {

                if (existed.getStatus() == 0) {
                    existed.setCreated(now);
                }

                //如果新设置的未生效，而原来的是生效中
                if (!isEffective(now, config.getStartTime(), config.getEndTime())) {
                    //新设置的未生效
                    long start = existed.getNewStartTime(), end = existed.getNewEndTime();
                    if (isEffective(now, start, end)) {
                        //原来的是生效中
                        existed.setConfValue(existed.getNewConfValue());
                        existed.setExtraValue(existed.getNewExtraValue());
                    } else {
                        existed.setCreated(now);
                    }
                }
                existed.setConfKey(config.getKey());
                existed.setUpdated(now);
                existed.setAdminUserName(config.getAdminUserName());
                existed.setStatus(config.getStatus());

                if (config.getStartTime() > 0 || config.getEndTime() > 0) {
                    //有生效时间
                    existed.setNewConfValue(confValue);
                    existed.setNewExtraValue(config.getExtraValue());
                }

                if (config.getStartTime() == 0 || config.getEndTime() == 0) {
                    //立即生效
                    existed.setConfValue(confValue);
                    existed.setExtraValue(config.getExtraValue());
                    existed.setNewConfValue("");
                    existed.setNewExtraValue("");
                }

                existed.setNewStartTime(config.getStartTime());
                existed.setNewEndTime(config.getEndTime());

                tokenConfigInfoMapper.updateByPrimaryKeySelective(existed);
            }
        }

        return true;
    }


    public boolean cancelTokenConfig(CancelBaseTokenConfigRequest request) {
        BaseTokenConfigInfo existed = getOneTokenConfig(request.getOrgId(), request.getToken(),
                request.getGroup(), request.getKey(), request.getLanguage(), true);
        if (existed == null) {
            return false;
        }

        existed.setAdminUserName(request.getAdminUserName());
        existed.setUpdated(System.currentTimeMillis());
        existed.setStatus(0);
        int count = tokenConfigInfoMapper.updateByPrimaryKeySelective(existed);

        return count == 1;
    }

    public List<BaseTokenConfigInfo> getTokenConfigsByGroup(long orgId, List<String> tokens, String group) {
        return getTokenConfigs(orgId, tokens, Arrays.asList(group), new ArrayList<>(), null,0, 0);
    }

    public List<BaseTokenConfigInfo> getTokenConfigsByGroup(long orgId, List<String> tokens, String group, String language) {
        return getTokenConfigs(orgId, tokens, Arrays.asList(group), new ArrayList<>(), language, 0, 0);
    }

    public List<BaseTokenConfigInfo> getTokenConfigs(long orgId, List<String> tokens, String group, List<String> keys) {
        return getTokenConfigs(orgId, tokens, Arrays.asList(group), keys, null, 0, 0);
    }

    public List<BaseTokenConfigInfo> getTokenConfigs(long orgId, List<String> tokens, List<String> groups, List<String> keys, String language, int pageSize, long lastId) {
        Example example = Example.builder(BaseTokenConfigInfo.class).orderByDesc("id").build();
        PageHelper.startPage(0, pageSize > 0 ? pageSize : Integer.MAX_VALUE);
        Example.Criteria criteria = example.createCriteria()
                .andEqualTo("status", 1);
        if (orgId > 0) {
            criteria.andEqualTo("orgId", orgId);
        }
        if (!CollectionUtils.isEmpty(groups)) {
            criteria.andIn("confGroup", groups);
        }
        if (!CollectionUtils.isEmpty(keys)) {
            criteria.andIn("confKey", keys);
        }
        if (StringUtils.isNotEmpty(language)) {
            criteria.andEqualTo("language", language);
        }
        if (CollectionUtils.isNotEmpty(tokens)) {
            criteria.andIn("token", tokens);
        }
        if (lastId > 0) {
            criteria.andLessThan("id", lastId);
        }
        List<BaseTokenConfigInfo> list = tokenConfigInfoMapper.selectByExample(example);
        if (CollectionUtils.isEmpty(list)) {
            return new ArrayList<>();
        }
        long now = System.currentTimeMillis();
        for (BaseTokenConfigInfo baseConfigInfo : list) {
            long start = baseConfigInfo.getNewStartTime(), end = baseConfigInfo.getNewEndTime();
            if (start == 0 && end == 0) {
                continue;
            }

            if (isEffective(now, start, end)) {
                baseConfigInfo.setConfValue(baseConfigInfo.getNewConfValue());
                baseConfigInfo.setExtraValue(baseConfigInfo.getNewExtraValue());
            } else  if (baseConfigInfo.getCreated().equals(baseConfigInfo.getUpdated())) {
                //并且是新加的，此时认为不可用
                baseConfigInfo.setIsOpen(false);
            }
        }


        return list;
    }

    public Pair<Boolean, Boolean> getTokenConfigSwitch(long orgId, String token, String group, String key, String language) {
        BaseTokenConfigInfo configInfo = getOneTokenConfig(orgId, token, group, key, language, true);
        boolean existed = configInfo != null && configInfo.getIsOpen();
        boolean open = existed && configInfo.getConfValue().toLowerCase().equals("true");
        return Pair.of(existed, open);
    }

    public boolean getTokenConfigSwitchStatus(long orgId, String token, String group, String key, String language) {
        return getTokenConfigSwitch(orgId, token, group, key, language).getRight();
    }



}
