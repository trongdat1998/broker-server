package io.bhex.broker.server.grpc.server.service;

import com.github.pagehelper.PageHelper;
import com.google.common.base.Strings;
import io.bhex.base.common.CancelBaseConfigRequest;
import io.bhex.base.common.EditBaseConfigsRequest;
import io.bhex.base.common.SwtichStatus;
import io.bhex.broker.server.model.BaseConfigInfo;
import io.bhex.broker.server.model.BaseConfigMeta;
import io.bhex.broker.server.primary.mapper.BaseConfigInfoMapper;
import io.bhex.broker.server.primary.mapper.BaseConfigMetaMapper;
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
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@Service
public class BaseConfigService  extends BaseConfigServiceAbstract {

    @Resource
    private BaseConfigInfoMapper configInfoMapper;

    @Resource
    private BaseConfigMetaMapper configMetaMapper;

    public List<BaseConfigMeta> getConfigMetas(String group, String key) {
        Example example = Example.builder(BaseConfigMeta.class).build();
        Example.Criteria criteria = example.createCriteria()
                .andEqualTo("confGroup", group);
        if (StringUtils.isNotEmpty(key)) {
            criteria.andEqualTo("confKey", key);
        }
        return configMetaMapper.selectByExample(example);
    }

    public BaseConfigInfo getOneConfig(long orgId, String group, String key, String language, boolean available) {
        Example example = Example.builder(BaseConfigInfo.class).build();
        Example.Criteria criteria = example.createCriteria()
                .andEqualTo("orgId", orgId)
                .andEqualTo("confGroup", group);
        if (StringUtils.isNotEmpty(key)) {
            criteria.andEqualTo("confKey", key);
        }
        if (StringUtils.isNotEmpty(language)) {
            criteria.andEqualTo("language", language);
        }
        if (available) {
            criteria.andEqualTo("status", 1);
        }
        BaseConfigInfo baseConfigInfo = configInfoMapper.selectOneByExample(example);
        if (baseConfigInfo == null) {
            return null;
        }
        long start = baseConfigInfo.getNewStartTime(), end = baseConfigInfo.getNewEndTime();
        if (start == 0 && end == 0) {
            return baseConfigInfo;
        }
        long now = System.currentTimeMillis();
        if (isEffective(now, baseConfigInfo.getNewStartTime(), baseConfigInfo.getNewEndTime())) {
            //当前时间在生效中
            baseConfigInfo.setConfValue(baseConfigInfo.getNewConfValue());
            baseConfigInfo.setExtraValue(baseConfigInfo.getNewExtraValue());
        } else  if (baseConfigInfo.getCreated().equals(baseConfigInfo.getUpdated())) {
            //并且是新加的，此时认为不可用
            baseConfigInfo.setIsOpen(false);
        }


        return baseConfigInfo;
    }


    @Transactional
    public boolean editConfigs(EditBaseConfigsRequest request) {
        List<EditBaseConfigsRequest.Config> configs = request.getConfigList();
        for (EditBaseConfigsRequest.Config config : configs) {
            long now = System.currentTimeMillis();
            String confValue = config.getValue();
            if (config.getSwitchStatus() != SwtichStatus.SWITCH_NOT_SWITCH) {
                confValue = config.getSwitchStatus() == SwtichStatus.SWITCH_OPEN ? "true" : "false";
            }
            BaseConfigInfo existed;
            if (config.getId() > 0) {
                existed = configInfoMapper.selectByPrimaryKey(config.getId());
            } else {
                existed = getOneConfig(config.getOrgId(), config.getGroup(), config.getKey(), config.getLanguage(), false);
            }
            boolean hasEffectiveTime = config.getStartTime() > 0 || config.getEndTime() > 0;
            if (existed == null) {
                BaseConfigInfo configInfo = BaseConfigInfo.builder()
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
                        .status(1)
                        .build();
                configInfoMapper.insertSelective(configInfo);
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
                configInfoMapper.updateByPrimaryKeySelective(existed);
            }
        }
        return true;
    }


    public boolean cancelConfig(CancelBaseConfigRequest request) {
        BaseConfigInfo existed = getOneConfig(request.getOrgId(), request.getGroup(), request.getKey(), request.getLanguage(), true);
        if (existed == null) {
            return false;
        }

        existed.setAdminUserName(request.getAdminUserName());
        existed.setUpdated(System.currentTimeMillis());
        existed.setStatus(0);
        int count = configInfoMapper.updateByPrimaryKeySelective(existed);

        return count == 1;
    }

    public boolean cancelConfigs(String group, String key) {

        return true;
    }
//    public List<BaseConfigInfo> getConfigsByGroupAndKey(String group, String key) {
//        return getConfigs(0L, Lists.newArrayList(group), Lists.newArrayList(key), null,  0, 0);
//    }

//    public List<BaseConfigInfo> getConfigsByGroup(long orgId, String group) {
//        return getConfigs(orgId, Arrays.asList(group), new ArrayList<>(), null, 0, 0);
//    }
//
//    public List<BaseConfigInfo> getConfigsByGroup(long orgId, String group, String language) {
//        return getConfigs(orgId, Arrays.asList(group), new ArrayList<>(), language, 0, 0);
//    }

//    public List<BaseConfigInfo> getConfigs(long orgId, String group, List<String> keys) {
//        return getConfigs(orgId, Arrays.asList(group), keys, null, 0, 0);
//    }


    public List<BaseConfigInfo> getConfigs(long orgId, List<String> groups, List<String> keys,
                                           String language, int pageSize, long lastId) {
        Example example = Example.builder(BaseConfigInfo.class).orderByDesc("id").build();

        PageHelper.startPage(0, pageSize > 0 ? pageSize : Integer.MAX_VALUE);

        Example.Criteria criteria = example.createCriteria()
                .andEqualTo("status", 1);
        if (orgId > 0) {
            criteria.andIn("orgId", Arrays.asList(orgId, 0L));
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
        if (lastId > 0) {
            criteria.andLessThan("id", lastId);
        }

        List<BaseConfigInfo> list = configInfoMapper.selectByExample(example);
        if (CollectionUtils.isEmpty(list)) {
            return new ArrayList<>();
        }

        //如果券商有設置並且有自定義的 保留券商設置的過濾掉自定義的
        Function<BaseConfigInfo, String> configMapToKey = c -> c.getConfGroup() + "-" + c.getConfKey() + "-" + Strings.nullToEmpty(c.getLanguage());
        Set<String> configKeysSet = list.stream().filter(c -> c.getOrgId() > 0).map(configMapToKey).collect(Collectors.toSet());
        list = list.stream().filter(c -> c.getOrgId() > 0 || !configKeysSet.contains(configMapToKey.apply(c))).collect(Collectors.toList());

        long now = System.currentTimeMillis();
        for (BaseConfigInfo baseConfigInfo : list) {
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

    public Pair<Boolean, Boolean> getConfigSwitch(long orgId, String group, String key, String language) {
        BaseConfigInfo configInfo = getOneConfig(orgId, group, key, language, true);
        boolean existed = configInfo != null && configInfo.getIsOpen();
        boolean open = existed && configInfo.getConfValue().toLowerCase().equals("true");
        return Pair.of(existed, open);
    }

    public boolean getConfigSwitchStatus(long orgId, String group, String key, String language) {
        return getConfigSwitch(orgId, group, key, language).getRight();
    }


    //针对key是userid的配置列表
    public List<BaseConfigInfo> getUserConfigs(long orgId, String group, int pageSize, long fromId, long userId) {
        Example example =  Example.builder(BaseConfigInfo.class)
                .orderByDesc("id")
                .build();
        PageHelper.startPage(0, pageSize);
        Example.Criteria criteria =   example.createCriteria()
                .andEqualTo("orgId", orgId)
                .andEqualTo("status", 1)
                .andEqualTo("confGroup", group);

        if (userId > 0) {
            criteria.andEqualTo("confKey", userId + "");
        }

        if (fromId > 0) {
            criteria.andLessThan("id", fromId);
        }
        List<BaseConfigInfo> configs = configInfoMapper.selectByExample(example);
        return configs;
    }


}
