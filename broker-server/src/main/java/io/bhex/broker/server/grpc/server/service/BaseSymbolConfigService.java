package io.bhex.broker.server.grpc.server.service;

import com.beust.jcommander.internal.Lists;
import com.github.pagehelper.PageHelper;
import com.google.common.base.Strings;
import io.bhex.base.common.CancelBaseSymbolConfigRequest;
import io.bhex.base.common.EditBaseSymbolConfigsRequest;
import io.bhex.base.common.SwtichStatus;
import io.bhex.broker.server.model.BaseSymbolConfigInfo;
import io.bhex.broker.server.primary.mapper.BaseSymbolConfigInfoMapper;
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
public class BaseSymbolConfigService extends BaseConfigServiceAbstract {


    @Resource
    private BaseSymbolConfigInfoMapper symbolConfigInfoMapper;


    public BaseSymbolConfigInfo getOneSymbolConfig(long orgId, String symbol, String group, String key, String language) {
        return getOneSymbolConfig(orgId, symbol, group, key, language, true);
    }

    public BaseSymbolConfigInfo getOneSymbolConfig(long orgId, String symbol, String group, String key, String language, boolean available) {
        Example example = Example.builder(BaseSymbolConfigInfo.class).build();
        Example.Criteria criteria = example.createCriteria()
                .andEqualTo("orgId", orgId)
                .andEqualTo("confGroup", group);
        if (StringUtils.isNotEmpty(key)) {
            criteria.andEqualTo("confKey", key);
        }
        if (StringUtils.isNotEmpty(language)) {
            criteria.andEqualTo("language", language);
        }
        if (StringUtils.isNotEmpty(symbol)) {
            criteria.andEqualTo("symbol", symbol);
        }
        if (available) {
            criteria.andEqualTo("status", 1);
        }

        BaseSymbolConfigInfo baseConfigInfo = symbolConfigInfoMapper.selectOneByExample(example);
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
        } else if (baseConfigInfo.getCreated().equals(baseConfigInfo.getUpdated())) {
            baseConfigInfo.setIsOpen(false);
        }
        return baseConfigInfo;
    }

    @Transactional
    public boolean editSymbolConfigs(EditBaseSymbolConfigsRequest request) {
        List<EditBaseSymbolConfigsRequest.Config> configs = request.getConfigList();
        for (EditBaseSymbolConfigsRequest.Config config : configs) {
            long now = System.currentTimeMillis();
            String confValue = config.getValue();
            if (config.getSwitchStatus() != SwtichStatus.SWITCH_NOT_SWITCH) {
                confValue = config.getSwitchStatus() == SwtichStatus.SWITCH_OPEN ? "true" : "false";
            }
            BaseSymbolConfigInfo existed;
            if (config.getId() > 0) {
                existed = symbolConfigInfoMapper.selectByPrimaryKey(config.getId());
            } else {
                existed = getOneSymbolConfig(config.getOrgId(), config.getSymbol(),
                        config.getGroup(), config.getKey(), config.getLanguage(), false);
            }
            boolean hasEffectiveTime = config.getStartTime() > 0 || config.getEndTime() > 0;
            if (existed == null) {
                BaseSymbolConfigInfo configInfo = BaseSymbolConfigInfo.builder()
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

                        .symbol(Strings.nullToEmpty(config.getSymbol()))
                        .status(1)
                        .build();
                symbolConfigInfoMapper.insertSelective(configInfo);
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

                symbolConfigInfoMapper.updateByPrimaryKeySelective(existed);
            }
        }
        return true;
    }


    public boolean cancelSymbolConfig(CancelBaseSymbolConfigRequest request) {
        BaseSymbolConfigInfo existed = getOneSymbolConfig(request.getOrgId(), request.getSymbol(),
                request.getGroup(), request.getKey(), request.getLanguage(), true);
        if (existed == null) {
            return false;
        }
        existed.setAdminUserName(request.getAdminUserName());
        existed.setUpdated(System.currentTimeMillis());
        existed.setStatus(0);
        int count = symbolConfigInfoMapper.updateByPrimaryKeySelective(existed);

        return count == 1;
    }

    public List<BaseSymbolConfigInfo> getSymbolConfigsByGroup(long orgId, List<String> symbols, String group) {
        return getSymbolConfigs(orgId, symbols, Arrays.asList(group), new ArrayList<>(), null, 0, 0);
    }

    public List<BaseSymbolConfigInfo> getSymbolConfigsByGroupAndKey(String group, String key) {
        return getSymbolConfigs(0L, null, Lists.newArrayList(group), Lists.newArrayList(key), null, 0, 0);
    }

    public List<BaseSymbolConfigInfo> getSymbolConfigsByGroup(long orgId, List<String> symbols, String group, String language) {
        return getSymbolConfigs(orgId, symbols, Arrays.asList(group), new ArrayList<>(), language,  0, 0);
    }

    public List<BaseSymbolConfigInfo> getSymbolConfigs(long orgId, List<String> symbols, String group, List<String> keys) {
        return getSymbolConfigs(orgId, symbols, Arrays.asList(group), keys, null, 0, 0);
    }

    public List<BaseSymbolConfigInfo> getSymbolConfigs(long orgId, List<String> symbols, List<String> groups, List<String> keys, String language, int pageSize, long lastId) {
        Example example = Example.builder(BaseSymbolConfigInfo.class).orderByDesc("id").build();
        PageHelper.startPage(0, pageSize > 0 ? pageSize : Integer.MAX_VALUE);
        Example.Criteria criteria = example.createCriteria()
                .andEqualTo("status", 1);
        if (orgId > 0) {
            criteria.andEqualTo("orgId", orgId);
        }
        if (CollectionUtils.isNotEmpty(groups)) {
            criteria.andIn("confGroup", groups);
        }
        if (CollectionUtils.isNotEmpty(keys)) {
            criteria.andIn("confKey", keys);
        }
        if (StringUtils.isNotEmpty(language)) {
            criteria.andEqualTo("language", language);
        }
        if (CollectionUtils.isNotEmpty(symbols)) {
            criteria.andIn("symbol", symbols);
        }
        if (lastId > 0) {
            criteria.andLessThan("id", lastId);
        }

        List<BaseSymbolConfigInfo> list = symbolConfigInfoMapper.selectByExample(example);
        if (CollectionUtils.isEmpty(list)) {
            return new ArrayList<>();
        }
        long now = System.currentTimeMillis();
        for (BaseSymbolConfigInfo baseConfigInfo : list) {
            long start = baseConfigInfo.getNewStartTime(), end = baseConfigInfo.getNewEndTime();
            if (start == 0 && end == 0) {
                continue;
            }

            if (isEffective(now, start, end)) {
                baseConfigInfo.setConfValue(baseConfigInfo.getNewConfValue());
                baseConfigInfo.setExtraValue(baseConfigInfo.getNewExtraValue());
            } else  if (baseConfigInfo.getCreated().equals(baseConfigInfo.getUpdated())) {
                baseConfigInfo.setIsOpen(false);
            }
        }


        return list;
    }

    public Pair<Boolean, Boolean> getSymbolConfigSwitch(long orgId, String symbol, String group, String key, String language) {
        BaseSymbolConfigInfo configInfo = getOneSymbolConfig(orgId, symbol, group, key, language, true);
        boolean existed = configInfo != null && configInfo.getIsOpen();
        boolean open = existed && configInfo.getConfValue().toLowerCase().equals("true");
        return Pair.of(existed, open);
    }


    public boolean getSymbolConfigSwitchStatus(long orgId, String symbol, String group, String key, String language) {
        return getSymbolConfigSwitch(orgId, symbol, group, key, language).getRight();
    }

    public boolean getSymbolConfigSwitchStatus(long orgId, String symbol, String group, String key) {
        return getSymbolConfigSwitch(orgId, symbol, group, key, null).getRight();
    }


}
