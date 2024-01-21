/**********************************
 *@项目名称: server-parent
 *@文件名称: io.bhex.broker.server.grpc.server.service
 *@Date 2018/12/2
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
package io.bhex.broker.server.grpc.server.service;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import io.bhex.broker.server.domain.CommonIni;
import io.bhex.broker.server.primary.mapper.CommonIniMapper;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import tk.mybatis.mapper.entity.Example;

import javax.annotation.Resource;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Service
public class CommonIniService {

    private static final String COMMON_INI_CACHE_KEY = "%s_%s_%s";

    @Resource
    private CommonIniMapper commonIniMapper;

    private static final Cache<String, CommonIni> COMMON_INI_CACHE = CacheBuilder.newBuilder()
            .initialCapacity(1024)
            .expireAfterWrite(10, TimeUnit.MINUTES).build();

    @Deprecated
    public List<CommonIni> getCommonIniList(Long orgId, List<String> iniNameList, String language) {
        if (CollectionUtils.isEmpty(iniNameList)) {
            return Lists.newArrayList();
        }
        Example example = Example.builder(CommonIni.class).build();
        example.createCriteria().andEqualTo("orgId", orgId)
                .andIn("iniName", iniNameList)
                .andEqualTo("language", language);
        return commonIniMapper.selectByExample(example);
    }

    public CommonIni getCommonIni(Long orgId, String iniName, String language) {
        return commonIniMapper.getByOrgIdAndInitNameAndLanguage(orgId, iniName, language);
    }

    public CommonIni getCommonIni(Long orgId, String iniName) {
        return getCommonIni(orgId, iniName, "");
    }

    public CommonIni getCommonIniFromCache(Long orgId, String iniName, String language) {
        String key = String.format(COMMON_INI_CACHE_KEY, orgId, iniName, language);
        CommonIni cachedCommonIni;
        try {
            cachedCommonIni = COMMON_INI_CACHE.get(key, () -> {
                CommonIni commonIni = commonIniMapper.getByOrgIdAndInitNameAndLanguage(orgId, iniName, language);
                if (commonIni == null) {
                    throw new RuntimeException("CommonIni with " + key + " not found");
                }
                return commonIni;
            });
        } catch (Exception e) {
            cachedCommonIni = commonIniMapper.getByOrgIdAndInitNameAndLanguage(orgId, iniName, language);
        }
        return cachedCommonIni;
    }

    public CommonIni getCommonIniFromCache(Long orgId, String iniName) {
        return getCommonIniFromCache(orgId, iniName, "");
    }

    public List<CommonIni> queryCommonIniByOrgIdAndIniNames(Long orgId, List<String> iniNameList) {
        if (CollectionUtils.isEmpty(iniNameList)) {
            return Lists.newArrayList();
        }
        Example example = Example.builder(CommonIni.class).build();
        example.createCriteria().andEqualTo("orgId", orgId)
                .andIn("iniName", iniNameList);
        return commonIniMapper.selectByExample(example);
    }

    public List<CommonIni> queryOrgCommonIni(Long orgId) {
        return commonIniMapper.queryOrgCommonIni(orgId);
    }

    public List<CommonIni> queryOrgCustomCommonIni(Long orgId) {
        return commonIniMapper.queryOrgCustomCommonIni(orgId);
    }

    /**
     * common_ini中的数据，都应该和org_id关联
     */
    @Deprecated
    public static CommonIni getCommonIni(String iniName) {
        return null;
    }

    public int insertOrUpdateCommonIni(CommonIni commonIni) {
        try {
            CommonIni dbIni = commonIniMapper.getByOrgIdAndInitNameAndLanguage(commonIni.getOrgId(), commonIni.getIniName(), commonIni.getLanguage());
            if (dbIni == null) {
                commonIni.setCreated(System.currentTimeMillis());
                commonIni.setUpdated(System.currentTimeMillis());
                return commonIniMapper.insert(commonIni);
            } else {
                dbIni.setIniValue(commonIni.getIniValue());
                dbIni.setUpdated(System.currentTimeMillis());
                return commonIniMapper.updateByPrimaryKeySelective(dbIni);
            }
        } finally {
            String key = String.format(COMMON_INI_CACHE_KEY, commonIni.getOrgId(), commonIni.getIniName(), commonIni.getLanguage());
            COMMON_INI_CACHE.invalidate(key);
        }
    }

    public String getStringValue(Long orgId, String iniName, String language) {
        CommonIni commonIni = getCommonIni(orgId, iniName, language);
        if (commonIni != null) {
            return commonIni.getIniValue();
        }
        return null;
    }

    public String getStringValue(Long orgId, String iniName) {
        return getStringValue(orgId, iniName, "");
    }

    public String getStringValueOrDefault(Long orgId, String iniName, String language, String defaultValue) {
        String iniValue = getStringValue(orgId, iniName, language);
        if (iniValue == null) {
            return defaultValue;
        }
        return iniValue;
    }

    public String getStringValueOrDefault(Long orgId, String iniName, String defaultValue) {
        return getStringValueOrDefault(orgId, iniName, "", defaultValue);
    }

    public Boolean getBooleanValue(Long orgId, String iniName, String language) {
        CommonIni commonIni = getCommonIni(orgId, iniName, language);
        if (commonIni != null) {
            return Boolean.parseBoolean(commonIni.getIniValue());
        }
        return null;
    }

    public Boolean getBooleanValue(Long orgId, String iniName) {
        return getBooleanValue(orgId, iniName, "");
    }

    public boolean getBooleanValueOrDefault(Long orgId, String iniName, String language, boolean defaultValue) {
        Boolean iniValue = getBooleanValue(orgId, iniName, language);
        if (iniValue == null) {
            return defaultValue;
        }
        return iniValue;
    }

    public boolean getBooleanValueOrDefault(Long orgId, String iniName, boolean defaultValue) {
        return getBooleanValueOrDefault(orgId, iniName, "", defaultValue);
    }

    public Integer getIntegerValue(Long orgId, String iniName, String language) {
        CommonIni commonIni = getCommonIni(orgId, iniName, language);
        if (commonIni != null) {
            return Ints.tryParse(commonIni.getIniValue());
        }
        return null;
    }

    public Integer getIntegerValue(Long orgId, String iniName) {
        return getIntegerValue(orgId, iniName, "");
    }

    public int getIntValueOrDefault(Long orgId, String iniName, String language, int defaultValue) {
        String iniValue = getStringValue(orgId, iniName, language);
        if (iniValue == null) {
            return defaultValue;
        }
        return Ints.tryParse(iniValue);
    }

    public int getIntValueOrDefault(Long orgId, String iniName, int defaultValue) {
        return getIntValueOrDefault(orgId, iniName, "", defaultValue);
    }

}
