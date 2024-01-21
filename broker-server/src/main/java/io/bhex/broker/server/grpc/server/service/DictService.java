package io.bhex.broker.server.grpc.server.service;

import com.google.common.base.Strings;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.bhex.broker.server.model.DictValue;
import io.bhex.broker.server.primary.mapper.DictValueMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 键值对服务(暂停使用)
 *
 * @author songxd
 * @date 2021-01-13
 */
@Service
@Slf4j
@Deprecated
public class DictService {

    @Resource
    private DictValueMapper dictValueMapper;

    private final Cache<String, String> DICT_TEXT_CACHE = CacheBuilder
            .newBuilder()
            .expireAfterWrite(10L, TimeUnit.MINUTES)
            .build();

    /**
     * 获取指定业务的键值对
     *
     * @param orgId
     * @param bizKey
     * @param language
     * @return
     */
    public List<DictValue> getKeyValueList(Long orgId, String bizKey, String language){
        orgId = 0L;
        return dictValueMapper.select(DictValue.builder().orgId(orgId).bizKey(bizKey).language(language).build());
    }

    /**
     * get dict text
     *
     * @param orgId
     * @param bizKey
     * @param value
     * @param language
     * @return
     */
    public String getText(Long orgId, String bizKey, Integer value, String language){
        orgId = 0L;
        String key = String.format("%s_%s_%s",orgId, bizKey,value);
        String textResult;
        try {
            Long finalOrgId = orgId;
            textResult = DICT_TEXT_CACHE.get(key, () -> {
                String text = dictValueMapper.getText(finalOrgId, bizKey, value, language);
                if(Strings.isNullOrEmpty(text)){
                    text = dictValueMapper.getText(finalOrgId, bizKey, value, "en_US");
                }
                return Strings.isNullOrEmpty(text) ? "" : text;
            });
        } catch (Exception e) {
            textResult = "";
        }
        return textResult;
    }
}
