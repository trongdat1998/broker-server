package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.DictValue;
import io.bhex.broker.server.model.staking.StakingProductOrder;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import tk.mybatis.mapper.common.Mapper;

/**
 * 键值对
 *
 * @author generator
 * @date 2021-01-13
 */
public interface DictValueMapper extends Mapper<DictValue> {
    /**
     * get text
     *
     * @param orgId
     * @param bizKey
     * @param kValue
     * @param language
     * @return
     */
    @Select("select dict_text from tb_dict_values where org_id=#{orgId} and biz_key=#{bizKey} and dict_value=#{kValue} and language=#{language} limit 1;")
    String getText(@Param("orgId") Long orgId, @Param("bizKey") String bizKey, @Param("kValue") Integer kValue, @Param("language") String language);
}
