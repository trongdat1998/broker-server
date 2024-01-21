package io.bhex.broker.server.primary.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import io.bhex.broker.server.model.Internationalization;

/**
 * @Author: yuehao  <hao.yue@bhex.com>
 * @CreateDate: 2019-03-08 12:00
 * @Copyright（C）: 2018 BHEX Inc. All rights reserved.
 */
@org.apache.ibatis.annotations.Mapper
public interface InternationalizationMapper extends tk.mybatis.mapper.common.Mapper<Internationalization> {

    @Select("SELECT * FROM tb_internationalization WHERE in_key = #{inKey} and env = #{env} and in_status = 0 limit 1")
    Internationalization queryInternationalizationByKey(@Param("inKey") String inKey, @Param("env") String env);
}
