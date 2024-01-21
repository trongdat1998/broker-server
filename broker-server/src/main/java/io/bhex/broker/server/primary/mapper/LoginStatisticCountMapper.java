package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.LoginStatisticCount;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

/**********************************
 *@项目名称: broker-parent
 *@文件名称: io.bhex.broker.service.user
 *@Date 2018/8/3
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
@Mapper
public interface LoginStatisticCountMapper extends tk.mybatis.mapper.common.Mapper<LoginStatisticCount> {
    @Select("select id from tb_login_statistic_count where statistic_date = #{statisticDate} and org_id = #{orgId}")
    Long queryIdLoginStatisticCountByTime(@Param("statisticDate") String statisticDate, @Param("orgId") Long orgId);
}
