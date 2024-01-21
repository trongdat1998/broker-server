package io.bhex.broker.server.primary.mapper;
import io.bhex.broker.server.model.LoginStatistic;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/**********************************
 *@项目名称: broker-parent
 *@文件名称: io.bhex.broker.service.user
 *@Date 2018/8/3
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
@Mapper
public interface LoginStatisticMapper extends tk.mybatis.mapper.common.Mapper<LoginStatistic> {

    @Select("select count(1) from tb_login_statistic where statistic_date = #{statisticDate}")
    Integer countLoginStatisticByTime(@Param("statisticDate")String statisticDate);


    @Select("select * from tb_login_statistic where statistic_date = #{statisticDate}")
    List<LoginStatistic> queryLoginStatisticListByTime(@Param("statisticDate")String statisticDate);
}
