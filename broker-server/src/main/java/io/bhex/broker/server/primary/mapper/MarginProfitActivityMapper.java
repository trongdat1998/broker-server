package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.MarginProfitActivity;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import java.util.List;

/**
 * @author JinYuYuan
 * @description
 * @date 2021-03-09 15:33
 */
@Mapper
public interface MarginProfitActivityMapper extends tk.mybatis.mapper.common.Mapper<MarginProfitActivity> {
    @Select("select * from tb_margin_profit_activity where org_id = #{orgId} AND join_date = #{joinDate} AND account_id = #{accountId}")
    MarginProfitActivity getProfitRecord(@Param("orgId")Long orgId,@Param("joinDate")Long joinDate,@Param("accountId")Long accountId);

    @Select("select * from tb_margin_profit_activity where org_id = #{orgId} and join_date = #{joinDate} order by profit_rate DESC")
    List<MarginProfitActivity> queryProfitRecordToCheck(@Param("orgId")Long orgId, @Param("joinDate")Long joinDate);

    @Select("select * from tb_margin_profit_activity where org_id = #{orgId} and join_date = #{joinDate} and day_ranking > 0 order by day_ranking limit #{limit}")
    List<MarginProfitActivity> queryTopProfitRecord(@Param("orgId")Long orgId, @Param("joinDate")Long joinDate,@Param("limit") Integer limit);

    @Update("update tb_margin_profit_activity set day_ranking =#{ranking} where org_id = #{orgId} and join_date = #{joinDate} and user_id = #{userId}")
    int updateRanking(@Param("orgId")Long orgId, @Param("joinDate")Long joinDate, @Param("userId")Long userId,@Param("ranking") Integer ranking);
}
