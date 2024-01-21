package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.ActivityExperienceFund;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import java.math.BigDecimal;
import java.util.List;

@Mapper
public interface ActivityExperienceFundMapper extends tk.mybatis.mapper.common.Mapper<ActivityExperienceFund> {


    @Select("select * from tb_activity_experience_fund where redeem_time < #{now} and status = 2 ")
    List<ActivityExperienceFund> getUnredeemTask(@Param("now") long now);

    @Select("select * from tb_activity_experience_fund where status = 2 and redeem_time between #{start} and #{end}")
    List<ActivityExperienceFund> getNewUnredeemTask(@Param("start") long start, @Param("end") long end);

    @Update("update tb_activity_experience_fund set redeem_amount = redeem_amount + #{amount} where id = #{id}")
    int incRedeemAmount(@Param("id") long id, @Param("amount") BigDecimal amount);
}
