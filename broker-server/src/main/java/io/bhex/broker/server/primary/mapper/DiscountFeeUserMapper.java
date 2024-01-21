package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.DiscountFeeUser;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

@Mapper
public interface DiscountFeeUserMapper extends tk.mybatis.mapper.common.Mapper<DiscountFeeUser> {

    @Update("update tb_discount_fee_user set status = 0 where org_id=#{orgId} and base_group_id = #{baseGroupId}")
    int updateStatusByBaseGroupId(@Param("orgId") Long orgId, @Param("baseGroupId") Long baseGroupId);


    @Select("select *from tb_discount_fee_user where org_id=#{orgId} and user_id = #{userId} and status = 1 and exchangeId = #{exchangeId} and symbolId = #{symbolId}")
    DiscountFeeUser selectDiscountFeeUserByUserId(@Param("orgId") Long orgId, @Param("userId") Long userId, @Param("exchangeId") Long exchangeId, @Param("symbolId") String symbolId);

    @Update("update tb_discount_fee_user set status = 0 where org_id=#{orgId} and base_group_id = #{baseGroupId} and user_id = #{userId}")
    int updateStatusByBaseGroupIdAndUserId(@Param("orgId") Long orgId, @Param("baseGroupId") Long baseGroupId, @Param("userId") Long userId);


}
