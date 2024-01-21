package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.FinanceInterest;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import tk.mybatis.mapper.common.special.InsertListMapper;

import java.util.List;

@Mapper
public interface FinanceInterestMapper extends tk.mybatis.mapper.common.Mapper<FinanceInterest>, InsertListMapper<FinanceInterest> {

    String TABLE_NAME = "tb_finance_interest";

    @Select(" select * from " + TABLE_NAME + " where org_id = #{orgId} "
            + " and product_id = #{productId} "
            + " and statistics_time <= #{startStatisticsTime} "
            + " order by statistics_time desc limit #{limit} ")
    List<FinanceInterest> getSevenFinanceInterest(@Param("orgId") Long orgId,
                                                  @Param("productId") Long productId,
                                                  @Param("startStatisticsTime") String startStatisticsTime,
                                                  @Param("limit") Integer limit);

    @Select(" select * from " + TABLE_NAME + " where org_id = #{orgId} "
            + " and product_id = #{productId} "
            + " and statistics_time = #{statisticsTime} ")
    FinanceInterest getFinanceInterestByOrgIdAndProductIdAndStatisticsTime(@Param("orgId") Long orgId,
                                                                           @Param("productId") Long productId,
                                                                           @Param("statisticsTime") String statisticsTime);
}
