package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.FinanceLimitStatistics;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import java.math.BigDecimal;

@Mapper
public interface FinanceLimitStatisticsMapper extends tk.mybatis.mapper.common.Mapper<FinanceLimitStatistics> {

    String TABLE_NAME = "tb_finance_limit_statistics";

    @Select(" select * from " + TABLE_NAME + " where "
            + " org_id = #{orgId} "
            + " and product_id = #{productId} "
            + " and user_id = #{userId} "
            + " and type = #{type} "
            + " and statistics_time = #{statisticsTime} for update")
    FinanceLimitStatistics getFinanceLimitStatisticsLock(@Param("orgId") Long orgId,
                                                         @Param("productId") Long productId,
                                                         @Param("userId") Long userId,
                                                         @Param("type") Integer type,
                                                         @Param("statisticsTime") String statisticsTime);

    @Select(" select * from " + TABLE_NAME + " where "
            + " org_id = #{orgId} "
            + " and product_id = #{productId} "
            + " and user_id = #{userId} "
            + " and type = #{type} "
            + " and statistics_time = #{statisticsTime} ")
    FinanceLimitStatistics getFinanceLimitStatistics(@Param("orgId") Long orgId,
                                                     @Param("productId") Long productId,
                                                     @Param("userId") Long userId,
                                                     @Param("type") Integer type,
                                                     @Param("statisticsTime") String statisticsTime);

    @Update(" update " + TABLE_NAME + " set used = used + #{used} , updated_at = #{updateTime} where id = #{id}")
    int updateFinanceLimitStatisticsUsed(@Param("id") Long id, @Param("used") BigDecimal used, @Param("updateTime") Long updateTime);

}
