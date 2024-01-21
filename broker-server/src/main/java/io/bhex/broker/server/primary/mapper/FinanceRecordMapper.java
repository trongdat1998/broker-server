package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.FinanceRecord;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import tk.mybatis.mapper.common.special.InsertListMapper;

import java.util.List;

@Mapper
public interface FinanceRecordMapper extends tk.mybatis.mapper.common.Mapper<FinanceRecord>, InsertListMapper<FinanceRecord> {

    String TABLE_NAME = "tb_finance_record";

    @Select(" select * from " + TABLE_NAME + " where id = #{id} for update")
    FinanceRecord getFinanceRecordLock(@Param("id") Long id);

    @Select(" select * from " + TABLE_NAME + " where id = #{id} and org_id=#{orgId} and user_id=#{userId} and status >= 0")
    FinanceRecord getFinanceRecord(@Param("id") Long id, @Param("orgId") Long orgId, @Param("userId") Long userId);

    @Select(" select * from " + TABLE_NAME + " where type = #{type} and status = #{status} limit #{limit}")
    List<FinanceRecord> getFinanceRecordByTypeAndStatus(@Param("type") Integer type, @Param("status") Integer status, @Param("limit") Integer limit);

    @Select(" select * from " + TABLE_NAME + " where type = 2 and status = 0 and product_id=#{productId} and statistics_time = #{statisticsTime} order by id desc limit #{startIndex}, #{limit}")
    List<FinanceRecord> queryUnGrantFinanceInterestRecordsByStatisticsTime(@Param("productId") Long productId,
                                                                           @Param("statisticsTime") String statisticsTime,
                                                                           @Param("startIndex") Integer startIndex,
                                                                           @Param("limit") Integer limit);

    @Select(" select * from " + TABLE_NAME + " where org_id = #{orgId} and user_id = #{userId} and id < #{beforeId} and status >= 0 order by id desc limit #{pageSize} ")
    List<FinanceRecord> getFinanceRecordList(@Param("orgId") Long orgId,
                                             @Param("userId") Long userId,
                                             @Param("beforeId") Long beforeId,
                                             @Param("pageSize") Integer pageSize);

    @Select(" select * from " + TABLE_NAME + " where org_id = #{orgId} and product_id=#{productId} and id > #{beforeId} and status = 1 order by id limit #{pageSize} ")
    List<FinanceRecord> getFinanceRecordListTemp(@Param("orgId") Long orgId, @Param("productId") Long productId, @Param("beforeId") Long beforeId
            , @Param("pageSize") Integer pageSize);
}
