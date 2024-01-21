package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.ConvertUserRecord;
import org.apache.ibatis.annotations.*;

@Mapper
public interface ConvertUserRecordMapper extends tk.mybatis.mapper.common.Mapper<ConvertUserRecord> {
    @Select("SELECT * FROM tb_convert_user_record WHERE broker_id=#{brokerId} AND convert_symbol_id=#{convertSymbolId} AND account_id=#{accountId}")
    ConvertUserRecord getByAccountId(@Param("brokerId") Long brokerId,
                                     @Param("convertSymbolId") Long convertSymbolId,
                                     @Param("accountId") Long accountId);

    @Select("SELECT * FROM tb_convert_user_record WHERE broker_id=#{brokerId} AND convert_symbol_id=#{convertSymbolId} AND account_id=#{accountId} FOR UPDATE")
    ConvertUserRecord lockByAccountId(@Param("brokerId") Long brokerId,
                                      @Param("convertSymbolId") Long convertSymbolId,
                                      @Param("accountId") Long accountId);

    @Insert("INSERT INTO tb_convert_user_record(account_id, convert_symbol_id, curr_date, current_quantity, total_quantity, created, updated) "
            + "VALUES(#{accountId}, #{convertSymbolId}, #{currDate}, #{currentQuantity}, #{totalQuantity}, #{created}, #{updated})")
    @Options(useGeneratedKeys = true, keyColumn = "id")
    int insertRecord(ConvertUserRecord userRecord);
}
