package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.ConvertBrokerRecord;
import org.apache.ibatis.annotations.*;

@Mapper
public interface ConvertBrokerRecordMapper extends tk.mybatis.mapper.common.Mapper<ConvertBrokerRecord> {
    @Select("SELECT * FROM tb_convert_broker_record WHERE broker_id=#{brokerId} AND convert_symbol_id=#{convertSymbolId}")
    ConvertBrokerRecord getByConvertSymbolId(@Param("brokerId") Long brokerId, @Param("convertSymbolId") Long convertSymbolId);

    @Select("SELECT * FROM tb_convert_broker_record WHERE broker_id=#{brokerId} AND convert_symbol_id=#{convertSymbolId} FOR UPDATE")
    ConvertBrokerRecord lockByConvertSymbolId(@Param("brokerId") Long brokerId, @Param("convertSymbolId") Long convertSymbolId);

    @Insert("INSERT INTO tb_convert_user_record(convert_symbol_id, curr_date, current_quantity, created, updated) "
            + "VALUES(#{convertSymbolId}, #{currDate}, #{currentQuantity}, #{created}, #{updated})")
    @Options(useGeneratedKeys = true, keyColumn = "id")
    int insertRecord(ConvertBrokerRecord brokerRecord);
}
