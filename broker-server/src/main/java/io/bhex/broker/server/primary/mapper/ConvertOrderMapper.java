package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.ConvertOrder;
import org.apache.ibatis.annotations.*;

import java.util.List;

@Mapper
public interface ConvertOrderMapper extends tk.mybatis.mapper.common.Mapper<ConvertOrder> {
    @Select("SELECT * FROM tb_convert_order WHERE broker_id=#{brokerId} AND account_id=#{accountId} AND client_order_id=#{clientOrderId}")
    ConvertOrder getByClientOrderId(@Param("brokerId") Long brokerId,
                                    @Param("accountId") Long accountId,
                                    @Param("clientOrderId") String clientOrderId);

    @Select("SELECT * FROM tb_convert_order WHERE status = #{status} ORDER BY id LIMIT #{limit}")
    List<ConvertOrder> queryByStatus(@Param("status") Integer status, @Param("limit") Integer limit);

    @Insert("INSERT INTO tb_convert_order(broker_id, user_id, order_id, client_order_id, convert_symbol_id, broker_account_id,"
            + " account_id, purchase_quantity, offerings_quantity, price, status, error_message, created, updated)"
            + " VALUES(#{brokerId}, #{userId}, #{orderId}, #{clientOrderId}, #{convertSymbolId}, #{brokerAccountId},"
            + " #{accountId}, #{purchaseQuantity}, #{offeringsQuantity}, #{price}, #{status}, #{errorMessage}, #{created}, #{updated})")
    @Options(useGeneratedKeys = true, keyColumn = "id")
    int insertRecord(ConvertOrder order);
}
