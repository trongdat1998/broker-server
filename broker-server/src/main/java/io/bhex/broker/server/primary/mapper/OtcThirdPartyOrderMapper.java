package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.OtcThirdPartyOrder;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import java.math.BigDecimal;

@Mapper
public interface OtcThirdPartyOrderMapper extends tk.mybatis.mapper.common.Mapper<OtcThirdPartyOrder> {
    @Select("SELECT * FROM tb_otc_third_party_order WHERE org_id=#{orgId} AND user_id=#{userId} AND client_order_id=#{clientOrderId};")
    OtcThirdPartyOrder getByClientOrderId(@Param("orgId") Long orgId,
                                          @Param("userId") Long userId,
                                          @Param("clientOrderId") String clientOrderId);

    @Update("UPDATE tb_otc_third_party_order SET status=#{status} WHERE id=#{id} AND org_id=#{orgId} AND status=#{oldStatus};")
    int updateOrderStatus(@Param("status") Integer status,
                          @Param("id") Long id,
                          @Param("orgId") Long orgId,
                          @Param("oldStatus") Integer oldStatus);

    @Update("UPDATE tb_otc_third_party_order SET status=#{status},error_message=#{errorMessage} WHERE id=#{id} AND org_id=#{orgId} AND status=#{oldStatus};")
    int updateOrderFailed(@Param("status") Integer status,
                          @Param("errorMessage") String errorMessage,
                          @Param("id") Long id,
                          @Param("orgId") Long orgId,
                          @Param("oldStatus") Integer oldStatus);

    @Update("UPDATE tb_otc_third_party_order SET status=#{status},token_amount=#{tokenAmount},currency_amount=#{currencyAmount}," +
            " fee_amount=#{feeAmount} WHERE id=#{id} AND org_id=#{orgId} AND status=#{oldStatus};")
    int updateOrderSuccess(@Param("status") Integer status,
                           @Param("currencyAmount") BigDecimal currencyAmount,
                           @Param("tokenAmount") BigDecimal tokenAmount,
                           @Param("feeAmount") BigDecimal feeAmount,
                           @Param("id") Long id,
                           @Param("orgId") Long orgId,
                           @Param("oldStatus") Integer oldStatus);

    @Select("SELECT * FROM tb_otc_third_party_order WHERE order_id=#{orderId} LIMIT 1;")
    OtcThirdPartyOrder getByOrderId(@Param("orderId") Long orderId);

}
