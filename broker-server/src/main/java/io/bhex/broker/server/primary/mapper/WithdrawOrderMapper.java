package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.WithdrawOrder;
import org.apache.ibatis.annotations.*;

import java.util.List;

/**********************************
 *@项目名称: server-parent
 *@文件名称: io.bhex.broker.server.mapper
 *@Date 2018/9/12
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
@Mapper
public interface WithdrawOrderMapper extends tk.mybatis.mapper.common.Mapper<WithdrawOrder> {

    String COLUMNS = "id, org_id, user_id, account_id, address_id, address, address_ext, is_inner_address, order_id, client_order_id, token_id, chain_type, fee_token_id, platform_fee, broker_fee, "
            + "miner_fee_token_id, miner_fee, is_auto_convert, convert_rate, quantity, arrival_quantity, status, order_status, need_broker_audit, need_check_card_no, card_no, "
            + "platform, ip, user_agent, language, app_base_header, current_fx_rate, btc_value, created, updated, remarks";

    @Insert("INSERT INTO tb_withdraw_order(id, org_id, user_id, account_id, address_id, address, address_ext, is_inner_address, order_id, client_order_id, token_id, chain_type, fee_token_id, platform_fee, broker_fee, "
            + "miner_fee_token_id, miner_fee, is_auto_convert, convert_rate, quantity, arrival_quantity, status, order_status, need_broker_audit, need_check_card_no, card_no, "
            + "platform, ip, user_agent, `language`, app_base_header, current_fx_rate, btc_value, created, updated, remarks) "
            + "VALUES(#{id}, #{orgId}, #{userId}, #{accountId}, #{addressId}, #{address}, #{addressExt}, #{isInnerAddress}, #{orderId}, #{clientOrderId}, #{tokenId}, #{chainType}, #{feeTokenId}, #{platformFee}, #{brokerFee}, "
            + "#{minerFeeTokenId}, #{minerFee}, #{isAutoConvert}, #{convertRate}, #{quantity}, #{arrivalQuantity}, #{status}, #{orderStatus}, #{needBrokerAudit}, #{needCheckCardNo}, #{cardNo}, "
            + "#{platform}, #{ip}, #{userAgent}, #{language}, #{appBaseHeader}, #{currentFxRate}, #{btcValue}, #{created}, #{updated}, #{remarks})")
    int insertRecord(WithdrawOrder withdrawOrder);

    @Update("UPDATE tb_withdraw_order SET order_status = #{orderStatus}, updated=#{updated} WHERE order_id=#{orderId}")
    int update(WithdrawOrder withdrawOrder);

    @Update("UPDATE tb_withdraw_order SET status=#{status}, updated=#{updated} WHERE id=#{id}")
    int updateStatusById(@Param("id") Long id, @Param("status") int status, @Param("updated") Long updated);

    @SelectProvider(type = WithdrawOrderSqlProvider.class, method = "queryOrders")
    List<WithdrawOrder> queryOrders(
            @Param("orgId") long orgId,
            @Param("accountId") long accountId,
            @Param("fromOrderId") long fromOrderId,
            @Param("endOrderId") long endOrderId,
            @Param("tokenId") String tokenId,
            @Param("limit") long limit,
            @Param("status") int status);

//    @Select("SELECT COUNT(1) FROM tb_withdraw_order WHERE user_id=#{userId} AND `order_status` IN (1, 3, 5, 6)")
//    int countByUserId(Long userId);

    @Select("SELECT " + COLUMNS + " FROM tb_withdraw_order WHERE user_id=#{userId} AND created >= #{startTime} AND created < #{endTime} AND `order_status` IN (1, 3, 5, 6)")
    List<WithdrawOrder> getByUserIdWithGiveTime(@Param("userId") Long userId, @Param("startTime") Long startTime, @Param("endTime") Long endTime);

    @Select("SELECT " + COLUMNS + " FROM tb_withdraw_order WHERE user_id=#{userId} AND created >= #{startTime} AND `order_status` IN (1, 3, 5, 6)")
    List<WithdrawOrder> queryWithdrawOrderWithIn24H(@Param("userId") Long userId, @Param("startTime") Long startTime);

    @Select("SELECT " + COLUMNS + " FROM tb_withdraw_order WHERE order_id = #{orderId}")
    WithdrawOrder getByOrderId(Long orderId);

    @Select("SELECT " + COLUMNS + " FROM tb_withdraw_order WHERE org_id = #{orgId} AND user_id = #{userId} AND client_order_id = #{clientOrderId}")
    WithdrawOrder getByClientIdAndUserId(@Param("orgId") Long orgId, @Param("userId") Long userId, @Param("clientOrderId") String clientOrderId);

}
