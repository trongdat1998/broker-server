package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.WithdrawPreApply;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

/**********************************
 *@项目名称: server-parent
 *@文件名称: io.bhex.broker.server.mapper
 *@Date 2018/9/12
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
@Mapper
public interface WithdrawPreApplyMapper extends tk.mybatis.mapper.common.Mapper<WithdrawPreApply> {

    String COLUMNS = "id, request_id, org_id, user_id, account_id, address_is_user_id, address_id, address, address_ext, is_inner_address, client_order_id, token_id, chain_type, fee_token_id, platform_fee, broker_fee, "
            + "miner_fee_token_id, miner_fee, is_auto_convert, convert_rate, quantity, convert_quantity, arrival_quantity, inner_withdraw_fee, need_broker_audit, need_check_id_card_no, card_no, has_check_id_card_no, "
            + "platform, ip, user_agent, language, app_base_header, current_fx_rate, btc_value, created, updated, expired, remarks";

    @Insert("INSERT INTO tb_withdraw_pre_apply(id, request_id, org_id, user_id, account_id, address_is_user_id, address_id, address, address_ext, is_inner_address, client_order_id, token_id, chain_type, fee_token_id, platform_fee, broker_fee, "
            + "miner_fee_token_id, miner_fee, is_auto_convert, convert_rate, quantity, convert_quantity, arrival_quantity, inner_withdraw_fee, need_broker_audit, need_check_id_card_no, card_no, has_check_id_card_no,"
            + "platform, ip, user_agent, `language`, app_base_header, current_fx_rate, btc_value, created, updated, expired, remarks) "
            + "VALUES(#{id}, #{requestId}, #{orgId}, #{userId}, #{accountId}, #{addressIsUserId}, #{addressId}, #{address}, #{addressExt}, #{isInnerAddress}, #{clientOrderId}, #{tokenId}, #{chainType}, #{feeTokenId}, #{platformFee}, #{brokerFee}, "
            + "#{minerFeeTokenId}, #{minerFee}, #{isAutoConvert}, #{convertRate}, #{quantity}, #{convertQuantity}, #{arrivalQuantity}, #{innerWithdrawFee}, #{needBrokerAudit}, #{needCheckIdCardNo}, #{cardNo}, #{hasCheckIdCardNo}, "
            + "#{platform}, #{ip}, #{userAgent}, #{language}, #{appBaseHeader}, #{currentFxRate}, #{btcValue}, #{created}, #{updated}, #{expired}, #{remarks})")
    int insertRecord(WithdrawPreApply withdrawPreApply);

    @Select("SELECT " + COLUMNS + " FROM tb_withdraw_pre_apply WHERE request_id = #{requestId} AND org_id=#{orgId} AND user_id=#{userId}")
    WithdrawPreApply getByRequestIdAndUserId(@Param("requestId") String requestId, @Param("orgId") Long orgId, @Param("userId") Long userId);

    @Select("SELECT " + COLUMNS + " FROM tb_withdraw_pre_apply WHERE id=#{id} FOR UPDATE")
    WithdrawPreApply lockById(Long id);

}
