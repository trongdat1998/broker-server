package io.bhex.broker.server.primary.mapper;


import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import io.bhex.broker.server.model.AccountTradeFeeConfig;

import java.util.List;

@Mapper
public interface AccountTradeFeeConfigMapper extends tk.mybatis.mapper.common.Mapper<AccountTradeFeeConfig> {

    @Delete("delete from tb_account_trade_fee_config where id=#{id} and org_id=#{orgId}")
    int deleteRecord(@Param("orgId") Long orgId, @Param("id") Long id);


    @Select("select *from tb_account_trade_fee_config where org_id=#{orgId} and user_id=#{userId} and account_id = #{accountId} and symbol_id = #{symbolId}")
    AccountTradeFeeConfig query(@Param("orgId") Long orgId, @Param("userId") Long userId, @Param("accountId") Long accountId, @Param("symbolId") String symbolId);

    @Select("select * from tb_account_trade_fee_config where org_id=#{orgId} and status = 1  and id > #{lastId} order by id asc limit #{limit}")
    List<AccountTradeFeeConfig> listAvailableConfigs(@Param("orgId") Long orgId, @Param("lastId") Long lastId, @Param("limit") Integer limit);

}
