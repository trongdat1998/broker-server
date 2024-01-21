package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.Account;
import io.bhex.broker.server.model.TransferLimit;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

/**
 * @author JinYuYuan
 * @description
 * @date 2020-05-21 14:54
 */
@Mapper
public interface TransferLimitMapper extends tk.mybatis.mapper.common.Mapper<TransferLimit>  {
    @Select("select * from tb_finance_transfer_limit where org_id=#{orgId} and token_id=#{tokenId}")
    TransferLimit getTransferLimit(@Param("orgId")Long orgId, @Param("tokenId")String tokenId);

}
