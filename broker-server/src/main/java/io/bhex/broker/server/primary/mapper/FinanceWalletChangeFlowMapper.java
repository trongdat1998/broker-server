package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.FinanceWalletChangeFlow;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/**********************************
 *@项目名称: server-parent
 *@文件名称: io.bhex.broker.server.mapper
 *@Date 2019-03-07
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
@Mapper
public interface FinanceWalletChangeFlowMapper extends tk.mybatis.mapper.common.Mapper<FinanceWalletChangeFlow> {

    String TABLE_NAME = "tb_finance_wallet_change_flow";

    @Select(" select * from " + TABLE_NAME + " where wallet_id=#{walletId} and  created>=#{startTime} and created<#{endTime} order by id desc limit 1")
    FinanceWalletChangeFlow findLastRecordLastDay(@Param("walletId") Long walletId, @Param("startTime") Long startTime, @Param("endTime") Long endTime);

    @Select(" select * from " + TABLE_NAME + " where wallet_id=#{walletId} and created>=#{startTime} order by id")
    List<FinanceWalletChangeFlow> queryWalletFlowBeforeTime(@Param("walletId") Long walletId,
                                                            @Param("startTime") Long startTime);

}
