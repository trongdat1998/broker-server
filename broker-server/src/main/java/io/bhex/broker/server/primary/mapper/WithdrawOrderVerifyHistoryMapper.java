package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.WithdrawOrderVerifyHistory;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.SelectProvider;

import java.util.List;

/**
 * @Description:
 * @Date: 2018/9/19 下午5:35
 * @Author: liwei
 * @Copyright（C）: 2018 BlueHelix Inc. All rights reserved.
 */
@Mapper
public interface WithdrawOrderVerifyHistoryMapper extends tk.mybatis.mapper.common.Mapper<WithdrawOrderVerifyHistory> {

    @SelectProvider(type = WithdrawOrderVerifyHistoryProvider.class, method = "queryOrdersHistory")
    List<WithdrawOrderVerifyHistory> queryOrdersHistory(
            @Param("orgId") long orgId,
            @Param("accountId") long accountId,
            @Param("fromOrderId") long fromOrderId,
            @Param("endOrderId") long endOrderId,
            @Param("tokenId") String tokenId,
            @Param("limit") long limit,
            @Param("status") Integer status,
            @Param("bhWithdrawOrderId") long bhWithdrawOrderId);

}
