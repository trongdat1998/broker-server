package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.BalanceBatchTransferTask;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

@Mapper
public interface BalanceBatchTransferTaskMapper extends tk.mybatis.mapper.common.Mapper<BalanceBatchTransferTask> {

    @Select("SELECT * FROM tb_balance_batch_transfer_task WHERE org_id=#{orgId} AND client_order_id=#{clientOrderId} AND type=#{type} FOR UPDATE")
    BalanceBatchTransferTask lockByClientOrderId(@Param("orgId") Long orgId,
                                                 @Param("clientOrderId") String clientOrderId,
                                                 @Param("type") Integer type);

    @Select("SELECT * FROM tb_balance_batch_transfer_task WHERE org_id=#{orgId} AND client_order_id=#{clientOrderId} AND type=#{type}")
    BalanceBatchTransferTask getByClientOrderId(@Param("orgId") Long orgId,
                                                @Param("clientOrderId") String clientOrderId,
                                                @Param("type") Integer type);

    @Select({"<script>"
            , "SELECT * FROM tb_balance_batch_transfer_task "
            , "WHERE org_id=#{orgId} "
            , "<if test=\"type != null\">AND type = #{type}</if> "
            , "<if test=\"fromId != null and fromId &gt; 0\">AND id &lt; #{fromId}</if> "
            , "ORDER BY id DESC limit #{limit} "
            , "</script>"
    })
    List<BalanceBatchTransferTask> queryTaskList(@Param("orgId") Long orgId, @Param("type") Integer type, @Param("fromId") Long fromId, @Param("limit") Integer limit);

}
