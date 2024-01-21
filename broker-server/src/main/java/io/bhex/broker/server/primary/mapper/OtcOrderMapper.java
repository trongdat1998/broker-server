package io.bhex.broker.server.primary.mapper;

import java.util.Date;

import io.bhex.broker.server.model.OtcOrder;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Update;
import tk.mybatis.mapper.common.Mapper;

/**
 * 订单mapper
 *
 * @author lizhen
 * @date 2018-09-14
 */
@org.apache.ibatis.annotations.Mapper
public interface OtcOrderMapper extends Mapper<OtcOrder> {

    @Update("update tb_otc_order set status = #{status}, update_date = #{updateDate} where id = #{id} "
        + "and status = #{oldStatus}")
    int updateOtcOrderStatus(@Param("id") Long id,
                             @Param("status") Integer status,
                             @Param("oldStatus") Integer oldStatus,
                             @Param("updateDate") Date updateDate);
}
