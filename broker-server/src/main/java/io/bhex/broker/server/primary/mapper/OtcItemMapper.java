package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.OtcItem;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;
import tk.mybatis.mapper.common.Mapper;

import java.util.Date;

/**
 * 商品/广告mapper
 *
 * @author lizhen
 * @date 2018-09-13
 */
@org.apache.ibatis.annotations.Mapper
public interface OtcItemMapper extends Mapper<OtcItem> {

    @Update("update tb_otc_item set status = #{status}, update_date = #{updateDate} where id = #{id} "
            + "and status = #{oldStatus}")
    int updateOtcItemStatus(@Param("id") Long id,
                            @Param("status") Integer status,
                            @Param("oldStatus") Integer oldStatus,
                            @Param("updateDate") Date updateDate);

    @Select("select currency_id from tb_otc_item where item_id = #{itemId}")
    String queryOtcItemCurrencyIdByItemId(@Param("itemId") Long itemId);
}
