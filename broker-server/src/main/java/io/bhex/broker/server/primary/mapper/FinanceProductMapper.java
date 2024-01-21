package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.FinanceProduct;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import java.math.BigDecimal;
import java.util.List;

@Mapper
public interface FinanceProductMapper extends tk.mybatis.mapper.common.Mapper<FinanceProduct> {

    String TABLE_NAME = "tb_finance_product";

    @Select("SELECT * FROM " + TABLE_NAME + " WHERE type=#{type} AND `status` > 0")
    List<FinanceProduct> queryVisibleProductListByType(@Param("type") Integer type);
}
