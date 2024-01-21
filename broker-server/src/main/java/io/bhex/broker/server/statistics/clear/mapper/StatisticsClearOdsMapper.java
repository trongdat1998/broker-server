package io.bhex.broker.server.statistics.clear.mapper;

import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.SelectProvider;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 待确认
 * @Description:
 * @Date: 2019/11/1 上午10:22
 * @Author: liwei
 * @Copyright（C）: 2018 BlueHelix Inc. All rights reserved.
 */
@org.apache.ibatis.annotations.Mapper
public interface StatisticsClearOdsMapper {

    @Select("${sql}")
    List<HashMap<String, Object>> queryGroupDataList(@Param("sql") String sql);

    @Select("select contract_multiplier contractMultiplier,is_reverse isReverse from ods_symbol_futures where symbol_id = #{symbolId} limit 1")
    HashMap<String, Object> queryFutureSymbol(@Param("symbolId") String symbolId);
}
