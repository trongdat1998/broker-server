package io.bhex.broker.server.statistics.statistics.mapper;

import io.bhex.broker.server.primary.mapper.DBToolsSqlProvider;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.SelectProvider;

import java.util.List;
import java.util.Map;

@org.apache.ibatis.annotations.Mapper
public interface StatisticsDBToolsMapper {
    @SelectProvider(type = DBToolsSqlProvider.class, method = "fetchOne")
    Map<String, Object> fetchOne(@Param("table") String table,
                                        @Param("fields") String fields,
                                        @Param("conditions") List<Map<String,String>> conditions);
}
