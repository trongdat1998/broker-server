package io.bhex.broker.server.primary.mapper;

import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.SelectProvider;

import java.util.List;
import java.util.Map;

@org.apache.ibatis.annotations.Mapper
public interface BrokerDBToolsMapper {
    @SelectProvider(type = DBToolsSqlProvider.class, method = "fetchOne")
    public Map<String, Object> fetchOne(@Param("table") String table,
                                        @Param("fields") String fields,
                                        @Param("conditions") List<Map<String,String>> conditions);
}
