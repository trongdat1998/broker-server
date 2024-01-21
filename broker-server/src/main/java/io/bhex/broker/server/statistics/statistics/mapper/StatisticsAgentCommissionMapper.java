package io.bhex.broker.server.statistics.statistics.mapper;

import io.bhex.broker.server.model.AgentCommission;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;


@Mapper
public interface StatisticsAgentCommissionMapper extends tk.mybatis.mapper.common.Mapper<AgentCommission> {

    @Select({"<script>"
            , "SELECT * "
            , "FROM rpt_agent_commission "
            , "<if test=\"userId != null and userId &gt; 0\">where user_id = #{userId}</if> "
            , "ORDER BY dt DESC "
            , "LIMIT #{start},#{end} "
            , "</script>"})
    List<AgentCommission> queryAgentCommissionList(@Param("orgId") Long orgId,
                                         @Param("userId") Long userId,
                                         @Param("start") Integer start,
                                         @Param("end") Integer end);
}
