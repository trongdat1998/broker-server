package io.bhex.broker.server.statistics.statistics.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

import io.bhex.broker.server.model.BhexRate;


@Mapper
public interface StatisticsBhexRateMapper extends tk.mybatis.mapper.common.Mapper<BhexRate> {

    @Select("select * from clear_snp_bhex_rate where token_id = #{tokenId} order by dt desc limit 0,7")
    List<BhexRate> getRecords(@Param("orgId") long orgId, @Param("tokenId") String tokenId);
}
