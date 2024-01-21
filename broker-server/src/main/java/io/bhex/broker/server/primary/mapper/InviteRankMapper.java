package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.InviteRank;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.SelectProvider;
import tk.mybatis.mapper.common.special.InsertListMapper;

import java.util.List;

@Mapper
public interface InviteRankMapper extends tk.mybatis.mapper.common.Mapper<InviteRank>, InsertListMapper<InviteRank> {

    @SelectProvider(type = InviteRankSqlProvider.class, method = "getInviteRankByTypeAndMonth")
    List<InviteRank> getInviteRankByTypeAndMonth(@Param("type") int type, @Param("month") long month);

    @SelectProvider(type = InviteRankSqlProvider.class, method = "getInviteRankMonthList")
    List<Long> getInviteRankMonthList();

    @SelectProvider(type = InviteRankSqlProvider.class, method = "buildInviteRank")
    List<InviteRank> buildInviteRank(@Param("time") long time, @Param("actType") int actType);

}
