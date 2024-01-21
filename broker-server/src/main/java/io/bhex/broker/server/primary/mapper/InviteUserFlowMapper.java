package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.InviteUserFlow;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.SelectProvider;
import tk.mybatis.mapper.common.special.InsertListMapper;

import java.util.List;

@Mapper
public interface InviteUserFlowMapper extends tk.mybatis.mapper.common.Mapper<InviteUserFlow>, InsertListMapper<InviteUserFlow> {

    @SelectProvider(type = InviteUserFlowSqlProvider.class, method = "getInviteUserFlowUserAccountId")
    List<Long> getInviteUserFlowUserAccountId(@Param("orgId") long orgId, @Param("type") int type,
                                              @Param("time") long time, @Param("start") int start, @Param("limit") int limit);

}
