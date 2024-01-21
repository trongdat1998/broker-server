package io.bhex.broker.server.statistics.statistics.mapper;

import io.bhex.broker.server.model.InsuranceFundBalanceSnap;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.List;

@Component
@Mapper
public interface StatisticsInsuranceFundBalanceMapper {

    @Select({"<script>"
            , "SELECT "
            + "     dt as date, account_id accountId, token_id tokenId, total, available "
            , " FROM snap_balance WHERE org_id=#{orgId} and account_id = #{accountId} "
            , "<if test=\"tokenId != null and tokenId != ''\">AND token_id = #{tokenId}</if> "
            , "<if test=\"startDate != null\">AND dt &lt; #{startDate}</if> "
            , "<if test=\"endDate != null\">AND dt &gt; #{endDate}</if> "
            , "ORDER BY dt  <if test=\"orderDesc\">DESC</if>  "
            , " LIMIT #{limit} "
            , "</script>"})
    List<InsuranceFundBalanceSnap> getBalanceSnapList(@Param("orgId") Long orgId, @Param("accountId") Long accountId,
                                           @Param("tokenId") String tokenId,
                                           @Param("startDate") Date startDate,
                                           @Param("endDate") Date endDate,
                                           @Param("limit") Integer limit,
                                           @Param("orderDesc") Boolean orderDesc);
}
