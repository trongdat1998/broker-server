package io.bhex.broker.server.statistics.statistics.mapper;

import io.bhex.broker.server.model.StatisticsRpcBrokerUserFee;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

@Mapper
public interface StatisticsRptBrokerUserFeeMapper {

    @Select(" <script> "
            + " SELECT account_id, token_id, token_fee FROM clear_rpt_broker_user_fee WHERE match_date = #{dt} and account_id in "
            + " <foreach item='item' index='index' collection='accountIds' open='(' separator=',' close=')'> "
            + " #{item} "
            + " </foreach> "
            + " </script> ")
    List<StatisticsRpcBrokerUserFee> queryBrokerUserFeeByAccountIds(@Param("accountIds")List<Long> accountIds, @Param("dt")String dt);
}
