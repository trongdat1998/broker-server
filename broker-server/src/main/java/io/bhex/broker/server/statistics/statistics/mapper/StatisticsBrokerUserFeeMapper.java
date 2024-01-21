package io.bhex.broker.server.statistics.statistics.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

import io.bhex.broker.server.model.BrokerUserFee;


@Mapper
public interface StatisticsBrokerUserFeeMapper extends tk.mybatis.mapper.common.Mapper<BrokerUserFee> {

    @Select(" <script> "
            + " SELECT broker_id,token_id,account_id,token_fee,maker_fee FROM clear_rpt_broker_user_fee WHERE match_date = #{matchDate} and broker_id = #{brokerId} and account_id in "
            + " <foreach item='item' index='index' collection='accountIds' open='(' separator=',' close=')'> "
            + " #{item} "
            + " </foreach> "
            + " </script> ")
    List<BrokerUserFee> queryBrokerUserFeeList(@Param("matchDate") String matchDate, @Param("brokerId") Long brokerId, @Param("accountIds") List<Long> accountIds);
}
