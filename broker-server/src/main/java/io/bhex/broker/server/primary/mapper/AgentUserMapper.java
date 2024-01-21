package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.AgentUser;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import java.math.BigDecimal;
import java.util.List;

@Mapper
public interface AgentUserMapper extends tk.mybatis.mapper.common.Mapper<AgentUser> {

    @Select("select *from tb_agent_user where org_id = #{orgId} and user_id= #{userId}")
    AgentUser queryAgentUserByUserId(@Param("orgId") Long orgId, @Param("userId") Long userId);


    @Update("update tb_agent_user set rate = #{rate} where org_id=#{orgId} and superior_user_id = #{superiorUserId}")
    void updateChildrenRateBySuperiorUserId(@Param("orgId") Long orgId, @Param("superiorUserId") Long superiorUserId, @Param("rate") BigDecimal rate);

    @Update("update tb_agent_user set contract_rate = #{rate} where org_id=#{orgId} and superior_user_id = #{superiorUserId}")
    void updateContractChildrenRateBySuperiorUserId(@Param("orgId") Long orgId, @Param("superiorUserId") Long superiorUserId, @Param("rate") BigDecimal rate);

    @Select("select *from tb_agent_user where org_id = #{orgId} and superior_user_id = #{superiorUserId}")
    List<AgentUser> queryAgentUserListBySuperiorUserId(@Param("orgId") Long orgId, @Param("superiorUserId") Long superiorUserId);

    @Select({"<script>"
            , "SELECT * "
            , "FROM tb_agent_user "
            , "WHERE org_id=#{orgId} "
            , "<if test=\"userId != null and userId &gt; 0\">AND user_id = #{userId}</if> "
            , "<if test=\"email != null and email != ''\">AND email = #{email}</if> "
            , "<if test=\"mobile != null and mobile != ''\">AND mobile = #{mobile}</if> "
            , "<if test=\"agentName != null and agentName != ''\">AND agent_name = #{agentName}</if> "
            , "ORDER BY register_time DESC "
            , "LIMIT #{start},#{end} "
            , "</script>"})
    List<AgentUser> queryBrokerUserList(@Param("orgId") Long orgId,
                                        @Param("userId") Long userId,
                                        @Param("email") String email,
                                        @Param("mobile") String mobile,
                                        @Param("agentName") String agentName,
                                        @Param("start") Integer start,
                                        @Param("end") Integer end);


    @Select({"<script>"
            , "SELECT * "
            , "FROM tb_agent_user "
            , "WHERE org_id=#{orgId} and is_agent = 1"
            , "<if test=\"userId != null and userId &gt; 0\">AND user_id = #{userId}</if> "
//            , "<if test=\"email != null and email != ''\">AND email = #{email}</if> "
//            , "<if test=\"mobile != null and mobile != ''\">AND mobile = #{mobile}</if> "
            , "<if test=\"agentName != null and agentName != ''\">AND agent_name = #{agentName}</if> "
            , "ORDER BY register_time DESC "
            , "LIMIT #{start},#{end} "
            , "</script>"})
    List<AgentUser> queryBrokerAgentList(@Param("orgId") Long orgId,
                                         @Param("userId") Long userId,
//                                        @Param("email") String email,
//                                        @Param("mobile") String mobile,
                                         @Param("agentName") String agentName,
                                         @Param("start") Integer start,
                                         @Param("end") Integer end);

    @Select("select count(1) from tb_agent_user where org_id = #{orgId} and ${sqlLevel} and status = 1")
    int queryUserCountByLevel(@Param("orgId") Long orgId, @Param("sqlLevel") String sqlLevel);

    @Select("select count(1) from tb_agent_user where org_id = #{orgId} and superior_user_id = #{superiorUserId} and user_id != #{superiorUserId} and is_agent = 1 and status = 1")
    int queryAgentCountByLevel(@Param("orgId") Long orgId, @Param("superiorUserId") Long superiorUserId);

    @Update("update tb_agent_user set ${userLevel} where org_id=#{orgId} and user_id = #{userId}")
    int updateUserLevelByUserId(@Param("orgId") Long orgId, @Param("userId") Long userId, @Param("userLevel") String userLevel);

    @Select("select * from tb_agent_user where org_id = #{orgId} and level = #{level} and is_agent = 1")
    List<AgentUser> queryAgentUserListByLevel(@Param("orgId") Long orgId, @Param("level") Integer level);

    @Update("update tb_agent_user set status = 0 where org_id = #{orgId} and ${userLevel}")
    int cancelAgentUserByLevel(@Param("orgId") Long orgId, @Param("userLevel") String userLevel);

    @Update("update tb_agent_user set status = 1 where org_id = #{orgId} and ${userLevel}")
    int wakeUpAgentUserByLevel(@Param("orgId") Long orgId, @Param("userLevel") String userLevel);

    @Select("select * from tb_agent_user where org_id = #{orgId} and ${userLevel}")
    List<AgentUser> queryAllAgentUserListByLevel(@Param("orgId") Long orgId, @Param("userLevel") String userLevel);

    @Select("select * from tb_agent_user where org_id = #{orgId} and ${userLevel} for update")
    List<AgentUser> lockAgentUserByLevel(@Param("orgId") Long orgId, @Param("userLevel") String userLevel);

    @Select("select count(1) from tb_agent_user where org_id = #{orgId} and ${sqlLevel}")
    int queryAllUserCountByLevel(@Param("orgId") Long orgId, @Param("sqlLevel") String sqlLevel);

    @Select("select * from tb_agent_user where org_id = #{orgId} limit 0,1")
    AgentUser queryAgentUserGetOne(@Param("orgId") Long orgId);

    @Update("update tb_agent_user set rate =0,children_default_rate=0,contract_rate=0,contract_children_default_rate=0,is_abs = 1 where org_id=#{orgId}")
    int cleanAgentAllRateByOrgId(@Param("orgId") Long orgId);

    @Update("update tb_agent_user set rate =0,children_default_rate=0,is_abs = 1 where org_id=#{orgId} and ${sqlLevel} and user_id != #{userId}")
    int cleanAgentCoinRateByUserId(@Param("orgId") Long orgId, @Param("sqlLevel") String sqlLevel, @Param("userId") Long userId);

    @Update("update tb_agent_user set contract_rate=0,contract_children_default_rate=0,is_abs = 1 where org_id=#{orgId} and ${sqlLevel} and user_id != #{userId}")
    int cleanAgentContractRateByUserId(@Param("orgId") Long orgId, @Param("sqlLevel") String sqlLevel, @Param("userId") Long userId);
}