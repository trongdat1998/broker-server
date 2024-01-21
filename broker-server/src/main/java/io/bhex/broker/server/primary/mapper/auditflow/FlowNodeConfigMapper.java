package io.bhex.broker.server.primary.mapper.auditflow;

import io.bhex.broker.server.model.auditflow.FlowApprovedRecordPO;
import io.bhex.broker.server.model.auditflow.FlowNodeConfigPO;
import io.bhex.broker.server.model.staking.StakingAssetSnapshot;
import io.bhex.broker.server.model.staking.StakingProductOrder;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.SelectProvider;
import org.apache.ibatis.annotations.Update;
import tk.mybatis.mapper.common.Mapper;
import tk.mybatis.mapper.common.special.InsertListMapper;

import java.util.List;
import java.util.Map;

/**
 * Flow node config mapper
 *
 * @author generator
 * @date 2021-01-12
 */
public interface FlowNodeConfigMapper extends Mapper<FlowNodeConfigPO>, InsertListMapper<FlowNodeConfigPO> {

    /**
     * update flow node status
     *
     * @param orgId        org id
     * @param flowConfigId flow config id
     * @param status       0=delete 1=enable
     * @param updateTime   update time
     * @return
     */
    @Update(" update tb_flow_node_config set status = #{status}, updated_at = #{updateTime} where org_id = #{orgId} and flow_config_id = #{flowConfigId}; ")
    int updateStatus(@Param("orgId") Long orgId, @Param("flowConfigId") Integer flowConfigId
            , @Param("status") Integer status, @Param("updateTime") Long updateTime);

    /**
     * 获取已审批单据
     *
     * @param orgId
     * @param flowConfigId
     * @param flowNodeId
     * @param level
     * @return
     */
    @SelectProvider(type = FlowNodeConfigMapper.Provider.class, method = "getNextNodeConfigSql")
    FlowNodeConfigPO getNextNodeConfig(@Param("orgId") Long orgId, @Param("flowConfigId") Integer flowConfigId
            , @Param("flowNodeId") Integer flowNodeId, @Param("level") Integer level);

    class Provider {
        public String getNextNodeConfigSql(Map<String, Object> parameter) {
            Integer flowNodeId = (Integer) parameter.get("flowNodeId");
            StringBuilder sqlBuilder = new StringBuilder();
            sqlBuilder.append("select * from tb_flow_node_config t1 where t1.org_id=#{orgId} and t1.flow_config_id = #{flowConfigId} and t1.status=1 ");
            if (flowNodeId > 0) {
                sqlBuilder.append(" and t1.id != #{flowNodeId} ");
            }
            sqlBuilder.append(" and t1.level > #{level} ");
            sqlBuilder.append(" order by t1.level asc limit 0, 1;");
            return sqlBuilder.toString();
        }
    }

    /**
     * get flow nodes
     *
     * @param orgId
     * @param flowConfigId
     * @return
     */
    @Select("SELECT * FROM tb_flow_node_config t1 WHERE t1.org_id = #{orgId} and t1.flow_config_id = #{flowConfigId} and status=1 order by level;")
    List<FlowNodeConfigPO> listFlowNodes(@Param("orgId") Long orgId, @Param("flowConfigId") Integer flowConfigId);
}