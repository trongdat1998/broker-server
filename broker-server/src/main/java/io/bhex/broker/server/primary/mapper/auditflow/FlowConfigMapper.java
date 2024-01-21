package io.bhex.broker.server.primary.mapper.auditflow;

import io.bhex.broker.server.model.auditflow.FlowConfigPO;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Update;

/**
 * FlowConfig Mapper
 *
 * @author generator
 * @date 2021-01-12
 */
@Mapper
public interface FlowConfigMapper extends tk.mybatis.mapper.common.Mapper<FlowConfigPO> {
    /**
     * update forbidden status
     *
     * @param orgId
     * @param flowConfigId
     * @param userId
     * @param forbiddenStatus
     * @param updateTime
     * @return
     */
    @Update(" update tb_flow_config set status = #{forbiddenStatus},modify_user = #{userId}, updated_at = #{updateTime} where org_id = #{orgId} and id = #{flowConfigId}; ")
    int updateForbiddenStatus(@Param("orgId") Long orgId, @Param("flowConfigId") Integer flowConfigId, @Param("userId") Long userId, @Param("forbiddenStatus") Integer forbiddenStatus, @Param("updateTime") Long updateTime);
}
