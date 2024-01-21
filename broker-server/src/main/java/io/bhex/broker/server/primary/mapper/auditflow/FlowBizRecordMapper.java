package io.bhex.broker.server.primary.mapper.auditflow;

import io.bhex.broker.server.model.auditflow.FlowApprovedRecordPO;
import io.bhex.broker.server.model.auditflow.FlowBizRecordPO;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.SelectProvider;
import org.apache.ibatis.annotations.Update;
import tk.mybatis.mapper.common.Mapper;

import java.util.List;
import java.util.Map;

/**
 * Flow biz records mapper
 *
 * @author generator
 * @date 2021-01-12
 */
public interface FlowBizRecordMapper extends Mapper<FlowBizRecordPO> {

    /**
     * update status
     *
     * @param orgId
     * @param recordId
     * @param status
     * @param updateTime
     * @return
     */
    @Update(" update tb_flow_biz_record set status = #{status}, updated_at = #{updateTime} where org_id = #{orgId} and id = #{recordId}; ")
    int updateStatus(@Param("orgId") Long orgId, @Param("recordId") Integer recordId, @Param("status") Integer status, @Param("updateTime") Long updateTime);

    /**
     * get audit count
     *
     * @param orgId
     * @param flowConfigId
     * @return
     */
    @Select("SELECT count(*) FROM tb_flow_biz_record t1 WHERE t1.org_id = #{orgId} and t1.flow_config_id = #{flowConfigId} and status=0;")
    int getAuditCount(@Param("orgId") Long orgId, @Param("flowConfigId") Integer flowConfigId);

    /**
     * update flow node info
     * @param orgId
     * @param recordId
     * @param flowNodeId
     * @param level
     * @param approver
     * @param approverName
     * @param updateTime
     * @return
     */
    @Update(" update tb_flow_biz_record set flow_node_id = #{flowNodeId}, current_level = #{level}, approver = #{approver}, approver_name = #{approverName}, updated_at = #{updateTime} where org_id = #{orgId} and id = #{recordId}; ")
    int updateFlowNodeInfo(@Param("orgId") Long orgId, @Param("recordId") Integer recordId, @Param("flowNodeId") Integer flowNodeId
            , @Param("level") Integer level, @Param("approver") Long approver, @Param("approverName") String approverName, @Param("updateTime") Long updateTime);

    /**
     * get audit records
     *
     * @param orgId
     * @param bizType
     * @param userId
     * @param startId
     * @param pageSize
     * @return
     */
    @SelectProvider(type = Provider.class, method = "getAuditRecordSql")
    List<FlowBizRecordPO> listAuditRecord(@Param("orgId") Long orgId, @Param("bizType") Integer bizType
            , @Param("userId") Long userId, @Param("lastId") Integer startId, @Param("pageSize") Integer pageSize);

    /**
     * get approved records
     *
     * @param orgId
     * @param bizType
     * @param userId
     * @param startId
     * @param pageSize
     * @return
     */
    @SelectProvider(type = Provider.class, method = "getApprovedRecordSql")
    List<FlowApprovedRecordPO> listApprovedRecord(@Param("orgId") Long orgId, @Param("bizType") Integer bizType
            , @Param("userId") Long userId, @Param("lastId") Integer startId, @Param("pageSize") Integer pageSize);

    class Provider {

        public String getAuditRecordSql(Map<String, Object> parameter){
            Integer lastId = (Integer) parameter.get("lastId");
            Long userId = (Long) parameter.get("userId");
            Integer bizType = (Integer) parameter.get("bizType");
            StringBuilder sqlBuilder = new StringBuilder();
            sqlBuilder.append("select * from tb_flow_biz_record t1 where t1.org_id = #{orgId} and t1.status=0 ");
            if (bizType > 0) {
                sqlBuilder.append(" and t1.biz_type = #{bizType} ");
            }
            if (userId > 0) {
                sqlBuilder.append(" and (t1.approver = #{userId} or t1.applicant = #{userId} ) ");
            }
            if (lastId > 0) {
                sqlBuilder.append(" and t1.id < #{lastId} ");
            }

            sqlBuilder.append(" order by t1.id desc limit 0, #{pageSize};");

            return sqlBuilder.toString();
        }

        public String getApprovedRecordSql(Map<String, Object> parameter) {

            Integer lastId = (Integer) parameter.get("lastId");
            Long userId = (Long) parameter.get("userId");
            Integer bizType = (Integer) parameter.get("bizType");

            StringBuilder sqlBuilder = new StringBuilder();
            sqlBuilder.append("select t1.id as recordId, t1.org_id, t1.flow_config_id,t1.biz_id,t1.biz_type,t1.biz_title,t1.applicant,t1.applicant_name,t1.apply_date,t2.created_at as audit_date,t2.approver,t2.approver_name,t2.level,t2.approval_status,t2.approval_note from tb_flow_biz_record t1,tb_flow_audit_logs t2 where t1.org_id=t2.org_id and t1.flow_config_id=t2.flow_config_id and t1.id=t2.record_id ");
            sqlBuilder.append(" and t1.org_id = #{orgId}");
            if (bizType > 0) {
                sqlBuilder.append(" and t1.biz_type = #{bizType} ");
            }
            if (userId > 0) {
                sqlBuilder.append(" and (t2.approver = #{userId} or t1.applicant = #{userId} ) ");
            }
            if (lastId > 0) {
                sqlBuilder.append(" and t2.id < #{lastId} ");
            }

            sqlBuilder.append(" order by t2.id desc limit 0, #{pageSize};");

            return sqlBuilder.toString();
        }
    }
}