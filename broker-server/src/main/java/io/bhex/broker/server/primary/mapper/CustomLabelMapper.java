package io.bhex.broker.server.primary.mapper;

import com.google.common.base.Strings;
import io.bhex.broker.server.model.CustomLabel;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;
import org.apache.ibatis.annotations.UpdateProvider;
import org.apache.ibatis.jdbc.SQL;
import org.springframework.stereotype.Repository;
import tk.mybatis.mapper.common.Mapper;

import java.util.List;
import java.util.Objects;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.primary.mapper
 * @Author: ming.xu
 * @CreateDate: 2019/12/11 4:25 PM
 * @Copyright（C）: 2019 BHEX Inc. All rights reserved.
 */
@Repository
@org.apache.ibatis.annotations.Mapper
public interface CustomLabelMapper extends Mapper<CustomLabel> {

    String TABLE_NAME = "tb_custom_label";

    @Update("update " + TABLE_NAME + " set is_del = 1, updated_at = #{updatedAt} where org_id=#{orgId} and label_id=#{labelId}")
    int delCustomLabel(@Param("orgId") Long orgId, @Param("labelId") Long labelId, @Param("updatedAt") Long updatedAt);

    @Select("select * from " + TABLE_NAME + " where org_id=#{orgId} and label_id=#{labelId} and language=#{language}")
    CustomLabel selectByLabelIdAndLanguage(@Param("orgId") Long orgId, @Param("labelId") Long labelId, @Param("language") String language);

    @Select("select * from " + TABLE_NAME + " where org_id=#{orgId} and label_id=#{labelId} and is_del = 0 order by updated_at desc limit 1")
    CustomLabel getOneByLabelId(@Param("orgId") Long orgId, @Param("labelId") Long labelId);

    @Select("select label_id from " + TABLE_NAME + " where org_id=#{orgId} and is_del = 0 group by label_id having label_id> #{fromId} order by label_id limit #{limit}")
    List<Long> pageableByFromId(@Param("orgId") Long orgId, @Param("fromId") Long fromId, @Param("limit") Integer limit);

    @Select("select label_id from " + TABLE_NAME + " where org_id=#{orgId} and is_del = 0 group by label_id having label_id< #{endId} order by label_id limit #{limit}")
    List<Long> pageableByEndId(@Param("orgId") Long orgId, @Param("endId") Long endId, @Param("limit") Integer limit);

    @Update("update " + TABLE_NAME + " set user_ids_str = #{userIds},user_count=#{userCount},updated_at=#{now} where org_id = #{orgId} and label_id = #{labelId}")
    int updateByLabelId(@Param("orgId") Long orgId, @Param("labelId") Long labelId, @Param("userIds") String userIds, @Param("userCount") int user_count, @Param("now") long now);

    @UpdateProvider(type = SqlProvider.class, method = "update")
    int updateRecord(CustomLabel customLabel);

    public class SqlProvider {

        public String update(CustomLabel customLabel) {
            return new SQL() {
                {
                    UPDATE(TABLE_NAME);
                    if (Objects.nonNull(customLabel.getOrgId()) && customLabel.getOrgId() != 0L) {
                        SET("org_id = #{orgId}");
                    }
                    if (Objects.nonNull(customLabel.getLabelId()) && customLabel.getLabelId() != 0L) {
                        SET("label_id = #{labelId}");
                    }
                    if (!Strings.isNullOrEmpty(customLabel.getLanguage())) {
                        SET("language = #{language}");
                    }
                    if (!Strings.isNullOrEmpty(customLabel.getLabelValue())) {
                        SET("label_value = #{labelValue}");
                    }
                    if (!Strings.isNullOrEmpty(customLabel.getColorCode())) {
                        SET("color_code = #{colorCode}");
                    }
                    if (Objects.nonNull(customLabel.getUserCount())) {
                        SET("user_count = #{userCount}");
                    }
                    if (!Strings.isNullOrEmpty(customLabel.getUserIdsStr())) {
                        SET("user_ids_str = #{userIdsStr}");
                    }
                    if (Objects.nonNull(customLabel.getStatus())) {
                        SET("status = #{status}");
                    }
                    if (Objects.nonNull(customLabel.getIsDel())) {
                        SET("is_del = #{isDel}");
                    }
                    if (Objects.nonNull(customLabel.getCreatedAt()) && customLabel.getCreatedAt() != 0L) {
                        SET("created_at = #{createdAt}");
                    }
                    if (Objects.nonNull(customLabel.getUpdatedAt()) && customLabel.getUpdatedAt() != 0L) {
                        SET("updated_at = #{updatedAt}");
                    }
                    WHERE("id = #{id}");
                }
            }.toString();
        }
    }
}
