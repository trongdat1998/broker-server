package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.CustomLabelUser;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;
import tk.mybatis.mapper.common.Mapper;

import java.util.List;

/**
 * @Description:
 * @Date: 2020/4/17 上午11:59
 * @Author: liwei
 * @Copyright（C）: 2018 BlueHelix Inc. All rights reserved.
 */
@org.apache.ibatis.annotations.Mapper
public interface CustomLabelUserMapper extends Mapper<CustomLabelUser> {

    String TABLE_NAME = "tb_custom_label_user";

    @Select("select * from " + TABLE_NAME + " where org_id=#{orgId} and label_id=#{labelId} and user_id=#{userId}")
    CustomLabelUser getByLabelAndUserId(@Param("orgId") Long orgId, @Param("labelId") Long labelId, @Param("userId") Long userId);

    @Select("select user_id from " + TABLE_NAME + " where org_id=#{orgId} and label_id=#{labelId} and is_del = 0")
    List<Long> getUserIdsByLabel(@Param("orgId") Long orgId, @Param("labelId") Long labelId);

    @Update("update " + TABLE_NAME + " set is_del = 1 where org_id=#{orgId} and label_id=#{labelId}")
    int deleteLabelUser(@Param("orgId") Long orgId, @Param("labelId") Long labelId);

}
