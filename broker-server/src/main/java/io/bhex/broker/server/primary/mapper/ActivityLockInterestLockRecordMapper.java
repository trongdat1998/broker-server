package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.ActivityLockInterestLockRecord;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;
import org.springframework.stereotype.Component;
import tk.mybatis.mapper.common.Mapper;

import java.math.BigDecimal;
import java.util.List;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.mapper
 * @Author: ming.xu
 * @CreateDate: 2019/6/4 3:06 PM
 * @Copyright（C）: 2019 BHEX Inc. All rights reserved.
 */
@Component
@org.apache.ibatis.annotations.Mapper
public interface ActivityLockInterestLockRecordMapper extends Mapper<ActivityLockInterestLockRecord> {

    @Select("select *from tb_activity_lock_interest_lock_record where org_id =#{orgId} and project = #{project} and user_id = #{userId}")
    ActivityLockInterestLockRecord queryActivityLockInterestLockRecordByUserId(@Param("orgId") Long orgId, @Param("project") String project, @Param("userId") Long userId);

    @Update("update tb_activity_lock_interest_lock_record set is_del = 1 where org_id=#{orgId} and project = #{project}")
    void updateByProject(@Param("orgId") Long orgId, @Param("project") String project);

    @Select("SELECT distinct user_id FROM tb_activity_lock_interest_lock_record WHERE org_id=#{orgId} AND project=#{project} ")
    List<Long> queryActivityUserIds(@Param("orgId") Long orgId, @Param("project") String project);

    @Select("SELECT * FROM tb_activity_lock_interest_lock_record WHERE org_id = #{orgId} and project = #{project} and user_id = #{userId} FOR UPDATE")
    ActivityLockInterestLockRecord lockByUserId(@Param("orgId") Long orgId, @Param("project") String project, @Param("userId") Long userId);

    @Update("UPDATE tb_activity_lock_interest_lock_record SET release_amount=release_amount + #{unlockAmount}, last_amount=last_amount - #{unlockAmount} WHERE id=#{id}")
    int updateUnlockData(@Param("id") Long id, @Param("unlockAmount") BigDecimal unlockAmount);

}
