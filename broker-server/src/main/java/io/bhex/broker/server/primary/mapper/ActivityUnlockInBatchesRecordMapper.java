package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.ActivityUnlockInBatchesRecord;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Update;
import org.springframework.stereotype.Repository;
import tk.mybatis.mapper.common.Mapper;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.primary.mapper
 * @Author: ming.xu
 * @CreateDate: 2019/12/30 2:51 PM
 * @Copyright（C）: 2019 BHEX Inc. All rights reserved.
 */
@org.apache.ibatis.annotations.Mapper
@Repository
public interface ActivityUnlockInBatchesRecordMapper extends Mapper<ActivityUnlockInBatchesRecord> {

    String TABLE_NAME = "tb_activity_unlock_in_batches_record";

    @Update("update " + TABLE_NAME + " set unlocked_status = #{status}, updated_time = now() where id= #{id}")
    int updateUnlockedStatusById(@Param("id") Long id, @Param("status") Integer status);
}
