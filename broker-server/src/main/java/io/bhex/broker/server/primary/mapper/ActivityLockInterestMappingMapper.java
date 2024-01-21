package io.bhex.broker.server.primary.mapper;

import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;
import org.apache.ibatis.annotations.UpdateProvider;
import org.apache.ibatis.jdbc.SQL;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.List;

import io.bhex.broker.server.model.ActivityLockInterestMapping;
import lombok.extern.slf4j.Slf4j;
import tk.mybatis.mapper.common.Mapper;

@Component
@org.apache.ibatis.annotations.Mapper
public interface ActivityLockInterestMappingMapper extends Mapper<ActivityLockInterestMapping> {

    String TABLE_NAME = " tb_activity_lock_interest_mapping ";

    String COLUMNS = " * ";

    @Select("select " + COLUMNS + " from " + TABLE_NAME + " where broker_id = #{brokerId} and project_id = #{projectId} order by created_time desc")
    List<ActivityLockInterestMapping> queryMappingList(@Param("brokerId") Long brokerId, @Param("projectId") Long projectId);

    @Select("select count(1) from " + TABLE_NAME + " where broker_id = #{brokerId} and project_id = #{projectId}")
    List<ActivityLockInterestMapping> queryCountMappingList(@Param("brokerId") Long brokerId, @Param("projectId") Long projectId);

    @Select("select count(1) from " + TABLE_NAME + " where broker_id = #{brokerId} and project_id = #{projectId}")
    int queryCountMappingCount(@Param("brokerId") Long brokerId, @Param("projectId") Long projectId);

    @Select("select " + COLUMNS + " from " + TABLE_NAME + " where broker_id = #{brokerId} and project_id = #{projectId} and transfer_status = #{status} order by create_time desc")
    List<ActivityLockInterestMapping> queryMappingListTransferStatus(@Param("brokerId") Long brokerId, @Param("projectId") Long projectId, @Param("status") Integer status);

    @Select("select " + COLUMNS + " from " + TABLE_NAME + " where broker_id = #{brokerId} and project_id = #{projectId} and unlock_status = #{status} order by create_time desc")
    List<ActivityLockInterestMapping> queryMappingListByUnlockStatus(@Param("brokerId") Long brokerId, @Param("projectId") Long projectId, @Param("status") Integer status);

    @Update("update tb_activity_lock_interest_mapping set transfer_status = #{status},transfer_time = now() where id= #{id}")
    int updateTransferStatusById(@Param("id") Long id, @Param("status") Integer status);

    @Update("update tb_activity_lock_interest_mapping set unlock_status = #{status},unlock_time = now() where id= #{id}")
    int updateLockStatusById(@Param("id") Long id, @Param("status") Integer status);

    @Select("select " + COLUMNS + " from " + TABLE_NAME + " where broker_id = #{brokerId} and project_id = #{projectId} and account_id = #{accountId}")
    ActivityLockInterestMapping queryMappingInfoByUserId(@Param("brokerId") Long brokerId, @Param("projectId") Long projectId, @Param("accountId") Long accountId);

    @Select("select " + COLUMNS + " from " + TABLE_NAME + " where broker_id = #{brokerId} and project_id = #{projectId} and user_id = #{userId}")
    List<ActivityLockInterestMapping> queryMappingInfoListByUserId(@Param("brokerId") Long brokerId, @Param("projectId") Long projectId, @Param("userId") Long userId);

    @Select("select count(1) from " + TABLE_NAME + " where project_id = #{projectId} and (unlock_status <>0 or transfer_status<>0)")
    long countInvalidStatusRecord(@Param("projectId") Long projectId);

    @Select("select broker_id,project_id from tb_activity_lock_interest_mapping where unlock_status = 2 or transfer_status = 2 group by project_id")
    List<ActivityLockInterestMapping> queryNotFinishedActivity();

    @Select("select count(1) from tb_activity_lock_interest_mapping where project_id = #{projectId} and broker_id = #{brokerId} and (transfer_status = 1 or unlock_status = 1 or transfer_status = 2 or unlock_status = 2)")
    int queryActivityHasTransferCount(@Param("brokerId") Long brokerId, @Param("projectId") Long projectId);

    @Select("select sum(lucky_amount) from tb_activity_lock_interest_mapping where project_id = #{projectId} and broker_id = #{brokerId}")
    BigDecimal queryActivitySumLuckyAmount(@Param("brokerId") Long brokerId, @Param("projectId") Long projectId);

    @Select("select sum(amount) from tb_activity_lock_interest_mapping where project_id = #{projectId} and broker_id = #{brokerId}")
    BigDecimal queryUserAmount(@Param("brokerId") Long brokerId, @Param("projectId") Long projectId);

    @Select("select count(1) from tb_activity_lock_interest_mapping where project_id = #{projectId} and broker_id = #{brokerId}")
    int queryActivityCountOrder(@Param("brokerId") Long brokerId, @Param("projectId") Long projectId);

    @Select("select ifnull(project_id,0) as project_id,ifnull(broker_id,0) as broker_id,ifnull(sum(amount),0) as amount,ifnull(sum(use_amount),0) as use_amount ,ifnull(sum(lucky_amount),0) as lucky_amount,ifnull(sum(back_amount),0) as back_amount  from tb_activity_lock_interest_mapping where project_id = #{projectId} and broker_id = #{brokerId}")
    ActivityLockInterestMapping sumActivityLockInterestMapping(@Param("brokerId") Long brokerId, @Param("projectId") Long projectId);

    @UpdateProvider(type = ActivityLockInterestMappingProvider.class, method = "batchUpdateActivityLockInterestMapping")
    int batchUpdateActivityLockInterestMapping(List<ActivityLockInterestMapping> interestMappingList,
                                               @Param("brokerId") Long brokerId,
                                               @Param("projectId") Long projectId);

    @Slf4j
    class ActivityLockInterestMappingProvider {
        public String batchUpdateActivityLockInterestMapping(List<ActivityLockInterestMapping> interestMappingList,
                                                             @Param("brokerId") Long brokerId,
                                                             @Param("projectId") Long projectId) {
            StringBuffer sbf = new StringBuffer();
            interestMappingList.forEach(item -> {
                String sql = new SQL() {
                    {
                        if (interestMappingList.isEmpty()) {
                            throw new RuntimeException();
                        }
                        Long id = item.getId();
                        BigDecimal useAmount = item.getUseAmount();
                        BigDecimal luckyAmount = item.getLuckyAmount();
                        BigDecimal backAmount = item.getBackAmount();
                        UPDATE(TABLE_NAME);
                        SET("use_amount = " + useAmount.toPlainString());
                        SET("lucky_amount = " + luckyAmount.toPlainString());
                        SET("back_amount = " + backAmount.toPlainString());
                        WHERE("id = " + id);
                        WHERE("broker_id = " + brokerId);
                        WHERE("project_id = " + projectId);
                        WHERE("transfer_status = 0");
                        WHERE("unlock_status = 0");
                    }
                }.toString();
                sbf.append(sql + ";");
            });
            log.info("batchUpdateActivityLockInterestMapping sql {}", sbf.toString());
            return sbf.toString();
        }
    }
}
