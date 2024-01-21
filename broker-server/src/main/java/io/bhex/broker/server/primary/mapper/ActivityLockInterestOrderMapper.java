package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.ActivityLockInterestOrder;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.SelectProvider;
import org.apache.ibatis.annotations.Update;
import org.springframework.stereotype.Component;
import tk.mybatis.mapper.common.Mapper;

import java.util.List;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.mapper
 * @Author: ming.xu
 * @CreateDate: 2019/6/4 3:11 PM
 * @Copyright（C）: 2019 BHEX Inc. All rights reserved.
 */
@Component
@org.apache.ibatis.annotations.Mapper
public interface ActivityLockInterestOrderMapper extends Mapper<ActivityLockInterestOrder> {

    public String COLUMNS = " * ";

    public String TABLE_NAME = " tb_activity_lock_interest_order ";

    @Select("select *from tb_activity_lock_interest_order where project_id = #{projectId} and status = #{status}")
    List<ActivityLockInterestOrder> queryOrderIdListByStatus(@Param("projectId") Long projectId, @Param("status") Integer status);

    @Select("select user_id,account_id,gift_token_id,sum(gift_amount) as gift_amount from tb_activity_lock_interest_order where project_id in(${activitys}) and broker_id = #{brokerId} and status = 1 and gift_token_id is not null and gift_amount > 0.00 group by user_id,gift_token_id")
    List<ActivityLockInterestOrder> queryOrderGiftAmountByProjectId(@Param("brokerId") Long brokerId, @Param("activitys") String activitys);

    @SelectProvider(type = ActivityOrderProvider.class, method = "queryAllOrderByPage")
    List<ActivityLockInterestOrder> queryAllOrderByPage(@Param("brokerId") Long brokerId, @Param("accountId") Long accountId, @Param("fromId") Long fromId, @Param("endId") Long endId, @Param("limit") Integer limit);

    @SelectProvider(type = ActivityOrderProvider.class, method = "orgQueryOrderByPage")
    List<ActivityLockInterestOrder> orgQueryOrderByPage(@Param("brokerId") Long brokerId, @Param("projectCode") String projectCode, @Param("fromId") Long fromId, @Param("endId") Long endId, @Param("limit") Integer limit, @Param("orderDesc") Boolean orderDesc);

    @SelectProvider(type = ActivityOrderProvider.class, method = "orgQueryUserOrderByPage")
    List<ActivityLockInterestOrder> orgQueryUserOrderByPage(@Param("brokerId") Long brokerId, @Param("userId") Long userId, @Param("fromId") Long fromId, @Param("endId") Long endId, @Param("limit") Integer limit, @Param("orderDesc") Boolean orderDesc);

    @Select("select count(distinct user_id)  from tb_activity_lock_interest_order where project_id =#{projectId} and status = 1 ")
    Integer countUser(@Param("projectId") Long projectId);

    @SelectProvider(type = ActivityOrderProvider.class, method = "queryAllOrderByPageByUidAndProjectId")
    List<ActivityLockInterestOrder> adminQueryAllOrderByPage(@Param("brokerId") Long brokerId, @Param("projectId") Long projectId, @Param("projectCode") String projectCode, @Param("userId") Long userId, @Param("fromId") Long fromId, @Param("endId") Long endId, @Param("limit") Integer limit);

    @Update("update tb_activity_lock_interest_order set user_transfer_status = #{userTransferStatus} where id = #{id}")
    int updateUserTransferStatusById(@Param("id") Long id, @Param("userTransferStatus") Integer userTransferStatus);

    @Update("update tb_activity_lock_interest_order set gift_transfer_status = #{giftTransferStatus} where id = #{id}")
    int updateGiftTransferStatusById(@Param("id") Long id, @Param("giftTransferStatus") Integer giftTransferStatus);
}
