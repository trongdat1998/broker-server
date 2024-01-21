package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.AppBusinessPushRecord;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Update;

@Mapper
public interface AppBusinessPushRecordMapper  extends tk.mybatis.mapper.common.Mapper<AppBusinessPushRecord> {

    @Update("update tb_app_business_push_record set delivery_status=#{deliveryStatus},delivery_time=#{deliveryTime} where req_order_id=#{reqOrderId}")
    int updateDeliveryStatus(@Param("reqOrderId") String reqOrderId, @Param("deliveryStatus") String deliveryStatus, @Param("deliveryTime") long deliveryTime);

    @Update("update tb_app_business_push_record set click_time=#{clickTime} where req_order_id=#{reqOrderId}")
    int updateClickTime(@Param("reqOrderId") String reqOrderId, @Param("clickTime") long deliveryTime);
}
