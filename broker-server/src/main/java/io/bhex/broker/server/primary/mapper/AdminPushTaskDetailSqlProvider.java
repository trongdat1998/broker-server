package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.AdminPushTaskDetail;
import org.apache.ibatis.jdbc.SQL;

/**
 * @author wangsc
 * @description 分组推送的统计
 * @date 2020-08-04 14:50
 */
public class AdminPushTaskDetailSqlProvider {
    public static final String TABLE_NAME = "tb_admin_push_task_detail";

    public String updateDetailStatisticsNum(AdminPushTaskDetail taskDetail) {
        return new SQL() {
            {
                UPDATE(TABLE_NAME);
                if (taskDetail.getDeliveryCount() !=null && taskDetail.getDeliveryCount() > 0L) {
                    SET("delivery_count = delivery_count + #{deliveryCount}");
                }
                if (taskDetail.getClickCount() !=null && taskDetail.getClickCount() > 0L) {
                    SET("click_count = click_count + #{clickCount}");
                }
                SET("updated = #{updated}");
                WHERE("req_order_id = #{reqOrderId}");
            }
        }.toString();
    }
}
