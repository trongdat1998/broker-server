package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.AdminPushTaskStatistics;
import org.apache.ibatis.jdbc.SQL;

/**
 * @author wangsc
 * @description
 * @date 2020-07-30 16:46
 */
public class AdminPushTaskStatisticsSqlProvider {

    public static final String TABLE_NAME = "tb_admin_push_task_statistics";

    public String updateTaskStatisticsNum(AdminPushTaskStatistics taskStatistics) {
                return new SQL() {
                    {
                        UPDATE(TABLE_NAME);
                        if (taskStatistics.getDeliveryCount() !=null && taskStatistics.getDeliveryCount() > 0L) {
                            SET("delivery_count = delivery_count + #{deliveryCount}");
                        }
                        if (taskStatistics.getEffectiveCount() !=null && taskStatistics.getEffectiveCount() > 0L) {
                            SET("effective_count = effective_count + #{effectiveCount}");
                        }
                        if (taskStatistics.getClickCount() !=null && taskStatistics.getClickCount() > 0L) {
                            SET("click_count = click_count + #{clickCount}");
                        }
                        if (taskStatistics.getCancelCount() !=null && taskStatistics.getCancelCount() > 0L) {
                            SET("cancel_count = cancel_count + #{cancelCount}");
                        }
                        if (taskStatistics.getUninstallCount() !=null && taskStatistics.getUninstallCount() > 0L) {
                            SET("uninstall_count = uninstall_count + #{uninstallCount}");
                        }
                        if (taskStatistics.getUnsubscribeCount() !=null && taskStatistics.getUnsubscribeCount() > 0L) {
                            SET("unsubscribe_count = unsubscribe_count + #{unsubscribeCount}");
                        }
                        SET("updated = #{updated}");
                        WHERE("task_id = #{taskId} and task_round = #{taskRound}");
                    }
                }.toString();
    }
}
