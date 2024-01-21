package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.ActivityDrawItem;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.SelectProvider;

@Mapper
public interface ActivityDrawItemMapper extends tk.mybatis.mapper.common.Mapper<ActivityDrawItem> {

    @SelectProvider(type = ActivityDrawItemSqlProvider.class, method = "getActivityDrawItemLock")
    ActivityDrawItem getActivityDrawItemLock(@Param("id") Long id);


    class ActivityDrawItemSqlProvider {

        static final String TABLE_NAME = "tb_activity_draw_item";

        public static String getActivityDrawItemLock() {

            StringBuffer sql = new StringBuffer();
            sql.append(" select *  from ");
            sql.append(TABLE_NAME);
            sql.append(" where id = #{id} ");
            sql.append(" for update ");

            return sql.toString();
        }
    }

}
