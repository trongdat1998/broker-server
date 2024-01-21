package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.ActivityDrawChance;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.SelectProvider;

@Mapper
public interface ActivityDrawChanceMapper extends tk.mybatis.mapper.common.Mapper<ActivityDrawChance> {

    @SelectProvider(type = ActivityDrawChanceSqlProvider.class, method = "getActivityDrawChanceLock")
    ActivityDrawChance getActivityDrawChanceLock(@Param("id") long id);

    class ActivityDrawChanceSqlProvider {

        static final String TABLE_NAME = "tb_activity_draw_chance";

        public String getActivityDrawChanceLock() {


            StringBuffer sql = new StringBuffer();
            sql.append(" select * from ");
            sql.append(TABLE_NAME);
            sql.append(" where id = #{id} ");
            sql.append(" for update ");

            return sql.toString();
        }
    }

}
