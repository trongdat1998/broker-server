package io.bhex.broker.server.primary.mapper;


import io.bhex.broker.server.model.AdminPushTaskStatistics;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.UpdateProvider;

/**
 * author: wangshouchao
 * Date: 2020/07/25 06:25:33
 */
@Mapper
public interface AdminPushTaskStatisticsMapper extends tk.mybatis.mapper.common.Mapper<AdminPushTaskStatistics>{

    /**
     * 更新统计相关的数量
     */
    @UpdateProvider(type = AdminPushTaskStatisticsSqlProvider.class, method = "updateTaskStatisticsNum")
    int updateTaskStatisticsNum(AdminPushTaskStatistics taskStatistics);

}