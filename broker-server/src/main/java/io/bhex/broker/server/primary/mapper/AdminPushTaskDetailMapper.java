package io.bhex.broker.server.primary.mapper;


import io.bhex.broker.server.model.AdminPushTaskDetail;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.UpdateProvider;

/**
 * author: wangshouchao
 * Date: 2020/07/25 06:23:37
 */
@Mapper
public interface AdminPushTaskDetailMapper extends tk.mybatis.mapper.common.Mapper<AdminPushTaskDetail>{

    /**
     * 更新分组请求相关的数量
     */
    @UpdateProvider(type = AdminPushTaskDetailSqlProvider.class, method = "updateDetailStatisticsNum")
    int updateDetailStatisticsNum(AdminPushTaskDetail taskDetail);
}