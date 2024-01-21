package io.bhex.broker.server.grpc.server.service;

import com.github.pagehelper.PageHelper;
import io.bhex.broker.grpc.useraction.QueryLogsReply;
import io.bhex.broker.grpc.useraction.QueryLogsRequest;
import io.bhex.broker.grpc.useraction.UserActionRecord;
import io.bhex.broker.server.model.UserActionLog;
import io.bhex.broker.server.primary.mapper.UserActionLogMapper;
import io.bhex.broker.server.util.BeanCopyUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import tk.mybatis.mapper.entity.Example;

import javax.annotation.Resource;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @Description:
 * @Date: 2019/12/6 下午1:51
 * @Author: liwei
 * @Copyright（C）: 2018 BlueHelix Inc. All rights reserved.
 */
@Slf4j
@Service
public class UserActionLogService {

    @Resource
    private UserActionLogMapper userActionLogMapper;

    public QueryLogsReply queryLogs(QueryLogsRequest request) {
        Example example =  Example.builder(UserActionLog.class)
                .orderByDesc("id")
                .build();
        PageHelper.startPage(0, request.getPageSize());
        Example.Criteria criteria =   example.createCriteria()
                .andEqualTo("orgId", request.getHeader().getOrgId())
                .andEqualTo("userId", request.getHeader().getUserId())
                .andEqualTo("status", 1);

        if (request.getFromId() > 0) {
            criteria.andLessThan("id", request.getFromId());
        }
        List<UserActionLog> logs = userActionLogMapper.selectByExample(example);
        if (CollectionUtils.isEmpty(logs)) {
            return QueryLogsReply.newBuilder().build();
        }

        List<UserActionRecord> grpcLogs = logs.stream().map(l -> {
            UserActionRecord.Builder builder = UserActionRecord.newBuilder();
            BeanCopyUtils.copyPropertiesIgnoreNull(l, builder);
            return builder.build();
        }).collect(Collectors.toList());
        return QueryLogsReply.newBuilder().addAllUserAction(grpcLogs).build();
    }

}
