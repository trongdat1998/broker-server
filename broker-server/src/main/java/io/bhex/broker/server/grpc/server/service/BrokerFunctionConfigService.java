package io.bhex.broker.server.grpc.server.service;

import com.google.common.collect.Lists;
import io.bhex.broker.grpc.function.config.BrokerFunction;
import io.bhex.broker.grpc.function.config.GetBrokerFunctionConfigResponse;
import io.bhex.broker.server.domain.InviteActivityStatus;
import io.bhex.broker.server.primary.mapper.BrokerFunctionConfigMapper;
import io.bhex.broker.server.model.BrokerFunctionConfig;
import io.bhex.broker.server.model.InviteActivity;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import javax.annotation.Resource;
import java.util.List;

@Slf4j
@Service
public class BrokerFunctionConfigService {

    @Resource
    BrokerFunctionConfigMapper brokerFunctionConfigMapper;

    @Autowired
    InviteService inviteService;


    public GetBrokerFunctionConfigResponse getBrokerFunctionConfigList(Long brokerId) {

        // 特殊活动特殊增加
        List<BrokerFunctionConfig> configList = Lists.newArrayList();
        InviteActivity inviteActivity = inviteService.getInviteFeeBackActivity(brokerId);
        if (inviteActivity != null && inviteActivity.getStatus().equals(InviteActivityStatus.ONLINE.getStatus())) {
            BrokerFunctionConfig config = BrokerFunctionConfig.builder()
                    .brokerId(brokerId)
                    .function(BrokerFunction.GENERAL_TRADE_FEE_BACK.name())
                    .status(1)
                    .build();
            configList.add(config);
        }

        // 获取正常功能配置
        BrokerFunctionConfig configCondition = BrokerFunctionConfig.builder()
                .brokerId(brokerId)
                .status(1)
                .build();

        List<BrokerFunctionConfig> list = brokerFunctionConfigMapper.select(configCondition);
        if (!CollectionUtils.isEmpty(list)) {
            configList.addAll(list);
        }

        return GetBrokerFunctionConfigResponse.newBuilder()
                .addAllConfigList(this.convertFunctionConfigList(configList))
                .build();
    }

    public GetBrokerFunctionConfigResponse getALLBrokerFunctionConfigList(Long brokerId, String function) {
        BrokerFunctionConfig configCondition = BrokerFunctionConfig.builder()
            .status(1)
            .build();
        if (brokerId != null && brokerId > 0) {
            configCondition.setBrokerId(brokerId);
        }
        if (StringUtils.isNotBlank(function)) {
            configCondition.setFunction(function);
        }
        GetBrokerFunctionConfigResponse.Builder responseBuilder = GetBrokerFunctionConfigResponse.newBuilder();
        List<BrokerFunctionConfig> list = brokerFunctionConfigMapper.select(configCondition);
        if (!CollectionUtils.isEmpty(list)) {
            responseBuilder.addAllConfigList(convertFunctionConfigList(list));
        }

        return responseBuilder.build();
    }

    public void setBrokerFunctionConfig(Long brokerId, String function, int status) {
        brokerFunctionConfigMapper.setBrokerFunctionConfig(brokerId, function, status);
    }

    public List<io.bhex.broker.grpc.function.config.BrokerFunctionConfig> convertFunctionConfigList(List<BrokerFunctionConfig> configList) {
        if (CollectionUtils.isEmpty(configList)) {
            return Lists.newArrayList();
        }
        List<io.bhex.broker.grpc.function.config.BrokerFunctionConfig> list = Lists.newArrayList();
        for (BrokerFunctionConfig config : configList) {
            list.add(convertFunctionConfig(config));
        }
        return list;
    }

    public io.bhex.broker.grpc.function.config.BrokerFunctionConfig convertFunctionConfig(BrokerFunctionConfig config) {
        return io.bhex.broker.grpc.function.config.BrokerFunctionConfig.newBuilder()
                .setBrokerId(config.getBrokerId())
                .setFunction(config.getFunction())
                .setStatus(config.getStatus())
                .build();
    }


}
