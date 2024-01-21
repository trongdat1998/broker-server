package io.bhex.broker.server.grpc.server.service.staking;

import com.alibaba.fastjson.JSON;
import io.bhex.broker.grpc.staking.StakingProductType;
import io.bhex.broker.server.model.staking.StakingPoolRebate;
import io.bhex.broker.server.primary.mapper.StakingPoolRebateMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

/**
 * 申购支付资产处理
 * @author songxd
 * @date 2020-09-01
 */
@Slf4j
@Component
public class StakingSubscribeListener implements ApplicationListener<StakingSubscribeEvent> {

    @Resource
    private StakingProductTimeStrategy stakingProductTimeStrategy;

    @Resource
    private StakingProductLockedPositionStrategy stakingProductLockedPositionStrategy;

    @Resource
    private StakingProductCurrentStrategy stakingProductCurrentStrategy;

    /**
     * 转账 + 资产处理
     * @param event
     */
    @Async
    @Override
    public void onApplicationEvent(StakingSubscribeEvent event) {
        try {
            switch (event.getProductType()){
                case StakingProductType.FI_CURRENT_VALUE:
                    stakingProductCurrentStrategy.processSubscribeEvent(event);
                    break;
                case StakingProductType.FI_TIME_VALUE:
                    stakingProductTimeStrategy.processSubscribeEvent(event);
                    break;
                case StakingProductType.LOCK_POSITION_VALUE:
                    stakingProductLockedPositionStrategy.processSubscribeEvent(event);
                    break;
                default:
                    break;
            }
        } catch (Exception e) {
            log.error(" StakingSubscribeListener exception:{} event :{}", e, event.toString());
        }
    }
}
