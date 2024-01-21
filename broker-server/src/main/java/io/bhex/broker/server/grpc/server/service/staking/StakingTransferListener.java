package io.bhex.broker.server.grpc.server.service.staking;

import com.alibaba.fastjson.JSON;
import io.bhex.broker.grpc.staking.StakingProductType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

/**
 * 转账派息处理
 * @author songxd
 * @date 2020-09-01
 */
@Slf4j
@Component
public class StakingTransferListener implements ApplicationListener<StakingTransferEvent> {

    @Resource
    private StakingProductTimeStrategy stakingProductTimeStrategy;

    @Resource
    private StakingProductLockedPositionStrategy stakingProductLockedPositionStrategy;

    @Resource
    private StakingProductCurrentStrategy stakingProductCurrentStrategy;

    /**
     * 转账 + 资产处理
     * @param event 转账事件
     */
    @Async
    @Override
    public void onApplicationEvent(StakingTransferEvent event) {
        try {
            switch (event.getProductType()){
                case StakingProductType.FI_CURRENT_VALUE:
                    stakingProductCurrentStrategy.dispatchInterest(event);
                    break;
                case StakingProductType.FI_TIME_VALUE:
                    stakingProductTimeStrategy.dispatchInterest(event);
                    break;
                case StakingProductType.LOCK_POSITION_VALUE:
                    stakingProductLockedPositionStrategy.dispatchInterest(event);
                    break;
                default:
                    break;
            }
        } catch (Exception e) {
            log.error(" staking StakingTransferListener exception:{} event :{}", e, event.toString());
        }
    }
}
