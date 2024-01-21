package io.bhex.broker.server.grpc.server.service.staking;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

/**
 * 赎回事件处理
 *
 * @author songxd
 * @date 2020-09-08
 */
@Slf4j
@Component
public class StakingCurrentRedeemListener implements ApplicationListener<StakingRedeemEvent> {

    @Resource
    private StakingProductCurrentStrategy stakingProductCurrentStrategy;

    /**
     * 赎回事件处理
     *
     * @param event 赎回事件
     */
    @Override
    public void onApplicationEvent(StakingRedeemEvent event) {
        try {
            stakingProductCurrentStrategy.processRedeemEvent(event);
        } catch (Exception e) {
            log.error("StakingCurrentRedeemListener exception:{} event :{}", e, event.toString());
        }
    }
}
