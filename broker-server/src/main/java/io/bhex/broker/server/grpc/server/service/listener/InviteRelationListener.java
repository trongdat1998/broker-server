package io.bhex.broker.server.grpc.server.service.listener;

import io.bhex.broker.common.util.JsonUtil;
import io.bhex.broker.server.grpc.server.service.AutoAirdropService;
import io.bhex.broker.server.grpc.server.service.InviteService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class InviteRelationListener {

    @Autowired
    InviteService inviteService;

    @Autowired
    AutoAirdropService autoAirdropService;

    @Async
    @EventListener
    public void invokeEvent(InviteRelationEvent event) {
        try {
            // 空投注册送礼
            autoAirdropService.registerAirdrop(event.getOrgId(), event.getAccountId());

            // 如果没有邀请人， 则直接返回即可
            if (event.getInviteId() == null || event.getInviteId() <= 0) {
                return;
            }
            // 有邀请人 ，查找邀请关系， 记录关系
            inviteService.findInviteRelation(event.getOrgId(), event.getInviteId(), event.getUserId(), event.getAccountId(), event.getName());
        } catch (Exception e) {
            log.error("findInviteRelation exception:{}", JsonUtil.defaultGson().toJson(event), e);
        }
    }

}
