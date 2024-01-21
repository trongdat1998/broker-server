package io.bhex.broker.server.grpc.server.service;

import com.google.common.base.Strings;
import io.bhex.broker.server.grpc.server.service.listener.RedPacketReceivedEvent;
import io.bhex.broker.server.model.RedPacket;
import io.bhex.broker.server.model.RedPacketReceiveDetail;
import io.bhex.broker.server.primary.mapper.RedPacketMapper;
import io.bhex.broker.server.primary.mapper.RedPacketReceiveDetailMapper;
import io.bhex.broker.server.util.RedisLockUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.event.EventListener;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import tk.mybatis.mapper.entity.Example;

import javax.annotation.Resource;
import java.util.List;
import java.util.Set;

@Slf4j
@Service
public class RedPacketTaskHandleService {

    public static final String RED_PACKET_RECEIVED_QUEUE = "RP_RECEIVED_IDS";

    @Resource(name = "stringRedisTemplate")
    private StringRedisTemplate redisTemplate;

    @Resource
    private RedPacketService redPacketService;

    @Resource
    private RedPacketReceiveDetailMapper redPacketReceiveDetailMapper;

    @Resource
    private RedPacketMapper redPacketMapper;

    /**
     * 抢红包时间，释放红包剩余金额和剩余个数
     *
     * @param event
     */
    @Async
    @EventListener
    public void invokeReceivedEvent(RedPacketReceivedEvent event) {
        try {
            redPacketService.handleReceiveRedPacket(event.getReceiveId());
        } catch (Exception e) {
            redisTemplate.opsForList().leftPush(RED_PACKET_RECEIVED_QUEUE, String.valueOf(event.getReceiveId()));
            log.error("[EVENT] handle red_packet received error, receivedId:{}", event.getReceiveId());
        }
    }

    /**
     * 抢红包标记成功，但是转账失败的情况
     */
    @Scheduled(cron = "0/10 0 * * * ?")
    public void handleFailedReceivedEvent() {
        boolean lock = RedisLockUtils.tryLock(redisTemplate, "handleFailedReceivedEventTask", 10000);
        if (lock) {
            String failedReceivedIdStr = null;
            while (!Strings.isNullOrEmpty((failedReceivedIdStr = redisTemplate.opsForList().rightPop(RED_PACKET_RECEIVED_QUEUE)))) {
                try {
                    redPacketService.handleReceiveRedPacket(Long.valueOf(failedReceivedIdStr));
                } catch (Exception e) {
                    redisTemplate.opsForList().leftPush(RED_PACKET_RECEIVED_QUEUE, failedReceivedIdStr);
                    log.error("[SCHEDULE] handle red_packet received error, receivedId:{}", failedReceivedIdStr);
                }
            }
        }
    }

    @Scheduled(cron = "25 0/1 * * * ?")
    public void refundTask() {
        boolean lock = RedisLockUtils.tryLock(redisTemplate, "redPacketRefundTask", 60000);
        if (lock) {
            Set<String> cachedRedPacketIds = redisTemplate.opsForZSet().rangeByScore(RedPacketService.RED_PACKET_IDS_FOR_EXPIRED_SCAN, 0, System.currentTimeMillis());
            if (cachedRedPacketIds != null && !cachedRedPacketIds.isEmpty()) {
                Long redPacketId = null;
                for (String cachedRedPacketId : cachedRedPacketIds) {
                    if (cachedRedPacketId != null) {
                        redPacketId = Long.valueOf(cachedRedPacketId);
                        try {
                            redPacketService.refundRedPacket(redPacketId);
                            redisTemplate.opsForZSet().remove(RedPacketService.RED_PACKET_IDS_FOR_EXPIRED_SCAN, cachedRedPacketId);
                        } catch (Exception e) {
                            log.error("refund redPacket error, redPacketId:{}", redPacketId, e);
                        }
                    }
                }
            }
            /*
             * 查漏补缺
             */
            Example example = Example.builder(RedPacket.class).orderByDesc("id").build();
            Example.Criteria criteria = example.createCriteria();
            criteria.andLessThan("expired", System.currentTimeMillis() + 100);
            criteria.andGreaterThan("remainAmount", 0);
            criteria.andNotEqualTo("status", 1);
            List<RedPacket> expiredRedPacketList = redPacketMapper.selectByExample(example);
            if (!CollectionUtils.isEmpty(expiredRedPacketList)) {
                for (RedPacket redPacket : expiredRedPacketList) {
                    try {
                        redPacketService.refundRedPacket(redPacket.getId());
                        redisTemplate.opsForZSet().remove(RedPacketService.RED_PACKET_IDS_FOR_EXPIRED_SCAN, redPacket.getId().toString());
                    } catch (Exception e) {
                        log.error("refund redPacket error, redPacketId:{}", redPacket.getId(), e);
                    }
                }
            }
        }
    }

    @Scheduled(cron = "0/15 * * * * ?")
    public void receiveTransferFailHandle() {
        boolean lock = RedisLockUtils.tryLock(redisTemplate, "receiveTransferFailHandle", 60000);
        if (lock) {
            // 找出已经成功打开红包但是转账失败的记录
            List<RedPacketReceiveDetail> unTransferReceiveRecord = redPacketReceiveDetailMapper.select(RedPacketReceiveDetail.builder().status(1).transferStatus(0).build());
            for (RedPacketReceiveDetail receiveDetail : unTransferReceiveRecord) {
                redPacketService.receiverRedPacketTransfer(receiveDetail, 0);
            }
        }
    }

}
