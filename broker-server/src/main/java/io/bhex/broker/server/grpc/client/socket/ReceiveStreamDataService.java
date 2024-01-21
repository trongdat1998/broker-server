/**********************************
 *@项目名称: broker-parent
 *@文件名称: io.bhex.broker.websocket
 *@Date 2018/8/14
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
package io.bhex.broker.server.grpc.client.socket;

import com.google.common.collect.Lists;
import io.bhex.base.account.SubReply;
import io.bhex.base.account.SubRequest;
import io.bhex.base.account.SubscribeServiceGrpc;
import io.bhex.base.account.Trade;
import io.bhex.broker.server.BrokerServerProperties;
import io.bhex.broker.server.domain.BrokerServerConstants;
import io.bhex.broker.server.domain.ReceiveTopic;
import io.bhex.broker.server.grpc.client.config.GrpcClientConfig;
import io.bhex.broker.server.grpc.server.service.BrokerService;
import io.bhex.broker.server.grpc.server.service.InviteService;
import io.bhex.broker.server.model.Broker;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import io.prometheus.client.Histogram;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.Resource;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

@Slf4j
//@Service
public class ReceiveStreamDataService {

    private static AtomicInteger count = new AtomicInteger(0); //线程安全的计数变量

    private static ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();

    private AtomicLong lastHeartBeat = new AtomicLong();

    private AtomicReference<SubscribeServiceGrpc.SubscribeServiceStub> stubReference = new AtomicReference<>();

    @Autowired
    BrokerService brokerService;

    @Autowired
    InviteService inviteService;

    @Autowired
    GrpcClientConfig grpcClientConfig;

    @Resource
    BrokerServerProperties brokerServerProperties;

    private static final Histogram HISTOGRAM = Histogram.build()
            .namespace("broker")
            .subsystem("stream")
            .name("stream_data_delay_milliseconds")
            .labelNames("stream_type")
            .buckets(BrokerServerConstants.CONTROLLER_TIME_BUCKETS)
            .help("Histogram of stream handle latency in milliseconds")
            .register();

    //    @EventListener(value = {ContextRefreshedEvent.class})
    public void init() {
        boolean startStream = brokerServerProperties.isStartStream();
        if (!startStream) {
            return;
        }
        // 充值大赛
        initStream();
        lastHeartBeat.set(System.currentTimeMillis());
        executorService.scheduleAtFixedRate(() -> {
            Long noHeartBeatSeconds = System.currentTimeMillis() - lastHeartBeat.get();
            if (noHeartBeatSeconds >= 40 * 1000) {
                log.warn("[ALERT] last heartbeat time:{}, stream has no HeartBeat message great than {} milliseconds, try connect......", lastHeartBeat.get(), noHeartBeatSeconds);
                initStream();
            }
        }, 8, 8, TimeUnit.SECONDS);
    }

    private void initStream() {
        log.info("Stream init......, count:{}", count.incrementAndGet());
        if (count.get() > 0) {
            log.error("Stream restarted, count:{}, please check!!!", count.get());
        }
        if (stubReference.get() != null) {
            if (stubReference.get().getChannel() != null) {
                ManagedChannel channel = (ManagedChannel) stubReference.get().getChannel();
                log.info("last stub has a channel, channel status:{}, isTerminates:{}, isShutdown:{}", channel.getState(false), channel.isTerminated(), channel.isShutdown());
                if (!channel.isShutdown()) {
                    channel.shutdownNow();
                }
                while (!channel.isTerminated()) {
                    log.info("last stub has a channel, invoke shutdownNow, and channel is not terminated, wait for termination");
                    try {
                        channel.awaitTermination(5, TimeUnit.SECONDS);
                    } catch (Exception e) {
                        log.error("channel awaitTermination error", e);
                    }
                    log.info("channel in last stub is terminated");
                }
            }
        }
        List<Long> brokerIdList = brokerService.queryBrokerAll().stream().filter(broker -> broker.getStatus() == 1)
                .map(Broker::getOrgId).collect(Collectors.toList());
        //特殊处理(未启用 不做处理)
        SubRequest request = SubRequest.newBuilder().addAllOrgId(brokerIdList).addAllTopics(Lists.newArrayList("Trade")).build();
        SubscribeServiceGrpc.SubscribeServiceStub stub = grpcClientConfig.subscribeServiceStub(0L);
        stubReference.set(stub);
        stub.tradeStream(request, getStreamObserver());
        log.info("Stream subscribed");
    }

    private StreamObserver<SubReply> getStreamObserver() {
        return new StreamObserver<SubReply>() {
            @Override
            public void onNext(SubReply reply) {
                try {
                    lastHeartBeat.set(System.currentTimeMillis());
                    if (reply.getType().equalsIgnoreCase(ReceiveTopic.HEARTBEAT.getTopic())) {
                        log.info("Stream message HeartBeat......");
                    }
                    if (reply.getType().equalsIgnoreCase(ReceiveTopic.TRADE.getTopic())) {
                        // 实时更新有效邀请人数
                        // inviteService.updateInviteVaildCount(reply.getTradesList());
                        Trade trade = reply.getTrades(0);
                        HISTOGRAM.labels("trade").observe(System.currentTimeMillis() - trade.getMatchTime());
                    }
                } catch (Exception e) {
                    log.warn("Stream onNext occurred a Exception, but I cannot throw it, so sad!", e);
                }
            }

            @Override
            public void onError(Throwable throwable) {
                log.error("[ALERT] Stream onError......", throwable);
            }

            @Override
            public void onCompleted() {
                log.info("Stream onCompleted......");
            }
        };
    }

}
