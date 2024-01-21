package io.bhex.broker.server.grpc.server.service;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.bhex.broker.grpc.point.EventSwitchInfo;
import io.bhex.broker.server.primary.mapper.EventSwitchMapper;
import io.bhex.broker.server.model.EventSwitch;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.sql.Timestamp;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * 事件开关
 * <p>
 * 用于其他服务控制操作使用，如：点卡
 *
 * @author zhaojiankun
 * @date 2018/09/14
 */
@Service
public class EventSwitchService {

    /**
     * 默认时间：2000-01-01 00:00:00
     * 开关设置的`closeEndTime`（关闭截止时间）小于等于默认时间时，判定该开关永久关闭。
     */
    public static final Timestamp DEFAULT_TIMESTAMP = new Timestamp(946656000000L);
    public static final Timestamp CLOSE_TIMESTAMP = new Timestamp(915120000000L);

    private static final EventSwitchInfo DEFAULT_EVENT_SWITCH_INFO = EventSwitchInfo.newBuilder().setOpen(true).build();

    /**
     * 点卡元数据缓存
     */
    private Cache<String, EventSwitch> eventSwitchCache = CacheBuilder
            .newBuilder()
            .maximumSize(100L)
            .expireAfterWrite(10L, TimeUnit.SECONDS)
            .build();

    @Resource
    private EventSwitchMapper eventSwitchMapper;

    public void open(String eventSwitchId) {
        final Timestamp now = new Timestamp(System.currentTimeMillis());

        eventSwitchMapper.updateIsOpen(eventSwitchId, true, DEFAULT_TIMESTAMP, now);
        eventSwitchCache.invalidate(eventSwitchId);
    }

    public void close(String eventSwitchId) {

        eventSwitchCache.invalidate(eventSwitchId);

        final Timestamp now = new Timestamp(System.currentTimeMillis());

        if (eventSwitchMapper.updateIsOpen(eventSwitchId, false, DEFAULT_TIMESTAMP, now) > 0) {
            return;
        }
        EventSwitch eventSwitch = eventSwitchMapper.selectByPrimaryKey(eventSwitchId);
        if (eventSwitch != null) {
            return;
        }

        eventSwitch = EventSwitch.builder()
                .eventSwitchId(eventSwitchId)
                .closeEndTime(CLOSE_TIMESTAMP)
                .isOpen(false)
                .createdAt(now)
                .updatedAt(now)
                .build();
        eventSwitchMapper.insert(eventSwitch);
    }

    public void close(String eventSwitchId, Timestamp closeEndTime) {

        eventSwitchCache.invalidate(eventSwitchId);

        final Timestamp now = new Timestamp(System.currentTimeMillis());

        if (eventSwitchMapper.updateIsOpen(eventSwitchId, false, closeEndTime, now) > 0) {
            return;
        }
        EventSwitch eventSwitch = eventSwitchMapper.selectByPrimaryKey(eventSwitchId);
        if (eventSwitch != null) {
            return;
        }

        eventSwitch = EventSwitch.builder()
                .eventSwitchId(eventSwitchId)
                .closeEndTime(closeEndTime)
                .isOpen(false)
                .createdAt(now)
                .updatedAt(now)
                .build();
        eventSwitchMapper.insert(eventSwitch);
    }

    public boolean isOpen(String eventSwitchId) {
        final EventSwitch eventSwitch = queryEventSwitch(eventSwitchId);

        if (eventSwitch == null) {
            return true;
        }
        if (eventSwitch.getIsOpen()) {
            return true;
        }

        long closeEndTime = eventSwitch.getCloseEndTime().getTime();
        if (closeEndTime <= DEFAULT_TIMESTAMP.getTime()) {
            return false;
        }
        return closeEndTime < System.currentTimeMillis();
    }

    private EventSwitch queryEventSwitch(String eventSwitchId) {
        try {
            return eventSwitchCache.get(eventSwitchId, () -> {
                EventSwitch eventSwitch = eventSwitchMapper.selectByPrimaryKey(eventSwitchId);
                if (eventSwitch == null) {
                    eventSwitch = new EventSwitch();
                    eventSwitch.setEventSwitchId(eventSwitchId);
                    eventSwitch.setCreatedAt(new Timestamp(System.currentTimeMillis()));
                    eventSwitch.setIsOpen(true);
                }
                return eventSwitch;
            });
        } catch (ExecutionException e) {
            return eventSwitchMapper.selectByPrimaryKey(eventSwitchId);
        }
    }

    public EventSwitchInfo getEventSwitchInfo(String eventSwitchId) {
        final EventSwitch eventSwitch = queryEventSwitch(eventSwitchId);
        if (eventSwitch == null) {
            return DEFAULT_EVENT_SWITCH_INFO;
        }
        return parseEventSwitchInfo(eventSwitch);
    }

    private EventSwitchInfo parseEventSwitchInfo(final EventSwitch eventSwitch) {
        final EventSwitchInfo.Builder builder =
                EventSwitchInfo.newBuilder();

        builder.setEventSwitchId(eventSwitch.getEventSwitchId());
        if (!eventSwitch.getIsOpen()) {
            if (eventSwitch.getCloseEndTime().getTime()
                    <= DEFAULT_TIMESTAMP.getTime()) {
                builder.setOpen(false);

            } else {
                long closeEndTime = eventSwitch.getCloseEndTime().getTime();
                builder.setOpen(closeEndTime < System.currentTimeMillis());
                if (!builder.getOpen()) {
                    builder.setCloseEndTime(closeEndTime);
                }
            }
        } else {
            builder.setOpen(eventSwitch.getIsOpen());
        }
        return builder.build();
    }

    public List<EventSwitchInfo> allSwitch() {
        final List<EventSwitch> eventSwitchList = eventSwitchMapper.selectAll();
        final List<EventSwitchInfo> eventSwitchInfoList = new LinkedList<>();

        for (EventSwitch eventSwitch : eventSwitchList) {
            eventSwitchInfoList.add(parseEventSwitchInfo(eventSwitch));
        }
        return eventSwitchInfoList;
    }

    public static class EventSwitchIdProvider {

        /**
         * 点卡交易总开关
         */
        private static final String POINT_TRADE_SWITCH_ID_PREFIX = "point_trade_";

        /**
         * 点卡套餐交易开关
         */
        private static final String POINT_PACK_TRADE_SWITCH_ID_PREFIX = "point_pack_trade_";

        public static String getPointTradeSwitchId(String pointCardId) {

            return POINT_TRADE_SWITCH_ID_PREFIX + pointCardId;
        }

        public static String getPointPackTradeSwitchId(String pointCardId, Long pointPackId) {

            return POINT_PACK_TRADE_SWITCH_ID_PREFIX + pointCardId + "_" + pointPackId;
        }

    }

}
