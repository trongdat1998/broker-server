package io.bhex.broker.server.grpc.server.service.activity;

import com.google.common.base.Strings;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import com.google.common.primitives.Doubles;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.common.util.JsonUtil;
import io.bhex.broker.grpc.activity.draw.*;
import io.bhex.broker.grpc.common.Header;
import io.bhex.broker.grpc.deposit.DepositOrder;
import io.bhex.broker.grpc.deposit.QueryDepositOrdersResponse;
import io.bhex.broker.server.domain.ActivityDrawStatus;
import io.bhex.broker.server.domain.ActivityDrawType;
import io.bhex.broker.server.grpc.server.service.AccountService;
import io.bhex.broker.server.grpc.server.service.DepositService;
import io.bhex.broker.server.grpc.server.service.listener.ActivityDrawEvent;
import io.bhex.broker.server.primary.mapper.*;
import io.bhex.broker.server.model.*;
import io.bhex.broker.server.model.ActivityDrawChance;
import io.bhex.broker.server.model.ActivityDrawItem;
import io.bhex.broker.server.model.ActivityDrawRecord;
import io.bhex.broker.server.primary.mapper.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.ibatis.session.RowBounds;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;
import tk.mybatis.mapper.entity.Example;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
public class ActivityDrawService {

    /**
     * 活动条件 : token
     */
    public static final String ACTIVITY_CONDITION_TOKEN_ID = "ETH";

    /**
     * 活动条件 : token amount
     */
    public static final BigDecimal ACTIVITY_CONDITION_TOKEN_AMOUNT = new BigDecimal(1);

    public static final int ACTIVITY_RANDOM_MAX = 100000;

    @Autowired
    AccountService accountService;

    @Autowired
    DepositService depositService;

    @Autowired
    ActivityDrawService activityDrawService;

    @Resource
    AccountMapper accountMapper;

    @Resource
    UserMapper userMapper;

    @Resource
    ActivityDrawChanceMapper activityDrawChanceMapper;

    @Resource
    ActivityDrawItemMapper activityDrawItemMapper;

    @Resource
    ActivityDrawRecordMapper activityDrawRecordMapper;

    @Autowired
    ApplicationContext applicationContext;

    private LoadingCache<Long, List<ActivityDrawItem>> drawItemCache = CacheBuilder.newBuilder()
            .expireAfterWrite(10, TimeUnit.SECONDS)
            .build(new CacheLoader<Long, List<ActivityDrawItem>>() {
                @Override
                public List<ActivityDrawItem> load(Long brokerId) throws Exception {
                    ActivityDrawItem itemCondition = ActivityDrawItem.builder()
                            .brokerId(brokerId)
                            .build();
                    return activityDrawItemMapper.select(itemCondition);
                }
            });

    private LoadingCache<Long, ActivityDrawItem> singleDrawItemCache = CacheBuilder.newBuilder()
            .expireAfterAccess(10, TimeUnit.SECONDS)
            .build(new CacheLoader<Long, ActivityDrawItem>() {
                @Override
                public ActivityDrawItem load(Long aLong) throws Exception {
                    ActivityDrawItem itemCondition = ActivityDrawItem.builder()
                            .id(aLong)
                            .build();
                    return activityDrawItemMapper.selectOne(itemCondition);
                }
            });

    public String getActivityDrawUserName(Long accountId) {
        Account account = accountMapper.getAccountByAccountId(accountId);
        if (account == null) {
            return null;
        }
        User user = userMapper.getByUserId(account.getUserId());
        return StringUtils.isEmpty(user.getMobile()) ? user.getEmail() : user.getMobile();
    }

    public GetActivityDrawUserNameResponse getActivityDrawUserName(String ticket) {
        ActivityDrawChance chance = activityDrawChanceMapper.selectOne(ActivityDrawChance.builder().shareTicket(ticket).build());
        if (chance == null) {
            return GetActivityDrawUserNameResponse.getDefaultInstance();
        }

        String nickName = this.getActivityDrawUserName(chance.getAccountId());
        if (StringUtils.isEmpty(nickName)) {
            return GetActivityDrawUserNameResponse.getDefaultInstance();
        }
        return GetActivityDrawUserNameResponse.newBuilder()
                .setNickName(nickName)
                .build();
    }

    public GetActivityDrawItemListResponse getActivityDrawItemListResponse(Long brokerId) {
        return buildActivityDrawItemListResponse(this.getActivityDrawItemListCache(brokerId));
    }


    public List<ActivityDrawItem> getActivityDrawItemListCache(Long brokerId) {
        try {
            return drawItemCache.get(brokerId);
        } catch (Exception e) {
            log.error(" getActivityDrawItemListCache exception:brokerId:{}", brokerId, e);
        }
        return this.getActivityDrawItemList(brokerId);
    }

    public List<ActivityDrawItem> getActivityDrawItemList(Long brokerId) {
        ActivityDrawItem itemCondition = ActivityDrawItem.builder()
                .brokerId(brokerId)
                .build();
        List<ActivityDrawItem> list = activityDrawItemMapper.select(itemCondition);
        if (CollectionUtils.isEmpty(list)) {
            return Lists.newArrayList();
        }
        return list;
    }

    public GetUserActivityDrawChanceResponse getUserActivityDrawChance(Header header) {

        Long accountId = accountService.getAccountId(header.getOrgId(), header.getUserId());
        if (accountId == null) {
            log.info(" getUserActivityDrawInfo failed: userId:{} no mainAccount!", header.getUserId());
            return GetUserActivityDrawChanceResponse.newBuilder()
                    .setRet(BrokerErrorCode.ACCOUNT_NOT_EXIST.code())
                    .build();
        }

        ActivityDrawChance chance = this.computeUserDrawChance(header.getOrgId(), header.getUserId(), accountId);


        return GetUserActivityDrawChanceResponse.newBuilder()
                .setChance(this.buildActivityDrawChance(chance))
                .build();
    }

    public ActivityDrawChance computeUserDrawChance(Long orgId, Long userId, Long accountId) {
        Header header = Header.newBuilder().setUserId(userId).build();

        ActivityDrawChance chance = this.getActivtyDrawChanceByAccountId(accountId);
        if (chance != null) {
            return chance;
        }

        // 若用户没有创建过使用机会， 计算一下是否可以获得抽奖机会
        boolean getChangeFlag = false;
        BigDecimal exchangeAmount = BigDecimal.ZERO;
        long fromId = 0;
        while (true) {
            // 获取用户的充值记录
            QueryDepositOrdersResponse response = depositService.queryDepositOrder(header, null, fromId, 0L, 0L, 0L, 100);
            List<DepositOrder> depositOrderList = response.getOrdersList();
            if (CollectionUtils.isEmpty(depositOrderList)) {
                break;
            }

            for (DepositOrder depositOrder : depositOrderList) {
                fromId = depositOrder.getOrderId();

                // 统计充值记录折算成条件Token的数量
                BigDecimal amount = accountService.exchangeToken(orgId, depositOrder.getTokenId(),
                        ACTIVITY_CONDITION_TOKEN_ID, new BigDecimal(depositOrder.getQuantity()));
                exchangeAmount = exchangeAmount.add(amount);
            }

            if (exchangeAmount.compareTo(ACTIVITY_CONDITION_TOKEN_AMOUNT) > 0) {
                getChangeFlag = true;
                break;
            }
        }

        // 未满足条件
        if (!getChangeFlag) {
            // 返回一个没有机会的返回
            return ActivityDrawChance.builder()
                    .accountId(accountId)
                    .count(0)
                    .usedCount(0)
                    .status(ActivityDrawStatus.NORMAL.getStatus())
                    .build();
        }

        // 生成一个ticket ， 保证不能重复的
        String shareTicket;
        while (true) {
            shareTicket = RandomStringUtils.randomAlphabetic(16);
            ActivityDrawChance chanceTicketCondition = ActivityDrawChance.builder()
                    .accountId(accountId)
                    .build();

            List<ActivityDrawChance> chanceList = activityDrawChanceMapper.select(chanceTicketCondition);
            if (CollectionUtils.isEmpty(chanceList)) {
                break;
            }
        }

        chance = ActivityDrawChance.builder()
                .accountId(accountId)
                .shareTicket(shareTicket)
                .count(1)
                .usedCount(0)
                .status(ActivityDrawStatus.NORMAL.getStatus())
                .createdAt(new Timestamp(System.currentTimeMillis()))
                .updatedAt(new Timestamp(System.currentTimeMillis()))
                .build();

        // 入库
        activityDrawChanceMapper.insert(chance);
        return chance;
    }


    public UserLuckDrawResponse userLuckDraw(Header header) {
        Long accountId = accountService.getAccountId(header.getOrgId(), header.getUserId());
        if (accountId == null) {
            log.info(" userLuckDraw failed: userId:{} no mainAccount!", header.getUserId());
            return UserLuckDrawResponse.newBuilder().setRet(BrokerErrorCode.ACCOUNT_NOT_EXIST.code()).build();
        }


        ActivityDrawChance chance = this.getActivtyDrawChanceByAccountId(accountId);
        if (chance == null || chance.getCount() <= chance.getUsedCount()) {
            log.info(" userLuckDraw failed: chance is null or count finish!   brokerId:{} userId:{} chance:{}", header.getOrgId(), header.getUserId(), JsonUtil.defaultGson().toJson(chance));
            return UserLuckDrawResponse.newBuilder().setRet(BrokerErrorCode.ACTIVITY_DRAW_CHANCE_EMPTY.code()).build();
        }


        List<ActivityDrawItem> itemList = this.getActivityDrawItemListCache(header.getOrgId());
        if (CollectionUtils.isEmpty(itemList)) {
            log.info(" userLuckDraw failed: draw item is null!   brokerId:{} userId:{}", header.getOrgId(), header.getUserId());
            return UserLuckDrawResponse.newBuilder().setRet(BrokerErrorCode.ACTIVITY_DRAW_ITEM_NOT_EXIST.code()).build();
        }


        // 根据用户抽奖概率进行一次排序  根据概率 从小到大
        Collections.sort(itemList, new Comparator<ActivityDrawItem>() {
            @Override
            public int compare(ActivityDrawItem o1, ActivityDrawItem o2) {
                return Doubles.compare(o1.getUserProbability(), o2.getUserProbability());
            }
        });

        List<Long> banList = Lists.newArrayList();
        while (true) {
            // 抽个奖
            ActivityDrawItem drawItem = this.luckDraw(itemList, ActivityDrawType.USER, banList);
            // 有些将可能过期或者失效了， 所以会有抽不到的时候， 这个时候就再抽一次
            if (drawItem == null) {
                continue;
            }

            ActivityDrawRecord record = ActivityDrawRecord.builder()
                    .targetAccountId(accountId)
                    .itemId(drawItem.getId())
                    .itemName(drawItem.getName())
                    .type(ActivityDrawType.USER.getType())
                    .build();

            try {
                activityDrawService.executeUserLuckDraw(record, chance.getId());
                return UserLuckDrawResponse.newBuilder()
                        .setItem(this.buildActivityDrawItem(drawItem))
                        .build();
            } catch (BrokerException brokerException) {
                BrokerErrorCode errorCode = BrokerErrorCode.fromCode(brokerException.getCode());
                if (errorCode == null) {
                    throw brokerException;
                }
                if (errorCode.equals(BrokerErrorCode.ACTIVITY_DRAW_ITEM_OUT_LIMIT)) {
                    banList.add(drawItem.getId());
                    log.info(" executeUserLuckDraw failed : record:{} chanceId:{}", JsonUtil.defaultGson().toJson(record), chance.getId());
                    continue;
                } else {
                    // 其他的业务异常直接返回
                    return UserLuckDrawResponse.newBuilder().setRet(errorCode.code()).build();
                }
            }

        }
    }


    public GetShareUserActivityDrawChanceResponse getShareUserActivityDrawChance(Header header, String ticket, String openId) {
        ActivityDrawChance chanceCondition = ActivityDrawChance.builder()
                .shareTicket(ticket)
                .build();

        ActivityDrawChance chance = activityDrawChanceMapper.selectOne(chanceCondition);
        if (chance == null) {
            return GetShareUserActivityDrawChanceResponse.newBuilder()
                    .setRet(BrokerErrorCode.ACTIVITY_DRAW_CHANCE_EMPTY.code())
                    .build();
        }

        ActivityDrawRecord recordCondition = ActivityDrawRecord.builder()
                .targetAccountId(chance.getAccountId())
                .openId(openId)
                .build();

        List<ActivityDrawRecord> recordList = activityDrawRecordMapper.select(recordCondition);
        if (!CollectionUtils.isEmpty(recordList)) {
            return GetShareUserActivityDrawChanceResponse.newBuilder()
                    .setRet(BrokerErrorCode.ACTIVITY_DRAW_CHANCE_EMPTY.code())
                    .build();
        }

        ActivityDrawChance returnChance = ActivityDrawChance.builder()
                .count(0)
                .usedCount(0)
                .status(ActivityDrawStatus.NORMAL.getStatus())
                .build();

        return GetShareUserActivityDrawChanceResponse.newBuilder()
                .setChance(this.buildActivityDrawChance(returnChance))
                .build();
    }


    public ShareUserLuckDrawResponse shareUserLuckDraw(Header header, String ticket, String openId, String userName, String headerUrl) {

        ActivityDrawChance chanceCondition = ActivityDrawChance.builder()
                .shareTicket(ticket)
                .build();

        ActivityDrawChance chance = activityDrawChanceMapper.selectOne(chanceCondition);
        if (chance == null) {
            log.info(" shareUserLuckDraw failed: ticket is invaild::  ticket:{}  openId:", ticket, openId);
            return ShareUserLuckDrawResponse.newBuilder().setRet(BrokerErrorCode.ACTIVITY_DRAW_SHARE_TICKET_INVALID.code()).build();
        }

        ActivityDrawRecord recordCondition = ActivityDrawRecord.builder()
                .targetAccountId(chance.getAccountId())
                .openId(openId)
                .build();
        List<ActivityDrawRecord> recordList = activityDrawRecordMapper.select(recordCondition);
        if (!CollectionUtils.isEmpty(recordList)) {
            return ShareUserLuckDrawResponse.newBuilder().setRet(BrokerErrorCode.ACTIVITY_DRAW_CHANCE_EMPTY.code()).build();
        }

        List<ActivityDrawItem> itemList = this.getActivityDrawItemListCache(header.getOrgId());
        if (CollectionUtils.isEmpty(itemList)) {
            log.info(" shareUserLuckDraw failed: draw item is null!   brokerId:{} userId:{}", header.getOrgId(), header.getUserId());
            return ShareUserLuckDrawResponse.newBuilder().setRet(BrokerErrorCode.ACTIVITY_DRAW_ITEM_NOT_EXIST.code()).build();
        }


        // 根据分享用户抽奖概率进行一次排序  根据概率 从小到大
        Collections.sort(itemList, new Comparator<ActivityDrawItem>() {
            @Override
            public int compare(ActivityDrawItem o1, ActivityDrawItem o2) {
                return Doubles.compare(o1.getShareProbability(), o2.getShareProbability());
            }
        });

        List<Long> banList = Lists.newArrayList();
        while (true) {
            // 抽个奖
            ActivityDrawItem drawItem = this.luckDraw(itemList, ActivityDrawType.USER, banList);
            // 有些将可能过期或者失效了， 所以会有抽不到的时候， 这个时候就再抽一次
            if (drawItem == null) {
                continue;
            }

            ActivityDrawRecord record = ActivityDrawRecord.builder()
                    .targetAccountId(chance.getAccountId())
                    .itemId(drawItem.getId())
                    .itemName(drawItem.getName())
                    .type(ActivityDrawType.SHARE.getType())
                    .openId(openId)
                    .userName(userName)
                    .headerUrl(headerUrl)
                    .build();

            try {
                activityDrawService.executeLuckDraw(record);

                // 异步时间监听，触发发券的行为
                applicationContext.publishEvent(
                        ActivityDrawEvent.builder()
                                .ticket(chance.getShareTicket())
                                .build()
                );


                return ShareUserLuckDrawResponse.newBuilder()
                        .setItem(this.buildActivityDrawItem(drawItem))
                        .build();
            } catch (BrokerException brokerException) {
                BrokerErrorCode errorCode = BrokerErrorCode.fromCode(brokerException.getCode());
                if (errorCode == null) {
                    throw brokerException;
                }
                if (errorCode.equals(BrokerErrorCode.ACTIVITY_DRAW_ITEM_OUT_LIMIT)) {
                    banList.add(drawItem.getId());
                    log.info(" executeLuckDraw failed : record:{} chanceId:{}", JsonUtil.defaultGson().toJson(record), chance.getId());
                    continue;
                } else {
                    // 其他的业务异常直接返回
                    return ShareUserLuckDrawResponse.newBuilder().setRet(errorCode.code()).build();
                }
            }

        }

    }


    @Transactional(rollbackFor = Exception.class)
    public void executeUserLuckDraw(ActivityDrawRecord record, Long chanceId) {

        Account account = accountMapper.getByAccountIdLock(record.getTargetAccountId());
        if (account == null) {
            throw new BrokerException(BrokerErrorCode.ACCOUNT_NOT_EXIST);
        }


        ActivityDrawChance chance = activityDrawChanceMapper.getActivityDrawChanceLock(chanceId);
        if (chance == null || chance.getUsedCount() >= chance.getCount()) {
            // 没有机会的异常
            throw new BrokerException(BrokerErrorCode.ACTIVITY_DRAW_CHANCE_EMPTY);
        }

        // 更新奖励数量
        this.executeLuckDraw(record);

        // 更新用户抽奖机会数量
        chance.setUsedCount(chance.getUsedCount() + 1);
        chance.setUpdatedAt(new Timestamp(System.currentTimeMillis()));
        activityDrawChanceMapper.updateByPrimaryKeySelective(chance);

    }

    @Transactional(rollbackFor = Exception.class)
    public void executeLuckDraw(ActivityDrawRecord record) {
        // 锁表获取奖品
        ActivityDrawItem item = activityDrawItemMapper.getActivityDrawItemLock(record.getItemId());
        if (item == null) {
            log.info(" execute luck draw failed:::  draw item is null: record:{}", JsonUtil.defaultGson().toJson(record));
            throw new BrokerException(BrokerErrorCode.ACTIVITY_DRAW_ITEM_NOT_EXIST);
        }

        // 奖励已获取数量 大于 奖励限制数量
        if (item.getDoneCount() >= item.getLimitCount()) {
            log.info(" execute luck draw failed:::  draw item out limit \n record:{} \n item:{}", JsonUtil.defaultGson().toJson(record), JsonUtil.defaultGson().toJson(item));
            throw new BrokerException(BrokerErrorCode.ACTIVITY_DRAW_ITEM_OUT_LIMIT);
        }

        // 更新奖品的获奖个数
        item.setDoneCount(item.getDoneCount() + 1);
        item.setUpdatedAt(new Timestamp(System.currentTimeMillis()));
        activityDrawItemMapper.updateByPrimaryKeySelective(item);


        // 插入抽奖记录
        record.setStatus(ActivityDrawStatus.NORMAL.getStatus());
        record.setCreatedAt(new Timestamp(System.currentTimeMillis()));
        record.setUpdatedAt(new Timestamp(System.currentTimeMillis()));
        activityDrawRecordMapper.insert(record);

        // 触发更新缓存
        drawItemCache.refresh(item.getBrokerId());

    }


    public ActivityDrawItem luckDraw(List<ActivityDrawItem> itemList, ActivityDrawType drawType, List<Long> banList) {

        // 在最大值范围内获取一个随机数
        int random = RandomUtils.nextInt(0, ACTIVITY_RANDOM_MAX);

        int startRange = 0;
        ActivityDrawItem defaultItem = null;
        for (ActivityDrawItem item : itemList) {
            // 如果token为空，则为谢谢奖
            if (StringUtils.isEmpty(item.getToken())) {
                defaultItem = item;
            }
            double probability;
            // 根据抽奖的类型获取当前奖项的概率
            if (ActivityDrawType.USER.equals(drawType)) {
                probability = item.getUserProbability();
            } else {
                probability = item.getShareProbability();
            }

            // 如果当前奖项在禁止获奖的列表中，则跳过该奖项
            if (banList.contains(item.getId())) {
                continue;
            }

            // 如果当前奖项已中奖数量大于限制数量，则跳过该奖项
            if (item.getDoneCount() >= item.getLimitCount()) {
                continue;
            }

            // 如果概率为0 就直接跳过
            if (probability <= 0) {
                continue;
            }

            // 开始范围 + （概率 * 最大值） 就是当前奖项的区间范围
            int endRange = startRange + (int) (probability * ACTIVITY_RANDOM_MAX);
            // 随机数 （大于等于 开始 并且 小于 结束） ，意味落在该奖项区间
            if (random >= startRange && random < endRange) {
                return item;
            }

            // 若不在此区间，则将开始范围变为该奖项的结束范围值
            startRange = endRange;
        }

        //所有奖项都没通过的话，就返回谢谢奖
        return defaultItem;
    }


    public void sendActivityDrawTicket(String ticket) {
        ActivityDrawChance chanceCondition = ActivityDrawChance.builder()
                .shareTicket(ticket)
                .build();

        ActivityDrawChance chance = activityDrawChanceMapper.selectOne(chanceCondition);
        if (chance == null) {
            log.info(" sendActivityDrawTicket failed: ticket is invaild::  ticket:{} ", ticket);
            return;
        }

        // 如果机会已经用过了，则认为是已经发过券了
        if (chance.getStatus() == 1) {
            log.info(" sendActivityDrawTicket finished:  chance is used! :: ticket:{}", ticket);
            return;
        }

        ActivityDrawRecord recordCondition = ActivityDrawRecord.builder()
                .targetAccountId(chance.getAccountId())
                .build();
        List<ActivityDrawRecord> recordList = activityDrawRecordMapper.select(recordCondition);
        if (CollectionUtils.isEmpty(recordList) || recordList.size() < 7) {
            log.info(" sendActivityDrawTicket finished:  draw record less than 7! :: ticket:{}", ticket);
            return;
        }

        List<ActivityDrawRecord> drawRecordList = Lists.newArrayList();
        for (int i = 0; i < 7; i++) {
            drawRecordList.add(recordList.get(i));
        }
        ActivityDrawRecord luckRecord = null;
        try {
            luckRecord = activityDrawService.getActivityDrawLuckRecord(chance, drawRecordList);
        } catch (BrokerException e) {
            log.info(" sendActivityDrawTicket getActivityDrawLuckRecord failed: {}-{}", e.getCode(), e.getMessage());
            return;
        }

        if (luckRecord == null) {
            log.info(" sendActivityDrawTicket getActivityDrawLuckRecord failed ::: luck record is null!!! chance:{}, recordLits:{} ",
                    JsonUtil.defaultGson().toJson(chance), JsonUtil.defaultGson().toJson(drawRecordList));
            return;
        }

        log.info(" sendActivityDrawTicket  final drawItem:{}", JsonUtil.defaultGson().toJson(luckRecord));
        // TODO 这里发券去
        ActivityDrawItem item = null;
        try {
            item = singleDrawItemCache.get(luckRecord.getItemId());
            // 发给谁
            Long toAccountId = luckRecord.getTargetAccountId();
            // 发哪个币
            String token = item.getToken();
            // 发多少
            Double amount = item.getAmount();


        } catch (ExecutionException e) {
            e.printStackTrace();
        }


    }

    @Transactional(rollbackFor = Exception.class)
    public ActivityDrawRecord getActivityDrawLuckRecord(ActivityDrawChance chance, List<ActivityDrawRecord> recordList) {

        ActivityDrawChance lockChance = activityDrawChanceMapper.getActivityDrawChanceLock(chance.getId());
        if (lockChance == null) {
            throw new BrokerException(BrokerErrorCode.ACTIVITY_DRAW_CHANCE_EMPTY);
        }

        if (lockChance.getStatus() != ActivityDrawStatus.NORMAL.getStatus()) {
            throw new BrokerException(BrokerErrorCode.ACTIVITY_DRAW_CHANCE_EMPTY);
        }

        // 根据奖项排序  从小到大  价值排序数越小，则奖励价值越大
        Collections.sort(recordList, new Comparator<ActivityDrawRecord>() {
            @Override
            public int compare(ActivityDrawRecord o1, ActivityDrawRecord o2) {
                try {
                    ActivityDrawItem item1 = singleDrawItemCache.get(o1.getItemId());
                    ActivityDrawItem item2 = singleDrawItemCache.get(o2.getItemId());

                    return item1.getValueSort().compareTo(item2.getValueSort());

                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
                return 0;
            }
        });

        // 取第一个为中奖纪录
        ActivityDrawRecord luckRecord = recordList.get(0);

        // 修改chance 状态为 used ,
        lockChance.setStatus(ActivityDrawStatus.USED.getStatus());
        lockChance.setUpdatedAt(new Timestamp(System.currentTimeMillis()));
        activityDrawChanceMapper.updateByPrimaryKeySelective(lockChance);
        // 修改record 状态为 中奖
        luckRecord.setStatus(ActivityDrawStatus.USED.getStatus());
        luckRecord.setUpdatedAt(new Timestamp(System.currentTimeMillis()));
        activityDrawRecordMapper.updateByPrimaryKeySelective(luckRecord);

        return luckRecord;
    }

    public GetDrawRecordListResponse getLastDrawRecordList(Header header) {
        return this.getDrawRecordList(null, null, null, null, 20, " id desc");
    }

    public GetDrawRecordListResponse getUserDrawRecordList(Header header, Integer type, Long fromId, Long endId, Integer limit) {
        Long accountId = accountService.getMainAccountId(header);
        if (accountId == null) {
            return GetDrawRecordListResponse.getDefaultInstance();
        }
        return this.getDrawRecordList(accountId, type, fromId, endId, limit, null);
    }

    public GetDrawRecordListResponse getShareDrawRecordList(Header header, String ticket, Integer type, Long fromId, Long endId, Integer limit) {
        ActivityDrawChance chanceCondition = ActivityDrawChance.builder()
                .shareTicket(ticket)
                .build();

        ActivityDrawChance chance = activityDrawChanceMapper.selectOne(chanceCondition);
        if (chance == null) {
            return GetDrawRecordListResponse.getDefaultInstance();
        }
        return this.getDrawRecordList(chance.getAccountId(), type, fromId, endId, limit, null);
    }

    public GetDrawRecordListResponse getDrawRecordList(Long accountId, Integer type, Long fromId, Long endId, Integer limit, String ordreBy) {

        Example example = Example.builder(ActivityDrawRecord.class)
                .build();

        Example.Criteria criteria = example.createCriteria();
        if (accountId != null && accountId > 0) {
            criteria.andEqualTo("targetAccountId", accountId);
        }

        if (type != null && type > 0) {
            criteria.andEqualTo("type", type);
        }

        if (fromId != null && fromId > 0) {
            criteria.andGreaterThanOrEqualTo("id", fromId);
        }

        if (endId != null && endId > 0) {
            criteria.andLessThanOrEqualTo("id", endId);
        }

        if (StringUtils.isEmpty(ordreBy)) {
            example.setOrderByClause("id asc ");
        } else {
            example.setOrderByClause(ordreBy);
        }

        List<ActivityDrawRecord> list = activityDrawRecordMapper.selectByExampleAndRowBounds(example, new RowBounds(0, limit));
        return GetDrawRecordListResponse.newBuilder()
                .addAllRecordList(this.buildActivityDrawRecordList(list))
                .build();
    }

    public ActivityDrawChance getActivtyDrawChanceByAccountId(Long accountId) {
        ActivityDrawChance chanceCondition = ActivityDrawChance.builder()
                .accountId(accountId)
                .build();

        return activityDrawChanceMapper.selectOne(chanceCondition);
    }

    public GetActivityDrawItemListResponse buildActivityDrawItemListResponse(List<ActivityDrawItem> list) {
        if (CollectionUtils.isEmpty(list)) {
            return GetActivityDrawItemListResponse.getDefaultInstance();
        }

        List<io.bhex.broker.grpc.activity.draw.ActivityDrawItem> itemList = Lists.newArrayList();
        for (ActivityDrawItem item : list) {
            itemList.add(this.buildActivityDrawItem(item));
        }

        return GetActivityDrawItemListResponse.newBuilder()
                .addAllItemList(itemList)
                .build();
    }

    public List<io.bhex.broker.grpc.activity.draw.ActivityDrawRecord> buildActivityDrawRecordList(List<ActivityDrawRecord> list) {
        if (CollectionUtils.isEmpty(list)) {
            return Lists.newArrayList();
        }
        List<io.bhex.broker.grpc.activity.draw.ActivityDrawRecord> recordList = Lists.newArrayList();
        for (ActivityDrawRecord record : list) {
            recordList.add(this.buildActivityDrawRecord(record));
        }
        return recordList;
    }

    public io.bhex.broker.grpc.activity.draw.ActivityDrawRecord buildActivityDrawRecord(ActivityDrawRecord record) {
        return io.bhex.broker.grpc.activity.draw.ActivityDrawRecord.newBuilder()
                .setId(record.getId())
                .setTargetAccountId(record.getTargetAccountId())
                .setItemId(record.getItemId())
                .setItemName(record.getItemName())
                .setType(record.getType())
                .setOpenId(Strings.nullToEmpty(record.getOpenId()))
                .setUserName(Strings.nullToEmpty(record.getUserName()))
                .setHeaderUrl(Strings.nullToEmpty(record.getHeaderUrl()))
                .setStatus(record.getStatus())
                .setCreatedAt(record.getCreatedAt() == null ? System.currentTimeMillis() : record.getCreatedAt().getTime())
                .setUpdatedAt(record.getUpdatedAt() == null ? System.currentTimeMillis() : record.getUpdatedAt().getTime())
                .build();
    }

    public io.bhex.broker.grpc.activity.draw.ActivityDrawItem buildActivityDrawItem(ActivityDrawItem item) {
        return io.bhex.broker.grpc.activity.draw.ActivityDrawItem.newBuilder()
                .setId(item.getId())
                .setBrokerId(item.getBrokerId())
                .setName(item.getName())
                .setToken(item.getToken())
                .setAmount(item.getAmount().toString())
                .setTotalCount(item.getTotalCount())
                .setDoneCount(item.getDoneCount())
                .setLimitCount(item.getLimitCount())
                .setUserProbability(item.getUserProbability().toString())
                .setShareProbability(item.getShareProbability().toString())
                .setCreatedAt(item.getCreatedAt().getTime())
                .setUpdatedAt(item.getUpdatedAt().getTime())
                .build();
    }

    public io.bhex.broker.grpc.activity.draw.ActivityDrawChance buildActivityDrawChance(ActivityDrawChance chance) {
        return io.bhex.broker.grpc.activity.draw.ActivityDrawChance.newBuilder()
                .setId(chance.getId() == null ? 0 : chance.getId())
                .setAccountId(chance.getAccountId() == null ? 0 : chance.getAccountId())
                .setShareTicket(StringUtils.isEmpty(chance.getShareTicket()) ? "" : chance.getShareTicket())
                .setCount(chance.getCount() == null ? 0 : chance.getCount())
                .setUsedCount(chance.getUsedCount() == null ? 0 : chance.getUsedCount())
                .setStatus(chance.getStatus() == null ? 0 : chance.getStatus())
                .setCreatedAt(chance.getCreatedAt() == null ? System.currentTimeMillis() : chance.getCreatedAt().getTime())
                .setUpdatedAt(chance.getUpdatedAt() == null ? System.currentTimeMillis() : chance.getUpdatedAt().getTime())
                .build();
    }

}
