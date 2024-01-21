package io.bhex.broker.server.grpc.server.service;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;

import com.alibaba.fastjson.JSON;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Resource;

import io.bhex.base.account.BalanceDetail;
import io.bhex.base.account.BalanceDetailList;
import io.bhex.base.account.BusinessSubject;
import io.bhex.base.account.GetBalanceDetailRequest;
import io.bhex.base.account.LockBalanceReply;
import io.bhex.base.account.LockBalanceRequest;
import io.bhex.base.account.SyncTransferRequest;
import io.bhex.base.account.SyncTransferResponse;
import io.bhex.base.account.UnlockBalanceRequest;
import io.bhex.base.account.UnlockBalanceResponse;
import io.bhex.base.constants.ProtoConstants;
import io.bhex.base.idgen.api.ISequenceGenerator;
import io.bhex.base.proto.BaseRequest;
import io.bhex.base.proto.DecimalUtil;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.common.util.JsonUtil;
import io.bhex.broker.grpc.activity.lockInterest.ActivityInfo;
import io.bhex.broker.grpc.activity.lockInterest.CreateLockInterestOrderReply;
import io.bhex.broker.grpc.activity.lockInterest.CreateLockInterestOrderRequest;
import io.bhex.broker.grpc.activity.lockInterest.ListActivityReply;
import io.bhex.broker.grpc.activity.lockInterest.ListActivityRequest;
import io.bhex.broker.grpc.activity.lockInterest.ListLockOrderInfoReply;
import io.bhex.broker.grpc.activity.lockInterest.ListLockOrderInfoRequest;
import io.bhex.broker.grpc.activity.lockInterest.ListLockProjectInfoReply;
import io.bhex.broker.grpc.activity.lockInterest.ListLockProjectInfoRequest;
import io.bhex.broker.grpc.activity.lockInterest.OrgActivityInfo;
import io.bhex.broker.grpc.activity.lockInterest.OrgListActivityReply;
import io.bhex.broker.grpc.activity.lockInterest.OrgListActivityRequest;
import io.bhex.broker.grpc.activity.lockInterest.OrgQueryActivityOrderInfoByUserRequest;
import io.bhex.broker.grpc.activity.lockInterest.OrgQueryActivityOrderInfoReply;
import io.bhex.broker.grpc.activity.lockInterest.OrgQueryActivityOrderInfoRequest;
import io.bhex.broker.grpc.activity.lockInterest.ProjectCommonInfoReply;
import io.bhex.broker.grpc.activity.lockInterest.ProjectCommonInfoRequest;
import io.bhex.broker.grpc.activity.lockInterest.QueryActivityProjectInfoRequest;
import io.bhex.broker.grpc.activity.lockInterest.QueryActivityProjectInfoResponse;
import io.bhex.broker.grpc.activity.lockInterest.QueryAllActivityOrderInfoReply;
import io.bhex.broker.grpc.activity.lockInterest.QueryAllActivityOrderInfoRequest;
import io.bhex.broker.grpc.admin.ActivityType;
import io.bhex.broker.grpc.common.Header;
import io.bhex.broker.server.domain.ActivityConstant;
import io.bhex.broker.server.domain.BrokerServerConstants;
import io.bhex.broker.server.grpc.client.service.GrpcBalanceService;
import io.bhex.broker.server.grpc.client.service.GrpcBatchTransferService;
import io.bhex.broker.server.grpc.server.service.listener.IEOPanicBuyEvent;
import io.bhex.broker.server.grpc.server.service.po.ActivityLockInterestUserInfo;
import io.bhex.broker.server.grpc.server.service.po.ActivityTransfer;
import io.bhex.broker.server.grpc.server.service.po.ActivityTransferResult;
import io.bhex.broker.server.grpc.server.service.statistics.StatisticsBalanceService;
import io.bhex.broker.server.model.ActivityLockInterest;
import io.bhex.broker.server.model.ActivityLockInterestCommon;
import io.bhex.broker.server.model.ActivityLockInterestGift;
import io.bhex.broker.server.model.ActivityLockInterestLocal;
import io.bhex.broker.server.model.ActivityLockInterestMapping;
import io.bhex.broker.server.model.ActivityLockInterestOrder;
import io.bhex.broker.server.model.ActivityLockInterestWhiteList;
import io.bhex.broker.server.model.ActivityPurchaseLimit;
import io.bhex.broker.server.model.User;
import io.bhex.broker.server.model.UserLevel;
import io.bhex.broker.server.model.UserLevelConfig;
import io.bhex.broker.server.model.UserVerify;
import io.bhex.broker.server.primary.mapper.ActivityLockInterestBatchDetailMapper;
import io.bhex.broker.server.primary.mapper.ActivityLockInterestCommonMapper;
import io.bhex.broker.server.primary.mapper.ActivityLockInterestGiftMapper;
import io.bhex.broker.server.primary.mapper.ActivityLockInterestLocalMapper;
import io.bhex.broker.server.primary.mapper.ActivityLockInterestMapper;
import io.bhex.broker.server.primary.mapper.ActivityLockInterestMappingMapper;
import io.bhex.broker.server.primary.mapper.ActivityLockInterestOrderMapper;
import io.bhex.broker.server.primary.mapper.ActivityLockInterestWhiteListMapper;
import io.bhex.broker.server.primary.mapper.ActivityPurchaseLimitMapper;
import io.bhex.broker.server.primary.mapper.UserLevelConfigMapper;
import io.bhex.broker.server.primary.mapper.UserLevelMapper;
import io.bhex.broker.server.primary.mapper.UserVerifyMapper;
import io.bhex.broker.server.util.BaseReqUtil;
import io.bhex.broker.server.util.RedisLockUtils;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import tk.mybatis.mapper.entity.Example;
import tk.mybatis.mapper.util.StringUtil;

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.grpc.server.service
 * @Author: ming.xu
 * @CreateDate: 2019/6/4 2:38 PM
 * @Copyright（C）: 2019 BHEX Inc. All rights reserved.
 */
@Slf4j
@Service
public class ActivityLockInterestService {

    private final static Integer KYC_VERIFY_PASS = 2;

    private final static String LANGUAGE_ZH = "zh_CN";
    private final static String LANGUAGE_KO = "ko_KR";
    private final static String LANGUAGE_US = "en_US";
    private final static String LANGUAGE_JP = "ja_JP";
    private final static String LANGUAGE_HK = "zh_HK";

    private final static String PANIC_BUY_ORDER_TASK = "PANIC::BUY::ORDER::TASK";
    private final static String PANIC_BUY_ORDER_GIFT_TASK = "PANIC::BUY::ORDER::GIFT::TASK";

    private final static Integer ACTIVITY_TYPE_LOCK = 1;
    private final static Integer ACTIVITY_TYPE_IEO = 2;
    private final static Integer ACTIVITY_TYPE_PANIC_BUYING = 3;

    private final AccountService accountService;

    private final UserService userService;

    private final ActivityLockInterestMapper lockInterestMapper;

    private final ActivityLockInterestOrderMapper lockInterestOrderMapper;

    private final ActivityLockInterestCommonMapper lockInterestCommonMapper;

    private final ActivityLockInterestLocalMapper lockInterestLocalMapper;

    private final ActivityLockInterestGiftMapper lockInterestGiftMapper;

    private GrpcBalanceService grpcBalanceService;

    private BalanceService balanceService;

    private ActivityPurchaseLimitMapper purchaseLimitMapper;

    private UserVerifyMapper userVerifyMapper;

    @Resource(name = "stringRedisTemplate")
    private StringRedisTemplate redisTemplate;

    private ISequenceGenerator sequenceGenerator;

    private String pattern = "activity-list-%s-%s-%s-%s-%s";

    private String orgPattern = "activity-list-%s-%s-%s-%s";

    @Autowired
    private ActivityLockInterestMappingService activityLockInterestMappingService;

    @Resource
    private StatisticsBalanceService statisticsBalanceService;

    @Resource
    private ActivityLockInterestWhiteListMapper whiteListMapper;

    @Autowired
    private ApplicationContext applicationContext;

    @Autowired
    private GrpcBatchTransferService grpcBatchTransferService;

    @Resource
    ActivityLockInterestMapper activityLockInterestMapper;

    @Resource
    ActivityLockInterestCommonMapper activityLockInterestCommonMapper;

    @Resource
    private UserLevelMapper userLevelMapper;

    @Resource
    private UserLevelConfigMapper userLevelConfigMapper;

    @Resource
    private ActivityLockInterestMappingMapper activityLockInterestMappingMapper;

    @Resource
    private ActivityPurchaseLimitMapper activityPurchaseLimitMapper;

    @Autowired
    public ActivityLockInterestService(GrpcBalanceService grpcBalanceService, AccountService accountService, UserService userService, ActivityLockInterestOrderMapper lockInterestOrderMapper, ActivityLockInterestMapper lockInterestMapper, ActivityLockInterestCommonMapper lockInterestCommonMapper, ActivityLockInterestLocalMapper lockInterestLocalMapper, ActivityLockInterestGiftMapper lockInterestGiftMapper, BalanceService balanceService, ActivityPurchaseLimitMapper purchaseLimitMapper, UserVerifyMapper userVerifyMapper, ISequenceGenerator sequenceGenerator) {
        this.grpcBalanceService = grpcBalanceService;
        this.accountService = accountService;
        this.userService = userService;
        this.lockInterestMapper = lockInterestMapper;
        this.lockInterestOrderMapper = lockInterestOrderMapper;
        this.lockInterestCommonMapper = lockInterestCommonMapper;
        this.lockInterestLocalMapper = lockInterestLocalMapper;
        this.lockInterestGiftMapper = lockInterestGiftMapper;
        this.balanceService = balanceService;
        this.purchaseLimitMapper = purchaseLimitMapper;
        this.userVerifyMapper = userVerifyMapper;
        this.sequenceGenerator = sequenceGenerator;
    }

    private Boolean tryLock(Long projectId) {
        String cacheKey = String.format(BrokerServerConstants.ACTIVITY_UPDATE_LOCK_KEY, projectId);

        return Optional.ofNullable(redisTemplate
                .opsForValue()
                .setIfAbsent(cacheKey, TRUE.toString(), 30, TimeUnit.SECONDS))
                .orElse(FALSE);
    }

    private void removeLock(Long projectId) {
        String cacheKey = String.format(BrokerServerConstants.ACTIVITY_UPDATE_LOCK_KEY, projectId);
        redisTemplate.delete(cacheKey);
    }

    private BigDecimal getUserLimit(Long projectId, Long userId, BigDecimal userLimit) {
        if (userId == null || userId == 0) {
            return userLimit;
        }
        String key = String.format(BrokerServerConstants.ACTIVITY_USER_LIMIT_KEY, projectId);
        if (redisTemplate.hasKey(key)) {
            Object limitObj = redisTemplate.opsForHash().get(key, userId.toString());
            if (limitObj == null || Strings.isNullOrEmpty(limitObj.toString())) {
                return userLimit;
            }
            return new BigDecimal(limitObj.toString());
        } else {
            return userLimit;
        }
    }

    @Scheduled(cron = "3/10 * * * * ?")
    public void panicBuyOrderTransferTask() {
        Boolean locked = RedisLockUtils.tryLock(redisTemplate, PANIC_BUY_ORDER_TASK, 10 * 1000);
        if (!locked) {
            return;
        }
        Example example = new Example(ActivityLockInterestOrder.class);
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("type", 3); //1锁仓派息  2IEO 3 抢购 4 自由模式
        criteria.andEqualTo("sourceTransferStatus", 1);
        criteria.andIn("userTransferStatus", Arrays.asList(0, 2));

        List<ActivityLockInterestOrder> activityLockInterestOrderList = this.lockInterestOrderMapper.selectByExample(example);

        log.info("Panic buy order transfer task get lock now start list size {}", activityLockInterestOrderList.size());
        if (CollectionUtils.isEmpty(activityLockInterestOrderList)) {
            return;
        }
        activityLockInterestOrderList.forEach(s -> {
            try {
                panicBuyingAsynchronousTransfer(s.getId());
            } catch (Exception e) {
                log.info("panicBuyOrderTransferTask fail orderId {} error {}", s.getId(), e);
            }
        });
    }

    @Scheduled(cron = "5/50 * * * * ?")
    public void panicBuyOrderGiftTransferTask() {
        Boolean locked = RedisLockUtils.tryLock(redisTemplate, PANIC_BUY_ORDER_GIFT_TASK, 50 * 1000);
        if (!locked) {
            return;
        }
        Example example = new Example(ActivityLockInterestOrder.class);
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("type", 3); //1锁仓派息  2IEO 3 抢购 4 自由模式
        criteria.andEqualTo("sourceTransferStatus", 1);
        criteria.andEqualTo("giftOpen", 1);
        criteria.andIn("giftTransferStatus", Arrays.asList(0, 2));
        List<ActivityLockInterestOrder> activityLockInterestOrderList = this.lockInterestOrderMapper.selectByExample(example);
        log.info("Panic buy order gift transfer task get lock now start list size {}", activityLockInterestOrderList.size());
        if (CollectionUtils.isEmpty(activityLockInterestOrderList)) {
            return;
        }

        activityLockInterestOrderList.forEach(s -> {
            try {
                panicBuyingAsynchronousTransfer(s.getId());
            } catch (Exception e) {
                log.info("panicBuyOrderTransferTask fail orderId {} error {}", s.getId(), e);
            }
        });
    }

    public ListLockProjectInfoReply listProjectInfo(ListLockProjectInfoRequest request) {
        List<ListLockProjectInfoReply.ProjectInfo> infoList = new ArrayList<>();
        List<ActivityLockInterest> activityLockInterests = listProjectInfo(request.getHeader().getOrgId(), request.getProjectCode());
        log.info("listProjectInfo activityLockInterests {}", new Gson().toJson(activityLockInterests));
        List<Long> projectIds = activityLockInterests.stream().map(ActivityLockInterest::getId).collect(Collectors.toList());
        Map<Long, ActivityLockInterestLocal> localMap = lockInterestLocalMap(projectIds, request.getHeader());
        activityLockInterests.forEach(info -> {
            try {
                ListLockProjectInfoReply.ProjectInfo.Builder builder = ListLockProjectInfoReply.ProjectInfo.newBuilder();
                builder.setId(info.getId());
                builder.setBrokerId(info.getBrokerId());
                ActivityLockInterestLocal local = localMap.get(info.getId());
                if (Objects.nonNull(local)) {
                    builder.setProjectName(local.getProjectName());
                    builder.setTitle(local.getTitle());
                    builder.setDescript(local.getDescript());
                    builder.setFixedInterestRate(local.getFixedInterestRate());
                    builder.setFloatingRate(local.getFloatingRate());
                    builder.setLockedPeriod(local.getLockedPeriod());
                    builder.setCirculationStr(local.getCirculationStr());
                } else {
                    Example example = Example.builder(ActivityLockInterestLocal.class).build();
                    Example.Criteria criteria = example.createCriteria();
                    criteria.andEqualTo("language", Locale.US.toString());
                    criteria.andEqualTo("projectId", info.getId());
                    ActivityLockInterestLocal lockInterestLocal = lockInterestLocalMapper.selectOneByExample(example);
                    if (lockInterestLocal != null) {
                        builder.setProjectName(lockInterestLocal.getProjectName());
                        builder.setTitle(lockInterestLocal.getTitle());
                        builder.setDescript(lockInterestLocal.getDescript());
                        builder.setFixedInterestRate(lockInterestLocal.getFixedInterestRate());
                        builder.setFloatingRate(lockInterestLocal.getFloatingRate());
                        builder.setLockedPeriod(lockInterestLocal.getLockedPeriod());
                        builder.setCirculationStr(lockInterestLocal.getCirculationStr());
                    }
                }
                builder.setPlatformLimit(info.getPlatformLimit().toPlainString());
                builder.setMinPurchaseLimit(info.getMinPurchaseLimit().toPlainString());
                builder.setPurchaseTokenId(info.getPurchaseTokenId());
                builder.setPurchaseTokenName(info.getPurchaseTokenName());
                builder.setStartTime(info.getStartTime());
                builder.setEndTime(info.getEndTime());
                builder.setCreatedTime(info.getCreatedTime());
                builder.setUpdatedTime(info.getUpdatedTime());
                builder.setStatus(info.getStatus());
                builder.setPurchaseableQuantity(info.getPurchaseableQuantity().toPlainString());
                builder.setProjectType(info.getProjectType());
                builder.setSoldAmount(info.getSoldAmount().toPlainString());
                builder.setIsPurchaseLimit(info.getIsPurchaseLimit());
                builder.setProjectCode(info.getProjectCode());
                builder.setReceiveTokenId(info.getReceiveTokenId());
                builder.setReceiveTokenName(info.getReceiveTokenName());
                builder.setExchangeRate(info.getExchangeRate().toPlainString());
                builder.setTotalCirculation(info.getTotalCirculation().stripTrailingZeros().toPlainString());
                builder.setTotalReceiveCirculation(info.getTotalReceiveCirculation().stripTrailingZeros().toPlainString());
                builder.setValuationTokenQuantity(info.getValuationTokenQuantity().stripTrailingZeros().toPlainString());
                builder.setReceiveTokenQuantity(info.getReceiveTokenQuantity().stripTrailingZeros().toPlainString());

                builder.setDomain(StringUtils.isEmpty(info.getDomain()) ? "" : info.getDomain());
                builder.setBrowser(StringUtils.isEmpty(info.getBrowser()) ? "" : info.getBrowser());
                builder.setWhitePaper(StringUtils.isEmpty(info.getWhitePaper()) ? "" : info.getWhitePaper());
                List<String> stringList = new ArrayList<>();
                ActivityPurchaseLimit activityPurchaseLimit = this.activityPurchaseLimitMapper.getLimitById(info.getId());
                if (activityPurchaseLimit != null) {
                    builder.setVerifyKyc(activityPurchaseLimit.getVerifyKyc());
                    builder.setVerifyBalance(activityPurchaseLimit.getVerifyBalance());
                    builder.setVerifyBindPhone(activityPurchaseLimit.getVerifyBindPhone());
                    builder.setVerifyAvgBalance(activityPurchaseLimit.getVerifyAvgBalance());
                    builder.setBalanceRuleJson(activityPurchaseLimit.getBalanceRuleJson());
                    log.info("activityPurchaseLimit {}", new Gson().toJson(activityPurchaseLimit));
                    String levelLimit = activityPurchaseLimit.getLevelLimit();
                    List<String> levelConfig = Splitter.on(",").omitEmptyStrings().splitToList(levelLimit);
                    if (org.apache.commons.collections4.CollectionUtils.isNotEmpty(levelConfig)) {
                        if (StringUtils.isNotEmpty(activityPurchaseLimit.getLevelLimit())) {
                            Set<Long> levelConfigSet = new HashSet(levelConfig);
                            Example example = new Example(UserLevelConfig.class);
                            Example.Criteria criteria = example.createCriteria();
                            criteria.andEqualTo("orgId", info.getBrokerId());
                            criteria.andIn("id", levelConfigSet);
                            List<UserLevelConfig> configList = this.userLevelConfigMapper.selectByExample(example);
                            if (org.apache.commons.collections4.CollectionUtils.isNotEmpty(configList)) {
                                for (UserLevelConfig config : configList) {
                                    if (StringUtils.isEmpty(config.getLevelValue())) {
                                        continue;
                                    }
                                    JsonParser parser = new JsonParser();
                                    JsonArray jsonArray = parser.parse(config.getLevelValue()).getAsJsonArray();
                                    if (jsonArray.size() == 0) {
                                        continue;
                                    }
                                    stringList.add(jsonArray.get(0).getAsJsonObject().get("levelName").getAsString());
                                }
                            }
                        }
                    }
                    builder.setLevelLimit(org.apache.commons.collections4.CollectionUtils.isNotEmpty(stringList) ? Joiner.on(",").join(stringList).toString() : "");
                    log.info("stringList {}", Joiner.on(",").join(stringList));
                }
                // 用户可购买数额
                BigDecimal userLimit = getUserLimit(info.getId(), request.getHeader().getUserId(), info.getUserLimit());
                if (request.getHeader().getUserId() != 0) {
                    BigDecimal purchaseAmount = userPurchaseAmount(request.getHeader().getOrgId(), info.getId(), accountService.getMainAccountId(request.getHeader()));
                    userLimit = userLimit.subtract(purchaseAmount);
                    if (userLimit.compareTo(BigDecimal.ZERO) <= 0) {
                        userLimit = BigDecimal.ZERO;
                    }
                    if (userLimit.compareTo(info.getPurchaseableQuantity()) >= 0) {
                        userLimit = info.getPurchaseableQuantity();
                    }
                }
                ActivityLockInterestUserInfo activityLockInterestUserInfo
                        = this.activityLockInterestMappingService.queryUserProjectInfo(request.getHeader().getOrgId(), info.getId(), request.getHeader().getUserId());
                builder.setUserLimit(userLimit.toPlainString());
                builder.setUseAmount(activityLockInterestUserInfo.getUserAmount().setScale(4, RoundingMode.DOWN).toPlainString());
                builder.setBackAmount(activityLockInterestUserInfo.getBackAmount().setScale(4, RoundingMode.DOWN).toPlainString());
                builder.setLuckyAmount(activityLockInterestUserInfo.getLuckyAmount().setScale(4, RoundingMode.DOWN).toPlainString());
                infoList.add(builder.build());
            } catch (Exception ex) {
                log.info("listProjectInfo builder fail {}", ex.getMessage());
            }
        });
        ListLockProjectInfoReply reply = ListLockProjectInfoReply.newBuilder()
                .addAllProjectInfo(infoList)
                .build();
        return reply;
    }

    private List<ActivityLockInterest> listProjectInfo(Long brokerId, String projectCode) {
        List<ActivityLockInterest> activityLockInterests = lockInterestMapper.listProjectInfo(brokerId, projectCode);
        return activityLockInterests;
    }

    @Transactional(isolation = Isolation.READ_COMMITTED, propagation = Propagation.REQUIRED, rollbackFor = Throwable.class)
    public CreateLockInterestOrderReply createOrder(CreateLockInterestOrderRequest request) {
        long start = System.currentTimeMillis();
        Header header = request.getHeader();
        Boolean isSuper = FALSE;
        ActivityLockInterestWhiteList whiteList
                = this.whiteListMapper.queryByProjectId(header.getOrgId(), request.getProjectId());

        log.info("whiteList {} ", JSON.toJSONString(whiteList));
        if (whiteList != null && StringUtils.isNotEmpty(whiteList.getUserIdStr())) {
            HashSet<String> ids = whiteList.queryUserIds();
            if (ids.size() > 0 && ids.contains(String.valueOf(header.getUserId()))) {
                isSuper = TRUE;
            }
        }

        log.info("isSuper {} ", isSuper);
        // 校验是否有购买资格
        if (!isSuper) {
            verifyPurchaseLimit(request.getProjectId(), request.getHeader());
        }
        log.info("activity duration: verifyPurchaseLimit {} ms.", System.currentTimeMillis() - start);
        //必须购买整数份
        BigDecimal fractionalPart = new BigDecimal(request.getAmount()).remainder(BigDecimal.ONE);
        if (fractionalPart.compareTo(BigDecimal.ZERO) != 0) {
            throw new BrokerException(BrokerErrorCode.ORDER_QUANTITY_TOO_BIG);
        }
        // 购买产品
        // 1.检验产品剩余可购买总额，检验个人可购买限额，购买数量是否为最小下单的整数倍，检验余额是否充足
        ActivityLockInterest activityLockInterest = lockInterestMapper.getActivityLockInterestById(request.getProjectId());
        if (activityLockInterest == null) {
            log.error("Activity LOCK INTEREST new order error: project not exist. org id -> {}, error projectId -> {}.", request.getHeader().getOrgId(), request.getProjectId());
            throw new BrokerException(BrokerErrorCode.ORDER_FAILED);
        }
        // 校验请求的projectCode是否和记录一致，如果不一致，拒绝下单
        if (!activityLockInterest.getProjectCode().equals(request.getProjectCode())) {
            log.error("Activity LOCK INTEREST new order error: project not exist. org id -> {}, error projectId -> {}.", request.getHeader().getOrgId(), request.getProjectId());
            throw new BrokerException(BrokerErrorCode.ORDER_FAILED);
        }
        // 项目未开始或已结束 停止购买
        if (activityLockInterest.getStatus() != 1 || (activityLockInterest.getStartTime() != 0 && System.currentTimeMillis() < activityLockInterest.getStartTime())) {
            log.error("Activity LOCK INTEREST new order failed: activity not start. org id -> {}, error projectId -> {}, now time:{}, start time:{}.", request.getHeader().getOrgId(), request.getProjectId(), System.currentTimeMillis(), activityLockInterest.getStartTime());
            throw new BrokerException(BrokerErrorCode.ORDER_ACTIVITY_NOT_START);
        }
        if (activityLockInterest.getStatus() != 1 || (activityLockInterest.getEndTime() != 0 && System.currentTimeMillis() > activityLockInterest.getEndTime())) {
            log.error("Activity LOCK INTEREST new order failed: activity already end. org id -> {}, error projectId -> {}, now time:{}, end time:{}.", request.getHeader().getOrgId(), request.getProjectId(), System.currentTimeMillis(), activityLockInterest.getEndTime());
            throw new BrokerException(BrokerErrorCode.ORDER_ACTIVITY_ALREADY_END);
        }
        // 如果加锁成功，则继续下单
        Boolean lockSuccess = tryLock(activityLockInterest.getId());
        log.info("activity duration: tryLock {}.", lockSuccess);
        log.info("activity duration: tryLock {} ms.", System.currentTimeMillis() - start);
        if (lockSuccess) {
            try {
                ActivityLockInterest interestWithLock = lockInterestMapper.getByIdWithLock(request.getProjectId());
                log.info("activity duration: getByIdWithLock {} ms.", System.currentTimeMillis() - start);
                BigDecimal purchaseTokenAmount = interestWithLock.getMinPurchaseLimit().multiply(new BigDecimal(request.getAmount()));
                Long mainAccountId = accountService.getMainAccountId(request.getHeader());
                BigDecimal alreadyPurchase = userPurchaseAmount(request.getHeader().getOrgId(), request.getProjectId(), mainAccountId);
                log.info("activity duration: userPurchaseAmount {} ms.", System.currentTimeMillis() - start);
                // 判断是否为限购项目
                if (interestWithLock.getIsPurchaseLimit() == 1) {
                    //是否超过用户限额
                    if (getUserLimit(interestWithLock.getId(), request.getHeader().getUserId(), interestWithLock.getUserLimit()).compareTo(alreadyPurchase.add(purchaseTokenAmount)) < 0) {
                        log.info("Activity LOCK INTEREST new order error: user limit. userId: {}, alreadyPurchase: {}, purchaseTokenAmount -> {}, projectId: {}.", request.getHeader().getUserId(), alreadyPurchase, purchaseTokenAmount, request.getProjectId());
                        removeLock(interestWithLock.getId());
                        throw new BrokerException(BrokerErrorCode.ORDER_OVER_USER_LIMIT);
                    }
                    //是否超过平台限额
                    if (interestWithLock.getPurchaseableQuantity().compareTo(purchaseTokenAmount) < 0) {
                        log.info("Activity LOCK INTEREST new order error: platform limit. userId: {}, alreadyPurchase: {}, purchaseTokenAmount -> {}, projectId: {}.", request.getHeader().getUserId(), alreadyPurchase, purchaseTokenAmount, request.getProjectId());
                        removeLock(interestWithLock.getId());
                        throw new BrokerException(BrokerErrorCode.ORDER_OVER_PLATFORM_LIMIT);
                    }
                }

                // 2.创建订单（待支付）
                ActivityLockInterestCommon commonInfo = lockInterestCommonMapper.getCommonInfoByCode(request.getProjectCode(), request.getHeader().getOrgId());
                ActivityLockInterestOrder order = initOrder(request, mainAccountId, purchaseTokenAmount, interestWithLock.getPurchaseTokenId(), interestWithLock.getPurchaseTokenName(), commonInfo.getActivityType());
                log.info("activity duration: getCommonInfoByCode {} ms.", System.currentTimeMillis() - start);
                if (Objects.isNull(commonInfo)) {
                    log.error("Activity LOCK INTEREST new order error: project common info not exist. org id -> {}, error projectCode -> {}.", request.getHeader().getOrgId(), request.getProjectCode());
                    removeLock(interestWithLock.getId());
                    throw new BrokerException(BrokerErrorCode.ORDER_FAILED);
                }
                // 3.锁仓，成功后变更用户支付订单状态为支付成功
                if (commonInfo.getActivityType().intValue() == ACTIVITY_TYPE_LOCK ||
                        commonInfo.getActivityType().intValue() == ACTIVITY_TYPE_IEO) {
                    // 均摊  锁仓派息
                    lockOrder(request, mainAccountId, interestWithLock, purchaseTokenAmount, order);
                } else if (commonInfo.getActivityType().intValue() == ACTIVITY_TYPE_PANIC_BUYING) {
                    // 抢购
                    panicBuyingOrder(request, mainAccountId, interestWithLock, purchaseTokenAmount, order, commonInfo);
                }
                removeLock(interestWithLock.getId());
                log.info("activity duration: panicBuyingOrder {} ms.", System.currentTimeMillis() - start);
            } catch (BrokerException ex) {
                throw ex;
            } catch (Exception e) {
                log.info("Ieo activity createOrder error {}", e);
                removeLock(activityLockInterest.getId());
                throw new BrokerException(BrokerErrorCode.ORDER_ACTIVITY_SYSTEM_BUSY);
            }
        } else {
            // 加锁失败，则让用户重试
            throw new BrokerException(BrokerErrorCode.ORDER_ACTIVITY_SYSTEM_BUSY);
        }
        CreateLockInterestOrderReply reply = CreateLockInterestOrderReply.newBuilder()
                .build();
        return reply;
    }

    private void lockOrder(CreateLockInterestOrderRequest request, Long mainAccountId, ActivityLockInterest activityLockInterest, BigDecimal purchaseTokenAmount, ActivityLockInterestOrder order) {
        Boolean lockBalance = lockBalance(request.getHeader().getOrgId(), mainAccountId, request.getClientOrderId(), activityLockInterest.getPurchaseTokenId(), purchaseTokenAmount);
        if (lockBalance) {
            //变更订单状态
            log.info("Activity LOCK INTEREST new order success: orderId -> {}, purchaseTokenAmount -> {}.", order.getId(), purchaseTokenAmount);
            order.setStatus(ActivityLockInterestOrder.STATUS_PAY_SUCCESS);
            order.setLockedStatus(ActivityLockInterestOrder.LOCKED_STATUS_SUCCESS);
            order.setPurchaseTime(System.currentTimeMillis());
            Boolean isOk = lockInterestOrderMapper.updateByPrimaryKey(order) > 0;
            if (isOk) {
                log.info("Activity LOCK INTEREST new order success: update order info success orderId -> {}, purchaseTokenAmount -> {}.", order.getId(), purchaseTokenAmount);
            } else {
                log.error("Activity LOCK INTEREST new order success: update order info orderId -> {}, purchaseTokenAmount -> {}.", order.getId(), purchaseTokenAmount);
            }
            // 4.购买成功后 减除产品可购买限额 增加已售出数额
            updateSoldAmount(activityLockInterest, purchaseTokenAmount);
        } else {
            order.setStatus(ActivityLockInterestOrder.STATUS_PAY_FAILED);
            order.setLockedStatus(ActivityLockInterestOrder.LOCKED_STATUS_FAILED);
            Boolean isOk = lockInterestOrderMapper.updateByPrimaryKey(order) > 0;
            log.error("Activity LOCK INTEREST new order: lock balance error orderId -> {}, purchaseTokenAmount -> {}, update order status : {}.", order.getId(), purchaseTokenAmount, isOk);
            removeLock(activityLockInterest.getId());
            throw new BrokerException(BrokerErrorCode.INSUFFICIENT_BALANCE);
        }
    }

    /**
     * 抢购模式下单
     */
    private void panicBuyingOrder(CreateLockInterestOrderRequest request, Long mainAccountId, ActivityLockInterest activityLockInterest, BigDecimal purchaseTokenAmount, ActivityLockInterestOrder order, ActivityLockInterestCommon commonInfo) {
        Long orgId = request.getHeader().getOrgId();
        Long toAccountId = mainAccountId;
        Long sourceAccountId = accountService.getAccountId(orgId, commonInfo.getAssetUserId());
        String sourceTokenId = activityLockInterest.getPurchaseTokenId();
        String toTokenId = activityLockInterest.getReceiveTokenId();
        String sourceAmount = order.getAmount().setScale(ProtoConstants.PRECISION, RoundingMode.HALF_UP).toPlainString();
        BigDecimal receiveTokenAmount = order.getAmount().divide(activityLockInterest.getValuationTokenQuantity(), ProtoConstants.PRECISION, RoundingMode.HALF_UP).multiply(activityLockInterest.getReceiveTokenQuantity());
        String toAmount = receiveTokenAmount.setScale(ProtoConstants.PRECISION, RoundingMode.HALF_UP).toPlainString();
        BigDecimal giftAmount = BigDecimal.ZERO;
        String giftTokenId = "";
        Integer isLock = 0;
        Boolean haveGift = activityLockInterest.getHaveGift() == 1;

        // 是否有赠币，如有则计算赠币数量
        if (haveGift) {
            ActivityLockInterestGift giftInfo = getGiftInfo(activityLockInterest.getId(), orgId);
            if (Objects.nonNull(giftInfo)) {
                isLock = giftInfo.getGiftToLock();
                giftTokenId = giftInfo.getGiftTokenId();
                giftAmount = receiveTokenAmount.divide(giftInfo.getReceiveTokenQuantity(), ProtoConstants.PRECISION, RoundingMode.HALF_UP).multiply(giftInfo.getGiftTokenQuantity());
                log.info("Activity panic buying new order: haveGift. projectId : {}, isLock : {}, gift tokenId: {}, lock amount : {}.", activityLockInterest.getId(), isLock, giftTokenId, giftAmount.toPlainString());
            }
        }

        ActivityTransfer activityTransfer = ActivityTransfer.builder()
                .orgId(orgId)
                .toAccountId(toAccountId)
                .sourceAccountId(sourceAccountId)
                .toTokenId(toTokenId)
                .sourceTokenId(sourceTokenId)
                .toAmount(toAmount)
                .sourceAmount(sourceAmount)
                .lockAmount(giftAmount.setScale(ProtoConstants.PRECISION, RoundingMode.HALF_UP).toPlainString())
                .lockTokenId(giftTokenId)
                .isLock(isLock)
                .haveGift(haveGift)
                .userClientOrderId(order.getUserClientOrderId())
                .ieoAccountClientOrderId(order.getIeoAccountClientOrderId())
                .giftClientOrderId(order.getGiftClientOrderId())
                .build();
        ActivityTransferResult transferResult = balanceService.doActivityBuyAndAirdropAndLock(activityTransfer);
        if (transferResult.getResult()) {
            //变更订单状态
            log.info("Activity panic buying new order success: orderId -> {}, purchaseTokenAmount -> {}.", order.getId(), purchaseTokenAmount);
            order.setStatus(ActivityLockInterestOrder.STATUS_PAY_SUCCESS);
            order.setLockedStatus(ActivityLockInterestOrder.LOCKED_STATUS_SUCCESS);
            order.setGiftAmount(giftAmount);
            order.setGiftTokenId(giftTokenId);
            order.setPurchaseTime(System.currentTimeMillis());

            //用户抢购获得资产
            order.setUserAmount(new BigDecimal(toAmount));
            order.setUserTokenId(toTokenId);
            //运营账户获得资产
            order.setSourceAmount(new BigDecimal(sourceAmount));
            order.setSourceTransferStatus(1);
            order.setSourceTokenId(sourceTokenId);
            order.setSourceAccountId(sourceAccountId);
            //赠送资产
            order.setGiftOpen(haveGift ? 1 : 0);
            order.setGiftAmount(giftAmount.setScale(ProtoConstants.PRECISION, RoundingMode.HALF_UP));
            order.setGiftTokenId(giftTokenId);
            order.setIsLock(isLock);

            Boolean isOk = lockInterestOrderMapper.updateByPrimaryKey(order) > 0;
            log.info("Activity panic buying new order success: update order info orderId -> {}, purchaseTokenAmount -> {}, update order status : {}.", order.getId(), purchaseTokenAmount, isOk);
            // 购买成功后 减除产品可购买限额 增加已售出数额
            updateSoldAmount(activityLockInterest, purchaseTokenAmount);
            //异步处理成功抢购的转账
            applicationContext.publishEvent(IEOPanicBuyEvent.builder().id(order.getId()).build());
        } else {
            order.setStatus(ActivityLockInterestOrder.STATUS_PAY_FAILED);
            order.setLockedStatus(ActivityLockInterestOrder.LOCKED_STATUS_FAILED);
            Boolean isOk = lockInterestOrderMapper.updateByPrimaryKey(order) > 0;
            log.error("Activity panic buying new order error: haveGift. projectId : {}, accountId : {}, purchaseTokenId: {}, purchaseTokenAmount : {}. update order status : {}.", activityLockInterest.getId(), mainAccountId, activityLockInterest.getPurchaseTokenId(), order.getAmount(), isOk);
            removeLock(activityLockInterest.getId());
            throw new BrokerException(BrokerErrorCode.INSUFFICIENT_BALANCE);
        }
    }

    private void updateSoldAmount(ActivityLockInterest activityLockInterest, BigDecimal purchaseTokenAmount) {
        // 1 减除产品可购买限额
        activityLockInterest.setPurchaseableQuantity(activityLockInterest.getPurchaseableQuantity().subtract(purchaseTokenAmount));
        // 2 增加已售出数额
        activityLockInterest.setSoldAmount(activityLockInterest.getSoldAmount().add(purchaseTokenAmount));
        activityLockInterest.setRealSoldAmount(activityLockInterest.getRealSoldAmount().add(purchaseTokenAmount));
        // 3 更新
//        updateProjectInfoWithCache(activityLockInterest);
        lockInterestMapper.updateByPrimaryKey(activityLockInterest);
    }

    private ActivityLockInterestGift getGiftInfo(Long projectId, Long brokerId) {
        return lockInterestGiftMapper.getGiftInfoByProjectId(projectId, brokerId);
    }

    public String activityExpiredUnlock(Long brokerId, Long projectId) {
        ActivityLockInterest lockInterest = lockInterestMapper.getActivityLockInterestById(projectId);
        if (Objects.nonNull(lockInterest) && brokerId.equals(lockInterest.getBrokerId())) {
            if (System.currentTimeMillis() < lockInterest.getEndTime()) {
                log.error("Activity not end.");
                return "Activity not end.";
            } else {
                //获取购买成功活动订单信息
                List<ActivityLockInterestOrder> activityLockInterestOrders = lockInterestOrderMapper.queryOrderIdListByStatus(projectId, 1);
                activityLockInterestOrders.forEach(order -> {
                    if (order.getLockedStatus() == ActivityLockInterestOrder.LOCKED_STATUS_SUCCESS) {
                        Boolean unlockSuccess = unLockBalance(order.getBrokerId(), order.getAccountId(), order.getClientOrderId(), order.getTokenId(), order.getAmount());
                        log.info("Activity Unlock: status: {}, brokerId: {}, accountId: {}, clientOrderId: {}, tokenId: {}, lockAmount: {}", unlockSuccess, order.getBrokerId(), order.getAccountId(), order.getClientOrderId(), order.getTokenId(), order.getAmount());
                        if (unlockSuccess) {
                            order.setLockedStatus(ActivityLockInterestOrder.LOCKED_STATUS_UNLOCKED);
                            order.setUpdatedTime(System.currentTimeMillis());
                            Boolean updateSuccess = lockInterestOrderMapper.updateByPrimaryKey(order) > 0 ? true : false;
                            if (!updateSuccess) {
                                log.error("Activity Unlock Success, update order status error: brokerId: {}, accountId: {}, clientOrderId: {}, tokenId: {}, lockAmount: {}", order.getBrokerId(), order.getAccountId(), order.getClientOrderId(), order.getTokenId(), order.getAmount());
                            }
                        }
                    }
                });
                return "success";
            }
        }
        log.error("Activity not exist.");
        return "Activity not exist.";
    }

    public void verifyPurchaseLimit(Long projectId, Header header) {
        ActivityPurchaseLimit purchaseLimit = purchaseLimitMapper.getLimitById(projectId);
        if (Objects.isNull(purchaseLimit)) {
            return;
        }
        if (Objects.nonNull(purchaseLimit.getVerifyKyc()) && purchaseLimit.getVerifyKyc().equals(1)) {
            // kyc检验
            UserVerify dbUserVerify = userVerifyMapper.getByUserId(header.getUserId());
            if (dbUserVerify != null) {
                if (!dbUserVerify.isSeniorVerifyPassed()) {
                    throw new BrokerException(BrokerErrorCode.ACTIVITY_FAIL_IN_KYC_LEVEL_REQUIREMENT);
                }
            } else {
                throw new BrokerException(BrokerErrorCode.ACTIVITY_FAIL_IN_KYC_LEVEL_REQUIREMENT);
            }
        }

        if (Objects.nonNull(purchaseLimit.getVerifyBindPhone()) && purchaseLimit.getVerifyBindPhone().equals(1)) {
            // 校验是否绑定手机
            User user = userService.getUser(header.getUserId());
            if (user != null) {
                if (StringUtils.isEmpty(user.getMobile())) {
                    throw new BrokerException(BrokerErrorCode.ACTIVITY_FAIL_IN_PHONE_BINDING_REQUIREMENT);
                }
            } else {
                throw new BrokerException(BrokerErrorCode.ACTIVITY_FAIL_IN_PHONE_BINDING_REQUIREMENT);
            }
        }
        if (Objects.nonNull(purchaseLimit.getVerifyBalance()) && purchaseLimit.getVerifyBalance().equals(1) && Objects.nonNull(purchaseLimit.getBalanceRuleJson())) {
            // 如果有持仓校验，需要检验是否在白名单内
//            String key = String.format(BrokerServerConstants.ACTIVITY_WHITE_LIST, projectId.toString());
//            if (redisTemplate.hasKey(key)) {
//                Boolean isInWhiteList = redisTemplate.opsForSet().isMember(key, header.getUserId() + "");
//                if (isInWhiteList) {
//                    return;
//                }
//            }
            // 持仓校验
            String json = purchaseLimit.getBalanceRuleJson();
            if (StringUtils.isNotEmpty(json)) {
                Map<String, String> map = new HashMap<>();
                try {
                    map = JsonUtil.defaultGson().fromJson(json, Map.class);
                } catch (JsonSyntaxException e) {
                    log.error("verifyPurchaseLimit error: Balance Rule Json=> {}, projectId=> {}", json, projectId);
                }
                String quantity = map.get("positionVolume") != null ? map.get("positionVolume") : "";
                String token = map.get("positionToken") != null ? map.get("positionToken") : "";
                if (StringUtils.isEmpty(token) || StringUtils.isEmpty(quantity)) {
                    return;
                }

                Long accountId = accountService.getMainAccountId(header);
                List<BalanceDetail> balanceDetails = getBalance(accountId, Collections.singletonList(token), header.getOrgId());
                if (balanceDetails.size() == 0) {
                    throw new BrokerException(BrokerErrorCode.ACTIVITY_FAIL_IN_TOKEN_HOLDING_REQUIREMENT);
                }
                for (BalanceDetail balance : balanceDetails) {
                    BigDecimal total = DecimalUtil.toBigDecimal(balance.getTotal());
                    String limit = quantity;
                    if (StringUtils.isNotEmpty(limit)) {
                        BigDecimal l = new BigDecimal(limit);
                        if (total.compareTo(l) == -1) {
                            log.info("verifyPurchaseLimit: Insufficient assets. accoutId: {}, tokenId: {}, need: {}, own: {}, projectId: {}", accountId, balance.getTokenId(), l.toPlainString(), balance.getTotal(), projectId);
                        } else {
                            return;
                        }
                    }
                }
                throw new BrokerException(BrokerErrorCode.ACTIVITY_FAIL_IN_TOKEN_HOLDING_REQUIREMENT);
            }
        }

        //平均持仓校验
        verifyAvgBalance(purchaseLimit, projectId, header.getOrgId(), header.getUserId());

        //VIP等级限制
        verifyLevelLimit(purchaseLimit, projectId, header.getOrgId(), header.getUserId());
    }

    private void verifyLevelLimit(ActivityPurchaseLimit purchaseLimit, Long projectId, Long orgId, Long userId) {
        if (purchaseLimit == null || StringUtils.isEmpty(purchaseLimit.getLevelLimit())) {
            return;
        }
        String levelLimit = purchaseLimit.getLevelLimit();
        List<String> levelConfig = Splitter.on(",").omitEmptyStrings().splitToList(levelLimit);
        if (CollectionUtils.isEmpty(levelConfig)) {
            return;
        }
        Set<Long> levelConfigSet = new HashSet(levelConfig);
        log.info("verifyLevelLimit levelConfigSet info {}", new Gson().toJson(levelConfigSet));
        //获取到当前用户的所有level config id
        List<UserLevel> userLevelList = this.userLevelMapper.queryMyAvailableConfigs(orgId, userId);
        List<Long> userConfigList = userLevelList.stream().map(s -> s.getLevelConfigId()).collect(Collectors.toList());
        log.info("verifyLevelLimit userConfigList info {}", new Gson().toJson(userConfigList));
        Boolean limit = TRUE;
        for (int i = 0; i < userConfigList.size(); i++) {
            if (levelConfigSet.contains(String.valueOf(userConfigList.get(i)))) {
                limit = FALSE;
                break;
            }
        }
        if (limit) {
            throw new BrokerException(BrokerErrorCode.ACTIVITY_FAIL_IN_USER_LEVEL_REQUIREMENT);
        }
    }

    public void verifyAvgBalance(ActivityPurchaseLimit purchaseLimit, Long projectId, Long orgId, Long userId) {
        if (Objects.nonNull(purchaseLimit.getVerifyAvgBalance()) && purchaseLimit.getVerifyAvgBalance().equals(1) && Objects.nonNull(purchaseLimit.getBalanceRuleJson())) {
            String json = purchaseLimit.getBalanceRuleJson();
            if (StringUtils.isNotEmpty(json)) {
                Map<String, String> map = new HashMap<>();
                try {
                    map = JsonUtil.defaultGson().fromJson(json, Map.class);
                } catch (JsonSyntaxException e) {
                    log.error("verifyPurchaseLimit error: Balance Rule Json=> {}, projectId=> {}", json, projectId);
                }
                String verifyAvgBalanceToken = map.get(ActivityConstant.VERIFY_AVG_BALANCE_TOKEN);
                String verifyAvgBalanceVolume = map.get(ActivityConstant.VERIFY_AVG_BALANCE_VOLUME);
                String verifyAvgBalanceStartTime = map.get(ActivityConstant.VERIFY_AVG_BALANCE_START_TIME);
                String verifyAvgBalanceEndTime = map.get(ActivityConstant.VERIFY_AVG_BALANCE_END_TIME);
                if (StringUtil.isEmpty(verifyAvgBalanceToken) || StringUtils.isEmpty(verifyAvgBalanceVolume) || StringUtils.isEmpty(verifyAvgBalanceStartTime) || StringUtils.isEmpty(verifyAvgBalanceEndTime)) {
                    return;
                }
                Long accountId = accountService.getAccountId(orgId, userId);
                if (accountId == null || accountId.equals(0)) {
                    return;
                }
                //计算时间内持仓总量 除以天数 跟 持仓数币对 如果>持仓数 则允许购买 小于 则直接抛异常
                LocalDateTime startTime = LocalDateTime.ofInstant(new Date(Long.parseLong(verifyAvgBalanceStartTime)).toInstant(), ZoneId.of("UTC"));
                LocalDateTime endTime = LocalDateTime.ofInstant(new Date(Long.parseLong(verifyAvgBalanceEndTime)).toInstant(), ZoneId.of("UTC"));
                String startTimeStr = startTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
                String endTimeStr = endTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
                long day = Duration.between(startTime, endTime).toDays();
                if (day < 0) {
                    return;
                }
                Set<String> dateSet = new HashSet<>();
                for (int i = 0; i <= day; i++) {
                    dateSet.add(new DateTime(startTimeStr).plusDays(i).toString("yyyy-MM-dd"));
                }
                dateSet.add(endTimeStr);
                dateSet.add(startTimeStr);
                if (CollectionUtils.isEmpty(dateSet)) {
                    return;
                }
                BigDecimal total = statisticsBalanceService.queryTokenBalanceSnapshotCountByAccountId(orgId, accountId, verifyAvgBalanceToken, dateSet);
                BigDecimal totalValue = total.divide(new BigDecimal(dateSet.size()), 18, RoundingMode.DOWN).setScale(8, RoundingMode.DOWN);
                log.info("verifyAvgBalance userId {} accountId {} projectId {} startTimeStr {} endTimeStr {} total {} day {} totalValue {} verifyAvgBalanceVolume {} dateSet {} ", userId,
                        accountId, projectId, startTimeStr, endTimeStr, total.toPlainString(), dateSet.size(), totalValue.toPlainString(), verifyAvgBalanceVolume, new Gson().toJson(dateSet));
                if (totalValue.compareTo(new BigDecimal(verifyAvgBalanceVolume)) < 0) {
                    throw new BrokerException(BrokerErrorCode.ACTIVITY_FAIL_IN_AVERAGE_TOKEN_HOLDING_REQUIREMENT);
                }
            }
        }
    }

    public static void main(String[] args) {
        //计算时间内持仓总量 除以天数 跟 持仓数币对 如果>持仓数 则允许购买 小于 则直接抛异常
        LocalDateTime startTime = LocalDateTime.ofInstant(new Date(1602000000000L).toInstant(), ZoneId.of("UTC"));
        LocalDateTime endTime = LocalDateTime.ofInstant(new Date(1602777600000L).toInstant(), ZoneId.of("UTC"));
        String startTimeStr = startTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        String endTimeStr = endTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));

        System.out.println("starttime :" + startTimeStr);
        System.out.println("endTimeStr :" + endTimeStr);

        long day = Duration.between(startTime, endTime).toDays();
        System.out.println("day :" + day);

        Set<String> stringSet = new HashSet<>();
        for (int i = 0; i <= day; i++) {
            stringSet.add(new DateTime(startTimeStr).plusDays(i).toString("yyyy-MM-dd"));
            System.out.println(new DateTime(startTimeStr).plusDays(i).toString("yyyy-MM-dd"));
        }
        stringSet.add(endTimeStr);
        stringSet.add(startTimeStr);
        System.out.println(new Gson().toJson(stringSet));
        System.out.println("stringSet size:" + stringSet.size());

        System.out.println(new DateTime("2020-07-17").plusDays(30).toString("yyyy-MM-dd"));
    }

    private List<BalanceDetail> getBalance(Long accountId, Iterable<String> tokenIds, Long orgId) {
        GetBalanceDetailRequest request = GetBalanceDetailRequest.newBuilder()
                .setBaseRequest(BaseReqUtil.getBaseRequest(orgId))
                .setAccountId(accountId)
                .addAllTokenId(tokenIds)
                .build();

        BalanceDetailList balanceDetails = grpcBalanceService.getBalanceDetail(request);
        return balanceDetails.getBalanceDetailsList();
    }

    private Boolean lockBalance(Long brokerId, Long accountId, Long clientOrderId, String tokenId, BigDecimal lockAmount) {
        BaseRequest baseRequest = BaseRequest.newBuilder()
                .setOrganizationId(brokerId)
                .build();

        LockBalanceRequest request = LockBalanceRequest.newBuilder()
                .setBaseRequest(baseRequest)
                .setClientReqId(clientOrderId)
                .setAccountId(accountId)
                .setTokenId(tokenId)
                .setLockAmount(lockAmount.setScale(ProtoConstants.PRECISION, RoundingMode.DOWN).toPlainString())
                .build();

        LockBalanceReply reply = grpcBalanceService.lockBalance(request);
        if (reply.getCode() == LockBalanceReply.ReplyCode.SUCCESS) {
            return true;
        } else {
            log.error("Activity LOCK INTEREST new order error: lock balance error. error code: {}.", reply.getCodeValue());
            return false;
        }
    }

    private Boolean unLockBalance(Long brokerId, Long accountId, Long clientOrderId, String tokenId, BigDecimal lockAmount) {
        BaseRequest baseRequest = BaseRequest.newBuilder()
                .setOrganizationId(brokerId)
                .build();

        UnlockBalanceRequest request = UnlockBalanceRequest.newBuilder()
                .setBaseRequest(baseRequest)
                .setClientReqId(clientOrderId)
                .setAccountId(accountId)
                .setTokenId(tokenId)
                .setUnlockAmount(lockAmount.setScale(ProtoConstants.PRECISION, RoundingMode.DOWN).toPlainString())
                .build();

        UnlockBalanceResponse reply = grpcBalanceService.unLockBalance(request);
        if (reply.getCode() == UnlockBalanceResponse.ReplyCode.SUCCESS) {
            return true;
        } else {
            log.error("Activity LOCK INTEREST: unlock balance error. brokerId: {}, clientOrderId: {}, error code: {}.", brokerId, clientOrderId, reply.getCodeValue());
            return false;
        }
    }

    /**
     * 订单中生成的userClientOrderId，ieoAccountClientOrderId，giftClientOrderId三个数值仅限于在抢购模式下使用<br>
     * 均摊模式只有锁仓，锁仓使用的clientOrderId是从前端传入的，一路带到了bh<br> 锁仓派息也只有锁仓，使用的也是前端传入的clientOrderId
     */
    private ActivityLockInterestOrder initOrder(CreateLockInterestOrderRequest request, Long accountId, BigDecimal tokenAmount, String tokenId, String tokenName, Integer type) {
        ActivityLockInterestOrder order = new ActivityLockInterestOrder();
        order.setId(sequenceGenerator.getLong());
        order.setBrokerId(request.getHeader().getOrgId());
        order.setClientOrderId(request.getClientOrderId()); // --
        order.setAccountId(accountId);
        order.setUserId(request.getHeader().getUserId());
        order.setProjectId(request.getProjectId());
        order.setProjectCode(request.getProjectCode());
        order.setAmount(tokenAmount);
        order.setTokenId(tokenId);
        order.setTokenName(tokenName);
        order.setStatus(ActivityLockInterestOrder.STATUS_INIT);
        order.setLockedStatus(ActivityLockInterestOrder.LOCKED_STATUS_INIT);
        order.setPurchaseTime(0L);
        order.setGiftAmount(BigDecimal.ZERO);
        order.setGiftTokenId(StringUtils.EMPTY);
        order.setCreatedTime(System.currentTimeMillis());
        order.setUpdatedTime(System.currentTimeMillis());
        order.setUserClientOrderId(sequenceGenerator.getLong()); // 抢购模式：运营账户转账到用户的transferId
        order.setIeoAccountClientOrderId(sequenceGenerator.getLong()); // 抢购模式：用户发起购买的transferId
        order.setGiftClientOrderId(sequenceGenerator.getLong()); // 抢购模式：赠送用户的转账transferId(仅使用于有赠送的活动)


        order.setType(type);//1锁仓派息  2IEO 3 抢购 4 自由模式
        order.setUserAmount(BigDecimal.ZERO);
        order.setUserTokenId("");
        order.setUserTransferStatus(0);

        order.setSourceAmount(BigDecimal.ZERO);
        order.setSourceTransferStatus(0);
        order.setSourceAccountId(0l);
        order.setSourceTokenId("");

        order.setGiftOpen(0);
        order.setGiftAmount(BigDecimal.ZERO);
        order.setGiftTokenId("");
        order.setGiftTransferStatus(0);
        order.setIsLock(0);

        boolean isOk = lockInterestOrderMapper.insert(order) > 0;
        if (isOk) {
            return order;
        } else {
            throw new BrokerException(BrokerErrorCode.ORDER_FAILED);
        }
    }

    private BigDecimal userPurchaseAmount(Long brokerId, Long projectId, Long accountId) {
        List<ActivityLockInterestOrder> activityLockInterestOrders = listOrderInfo(brokerId, projectId, null, accountId);
        BigDecimal amount = BigDecimal.ZERO;
        for (ActivityLockInterestOrder order : activityLockInterestOrders) {
            amount = amount.add(order.getAmount());
        }
        return amount;
    }

    public ListLockOrderInfoReply listOrderInfo(ListLockOrderInfoRequest request) {
        ListLockOrderInfoReply.Builder builder = ListLockOrderInfoReply.newBuilder();
        List<ActivityLockInterestOrder> activityLockInterestOrders = listOrderInfo(request.getHeader().getOrgId(), null, request.getProjectCode(), accountService.getMainAccountId(request.getHeader()));
        if (!CollectionUtils.isEmpty(activityLockInterestOrders)) {
            List<ListLockOrderInfoReply.LockInterestOrderInfo> infos = new ArrayList<>();
            List<Long> projectIds = activityLockInterestOrders.stream().map(ActivityLockInterestOrder::getProjectId).collect(Collectors.toList());
            Map<Long, ActivityLockInterestLocal> localMap = lockInterestLocalMap(projectIds, request.getHeader());

            for (ActivityLockInterestOrder order : activityLockInterestOrders) {
                ListLockOrderInfoReply.LockInterestOrderInfo.Builder b = ListLockOrderInfoReply.LockInterestOrderInfo.newBuilder();
                b.setId(order.getId());
                b.setAccountId(order.getAccountId());
                b.setTokenId(order.getTokenId());
                b.setTokenName(order.getTokenName());
                b.setProjectId(order.getProjectId());
                b.setPurchaseTime(order.getPurchaseTime());
                b.setAmount(order.getAmount().toPlainString());

                ActivityLockInterestLocal local = localMap.get(order.getProjectId());
                if (Objects.nonNull(local)) {
                    b.setProjectName(local.getProjectName());
                } else {
                    Example example = Example.builder(ActivityLockInterestLocal.class).build();
                    Example.Criteria criteria = example.createCriteria();
                    criteria.andEqualTo("language", Locale.US.toString());
                    criteria.andEqualTo("projectId", order.getId());
                    ActivityLockInterestLocal lockInterestLocal = lockInterestLocalMapper.selectOneByExample(example);
                    if (lockInterestLocal != null) {
                        b.setProjectName(lockInterestLocal.getProjectName());
                    } else {
                        b.setProjectName("");
                    }
                }
                infos.add(b.build());
            }
            builder.addAllOrderInfo(infos);

        }
        return builder.build();
    }

    public Map<Long, ActivityLockInterestLocal> lockInterestLocalMap(List<Long> projectIds, Header header) {
        String language = getLanguage(header);
        if (CollectionUtils.isEmpty(projectIds)) {
            return new HashMap<>();
        }
        Example example = Example.builder(ActivityLockInterestLocal.class).build();
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("language", language);
        criteria.andIn("projectId", projectIds);
        List<ActivityLockInterestLocal> lockInterestLocals = lockInterestLocalMapper.selectByExample(example);
        Map<Long, ActivityLockInterestLocal> result = lockInterestLocals.stream().collect(Collectors.toMap(ActivityLockInterestLocal::getProjectId, Function.identity()));
        return result;
    }

    public List<ActivityLockInterestOrder> listOrderInfo(Long brokerId, Long projectId, String projectCode, Long accountId) {
        Example example = Example.builder(ActivityLockInterestOrder.class).build();
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("brokerId", brokerId);
        if (Objects.nonNull(projectId) && projectId != 0L) {
            criteria.andEqualTo("projectId", projectId);
        }
        if (!StringUtils.isEmpty(projectCode)) {
            criteria.andEqualTo("projectCode", projectCode);
        }
        criteria.andEqualTo("accountId", accountId);
        criteria.andEqualTo("status", ActivityLockInterestOrder.STATUS_PAY_SUCCESS);
        List<ActivityLockInterestOrder> activityLockInterestOrders = lockInterestOrderMapper.selectByExample(example);
        return activityLockInterestOrders;
    }

    private String getLanguage(Header header) {
        if (LANGUAGE_ZH.equals(header.getLanguage())) {
            return LANGUAGE_ZH;
        } else if (LANGUAGE_KO.equals(header.getLanguage())) {
            return LANGUAGE_KO;
        } else if (LANGUAGE_JP.equals(header.getLanguage())) {
            return LANGUAGE_JP;
        } else if (LANGUAGE_HK.equals(header.getLanguage())) {
            return LANGUAGE_HK;
        } else {
            return LANGUAGE_US;
        }
    }

    public ProjectCommonInfoReply projectCommonInfo(ProjectCommonInfoRequest request) {
        ActivityLockInterestCommon commonInfo = lockInterestCommonMapper.getCommonInfo(request.getProjectCode(), request.getHeader().getLanguage(), request.getHeader().getOrgId());
        if (commonInfo == null && !request.getHeader().getLanguage().equals(Locale.US.toString())) {
            commonInfo = lockInterestCommonMapper.getCommonInfo(request.getProjectCode(), Locale.US.toString(), request.getHeader().getOrgId());
        }
        ProjectCommonInfoReply.Builder builder = ProjectCommonInfoReply.newBuilder();
        Integer progressStatus = 0;
        if (Objects.nonNull(commonInfo)) {
            if (commonInfo.getActivityType().equals(ACTIVITY_TYPE_IEO)) {
                progressStatus = getIEOProgress(commonInfo);
            } else if (commonInfo.getActivityType().equals(ACTIVITY_TYPE_PANIC_BUYING)) {
                progressStatus = getPanicBuyingProgress(commonInfo);
            }

            builder.setId(commonInfo.getId())
                    .setBrokerId(commonInfo.getBrokerId())
                    .setProjectCode(commonInfo.getProjectCode())
                    .setBannerUrl(commonInfo.getBannerUrl())
                    .setDescription(commonInfo.getDescriptionHtmlSafe())
                    .setWechatUrl(commonInfo.getWechatUrl())
                    .setBlockBrowser(commonInfo.getBlockBrowser())
                    .setEndTime(commonInfo.getEndTime())
                    .setBrowserTitle(commonInfo.getBrowserTitle())
                    .setStatus(commonInfo.getStatus())
                    .setOnlineTime(commonInfo.getOnlineTime())
                    .setResultTime(commonInfo.getResultTime())
                    .setProgressStatus(progressStatus)
                    .setActivityType(commonInfo.getActivityType())
                    .setIntroduction(commonInfo.getIntroduction())
                    .setStartTime(commonInfo.getStartTime())
                    .setAbout(commonInfo.getAbout() != null ? commonInfo.getAbout() : "")
                    .setRule(commonInfo.getRule() != null ? commonInfo.getRule() : "");
        }
        return builder.build();
    }

    public QueryAllActivityOrderInfoReply queryAllActivityOrderInfo(QueryAllActivityOrderInfoRequest request) {
        QueryAllActivityOrderInfoReply.Builder builder = QueryAllActivityOrderInfoReply.newBuilder();
        List<ActivityLockInterestOrder> activityLockInterestOrders = queryAllOrder(accountService.getMainAccountId(request.getHeader()), request.getHeader().getOrgId(), request.getFromId(), request.getEndId(), request.getLimit());
        if (!CollectionUtils.isEmpty(activityLockInterestOrders)) {
            List<QueryAllActivityOrderInfoReply.LockInterestOrderInfo> infos = new ArrayList<>();
            List<Long> projectIds = activityLockInterestOrders.stream().map(ActivityLockInterestOrder::getProjectId).collect(Collectors.toList());
            Map<Long, ActivityLockInterestLocal> localMap = lockInterestLocalMap(projectIds, request.getHeader());
            List<ActivityLockInterest> projectInfoList = listProjectInfoByIds(request.getHeader().getOrgId(), projectIds);
            Map<Long, ActivityLockInterest> projectInfoMap = projectInfoList.stream().collect(Collectors.toMap(ActivityLockInterest::getId, Function.identity()));


            for (ActivityLockInterestOrder order : activityLockInterestOrders) {
                Boolean isExist = projectInfoMap.containsKey(order.getProjectId());
                if (!isExist) {
                    continue;
                }
                QueryAllActivityOrderInfoReply.LockInterestOrderInfo.Builder b = QueryAllActivityOrderInfoReply.LockInterestOrderInfo.newBuilder();
                b.setOrderId(order.getId());
                b.setAmount(order.getAmount().toPlainString());
                b.setPurchaseToken(order.getTokenId());
                b.setPurchaseTokenName(order.getTokenName());
                b.setPrice(projectInfoMap.get(order.getProjectId()).getMinPurchaseLimit().toPlainString());
                b.setOrderQuantity(order.getAmount().divide(projectInfoMap.get(order.getProjectId()).getMinPurchaseLimit()).toPlainString());
                b.setPurchaseTime(order.getPurchaseTime());
                ActivityLockInterestLocal local = localMap.get(order.getProjectId());
                if (Objects.nonNull(local)) {
                    b.setProjectName(local.getProjectName());
                } else {
                    Example example = Example.builder(ActivityLockInterestLocal.class).build();
                    Example.Criteria criteria = example.createCriteria();
                    criteria.andEqualTo("language", Locale.US.toString());
                    criteria.andEqualTo("projectId", order.getId());
                    ActivityLockInterestLocal lockInterestLocal = lockInterestLocalMapper.selectOneByExample(example);
                    if (lockInterestLocal != null) {
                        b.setProjectName(lockInterestLocal.getProjectName());
                    } else {
                        b.setProjectName("");
                    }
                }
                infos.add(b.build());
            }
            builder.addAllOrderInfo(infos);

        }
        return builder.build();
    }

    public OrgQueryActivityOrderInfoReply orgQueryActivityOrderInfo(OrgQueryActivityOrderInfoRequest request) {
        boolean orderDesc = true;
        if (request.getFromId() == 0 && request.getEndId() > 0) {
            orderDesc = false;
        }
        List<ActivityLockInterestOrder> activityLockInterestOrders = orgQueryOrder(request.getHeader().getOrgId(), request.getProjectCode(), request.getFromId(), request.getEndId(), request.getLimit(), orderDesc);

        if (CollectionUtils.isEmpty(activityLockInterestOrders)) {
            return OrgQueryActivityOrderInfoReply.newBuilder().addAllOrderInfo(new ArrayList<>()).build();
        }
        if (!orderDesc) {
            Collections.reverse(activityLockInterestOrders);
        }

        OrgQueryActivityOrderInfoReply.Builder builder = OrgQueryActivityOrderInfoReply.newBuilder();

        if (!CollectionUtils.isEmpty(activityLockInterestOrders)) {

            List<OrgQueryActivityOrderInfoReply.OrgLockInterestOrderInfo> infos = new ArrayList<>();
            ActivityLockInterest activityLockInterest = activityLockInterestMapper.getByCodeNoLock(request.getHeader().getOrgId(), request.getProjectCode());
            ActivityLockInterestCommon interestCommon = this.activityLockInterestCommonMapper.getCommonInfoByCode(activityLockInterest.getProjectCode(), activityLockInterest.getBrokerId());
            if (activityLockInterest == null || interestCommon == null) {
                return builder.build();
            }

            for (ActivityLockInterestOrder order : activityLockInterestOrders) {
                OrgQueryActivityOrderInfoReply.OrgLockInterestOrderInfo.Builder b = OrgQueryActivityOrderInfoReply.OrgLockInterestOrderInfo.newBuilder();

                b.setProjectCode(order.getProjectCode());
                b.setOrderId(order.getId());
                b.setClientOrderId(order.getClientOrderId());
                b.setAmount(order.getAmount().toPlainString());
                b.setPurchaseToken(order.getTokenId());
                b.setPurchaseTokenName(order.getTokenName());
                b.setOrderQuantity(order.getAmount().divide(activityLockInterest.getMinPurchaseLimit()).toPlainString());
                b.setPurchaseTime(order.getPurchaseTime());
                b.setReceiveTokenId(activityLockInterest.getReceiveTokenId());
                b.setReceiveTokenName(activityLockInterest.getReceiveTokenName());
                if (interestCommon.getActivityType().equals(ActivityType.EQUAL.getNumber())) {
                    if (interestCommon.getStatus().equals(4)) {
                        BigDecimal modulus;
                        // 如果发售总额大于卖出的数额，就1：1兑换
                        if (activityLockInterest.getTotalCirculation().compareTo(activityLockInterest.getSoldAmount()) >= 0) {
                            modulus = BigDecimal.ONE;
                        } else {
                            // 如果发售总额小于卖出的数额，就按比例兑换
                            modulus = activityLockInterest.getTotalCirculation().divide(activityLockInterest.getSoldAmount(), ProtoConstants.PRECISION, RoundingMode.DOWN);
                        }
                        BigDecimal useAmount
                                = order.getAmount().multiply(modulus).setScale(8, RoundingMode.DOWN);
                        BigDecimal ratio = activityLockInterest.getReceiveTokenQuantity().divide(activityLockInterest.getValuationTokenQuantity(), 18, RoundingMode.DOWN);
                        BigDecimal luckyAmount = useAmount.multiply(ratio).setScale(8, RoundingMode.DOWN);
                        b.setReceiveTokenQuantity(luckyAmount.stripTrailingZeros().toPlainString());
                    } else {
                        b.setReceiveTokenQuantity("--");
                    }
                } else if (interestCommon.getActivityType().equals(ActivityType.FLASH_SALE.getNumber())) {
                    BigDecimal ratio = activityLockInterest.getReceiveTokenQuantity().divide(activityLockInterest.getValuationTokenQuantity(), 18, RoundingMode.DOWN);
                    BigDecimal luckyAmount = order.getAmount().multiply(ratio).setScale(8, RoundingMode.DOWN);
                    b.setReceiveTokenQuantity(luckyAmount.stripTrailingZeros().toPlainString());
                } else {
                    b.setReceiveTokenQuantity("--");
                }
                b.setUserId(String.valueOf(order.getUserId()));
                infos.add(b.build());
            }
            builder.addAllOrderInfo(infos);
        }

        return builder.build();
    }

    private Cache<String, ActivityLockInterest> cacheInterest = CacheBuilder.newBuilder()
            .expireAfterWrite(10, TimeUnit.MINUTES)
            .build();

    private ActivityLockInterest getActivityLockInterest(Long orgId, String projectCode) {
        String cacheKey = orgId + "_" + projectCode;
        ActivityLockInterest interest = cacheInterest.getIfPresent(cacheKey);
        if (interest == null) {
            interest = activityLockInterestMapper.getByCodeNoLock(orgId, projectCode);
            if (interest != null) {
                cacheInterest.put(cacheKey, interest);
            }
        }
        return interest;
    }

    private Cache<String, ActivityLockInterestCommon> cacheInterestCommon = CacheBuilder.newBuilder()
            .expireAfterWrite(10, TimeUnit.MINUTES)
            .build();

    private ActivityLockInterestCommon getActivityLockInterestCommon(Long orgId, String projectCode) {
        String cacheKey = orgId + "_" + projectCode;
        ActivityLockInterestCommon interestCommon = cacheInterestCommon.getIfPresent(cacheKey);
        if (interestCommon == null) {
            interestCommon = activityLockInterestCommonMapper.getCommonInfoByCode(projectCode, orgId);
            if (interestCommon != null) {
                cacheInterestCommon.put(cacheKey, interestCommon);
            }
        }
        return interestCommon;
    }

    public OrgQueryActivityOrderInfoReply orgQueryActivityOrderInfoByUser(OrgQueryActivityOrderInfoByUserRequest request) {
        boolean orderDesc = true;
        if (request.getFromId() == 0 && request.getEndId() > 0) {
            orderDesc = false;
        }
        List<ActivityLockInterestOrder> activityLockInterestOrders = orgQueryOrderByUid(request.getHeader().getOrgId(), request.getUserId(), request.getFromId(), request.getEndId(), request.getLimit(), orderDesc);

        if (CollectionUtils.isEmpty(activityLockInterestOrders)) {
            return OrgQueryActivityOrderInfoReply.newBuilder().addAllOrderInfo(new ArrayList<>()).build();
        }
        if (!orderDesc) {
            Collections.reverse(activityLockInterestOrders);
        }

        OrgQueryActivityOrderInfoReply.Builder builder = OrgQueryActivityOrderInfoReply.newBuilder();
        if (!CollectionUtils.isEmpty(activityLockInterestOrders)) {
            List<OrgQueryActivityOrderInfoReply.OrgLockInterestOrderInfo> infos = new ArrayList<>();
            for (ActivityLockInterestOrder order : activityLockInterestOrders) {
                ActivityLockInterest activityLockInterest = getActivityLockInterest(order.getBrokerId(), order.getProjectCode());
                if (activityLockInterest == null) {
                    continue;
                }
                ActivityLockInterestCommon interestCommon = getActivityLockInterestCommon(activityLockInterest.getBrokerId(), activityLockInterest.getProjectCode());
                if (interestCommon == null) {
                    continue;
                }

                OrgQueryActivityOrderInfoReply.OrgLockInterestOrderInfo.Builder b = OrgQueryActivityOrderInfoReply.OrgLockInterestOrderInfo.newBuilder();

                b.setProjectCode(order.getProjectCode());
                b.setOrderId(order.getId());
                b.setClientOrderId(order.getClientOrderId());
                b.setAmount(order.getAmount().toPlainString());
                b.setPurchaseToken(order.getTokenId());
                b.setPurchaseTokenName(order.getTokenName());
                b.setOrderQuantity(order.getAmount().divide(activityLockInterest.getMinPurchaseLimit()).toPlainString());
                b.setPurchaseTime(order.getPurchaseTime());
                b.setReceiveTokenId(activityLockInterest.getReceiveTokenId());
                b.setReceiveTokenName(activityLockInterest.getReceiveTokenName());
                if (interestCommon.getActivityType().equals(ActivityType.EQUAL.getNumber())) {
                    if (interestCommon.getStatus().equals(4)) {
                        BigDecimal modulus;
                        // 如果发售总额大于卖出的数额，就1：1兑换
                        if (activityLockInterest.getTotalCirculation().compareTo(activityLockInterest.getSoldAmount()) >= 0) {
                            modulus = BigDecimal.ONE;
                        } else {
                            // 如果发售总额小于卖出的数额，就按比例兑换
                            modulus = activityLockInterest.getTotalCirculation().divide(activityLockInterest.getSoldAmount(), ProtoConstants.PRECISION, RoundingMode.DOWN);
                        }
                        BigDecimal useAmount
                                = order.getAmount().multiply(modulus).setScale(8, RoundingMode.DOWN);
                        BigDecimal ratio = activityLockInterest.getReceiveTokenQuantity().divide(activityLockInterest.getValuationTokenQuantity(), 18, RoundingMode.DOWN);
                        BigDecimal luckyAmount = useAmount.multiply(ratio).setScale(8, RoundingMode.DOWN);
                        b.setReceiveTokenQuantity(luckyAmount.stripTrailingZeros().toPlainString());
                    } else {
                        b.setReceiveTokenQuantity("--");
                    }
                } else if (interestCommon.getActivityType().equals(ActivityType.FLASH_SALE.getNumber())) {
                    BigDecimal ratio = activityLockInterest.getReceiveTokenQuantity().divide(activityLockInterest.getValuationTokenQuantity(), 18, RoundingMode.DOWN);
                    BigDecimal luckyAmount = order.getAmount().multiply(ratio).setScale(8, RoundingMode.DOWN);
                    b.setReceiveTokenQuantity(luckyAmount.stripTrailingZeros().toPlainString());
                } else {
                    b.setReceiveTokenQuantity("--");
                }
                b.setUserId(String.valueOf(order.getUserId()));
                infos.add(b.build());
            }
            builder.addAllOrderInfo(infos);
        }

        return builder.build();
    }

    public List<ActivityLockInterest> listProjectInfoByIds(Long brokerId, List<Long> projectIds) {
        if (CollectionUtils.isEmpty(projectIds)) {
            return null;
        }
        Example example = Example.builder(ActivityLockInterestOrder.class).build();
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("brokerId", brokerId);
        criteria.andIn("id", projectIds);

        List<ActivityLockInterest> activityLockInterests = lockInterestMapper.selectByExample(example);
        return activityLockInterests;
    }

    public List<ActivityLockInterestOrder> queryAllOrder(Long accountId, Long brokerId, Long fromId, Long endId, Integer limit) {
        List<ActivityLockInterestOrder> activityLockInterestOrders = lockInterestOrderMapper.queryAllOrderByPage(brokerId, accountId, fromId, endId, limit);
        return activityLockInterestOrders;
    }

    public List<ActivityLockInterestOrder> orgQueryOrder(Long brokerId, String projectCode, Long fromId, Long endId, Integer limit, boolean orderDesc) {
        List<ActivityLockInterestOrder> activityLockInterestOrders = lockInterestOrderMapper.orgQueryOrderByPage(brokerId, projectCode, fromId, endId, limit, orderDesc);
        return activityLockInterestOrders;
    }

    public List<ActivityLockInterestOrder> orgQueryOrderByUid(Long brokerId, Long userId, Long fromId, Long endId, Integer limit, boolean orderDesc) {
        List<ActivityLockInterestOrder> activityLockInterestOrders = lockInterestOrderMapper.orgQueryUserOrderByPage(brokerId, userId, fromId, endId, limit, orderDesc);
        return activityLockInterestOrders;
    }

    public List<ActivityLockInterestOrder> queryAllOrderByUidAndProjectId(Long brokerId, Long userId, Long projectId, String projectCode, Long fromId, Long endId, Integer limit) {
        List<ActivityLockInterestOrder> activityLockInterestOrders = lockInterestOrderMapper.adminQueryAllOrderByPage(brokerId, projectId, projectCode, userId, fromId, endId, limit);
        return activityLockInterestOrders;
    }

    public void panicBuyingAsynchronousTransfer(Long id) {
        Example example = new Example(ActivityLockInterestOrder.class);
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("id", id);
        ActivityLockInterestOrder lockInterestOrder = this.lockInterestOrderMapper.selectOneByExample(example);
        if (lockInterestOrder == null) {
            return;
        }

        //运营账户没收到钱
        if (lockInterestOrder.getSourceTransferStatus().equals(0)) {
            return;
        }

        if (lockInterestOrder.getUserTransferStatus().equals(0) || lockInterestOrder.getUserTransferStatus().equals(2)) {
            if (lockInterestOrder.getUserAmount().compareTo(BigDecimal.ZERO) > 0) {
                //用户获得目标的token 从指定账户转出
                SyncTransferRequest syncTransferRequest = SyncTransferRequest.newBuilder()
                        .setClientTransferId(lockInterestOrder.getUserClientOrderId())
                        .setSourceOrgId(lockInterestOrder.getBrokerId())
                        .setSourceFlowSubject(BusinessSubject.AIRDROP)
                        .setSourceAccountId(lockInterestOrder.getSourceAccountId())
                        .setFromPosition(false)
                        .setTokenId(lockInterestOrder.getUserTokenId())
                        .setAmount(lockInterestOrder.getUserAmount().setScale(ProtoConstants.PRECISION, RoundingMode.HALF_UP).toPlainString())
                        .setTargetAccountId(lockInterestOrder.getAccountId())
                        .setTargetOrgId(lockInterestOrder.getBrokerId())
                        .setTargetFlowSubject(BusinessSubject.AIRDROP)
                        .build();
                SyncTransferResponse userTransferResponse = SyncTransferResponse.getDefaultInstance();
                try {
                    userTransferResponse = grpcBatchTransferService.syncTransfer(syncTransferRequest);
                } catch (Exception ex) {
                    log.info("panicBuyingAsynchronousTransfer user fail id {} ex {}", lockInterestOrder.getId(), ex);
                    lockInterestOrderMapper.updateUserTransferStatusById(lockInterestOrder.getId(), 2);
                }
                if ((userTransferResponse.getCode() != SyncTransferResponse.ResponseCode.SUCCESS)) {
                    lockInterestOrderMapper.updateUserTransferStatusById(lockInterestOrder.getId(), 2);
                } else {
                    lockInterestOrderMapper.updateUserTransferStatusById(lockInterestOrder.getId(), 1);
                }
            } else {
                lockInterestOrderMapper.updateUserTransferStatusById(lockInterestOrder.getId(), 1);
            }
        }

        if (lockInterestOrder.getGiftOpen().equals(1)
                && (lockInterestOrder.getGiftTransferStatus().equals(0) || lockInterestOrder.getGiftTransferStatus().equals(2))) {
            if (lockInterestOrder.getGiftAmount().compareTo(BigDecimal.ZERO) > 0) {
                //尝试给用户增币转账
                SyncTransferRequest transferRequest = SyncTransferRequest.newBuilder()
                        .setClientTransferId(lockInterestOrder.getGiftClientOrderId())
                        .setSourceOrgId(lockInterestOrder.getBrokerId())
                        .setSourceFlowSubject(BusinessSubject.AIRDROP)
                        .setSourceAccountId(lockInterestOrder.getSourceAccountId())
                        .setFromPosition(false)
                        .setTokenId(lockInterestOrder.getGiftTokenId())
                        .setAmount(lockInterestOrder.getGiftAmount().setScale(ProtoConstants.PRECISION, RoundingMode.HALF_UP).toPlainString())
                        .setTargetAccountId(lockInterestOrder.getAccountId())
                        .setToPosition(lockInterestOrder.getIsLock().equals(0) ? false : true)
                        .setTargetOrgId(lockInterestOrder.getBrokerId())
                        .setTargetFlowSubject(BusinessSubject.AIRDROP)
                        .build();
                SyncTransferResponse giftTransferResponse = SyncTransferResponse.getDefaultInstance();
                try {
                    giftTransferResponse = grpcBatchTransferService.syncTransfer(transferRequest);
                } catch (Exception ex) {
                    log.info("panicBuyingAsynchronousTransfer gift fail id {} ex {}", lockInterestOrder.getId(), ex);
                    lockInterestOrderMapper.updateGiftTransferStatusById(lockInterestOrder.getId(), 2);
                }
                if ((giftTransferResponse.getCode() != SyncTransferResponse.ResponseCode.SUCCESS)) {
                    lockInterestOrderMapper.updateGiftTransferStatusById(lockInterestOrder.getId(), 2);
                } else {
                    lockInterestOrderMapper.updateGiftTransferStatusById(lockInterestOrder.getId(), 1);
                }
            } else {
                lockInterestOrderMapper.updateGiftTransferStatusById(lockInterestOrder.getId(), 1);
            }
        }
    }

    /**
     * 获取IEO活动进度状态值
     */
    private Integer getIEOProgress(ActivityLockInterestCommon commonInfo) {
        Long now = System.currentTimeMillis();
        if (commonInfo.getStatus().equals(4)) {
            return commonInfo.getStatus();
        }
        Integer progressStatus = 0;
        // 预热
        if (now < commonInfo.getStartTime()) {
            progressStatus = 1;
            // 开始购买
        } else if (now < commonInfo.getEndTime()) {
            progressStatus = 2;
            // 购买结束，等待公布结果
        } else if (now < commonInfo.getResultTime()) {
            progressStatus = 3;
        } else if (now > commonInfo.getResultTime()) {
            progressStatus = 3;
        }
        return progressStatus;
    }

    /**
     * 获取抢购活动进度状态值
     */
    private Integer getPanicBuyingProgress(ActivityLockInterestCommon commonInfo) {
        Long now = System.currentTimeMillis();
        Integer progressStatus = 0;
        // 预热
        if (now < commonInfo.getStartTime()) {
            progressStatus = 1;
            // 开始购买
        } else if (now < commonInfo.getEndTime()) {
            progressStatus = 2;
            // 购买结束
        } else if (now > commonInfo.getEndTime()) {
            progressStatus = 5;
        }
        return progressStatus;
    }

    public ListActivityReply listActivity(ListActivityRequest request) {

        long brokerId = request.getHeader().getOrgId();
        String language = request.getHeader().getLanguage();
        long fromId = request.getFromId();
        long endId = request.getEndId();
        int size = request.getSize();

        String cacheKey = String.format(pattern, brokerId, language, fromId, endId, size);

        ListActivityReply.Builder replyBuilder = ListActivityReply.newBuilder();

        String cacheContent = redisTemplate.opsForValue().get(cacheKey);
        if (StringUtils.isNoneBlank(cacheContent)) {
            List<CacheAbleActivity> list = JSON.parseArray(cacheContent, CacheAbleActivity.class);
            return replyBuilder.setCode(0).addAllActivityInfo(transform(list)).build();
        }

        List<CacheAbleActivity> list = buildCacheableActivityList(brokerId, language, fromId, endId, size);
        redisTemplate.opsForValue().set(cacheKey, JSON.toJSONString(list), 10, TimeUnit.SECONDS);
        return replyBuilder.setCode(0).addAllActivityInfo(transform(list)).build();
    }

    public OrgListActivityReply orgListActivity(OrgListActivityRequest request) {

        long brokerId = request.getHeader().getOrgId();
        long fromId = request.getFromId();
        long endId = request.getEndId();
        int size = request.getSize();

        String cacheKey = String.format(orgPattern, brokerId, fromId, endId, size);

        OrgListActivityReply.Builder replyBuilder = OrgListActivityReply.newBuilder();

        String cacheContent = redisTemplate.opsForValue().get(cacheKey);
        if (StringUtils.isNoneBlank(cacheContent)) {
            List<CacheOrgActivity> list = JSON.parseArray(cacheContent, CacheOrgActivity.class);
            return replyBuilder.setCode(0).addAllActivityInfo(orgActivityTransform(list)).build();
        }

        List<CacheOrgActivity> list = buildCacheOrgActivityList(brokerId, fromId, endId, size);
        redisTemplate.opsForValue().set(cacheKey, JSON.toJSONString(list), 10, TimeUnit.SECONDS);
        return replyBuilder.setCode(0).addAllActivityInfo(orgActivityTransform(list)).build();
    }

    private Iterable<? extends OrgActivityInfo> orgActivityTransform(List<CacheOrgActivity> list) {
        return list.stream().map(i -> OrgActivityInfo.newBuilder()
                .setId(i.getId())
                .setStartTime(i.getStartTime())
                .setEndTime(i.getEndTime())
                .addAllLocals(i.getLocals().stream().map(local -> OrgActivityInfo.OrgActivityLocalInfo.newBuilder().setLanguage(local.getLanguage()).setProjectName(local.getProjectName()).build()).filter(Objects::nonNull).collect(Collectors.toList()))
                .setActivityType(i.getActivityType().getNumber())
                .setPurchaseToken(i.getPurchaseToken())
                .setPlatformLimit(i.getPlatformLimit())
                .setUserLimit(i.getUserLimit())
                .setMinPurchaseLimit(i.getMinPurchaseLimit())
                .setSoldVolum(i.getSoldVolum())
                .setRealSoldVolum(i.getRealSoldVolum())
                .setOfferingToken(i.getOfferingToken())
                .setOfferingsPrice(i.getOfferingsPrice())
                .setProjectCode(i.getProjectCode())
                .setStatus(i.getStatus())
                .setIsShow(i.getIsShow())
                .build()).filter(Objects::nonNull).collect(Collectors.toList());
    }

    private Iterable<? extends ActivityInfo> transform(List<CacheAbleActivity> list) {
        return list.stream().map(i -> {
            return ActivityInfo.newBuilder()
                    .setId(i.getId())
                    .setBannerUrl(i.getBannerUrl())
                    .setStartTime(i.getStartTime())
                    .setEndTime(i.getEndTime())
                    .setName(i.getName())
                    .setIntroduction(StringEscapeUtils.unescapeHtml4(Strings.nullToEmpty(i.getIntroduction())))
                    .setStatus(i.getStatus())
                    .setOfferingsVolume(i.getOfferingVolume())
                    .setOfferingsToken(i.getOfferingToken())
                    .setProjectCode(i.getProjectCode())
                    .setIsShow(i.getIsShow())
                    .build();
        }).filter(Objects::nonNull).collect(Collectors.toList());
    }

    private List<CacheAbleActivity> buildCacheableActivityList(long brokerId, String language, long fromId, long endId, int size) {

        List<ActivityLockInterestCommon> commonList = lockInterestCommonMapper.listActivityByPage(brokerId, fromId, endId, size, language);
        if (CollectionUtils.isEmpty(commonList)) {
            return Lists.newArrayList();
        }
        Set<String> codeSet = commonList.stream().map(i -> i.getProjectCode()).collect(Collectors.toSet());

        Example projExp = new Example(ActivityLockInterest.class);
        projExp.createCriteria()
                .andIn("projectCode", codeSet);

        List<ActivityLockInterest> projectList = lockInterestMapper.selectByExample(projExp);

        Map<String, List<ActivityLockInterest>> activityMap = projectList.stream().collect(Collectors.groupingBy(i -> i.getProjectCode()));
        Example localeExp = new Example(ActivityLockInterestLocal.class);
        localeExp.createCriteria()
                .andIn("projectCode", activityMap.keySet())
                .andEqualTo("language", language);

        List<ActivityLockInterestLocal> localeList = lockInterestLocalMapper.selectByExample(localeExp);
        Map<String, List<ActivityLockInterestLocal>> localeMap = localeList.stream().collect(Collectors.groupingBy(i -> i.getProjectCode()));

        return commonList.stream().map(i -> {
            List<ActivityLockInterestLocal> sublocaleList = localeMap.get(i.getProjectCode());
            List<ActivityLockInterest> activityList = activityMap.get(i.getProjectCode());

            if (CollectionUtils.isEmpty(sublocaleList) || CollectionUtils.isEmpty(activityList)) {
                return null;
            }

            ActivityLockInterest activity = activityList.get(0);
            ActivityLockInterestLocal locale = sublocaleList.get(0);

            String totalVolume = "0";
            String offeringTokenName = "";
            if (activity.getTotalReceiveCirculation().compareTo(BigDecimal.ZERO) > 0) {
                totalVolume = activity.totalReceiveCirculationStr();
                offeringTokenName = activity.getReceiveTokenName();
            } else {
                totalVolume = locale.getCirculationStr();
            }

            CacheAbleActivity ca = new CacheAbleActivity();
            ca.setId(i.getId());
            ca.setBannerUrl(i.getBannerUrl());
            ca.setStartTime(i.getStartTime());
            ca.setEndTime(i.getEndTime());
            ca.setName(locale.getProjectName());
            ca.setIntroduction(i.getIntroductionWithDefault());
            ca.setStatus(i.getActivityStatus());
            if (activity.getPurchaseableQuantity().compareTo(BigDecimal.ZERO) < 1) {
                // 项目可购买数量小于0时 关闭
                ca.setStatus(2);
                // 如果项目为3（抢购模式）则可售余额为零时，项目结束
                if (i.getActivityType() == 3) {
                    ca.setStatus(5);
                }
            }
            ca.setOfferingVolume(totalVolume);
            ca.setOfferingToken(offeringTokenName);
            ca.setProjectCode(i.getProjectCode());
            ca.setIsShow(activity.getIsShow());
            return ca;

        }).filter(Objects::nonNull).collect(Collectors.toList());
    }

    private List<CacheOrgActivity> buildCacheOrgActivityList(long brokerId, long fromId, long endId, int size) {

        boolean orderDesc = true;
        if (fromId == 0 && endId > 0) {
            orderDesc = false;
        }

        List<ActivityLockInterest> activityLockInterestList = lockInterestMapper.orgListActivityByPage(brokerId, fromId, endId, size, orderDesc);

        if (CollectionUtils.isEmpty(activityLockInterestList)) {
            return Lists.newArrayList();
        }

        if (!orderDesc) {
            Collections.reverse(activityLockInterestList);
        }

        Set<String> codeSet = activityLockInterestList.stream().map(i -> i.getProjectCode()).collect(Collectors.toSet());

        Example projCommExp = new Example(ActivityLockInterestCommon.class);
        projCommExp.createCriteria()
                .andIn("projectCode", codeSet);
        List<ActivityLockInterestCommon> commonList = lockInterestCommonMapper.selectByExample(projCommExp);
        Map<String, List<ActivityLockInterestCommon>> commMap = commonList.stream().collect(Collectors.groupingBy(i -> i.getProjectCode()));

        Example localeExp = new Example(ActivityLockInterestLocal.class);
        localeExp.createCriteria().andIn("projectCode", codeSet);

        List<ActivityLockInterestLocal> localeList = lockInterestLocalMapper.selectByExample(localeExp);
        Map<String, List<ActivityLockInterestLocal>> localeMap = localeList.stream().collect(Collectors.groupingBy(i -> i.getProjectCode()));


        List<CacheOrgActivity> respList = new ArrayList<>();
        for (ActivityLockInterest activityLockInterest : activityLockInterestList) {
            CacheOrgActivity cacheOrgActivity = new CacheOrgActivity();

            cacheOrgActivity.setId(activityLockInterest.getId());
            cacheOrgActivity.setStartTime(activityLockInterest.getStartTime()); //活动开始时间
            cacheOrgActivity.setEndTime(activityLockInterest.getEndTime());
            cacheOrgActivity.setLocals(new ArrayList<>());

            if (localeMap.containsKey(activityLockInterest.getProjectCode()) && !localeMap.get(activityLockInterest.getProjectCode()).isEmpty()) {
                localeMap.get(activityLockInterest.getProjectCode()).forEach(p -> {
                    CacheOrgActivityLocal local = new CacheOrgActivityLocal();
                    local.setLanguage(p.getLanguage());
                    local.setProjectName(p.getProjectName());
                    cacheOrgActivity.getLocals().add(local);
                });
            }

            int value = ActivityType.UN_KNOW_VALUE;
            if (commMap.containsKey(activityLockInterest.getProjectCode()) && !commMap.get(activityLockInterest.getProjectCode()).isEmpty()) {
                value = commMap.get(activityLockInterest.getProjectCode()).get(0).getActivityType();
            }
            cacheOrgActivity.setActivityType(ActivityType.forNumber(value));
            cacheOrgActivity.setPurchaseToken(activityLockInterest.getPurchaseTokenId());
            cacheOrgActivity.setPlatformLimit(activityLockInterest.getPlatformLimit().stripTrailingZeros().toPlainString());
            cacheOrgActivity.setUserLimit(activityLockInterest.getUserLimit().stripTrailingZeros().toPlainString());
            cacheOrgActivity.setMinPurchaseLimit(activityLockInterest.getMinPurchaseLimit().stripTrailingZeros().toPlainString());
            cacheOrgActivity.setSoldVolum(activityLockInterest.getSoldAmount().stripTrailingZeros().toPlainString());
            cacheOrgActivity.setRealSoldVolum(activityLockInterest.getRealSoldAmount().stripTrailingZeros().toPlainString());
            cacheOrgActivity.setOfferingToken(activityLockInterest.getReceiveTokenId());

            String price = "--";
            //大于0
            if (Objects.nonNull(activityLockInterest.getValuationTokenQuantity()) &&
                    Objects.nonNull(activityLockInterest.getReceiveTokenQuantity()) &&
                    activityLockInterest.getValuationTokenQuantity().compareTo(BigDecimal.ZERO) > 0) {
                price = activityLockInterest.getValuationTokenQuantity().stripTrailingZeros().toPlainString() + ":" + activityLockInterest.getReceiveTokenQuantity().stripTrailingZeros().toPlainString();
            }
            cacheOrgActivity.setOfferingsPrice(price);
            cacheOrgActivity.setProjectCode(activityLockInterest.getProjectCode());
            cacheOrgActivity.setStatus(activityLockInterest.getStatus());
            cacheOrgActivity.setIsShow(activityLockInterest.getIsShow());

            respList.add(cacheOrgActivity);
        }

        return respList;
    }

    @Resource
    private ActivityLockInterestBatchDetailMapper detailMapper;


    public QueryActivityProjectInfoResponse queryActivityProjectInfo(QueryActivityProjectInfoRequest request) {
        ActivityLockInterestMapping activityLockInterestMapping
                = this.activityLockInterestMappingMapper.sumActivityLockInterestMapping(request.getOrgId(), request.getProejctId());
        return QueryActivityProjectInfoResponse
                .newBuilder()
                .setOrgId(request.getOrgId())
                .setProjectId(request.getProejctId())
                .setAmount(activityLockInterestMapping.getAmount().setScale(8, RoundingMode.DOWN).toPlainString())
                .setUseAmount(activityLockInterestMapping.getUseAmount().setScale(8, RoundingMode.DOWN).toPlainString())
                .setLuckyAmount(activityLockInterestMapping.getLuckyAmount().setScale(8, RoundingMode.DOWN).toPlainString())
                .setBackAmount(activityLockInterestMapping.getBackAmount().setScale(8, RoundingMode.DOWN).toPlainString())
                .build();
    }


    @Data
    public static class CacheAbleActivity {
        private Long id;
        private String bannerUrl;
        private Long startTime;
        private Long endTime;
        private String name;
        private String introduction;
        private String offeringVolume;
        private String offeringToken;
        private String projectCode;
        private Integer status;
        private Integer isShow;
    }

    @Data
    public static class CacheOrgActivity {
        private Long id;
        private Long startTime; //活动开始时间
        private Long endTime; //活动结束时间
        private List<CacheOrgActivityLocal> locals;
        private ActivityType activityType; //模式
        private String purchaseToken; //申购币种
        private String platformLimit; //平台总限额
        private String userLimit; //用户总限额
        private String minPurchaseLimit; //最小申购额度
        private String soldVolum; //出售总额
        private String realSoldVolum; //真实出售总额
        private String offeringToken; //发行币种
        private String offeringsPrice; //申购价格
        private String projectCode;
        private Integer status;
        private Integer isShow;
    }

    @Data
    public static class CacheOrgActivityLocal {
        private String projectName;
        private String language;
    }
}
