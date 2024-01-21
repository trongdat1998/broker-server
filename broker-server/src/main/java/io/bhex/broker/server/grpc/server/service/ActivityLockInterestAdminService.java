package io.bhex.broker.server.grpc.server.service;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

import com.alibaba.fastjson.JSON;
import com.github.pagehelper.Page;
import com.github.pagehelper.PageHelper;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Resource;

import io.bhex.base.account.BalanceDetail;
import io.bhex.base.account.GetBalanceDetailRequest;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.common.util.JsonUtil;
import io.bhex.broker.grpc.activity.lockInterest.ActivityOrderTaskToFailRequest;
import io.bhex.broker.grpc.activity.lockInterest.ActivityOrderTaskToFailResponse;
import io.bhex.broker.grpc.activity.lockInterest.CreateActivityOrderTaskRequest;
import io.bhex.broker.grpc.activity.lockInterest.CreateActivityOrderTaskResponse;
import io.bhex.broker.grpc.activity.lockInterest.ExecuteActivityOrderTaskRequest;
import io.bhex.broker.grpc.activity.lockInterest.ExecuteActivityOrderTaskResponse;
import io.bhex.broker.grpc.activity.lockInterest.ModifyActivityOrderInfoRequest;
import io.bhex.broker.grpc.activity.lockInterest.ModifyActivityOrderInfoResponse;
import io.bhex.broker.grpc.admin.ActivityCommonInfo;
import io.bhex.broker.grpc.admin.ActivityInfo;
import io.bhex.broker.grpc.admin.ActivityLocaleInfo;
import io.bhex.broker.grpc.admin.ActivityOrderProfile;
import io.bhex.broker.grpc.admin.ActivityProfile;
import io.bhex.broker.grpc.admin.ActivityProjectInfo;
import io.bhex.broker.grpc.admin.ActivityResult;
import io.bhex.broker.grpc.admin.ActivityStatus;
import io.bhex.broker.grpc.admin.ActivityType;
import io.bhex.broker.grpc.admin.AdminQueryAllActivityOrderInfoReply;
import io.bhex.broker.grpc.admin.AdminQueryAllActivityOrderInfoRequest;
import io.bhex.broker.grpc.admin.CalculateActivityRequest;
import io.bhex.broker.grpc.admin.FindActivityReply;
import io.bhex.broker.grpc.admin.FindActivityRequest;
import io.bhex.broker.grpc.admin.FindActivityResultReply;
import io.bhex.broker.grpc.admin.ListActivityOrderReply;
import io.bhex.broker.grpc.admin.ListActivityReply;
import io.bhex.broker.grpc.admin.ListActivityRequest;
import io.bhex.broker.grpc.admin.LockPeriod;
import io.bhex.broker.grpc.admin.OnlineRequest;
import io.bhex.broker.grpc.admin.ProjectStatus;
import io.bhex.broker.grpc.admin.Qualifier;
import io.bhex.broker.grpc.admin.QueryIeoWhiteListReply;
import io.bhex.broker.grpc.admin.QueryIeoWhiteListRequest;
import io.bhex.broker.grpc.admin.SaveActivityReply;
import io.bhex.broker.grpc.admin.SaveActivityRequest;
import io.bhex.broker.grpc.admin.SaveIeoWhiteListRequest;
import io.bhex.broker.grpc.common.BasicRet;
import io.bhex.broker.grpc.common.Header;
import io.bhex.broker.grpc.proto.AdminCommonResponse;
import io.bhex.broker.server.domain.ActivityConstant;
import io.bhex.broker.server.domain.ActivityMappingStatusType;
import io.bhex.broker.server.grpc.client.service.GrpcBalanceService;
import io.bhex.broker.server.model.ActivityLockInterest;
import io.bhex.broker.server.model.ActivityLockInterestBatchDetail;
import io.bhex.broker.server.model.ActivityLockInterestBatchTask;
import io.bhex.broker.server.model.ActivityLockInterestCommon;
import io.bhex.broker.server.model.ActivityLockInterestLocal;
import io.bhex.broker.server.model.ActivityLockInterestMapping;
import io.bhex.broker.server.model.ActivityLockInterestOrder;
import io.bhex.broker.server.model.ActivityLockInterestWhiteList;
import io.bhex.broker.server.model.ActivityPurchaseLimit;
import io.bhex.broker.server.model.Token;
import io.bhex.broker.server.model.User;
import io.bhex.broker.server.primary.mapper.ActivityLockInterestBatchDetailMapper;
import io.bhex.broker.server.primary.mapper.ActivityLockInterestBatchTaskMapper;
import io.bhex.broker.server.primary.mapper.ActivityLockInterestCommonMapper;
import io.bhex.broker.server.primary.mapper.ActivityLockInterestLocalMapper;
import io.bhex.broker.server.primary.mapper.ActivityLockInterestMapper;
import io.bhex.broker.server.primary.mapper.ActivityLockInterestMappingMapper;
import io.bhex.broker.server.primary.mapper.ActivityLockInterestOrderMapper;
import io.bhex.broker.server.primary.mapper.ActivityLockInterestWhiteListMapper;
import io.bhex.broker.server.primary.mapper.ActivityPurchaseLimitMapper;
import io.bhex.broker.server.primary.mapper.TokenMapper;
import io.bhex.broker.server.primary.mapper.UserMapper;
import io.bhex.broker.server.util.BaseReqUtil;
import io.bhex.broker.server.util.RedisLockUtils;
import lombok.extern.slf4j.Slf4j;
import tk.mybatis.mapper.entity.Example;

import static io.bhex.broker.server.model.ActivityLockInterestOrder.STATUS_PAY_SUCCESS;

@Slf4j
@Service
public class ActivityLockInterestAdminService {


    @Resource
    private ActivityPurchaseLimitMapper activityPurchaseLimitMapper;

    @Resource
    private ActivityLockInterestCommonMapper lockInterestCommonMapper;

    @Resource
    private ActivityLockInterestLocalMapper lockInterestLocalMapper;

    @Resource
    private ActivityLockInterestMapper lockInterestMapper;

    @Resource
    private ActivityLockInterestOrderMapper activityLockInterestOrderMapper;

    @Resource
    private ActivityLockInterestMappingMapper lockInterestMappingMapper;

    @Resource
    private ActivityLockInterestMappingService activityLockInterestMappingService;


    @Resource
    private ActivityLockInterestMappingMapper activityLockInterestMappingMapper;

    @Resource
    private ActivityLockInterestService activityLockInterestService;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private AccountService accountService;

    @Resource
    private GrpcBalanceService grpcBalanceService;

    @Resource
    private ActivityLockInterestMapper activityLockInterestMapper;

    @Resource
    private ActivityLockInterestCommonMapper activityLockInterestCommonMapper;

    @Resource
    private UserMapper userMapper;

    @Resource
    private ActivityLockInterestWhiteListMapper whiteListMapper;

    @Resource
    private TokenMapper tokenMapper;

    @Resource
    private ActivityLockInterestBatchTaskMapper activityLockInterestBatchTaskMapper;

    @Resource
    private ActivityLockInterestBatchDetailMapper detailMapper;

    private final ExecutorService unlockTransferExecutor = Executors.newFixedThreadPool(1);

    private final static String default_lang = "en_US";

    public ListActivityReply listActivity(ListActivityRequest request) {
        String language = request.getLanguage();
        long brokerId = request.getBrokerId();
        List<ActivityType> types = request.getActivityTypeList();
        List<Integer> typeInts = types.stream().map(i -> i.getNumber()).collect(Collectors.toList());

        //查出选择参数语言和默认语言的所有活动
        Example commonExp = new Example(ActivityLockInterestCommon.class);
        commonExp.excludeProperties("description", "introduction", "bannerUrl");
        commonExp.orderBy("createdTime").desc();
        commonExp.createCriteria()
                .andEqualTo("brokerId", brokerId)
                .andIn("language", Lists.newArrayList(language, default_lang))
                .andIn("activityType", typeInts);

        Page page = PageHelper.startPage(request.getPageNo(), request.getSize() * 2);
        List<ActivityLockInterestCommon> commonList = lockInterestCommonMapper.selectByExample(commonExp);
        if (CollectionUtils.isEmpty(commonList)) {
            return ListActivityReply.newBuilder()
                    .setCode(0)
                    .setMessage("success")
                    .build();
        }
        Map<String, List<ActivityLockInterestCommon>> groupCommon = commonList.stream()
                .collect(Collectors.groupingBy(i -> i.getLanguage()));
        //取出参数语言
        List<ActivityLockInterestCommon> commonLangList = groupCommon.getOrDefault(language, Lists.newArrayList());
        //如果小于参数请求数量，则取出默认语言补充
        if (commonLangList.size() < request.getSize()) {

            Set<String> existCode = commonLangList.stream()
                    .map(i -> i.getProjectCode()).collect(Collectors.toSet());

            List<ActivityLockInterestCommon> enCommonList = groupCommon.getOrDefault(default_lang, Lists.newArrayList())
                    .stream().filter(i -> !existCode.contains(i.getProjectCode())).collect(Collectors.toList());
            int diff = request.getSize() - commonLangList.size();
            if (enCommonList.size() >= diff) {
                commonLangList.addAll(enCommonList.subList(0, diff));
            } else {
                commonLangList.addAll(enCommonList);
            }
        }

        Comparator<ActivityLockInterestCommon> commonComparator = (obj1, obj2) -> obj2.getCreatedTime().compareTo(obj1.getCreatedTime());

        //重排序
        commonLangList = commonLangList.stream().sorted(commonComparator).collect(Collectors.toList());

        Map<String, ActivityLockInterestCommon> commonMap = commonLangList.stream().collect(Collectors.toMap(i -> i.getProjectCode(), i -> i));

        Example projExp = new Example(ActivityLockInterest.class);
        projExp.orderBy("createdTime").desc();
        projExp.createCriteria()
                .andIn("projectCode", commonMap.keySet());

        List<ActivityLockInterest> projectList = lockInterestMapper.selectByExample(projExp);
        Map<String, List<ActivityLockInterest>> projectGroup = projectList.stream().collect(Collectors.groupingBy(i -> i.getProjectCode()));
        //忽略一个common多个project的数据
        Map<String, ActivityLockInterest> projectMap = Maps.newHashMap();
        projectGroup.forEach((k, v) -> {
            if (v.size() == 1) {
                projectMap.put(k, v.get(0));
            }
        });

        Example multiLangExp = new Example(ActivityLockInterestLocal.class);
        multiLangExp.createCriteria()
                .andIn("projectCode", commonMap.keySet())
                .andEqualTo("language", language);

        List<ActivityLockInterestLocal> localList = lockInterestLocalMapper.selectByExample(multiLangExp);
        Map<String, ActivityLockInterestLocal> localMap = localList.stream().collect(Collectors.toMap(i -> i.getProjectCode(), i -> i));

        List<ActivityProfile> list = commonLangList.stream().filter(i -> projectMap.containsKey(i.getProjectCode()))
                .map(i -> {

                    //ActivityLockInterestCommon common=commonMap.get(i.getProjectCode());
                    String projectCode = i.getProjectCode();
                    ActivityLockInterestLocal local = localMap.get(projectCode);
                    ActivityLockInterest project = projectMap.get(projectCode);

                    String price = "--";
                    //大于0
                    if (Objects.nonNull(project.getValuationTokenQuantity()) &&
                            Objects.nonNull(project.getReceiveTokenQuantity()) &&
                            project.getValuationTokenQuantity().compareTo(BigDecimal.ZERO) > 0) {
                        price = bigDecimalToString(project.getValuationTokenQuantity()) + ":" + bigDecimalToString(project.getReceiveTokenQuantity());
                    }


                    String volume = "";
                    String projectName = "";

                    if (Objects.nonNull(local)) {
                        volume = local.getCirculationStr();
                        projectName = local.getProjectName();
                        if (StringUtils.isBlank(volume) && Objects.nonNull(project.getTotalCirculation())) {
                            volume = bigDecimalToString(project.getTotalReceiveCirculation());
                        }
                    }


                    ActivityType type = ActivityType.forNumber(i.getActivityType());
                    return ActivityProfile.newBuilder()
                            .setActivityId(project.getId())
                            .setActivityType(type)
                            .setName(projectName)
                            .setEndTime(i.getEndTime())
                            .setStartTime(i.getStartTime())
                            .setOfferingsPrice(price)
                            .setOfferingsToken(project.getReceiveTokenId())
                            .setOfferingsTokenName(project.getReceiveTokenName())
                            .setPurchaseToken(project.getPurchaseTokenId())
                            .setPurchaseTokenName(project.getPurchaseTokenName())
                            .setResultTime(i.getResultTime())
                            .setTotalVolume(volume)
                            .setStatus(getActivityStatus(type, i, project.getPurchaseableQuantity()))
                            .setIsShow(project.getIsShow())
                            .setProjectCode(project.getProjectCode())
                            .build();
                }).collect(Collectors.toList());


        return ListActivityReply.newBuilder()
                .setMessage("success")
                .addAllActivities(list)
                .setTotal((int) page.getTotal())
                .build();
    }


    public ActivityStatus getActivityStatus(ActivityType type, ActivityLockInterestCommon activity, BigDecimal totalVolume) {

        int status = 0;
        if (ActivityType.EQUAL == type) {
            status = this.getIEOProgress(activity);
        }

        if (ActivityType.FLASH_SALE == type) {
            status = this.getFlashSaleProgress(activity, totalVolume);
        }

        return ActivityStatus.forNumber(status);
    }

    private int getFlashSaleProgress(ActivityLockInterestCommon activity, BigDecimal totalVolume) {

        if (Objects.isNull(totalVolume)) {
            return 0;
        }

        if (BigDecimal.ZERO.compareTo(totalVolume) == 0) {
            return 5;
        }
        return getPanicBuyingProgress(activity);
    }


    /**
     * 获取IEO活动进度状态值
     */
    private Integer getIEOProgress(ActivityLockInterestCommon commonInfo) {
        if (commonInfo.getStatus().equals(4)) {
            return commonInfo.getStatus();
        }
        Long now = System.currentTimeMillis();
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

    @Transactional(rollbackFor = Exception.class)
    public SaveActivityReply saveActivity(SaveActivityRequest request) {
        SaveActivityReply.Builder builder = SaveActivityReply.newBuilder();
        ActivityInfo activityInfo = request.getActivityInfo();

        boolean isStart = false;

        if (StringUtils.isNotEmpty(activityInfo.getProjectCode())) {
            List<ActivityLockInterestCommon> alicList = listActivityCommon(activityInfo.getBrokerId(), activityInfo.getProjectCode());
            if (!CollectionUtils.isEmpty(alicList)) {
                ActivityLockInterestCommon alic = alicList.get(0);
                isStart = alic.getActivityStatus() > 1;
                if (alic.getActivityStatus() > 2) {
                    log.info("not modify code {} getActivityStatus {}", alic.getProjectCode(), alic.getActivityStatus());
                    //不能进行修改
                    return builder.setResult(false).build();
                }
            }
        }

        Map<String, ActivityProjectInfo> statusMap = Maps.newHashMap();
        Map<String, Long> projectCodeIdMap = Maps.newHashMap();
        List<Long> projectIdList = Lists.newArrayList();
        List<ActivityProjectInfo> projectInfoList = activityInfo.getActivityProjectInfoList();

        final boolean started = isStart;
        projectInfoList.forEach(info -> {
            if (!statusMap.containsKey(info.getProjectCode())) {
                statusMap.put(info.getProjectCode(), info);
            }

            boolean update = info.getId() > 0;
            ActivityLockInterest projectInfo = new ActivityLockInterest();
            projectInfo.setPlatformLimit(new BigDecimal(info.getPlatformLimit()));
            projectInfo.setUserLimit(new BigDecimal(info.getUserLimit()));
            projectInfo.setUpdatedTime(System.currentTimeMillis());
            projectInfo.setProjectType(info.getProjectTypeValue());
            projectInfo.setStatus(info.getStatusValue());
            projectInfo.setPurchaseableQuantity(new BigDecimal(info.getPurchaseableQuantity()));
            projectInfo.setValuationTokenQuantity(new BigDecimal(info.getValuationTokenQuantity()));
            projectInfo.setReceiveTokenQuantity(new BigDecimal(info.getOfferingsTokenQuantity()));
            projectInfo.setDomain(StringUtils.isEmpty(info.getDomain()) ? "" : info.getDomain());
            projectInfo.setBrowser(StringUtils.isEmpty(info.getBrowser()) ? "" : info.getBrowser());
            projectInfo.setWhitePaper(StringUtils.isEmpty(info.getWhitePaper()) ? "" : info.getWhitePaper());
            projectInfo.setVersion(info.getVersion());
            BigDecimal totalOfferingsVolume = BigDecimal.ZERO;
            if (StringUtils.isNoneBlank(info.getTotalOfferingsCirculation())) {
                totalOfferingsVolume = new BigDecimal(info.getTotalOfferingsCirculation());
            }
            projectInfo.setTotalReceiveCirculation(totalOfferingsVolume);
            projectInfo.setBaseProcessPercent(info.getBaseProcessPercent());
            //开始后不能修改
            if (!started) {
                projectInfo.setReceiveTokenId(info.getOfferingsTokenId());
                if (StringUtils.isEmpty(info.getOfferingsTokenName())) {
                    Token token = this.tokenMapper.getToken(info.getBrokerId(), info.getOfferingsTokenId());
                    projectInfo.setReceiveTokenName(token == null ? info.getOfferingsTokenId() : token.getTokenName());
                } else {
                    projectInfo.setReceiveTokenName(info.getOfferingsTokenName());
                }
                projectInfo.setPurchaseTokenId(info.getPurchaseTokenId());
                if (StringUtils.isEmpty(info.getPurchaseTokenName())) {
                    Token token = this.tokenMapper.getToken(info.getBrokerId(), info.getPurchaseTokenId());
                    projectInfo.setPurchaseTokenName(token == null ? info.getPurchaseTokenId() : token.getTokenName());
                } else {
                    projectInfo.setPurchaseTokenName(info.getPurchaseTokenName());
                }
                projectInfo.setStartTime(info.getStartTime());
                projectInfo.setEndTime(info.getEndTime());
                projectInfo.setMinPurchaseLimit(new BigDecimal(info.getMinPurchaseLimit()));
            }

            //不能修改的信息
            if (!update) {
                projectInfo.setBrokerId(info.getBrokerId());
                projectInfo.setProjectCode(info.getProjectCode());

                projectInfo.setExchangeRate(BigDecimal.ZERO);
                projectInfo.setSoldAmount(BigDecimal.ZERO);
                projectInfo.setRealSoldAmount(BigDecimal.ZERO);
                projectInfo.setIsPurchaseLimit(info.getIsPurchaseLimit());

                //兼容设置
                projectInfo.setProjectName(StringUtils.EMPTY);
                projectInfo.setDescript(StringUtils.EMPTY);
                projectInfo.setTitle(StringUtils.EMPTY);
                projectInfo.setLockedPeriod(0);
            }


            if (0 == info.getId()) {
                projectInfo.setIsShow(0);
                projectInfo.setCreatedTime(System.currentTimeMillis());
                projectInfo.setTotalCirculation(totalOfferingsVolume.multiply(new BigDecimal(info.getValuationTokenQuantity())));
                lockInterestMapper.insertSelective(projectInfo);
                log.error("Create activity  warn !!! orgId {} projectCode {} startTime {} endTime {}",
                        info.getBrokerId(), info.getProjectCode(), new DateTime(info.getStartTime()).toString("yyyy-MM-dd HH:mm:ss"), new DateTime(info.getEndTime()).toString("yyyy-MM-dd HH:mm:ss"));
            } else {
                projectInfo.setTotalCirculation(totalOfferingsVolume.multiply(new BigDecimal(info.getValuationTokenQuantity())));
                projectInfo.setId(info.getId());
                lockInterestMapper.updateByPrimaryKeySelective(projectInfo);
            }

            projectIdList.add(projectInfo.getId());

            if (!projectCodeIdMap.containsKey(projectInfo.getProjectCode())) {
                projectCodeIdMap.put(projectInfo.getProjectCode(), projectInfo.getId());
            }

            List<ActivityLocaleInfo> localeInfoList = info.getActivityLocaleInfoList();
            localeInfoList.forEach(locale -> {
                ActivityLockInterestLocal l = new ActivityLockInterestLocal();

                l.setProjectName(locale.getProjectName());
                l.setTitle(locale.getTitle());
                l.setLanguage(locale.getLanguage());

                l.setUpdatedTime(System.currentTimeMillis());
                l.setFixedInterestRate(locale.getFixedInterestRate());
                l.setFloatingRate(locale.getFloatingRate());

                l.setCirculationStr(locale.getCirculationStr());
                l.setTitle(locale.getTitle());
                if (info.getBrokerId() > 0) {
                    l.setBrokerId(info.getBrokerId());
                } else {
                    if (locale.getBrokerId() > 0) {
                        l.setBrokerId(locale.getBrokerId());
                    }
                }

                l.setProjectId(projectInfo.getId() != null ? projectInfo.getId() : 0);
                l.setProjectCode(StringUtils.isNotEmpty(locale.getProjectCode()) ? locale.getProjectCode() : "");
                l.setDescript(StringUtils.EMPTY);
                l.setLockedPeriod(StringUtils.EMPTY);

                if (0 == locale.getId()) {
                    l.setCreatedTime(System.currentTimeMillis());
                    lockInterestLocalMapper.insertSelective(l);
                } else {
                    l.setId(locale.getId());
                    lockInterestLocalMapper.updateByPrimaryKeySelective(l);
                }
            });

            //参赛门槛处理 增加VIP等级
            Qualifier qualifier = activityInfo.getQualifier();
            ActivityPurchaseLimit limit = new ActivityPurchaseLimit();
            limit.setBrokerId(activityInfo.getBrokerId());
            limit.setProjectId(info.getId());
            limit.setVerifyKyc(qualifier.getVerifyKyc() ? 1 : 0);
            limit.setVerifyBindPhone(qualifier.getVerifyMobile() ? 1 : 0);
            limit.setVerifyBalance(qualifier.getVerifyPosition() ? 1 : 0);
            limit.setProjectId(projectInfo.getId());
            limit.setVerifyAvgBalance(qualifier.getVerifyAvgBalance() ? 1 : 0);
            limit.setLevelLimit(StringUtils.isNotEmpty(qualifier.getLevelLimit()) ? qualifier.getLevelLimit() : "");
            Map<String, String> map = Maps.newHashMap();
            if (qualifier.getVerifyPosition()) {
                map.put("positionToken", qualifier.getPositionToken());
                map.put("positionVolume", qualifier.getPositionVolume());
            }

            if (qualifier.getVerifyAvgBalance()) {
                map.put(ActivityConstant.VERIFY_AVG_BALANCE_TOKEN, qualifier.getVerifyAvgBalanceToken());
                map.put(ActivityConstant.VERIFY_AVG_BALANCE_VOLUME, qualifier.getVerifyAvgBalanceVolume());
                map.put(ActivityConstant.VERIFY_AVG_BALANCE_START_TIME, qualifier.getVerifyAvgBalanceStartTime() > 0 ? String.valueOf(qualifier.getVerifyAvgBalanceStartTime()) : "");
                map.put(ActivityConstant.VERIFY_AVG_BALANCE_END_TIME, qualifier.getVerifyAvgBalanceStartTime() > 0 ? String.valueOf(qualifier.getVerifyAvgBalanceEndTime()) : "");
            }
            if (map.size() > 0) {
                limit.setBalanceRuleJson(JSON.toJSONString(map));
            } else {
                limit.setBalanceRuleJson("");
            }

            limit.setUpdatedTime(System.currentTimeMillis());
            if (0 == qualifier.getId()) {
                limit.setCreatedTime(System.currentTimeMillis());
                activityPurchaseLimitMapper.insertSelective(limit);
            } else {
                limit.setId(qualifier.getId());
                activityPurchaseLimitMapper.updateByPrimaryKeySelective(limit);
            }
        });

        List<ActivityCommonInfo> commonInfoList = activityInfo.getActivityCommonInfoList();
        commonInfoList.forEach(info -> {
            ActivityProjectInfo project = statusMap.get(activityInfo.getProjectCode());
            boolean update = info.getId() > 0;
            ActivityLockInterestCommon commonInfo = new ActivityLockInterestCommon();
            commonInfo.setBannerUrl(info.getBannerUrl());
            commonInfo.setDescription(StringEscapeUtils.escapeHtml4(info.getDescription()));
            commonInfo.setWechatUrl(info.getWechatUrl());
            commonInfo.setBlockBrowser(info.getBlockBrowser());
            commonInfo.setBrowserTitle(info.getBrowserTitle());
            commonInfo.setLanguage(info.getLanguage());
            commonInfo.setStatus(project.getStatus().getNumber());
            commonInfo.setUpdatedTime(System.currentTimeMillis());
            commonInfo.setAbout(StringUtils.isEmpty(info.getAbout()) ? "" : info.getAbout());
            commonInfo.setRule(StringUtils.isEmpty(info.getRule()) ? "" : info.getRule());
            //开始后不能修改 或者开始新增 可以赋值
            if (!started || (started && !update)) {
                commonInfo.setEndTime(project.getEndTime());
                commonInfo.setStartTime(project.getStartTime());
                commonInfo.setActivityType(activityInfo.getActivityTypeValue());
            }
            //不能修改的信息
            if (!update) {
                commonInfo.setBrokerId(activityInfo.getBrokerId());
                commonInfo.setProjectCode(activityInfo.getProjectCode());
            }

            commonInfo.setOnlineTime(project.getOnlineTime());
            commonInfo.setResultTime(project.getResultTime());
            commonInfo.setAssetUserId(activityInfo.getAssetUserId());
            commonInfo.setIntroduction(StringEscapeUtils.escapeHtml4(info.getIntroduction()));
            if (update) {
                commonInfo.setId(info.getId());
                lockInterestCommonMapper.updateByPrimaryKeySelective(commonInfo);
            } else {
                commonInfo.setCreatedTime(System.currentTimeMillis());
                lockInterestCommonMapper.insertSelective(commonInfo);
            }
        });
        return builder.setResult(true).setProjectId(projectIdList.get(0)).build();
    }

    private List<ActivityLockInterestCommon> listActivityCommon(long brokerId, String projectCode) {

        Example commonExp = new Example(ActivityLockInterestCommon.class);
        commonExp.createCriteria()
                .andEqualTo("brokerId", brokerId)
                .andEqualTo("projectCode", projectCode);
        return lockInterestCommonMapper.selectByExample(commonExp);
    }

    private ActivityLockInterest findProject(Long brokerId, String activityCode, Long projectId) {

        Example exp = new Example(ActivityLockInterest.class);
        Example.Criteria criteria = exp.createCriteria().andEqualTo("brokerId", brokerId);
        if (StringUtils.isNoneBlank(activityCode)) {
            criteria.andEqualTo("projectCode", activityCode);
        }

        if (Objects.nonNull(projectId) && projectId.longValue() > 0) {
            criteria.andEqualTo("id", projectId);
        }
        return lockInterestMapper.selectOneByExample(exp);
    }

    private String bigDecimalToString(BigDecimal bigDecimal) {
        if (Objects.isNull(bigDecimal)) {
            return "";
        }

        return bigDecimal.stripTrailingZeros().toPlainString();
    }

    public FindActivityReply findProject(FindActivityRequest request) {

        Long id = request.getActivityId();
        ActivityLockInterest activity = lockInterestMapper.selectByPrimaryKey(id);
        if (Objects.isNull(activity)) {
            return FindActivityReply.getDefaultInstance();
        }

        List<ActivityLockInterestCommon> commons = listActivityCommon(activity.getBrokerId(), activity.getProjectCode());

        Map<String, ActivityLockInterestCommon> commonMap = commons.stream().collect(Collectors.toMap(i -> i.getLanguage(), i -> i));

        Example localeExp = new Example(ActivityLockInterestLocal.class);
        localeExp.createCriteria()
                .andEqualTo("projectId", id);
        List<ActivityLockInterestLocal> locales = lockInterestLocalMapper.selectByExample(localeExp);

        Example limitExp = new Example(ActivityLockInterestLocal.class);
        limitExp.createCriteria()
                .andEqualTo("projectId", id);
        ActivityPurchaseLimit limit = activityPurchaseLimitMapper.selectOneByExample(limitExp);

        ActivityLockInterestCommon common = commons.get(0);

        List<ActivityLocaleInfo> localeList = locales.stream().map(i -> {
            ActivityLockInterestCommon alic = commonMap.get(i.getLanguage());
            return ActivityLocaleInfo.newBuilder()
                    .setBrokerId(i.getBrokerId())
                    .setCirculationStr(i.getCirculationStr())
                    .setId(i.getId())
                    .setBannerUrl(alic.getBannerUrl())
                    .setProjectCode(i.getProjectCode())
                    .setProjectName(i.getProjectName())
                    .setLanguage(i.getLanguage())
                    .build();

        }).collect(Collectors.toList());

        ActivityProjectInfo project = ActivityProjectInfo.newBuilder()
                .setId(activity.getId())
                .setBrokerId(activity.getBrokerId())
                .setProjectType(LockPeriod.forNumber(activity.getProjectType()))
                .setStartTime(activity.getStartTime())
                .setEndTime(activity.getEndTime())

                .setOnlineTime(common.getOnlineTime())
                .setResultTime(common.getResultTime())
                .setIsPurchaseLimit(activity.getIsPurchaseLimit())
                .setProjectCode(activity.getProjectCode())
                .setPlatformLimit(activity.platformLimitStr())

                .setMinPurchaseLimit(activity.minPurchaseLimitStr())
                .setUserLimit(activity.userLimitStr())
                .setPurchaseableQuantity(activity.purchaseableQuantityStr())
                .setPurchaseTokenId(activity.getPurchaseTokenId())
                .setPurchaseTokenName(activity.getPurchaseTokenName())
                .setOfferingsTokenId(activity.getReceiveTokenId())
                .setOfferingsTokenName(activity.getReceiveTokenName())

                .setStatus(ProjectStatus.forNumber(activity.getStatus()))
                .setOfferingsTokenQuantity(activity.receiveTokenQuantityStr())
                .setTotalOfferingsCirculation(activity.totalReceiveCirculationStr())
                .setValuationTokenQuantity(activity.valuationTokenQuantityStr())
                .setIsShow(activity.getIsShow())
                .setCreatedTime(activity.getCreatedTime())
                .setUpdatedTime(activity.getUpdatedTime())
                .setBaseProcessPercent(activity.getBaseProcessPercent())
                .addAllActivityLocaleInfo(localeList)
                .setDomain(StringUtils.isNotEmpty(activity.getDomain()) ? activity.getDomain() : "")
                .setBrowser(StringUtils.isNotEmpty(activity.getBrowser()) ? activity.getBrowser() : "")
                .setWhitePaper(StringUtils.isNotEmpty(activity.getWhitePaper()) ? activity.getWhitePaper() : "")
                .setVersion(activity.getVersion())
                .build();


        ActivityType activityType = ActivityType.forNumber(commons.stream().findFirst().get().getActivityType());
        List<ActivityCommonInfo> commonInfoList = commons.stream().map(i -> {
            return ActivityCommonInfo.newBuilder()
                    .setIntroduction(StringEscapeUtils.unescapeHtml4(i.getIntroductionWithDefault()))
                    .setDescription(StringEscapeUtils.unescapeHtml4(i.getDescriptionWithDefault()))
                    .setLanguage(i.getLanguage())
                    .setBannerUrl(i.getBannerUrl())
                    .setId(i.getId())
                    .setAbout(StringUtils.isNotEmpty(i.getAbout()) ? i.getAbout() : "")
                    .setRule(StringUtils.isNotEmpty(i.getRule()) ? i.getRule() : "")
                    .build();
        }).collect(Collectors.toList());


        Map<String, String> map = new HashMap<>();
        if (StringUtils.isNotEmpty(limit.getBalanceRuleJson())) {
            try {
                map = JsonUtil.defaultGson().fromJson(limit.getBalanceRuleJson(), Map.class);
            } catch (JsonSyntaxException e) {
                log.error("verifyPurchaseLimit error: Balance Rule Json=> {}, projectId=> {}", limit.getBalanceRuleJson(), limit.getProjectId());
            }
        }

        String quantitySnapshot = map.get("positionVolume") != null ? map.get("positionVolume") : "";
        String tokenSnapshot = map.get("positionToken") != null ? map.get("positionToken") : "";
        String verifyAvgBalanceToken = map.get(ActivityConstant.VERIFY_AVG_BALANCE_TOKEN) != null ? map.get(ActivityConstant.VERIFY_AVG_BALANCE_TOKEN) : "";
        String verifyAvgBalanceVolume = map.get(ActivityConstant.VERIFY_AVG_BALANCE_VOLUME) != null ? map.get(ActivityConstant.VERIFY_AVG_BALANCE_VOLUME) : "";
        String verifyAvgBalanceStartTime = map.get(ActivityConstant.VERIFY_AVG_BALANCE_START_TIME) != null ? map.get(ActivityConstant.VERIFY_AVG_BALANCE_START_TIME) : "0";
        String verifyAvgBalanceEndTime = map.get(ActivityConstant.VERIFY_AVG_BALANCE_END_TIME) != null ? map.get(ActivityConstant.VERIFY_AVG_BALANCE_END_TIME) : "0";

        Qualifier qualifier = Qualifier.newBuilder()
                .setId(limit.getId())
                .setVerifyKyc(limit.verifyKycBool())
                .setVerifyMobile(limit.verifyBindPhoneBool())
                .setVerifyPosition(limit.verifyBalanceBool())
                .setPositionVolume(quantitySnapshot)
                .setPositionToken(tokenSnapshot)
                .setVerifyAvgBalance(limit.getVerifyAvgBalance() != null ? limit.getVerifyAvgBalance().equals(0) ? Boolean.FALSE : Boolean.TRUE : Boolean.FALSE)
                .setVerifyAvgBalanceStartTime(Long.parseLong(verifyAvgBalanceStartTime))
                .setVerifyAvgBalanceEndTime(Long.parseLong(verifyAvgBalanceEndTime))
                .setVerifyAvgBalanceVolume(verifyAvgBalanceVolume)
                .setVerifyAvgBalanceToken(verifyAvgBalanceToken)
                .setLevelLimit(StringUtils.isNotEmpty(limit.getLevelLimit()) ? limit.getLevelLimit() : "")
                .build();

        ActivityInfo info = ActivityInfo.newBuilder()
                .addAllActivityCommonInfo(commonInfoList)
                .addAllActivityProjectInfo(Lists.newArrayList(project))
                .setBrokerId(activity.getBrokerId())
                .setProjectCode(activity.getProjectCode())
                .setActivityType(activityType)
                .setQualifier(qualifier)
                .setAssetUserId(common.getAssetUserId())
                .setStatus(common.getActivityStatus())
                .build();

        return FindActivityReply.newBuilder()
                .setActivity(info)
                .setMessage("success")
                .build();

    }

    @Transactional(rollbackFor = Throwable.class)
    public AdminCommonResponse calculatePurchaseResult(CalculateActivityRequest request) {

        AdminCommonResponse.Builder replyBuilder = AdminCommonResponse.newBuilder()
                .setErrorCode(-1);
        //判断是否已经进行过转账 解锁操作 如果有成功的记录则不允许再次计算
        if (this.lockInterestMappingMapper.queryActivityHasTransferCount(request.getBrokerId(), request.getProjectId()) > 0) {
            replyBuilder.setMsg("Already assigned");
            return replyBuilder.build();
        }

        long brokerId = request.getBrokerId();
        long projectId = request.getProjectId();
        String language = request.getLanguage();
        String actualOffingsVolume = request.getActualOffingsVolume();

        if (brokerId == 0) {
            replyBuilder.setMsg("Miss broker id");
            return replyBuilder.build();
        }

        if (projectId == 0) {
            replyBuilder.setMsg("Miss project id");
            return replyBuilder.build();
        }

        if (StringUtils.isBlank(language)) {
            replyBuilder.setMsg("Miss language");
            return replyBuilder.build();
        }

        ActivityLockInterest activity = lockInterestMapper.selectByPrimaryKey(projectId);
        if (Objects.isNull(activity)) {
            replyBuilder.setMsg("There isn't activity");
            return replyBuilder.build();
        }

        if (activity.getBrokerId().longValue() != brokerId) {
            replyBuilder.setMsg("BrokerId error");
            return replyBuilder.build();
        }
        long now = System.currentTimeMillis();
        if (activity.getEndTime() > now) {
            replyBuilder.setMsg("Activity isn't finished");
            return replyBuilder.build();
        }

        //删除未处理记录
        boolean success = activityLockInterestMappingService.deleteMappingRecordByProjectId(projectId);
        if (!success) {
            replyBuilder.setMsg("Fail to delete result");
            return replyBuilder.build();
        }

        //todo更新soldAmount
        if (StringUtils.isNoneBlank(actualOffingsVolume)) {
            ActivityLockInterest updateActivity = new ActivityLockInterest();
            updateActivity.setId(projectId);
            updateActivity.setSoldAmount(new BigDecimal(actualOffingsVolume));
            updateActivity.setUpdatedTime(System.currentTimeMillis());
            int rows = lockInterestMapper.updateByPrimaryKeySelective(updateActivity);
            if (rows != 1) {
                replyBuilder.setMsg("Fail to update sold amount");
                return replyBuilder.build();
            }
        }

        //重新计算记录
        try {
            activityLockInterestMappingService.saveActivityResult(brokerId, projectId);
        } catch (BrokerException be) {
            log.error(be.getMessage(), be);
            replyBuilder.setMsg("Fail to save activity result");
            return replyBuilder.build();
        }

        return replyBuilder.setErrorCode(0).setSuccess(true).setMsg("success").build();

    }


    @Transactional(rollbackFor = Throwable.class)
    public FindActivityResultReply findActivityResult(FindActivityRequest request) {

        FindActivityResultReply.Builder replyBuilder = FindActivityResultReply.newBuilder()
                .setCode(-1);

        long brokerId = request.getBrokerId();
        long projectId = request.getActivityId();
        String language = request.getLanguage();

        if (brokerId == 0) {
            replyBuilder.setMessage("Miss broker id");
            return replyBuilder.build();
        }

        if (projectId == 0) {
            replyBuilder.setMessage("Miss project id");
            return replyBuilder.build();
        }

        ActivityLockInterest activity = lockInterestMapper.selectByPrimaryKey(projectId);
        if (Objects.isNull(activity)) {
            replyBuilder.setMessage("There isn't activity");
            return replyBuilder.build();
        }

        if (activity.getBrokerId().longValue() != brokerId) {
            replyBuilder.setMessage("BrokerId error");
            return replyBuilder.build();
        }

        Example localeExp = new Example(ActivityLockInterestLocal.class);
        localeExp.createCriteria()
                .andEqualTo("brokerId", brokerId)
                .andEqualTo("projectId", projectId)
                .andEqualTo("language", language);

        ActivityLockInterestLocal locale = lockInterestLocalMapper.selectOneByExample(localeExp);
        String projectName = "";
        if (Objects.nonNull(locale)) {
            projectName = locale.getProjectName();
        }

        String actualAurchaseAmountStr = activity.soldAmountStr();
        log.info("projectCode {} actualAurchaseAmountStr {}", activity.getProjectCode(), actualAurchaseAmountStr);
        String totalPurchaseAmountStr = activity.getTotalReceiveCirculation().toPlainString();
        log.info("projectCode {}  totalPurchaseAmountStr {}", activity.getProjectCode(), totalPurchaseAmountStr);
        //每申购份数可得发行货币数量=申购系数(实际申购数量/计划申购数量)*最小申购数量*发行价格(1申购币种/发行币种数)
        BigDecimal offingsVolumeEachPurchaseUnit = activity.getPurchaseCoefficient()
                .multiply(activity.getMinPurchaseLimit())
                .multiply(activity.getExchangeProportion());

        BigDecimal realOffingsVolumeEachPurchaseUnit = activity.getRealPurchaseCoefficient()
                .multiply(activity.getMinPurchaseLimit())
                .multiply(activity.getExchangeProportion());

        log.info("projectCode {} purchaseCoefficient {}", activity.getProjectCode(), activity.getPurchaseCoefficient());

        log.info("projectCode {} minPurchaseLimit {}", activity.getProjectCode(), activity.getMinPurchaseLimit());

        log.info("projectCode {} exchangeProportion {}", activity.getProjectCode(), activity.getExchangeProportion());

        log.info("projectCode {} offingsVolumeEachPurchaseUnit {}", activity.getProjectCode(), offingsVolumeEachPurchaseUnit);

        Example projExp = new Example(ActivityLockInterestOrder.class);
        projExp.setDistinct(true);
        projExp.selectProperties("userId");
        projExp.createCriteria()
                .andEqualTo("projectId", projectId)
                .andEqualTo("status", STATUS_PAY_SUCCESS)
        ;

        Integer buyerCount = activityLockInterestOrderMapper.countUser(projectId);

        return replyBuilder.setCode(0)
                .setResult(
                        ActivityResult.newBuilder()
                                .setProjectName(projectName)
                                .setTotalPurchaseVolume(totalPurchaseAmountStr)
                                .setActualPurchaseVolume(actualAurchaseAmountStr)
                                .setBuyerCount(buyerCount)
                                .setOffingsVolumeEachPurchaseUnit(tokenVolumeToString(offingsVolumeEachPurchaseUnit))
                                .setRealSoldAmount(activity.getRealSoldAmount().toPlainString())
                                .setRealRaiseAmount(activity.getRealSoldAmount().divide(activity.getMinPurchaseLimit()).setScale(8, RoundingMode.DOWN).multiply(offingsVolumeEachPurchaseUnit).setScale(8, RoundingMode.DOWN).toPlainString())
                                .setRealOffingsVolumeEachPurchaseUnit(tokenVolumeToString(realOffingsVolumeEachPurchaseUnit))
                                .build()

                )
                .build();
    }

    public String tokenVolumeToString(BigDecimal tokenVolume) {
        if (Objects.isNull(tokenVolume)) {
            return "0";
        }
        return tokenVolume.setScale(8, RoundingMode.DOWN).stripTrailingZeros().toPlainString();
    }

    /**
     * 确认活动结果进行转账及解锁
     */
    public AdminCommonResponse confirmActivityResult(FindActivityRequest request) {
        AdminCommonResponse.Builder replyBuilder = AdminCommonResponse.newBuilder().setErrorCode(-1);
        long brokerId = request.getBrokerId();
        long projectId = request.getActivityId();
        log.info("Confirm activity result,brokerId={},projectId={}", brokerId, projectId);

        String lockKey = "activity-confirm-" + projectId;
        Boolean locked = RedisLockUtils.tryLock(stringRedisTemplate, lockKey, 5 * 60 * 1000);
        if (!locked) {
            return replyBuilder.setSuccess(false).setMsg("Duplicate request").build();
        }

        try {

            if (brokerId == 0) {
                return replyBuilder.setSuccess(false).setMsg("Miss broker id").build();
            }

            if (projectId == 0) {
                return replyBuilder.setSuccess(false).setMsg("Miss project id").build();
            }

            ActivityLockInterest activity = lockInterestMapper.selectByPrimaryKey(projectId);
            if (Objects.isNull(activity)) {
                return replyBuilder.setSuccess(false).setMsg("Activity isn't exist").build();
            }

            if (activity.getBrokerId().longValue() != brokerId) {
                return replyBuilder.setSuccess(false).setMsg("BrokerId error").build();
            }

            List<ActivityLockInterestOrder> activityLockInterestOrders = this.activityLockInterestOrderMapper.queryOrderIdListByStatus(projectId, 1);
            if (!CollectionUtils.isEmpty(activityLockInterestOrders)) {
                //是否已经生成记录 如果未生成按默认生成
                if (this.activityLockInterestMappingMapper.queryActivityCountOrder(brokerId, projectId) == 0) {
                    try {
                        activityLockInterestMappingService.saveActivityResult(brokerId, projectId);
                    } catch (Exception ex) {
                        log.error("Create IEO Result Error orgId {} projectCode {} ex {}", brokerId, activity.getProjectCode(), ex);
                        return replyBuilder.setSuccess(false).setMsg("Failed to generate allocation results").build();
                    }
                }

                if (this.activityLockInterestMappingMapper.queryActivityCountOrder(brokerId, projectId) > 0) {
                    BigDecimal luckyAmount = this.lockInterestMappingMapper.queryActivitySumLuckyAmount(brokerId, projectId);
                    if (luckyAmount.compareTo(BigDecimal.ZERO) <= 0) {
                        return replyBuilder.setSuccess(false).setMsg("Insufficient Balance").build();
                    }
                    Long accountId = getAccountId(activity.getProjectCode());
                    try {
                        checkBalance(luckyAmount, accountId, activity.getReceiveTokenId(), brokerId);
                    } catch (IllegalStateException ex) {
                        return replyBuilder.setSuccess(false).setMsg("Insufficient Balance").build();
                    }

                    //操作变成异步执行
                    unlockTransferExecutor.execute(() -> {
                        try {
                            activityLockInterestMappingService.handelLambActivityTransfer(brokerId, projectId, ActivityMappingStatusType.PENDING);
                            activityLockInterestMappingService.handelActivityUnLockBalance(brokerId, projectId, ActivityMappingStatusType.PENDING);
                        } catch (Exception ex) {
                            log.error("Confirm IEO Result Error Need to be handled orgId {} projectCode {} ex {}", brokerId, activity.getProjectCode(), ex);
                        }
                    });
                }
            }

            lockInterestCommonMapper.updateActivityLockInterestCommonResultTime(new Date().getTime(), activity.getProjectCode());
            this.lockInterestCommonMapper.updateActivityStatusByProjectCode(activity.getProjectCode());
            this.lockInterestMapper.updateActivityStatusByProjectCode(activity.getProjectCode());
            return replyBuilder.setMsg("success").setSuccess(true).build();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return replyBuilder.setMsg(e.getMessage()).build();
        } finally {
            RedisLockUtils.releaseLock(stringRedisTemplate, lockKey);
        }
    }

    private void checkBalance(BigDecimal expect, Long accountId, String offeringsTokenId, Long orgId) {

        GetBalanceDetailRequest request = GetBalanceDetailRequest.newBuilder()
                .setBaseRequest(BaseReqUtil.getBaseRequest(orgId))
                .setAccountId(accountId)
                .addAllTokenId(Lists.newArrayList(offeringsTokenId))
                .build();

        List<BalanceDetail> balance = grpcBalanceService.getBalanceDetail(request).getBalanceDetailsList();
        if (CollectionUtils.isEmpty((balance))) {
            log.warn("Miss token,token={},accountId={}", offeringsTokenId, accountId);
            throw new IllegalStateException("Miss token,token" + offeringsTokenId + ",accountId=" + accountId);
        }

        BalanceDetail balanceDetail = balance.get(0);
        String value = balanceDetail.getAvailable().getStr();
        BigDecimal available = new BigDecimal(value).setScale(balanceDetail.getAvailable().getScale());
        //余额不足
        if (expect.compareTo(available) > 0) {
            log.warn("Insufficient balance,token={},accountId={},expect {},available {}", offeringsTokenId, accountId, expect.toPlainString(), available.toPlainString());
            throw new IllegalStateException("Insufficient balance,token" + offeringsTokenId + ",accountId=" + accountId);
        }
    }

    private Long getAccountId(String projectCode) {

        Example exp = new Example(ActivityLockInterestCommon.class);
        exp.createCriteria().andEqualTo("projectCode", projectCode);

        List<ActivityLockInterestCommon> list = lockInterestCommonMapper.selectByExample(exp);
        ActivityLockInterestCommon common = list.stream().findFirst().get();
        if (Objects.isNull(common)) {
            log.warn("Hasn't any project common,code=" + projectCode);
            throw new IllegalStateException("Hasn't any project common,code=" + projectCode);
        }
        Long assetUserId = common.getAssetUserId();
        if (Objects.isNull(assetUserId)) {
            log.warn("Illegal asset user id,code=" + projectCode);
            throw new IllegalStateException("Illegal asset user id,code=" + projectCode);
        }

        return accountService.getAccountId(common.getBrokerId(), assetUserId);


    }

    public AdminCommonResponse onlineActivity(OnlineRequest request) {
        long projectId = request.getProjectId();
        long brokerId = request.getBrokerId();

        ActivityLockInterest project = findProject(brokerId, null, projectId);
        if (Objects.isNull(project)) {
            return AdminCommonResponse.newBuilder()
                    .setSuccess(false)
                    .setErrorCode(-1)
                    .setMsg("Project not exist")
                    .build();
        }

        if (lockInterestMapper.updateIsShow(request.getProjectId(), request.getOnlineStatus() ? 1 : 0) == 1) {
            return AdminCommonResponse.newBuilder().setSuccess(true).build();
        } else {
            return AdminCommonResponse.newBuilder().setSuccess(false).build();
        }
    }

    public ListActivityOrderReply listActivityOrderProfile(FindActivityRequest request) {

        long brokerId = request.getBrokerId();
        long projectId = request.getActivityId();

        Example exp = new Example(ActivityLockInterestOrder.class);
        exp.createCriteria()
                .andEqualTo("brokerId", brokerId)
                .andEqualTo("projectId", projectId);

        List<ActivityLockInterestOrder> list = activityLockInterestOrderMapper.selectByExample(exp);

        List<ActivityOrderProfile> aopList = list.stream().map(i -> {
            return ActivityOrderProfile.newBuilder()
                    .setCreateTime(i.getCreatedTime())
                    .setOrderId(i.getId())
                    .setPurchaseAmount(i.amountToString())
                    .setOrderStatus(i.getStatus())
                    .setUserId(i.getUserId())
                    .setTokenId(i.getTokenId())
                    .setProjectCode(i.getProjectCode())
                    .build();
        }).collect(Collectors.toList());

        return ListActivityOrderReply.newBuilder().setCode(0).addAllOrders(aopList).build();
    }

    public AdminQueryAllActivityOrderInfoReply adminQueryAllActivityOrderInfo(AdminQueryAllActivityOrderInfoRequest request) {
        if (request.getProjectId() == 0) {
            return AdminQueryAllActivityOrderInfoReply.newBuilder().build();
        }
        if (request.getOrgId() == 0) {
            return AdminQueryAllActivityOrderInfoReply.newBuilder().build();
        }
        Long userId = 0L;
        User user;
        if (request.getUserId() > 0) {
            user = userMapper.getByUserId(request.getUserId());
            if (user != null) {
                userId = user.getUserId();
            }
        } else if (StringUtils.isNotEmpty(request.getMobile())) {
            user = userMapper.getByMobileAndOrgId(request.getOrgId(), request.getMobile());
            if (user != null && user.getUserId() != null) {
                userId = user.getUserId();
            }
        } else if (StringUtils.isNotEmpty(request.getEmail())) {
            user = userMapper.getByEmail(request.getOrgId(), request.getEmail());
            if (user != null && user.getUserId() != null) {
                userId = user.getUserId();
            }
        }

        if (userId.equals(0)) {
            return AdminQueryAllActivityOrderInfoReply.newBuilder().build();
        }

        AdminQueryAllActivityOrderInfoReply.Builder builder = AdminQueryAllActivityOrderInfoReply.newBuilder();
        List<ActivityLockInterestOrder> activityLockInterestOrders = activityLockInterestService.queryAllOrderByUidAndProjectId(request.getOrgId(),
                request.getUserId(),
                Long.parseLong(String.valueOf(request.getProjectId())),
                request.getProjectCode(),
                request.getFromId(),
                request.getEndId(),
                request.getLimit());
        if (!CollectionUtils.isEmpty(activityLockInterestOrders)) {
            List<AdminQueryAllActivityOrderInfoReply.LockInterestOrderInfo> infos = new ArrayList<>();
            List<Long> projectIds = activityLockInterestOrders.stream().map(ActivityLockInterestOrder::getProjectId).collect(Collectors.toList());
            Map<Long, ActivityLockInterestLocal> localMap = activityLockInterestService.lockInterestLocalMap(projectIds, Header.newBuilder().setLanguage(request.getLanguage()).build());
            ActivityLockInterest activityLockInterest = activityLockInterestMapper.getByIdNoLock(request.getProjectId());
            ActivityLockInterestCommon interestCommon = this.activityLockInterestCommonMapper.getCommonInfoByCode(activityLockInterest.getProjectCode(), activityLockInterest.getBrokerId());
            if (activityLockInterest == null || interestCommon == null) {
                return AdminQueryAllActivityOrderInfoReply.newBuilder().build();
            }
            List<Long> orderIds = activityLockInterestOrders.stream().map(ActivityLockInterestOrder::getId).collect(Collectors.toList());
            Map<Long, ActivityLockInterestMapping> mappingMap = new HashMap<>();
            if (interestCommon.getActivityType().equals(ActivityType.EQUAL.getNumber()) && org.apache.commons.collections4.CollectionUtils.isNotEmpty(orderIds)) {
                Example example = new Example(ActivityLockInterestMapping.class);
                Example.Criteria criteria = example.createCriteria();
                criteria.andIn("orderId", orderIds);
                List<ActivityLockInterestMapping> interestMappingList = this.activityLockInterestMappingMapper.selectByExample(example);
                if (org.apache.commons.collections4.CollectionUtils.isNotEmpty(interestMappingList)) {
                    mappingMap = interestMappingList.stream().collect(Collectors.toMap(ActivityLockInterestMapping::getOrderId, Function.identity()));
                }
            }
            for (ActivityLockInterestOrder order : activityLockInterestOrders) {
                AdminQueryAllActivityOrderInfoReply.LockInterestOrderInfo.Builder b = AdminQueryAllActivityOrderInfoReply.LockInterestOrderInfo.newBuilder();
                b.setOrderId(order.getId());
                b.setAmount(order.getAmount().toPlainString());
                b.setPurchaseToken(order.getTokenId());
                b.setPurchaseTokenName(order.getTokenName());
                b.setPrice(activityLockInterest.getMinPurchaseLimit().toPlainString());
                b.setOrderQuantity(order.getAmount().divide(activityLockInterest.getMinPurchaseLimit()).toPlainString());
                b.setPurchaseTime(order.getPurchaseTime());
                b.setReceiveTokenId(activityLockInterest.getReceiveTokenId());
                b.setReceiveTokenName(activityLockInterest.getReceiveTokenName());
                if (interestCommon.getActivityType().equals(ActivityType.EQUAL.getNumber())) {
                    ActivityLockInterestMapping mapping = mappingMap.get(order.getId());
                    if (mapping != null) {
                        b.setReceiveTokenQuantity(mapping != null ? mapping.getLuckyAmount().setScale(8, RoundingMode.DOWN).stripTrailingZeros().toPlainString() : "0.00");
                        b.setMappingId(mapping != null ? mapping.getId() : 0l);
                        b.setBackAmount(mapping != null ? mapping.getBackAmount().setScale(8, RoundingMode.DOWN).stripTrailingZeros().toPlainString() : "0.00");
                        b.setUseAmount(mapping != null ? mapping.getUseAmount().setScale(8, RoundingMode.DOWN).stripTrailingZeros().toPlainString() : "0.00");
                    } else {
                        b.setReceiveTokenQuantity("--");
                        b.setMappingId(0l);
                        b.setBackAmount("0");
                        b.setUseAmount("0");
                    }
//                    if (interestCommon.getStatus().equals(4)) {
//                        BigDecimal modulus;
//                        // 如果发售总额大于卖出的数额，就1：1兑换
//                        if (activityLockInterest.getTotalCirculation().compareTo(activityLockInterest.getSoldAmount()) >= 0) {
//                            modulus = BigDecimal.ONE;
//                        } else {
//                            // 如果发售总额小于卖出的数额，就按比例兑换
//                            modulus = activityLockInterest.getTotalCirculation().divide(activityLockInterest.getSoldAmount(), ProtoConstants.PRECISION, RoundingMode.DOWN);
//                        }
//                        BigDecimal useAmount = order.getAmount().multiply(modulus).setScale(ProtoConstants.PRECISION, RoundingMode.DOWN);
//                        BigDecimal ratio = activityLockInterest.getReceiveTokenQuantity().divide(activityLockInterest.getValuationTokenQuantity(), ProtoConstants.PRECISION, RoundingMode.DOWN);
//                        BigDecimal luckyAmount = useAmount.multiply(ratio).setScale(ProtoConstants.PRECISION, RoundingMode.HALF_UP);
//                        b.setReceiveTokenQuantity(luckyAmount.setScale(8, RoundingMode.DOWN).stripTrailingZeros().toPlainString());
//                    } else {
//                        b.setReceiveTokenQuantity("--");
//                    }
                } else if (interestCommon.getActivityType().equals(ActivityType.FLASH_SALE.getNumber())) {
                    b.setReceiveTokenQuantity(order.getUserAmount().setScale(8, RoundingMode.DOWN).stripTrailingZeros().toPlainString());
                } else {
                    b.setReceiveTokenQuantity("--");
                }
                b.setUserId(String.valueOf(order.getUserId()));
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

    public void saveIeoWhiteList(SaveIeoWhiteListRequest request) {
        ActivityLockInterestWhiteList whiteList
                = this.whiteListMapper.queryByProjectId(request.getBrokerId(), request.getProjectId());
        if (whiteList != null) {
            this.whiteListMapper.updateUserStrByProjectId(request.getBrokerId(), request.getProjectId(), request.getUserIdStr());
        } else {
            ActivityLockInterestWhiteList lockInterestWhiteList = new ActivityLockInterestWhiteList();
            lockInterestWhiteList.setBrokerId(request.getBrokerId());
            lockInterestWhiteList.setProjectId(request.getProjectId());
            lockInterestWhiteList.setUserIdStr(request.getUserIdStr());
            this.whiteListMapper.insertSelective(lockInterestWhiteList);
        }
    }

    public QueryIeoWhiteListReply queryIeoWhiteList(QueryIeoWhiteListRequest request) {
        ActivityLockInterestWhiteList whiteList
                = this.whiteListMapper.queryByProjectId(request.getBrokerId(), request.getProjectId());
        if (whiteList == null) {
            return QueryIeoWhiteListReply.newBuilder().build();
        }

        return QueryIeoWhiteListReply
                .newBuilder()
                .setBrokerId(whiteList.getBrokerId())
                .setProjectId(whiteList.getProjectId())
                .setUserIdStr(whiteList.getUserIdStr())
                .build();
    }


    public CreateActivityOrderTaskResponse createActivityOrderTask(CreateActivityOrderTaskRequest request) {
        //查询当前活动的状态 如果结束不允许
        ActivityLockInterest activityLockInterest = lockInterestMapper.getActivityLockInterestById(request.getProjectId());
        log.info("createActivityOrderTask activityLockInterest projectId {} info {} ", request.getProjectId(), new Gson().toJson(activityLockInterest));
        // 项目未开始或已结束 停止购买
        if (activityLockInterest == null || activityLockInterest.getStatus().equals(0) || activityLockInterest.getStatus().equals(4)) {
            return CreateActivityOrderTaskResponse.newBuilder().setCode(1).build();
        }

        int count = this.activityLockInterestMappingMapper.queryActivityCountOrder(activityLockInterest.getBrokerId(), activityLockInterest.getId());
        if (count <= 0) {
            return CreateActivityOrderTaskResponse.newBuilder().setCode(2).build();
        }

        Example example = new Example(ActivityLockInterestBatchTask.class);
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("orgId", request.getOrgId());
        criteria.andEqualTo("projectId", request.getProjectId());
        criteria.andEqualTo("status", 0);
        List<ActivityLockInterestBatchTask> interestBatchTaskList = this.activityLockInterestBatchTaskMapper.selectByExample(example);
        //任务正在处理中
        if (!CollectionUtils.isEmpty(interestBatchTaskList)) {
            return CreateActivityOrderTaskResponse.newBuilder().setCode(3).build();
        }
        //创建任务返回主键ID
        ActivityLockInterestBatchTask activityLockInterestBatchTask = new ActivityLockInterestBatchTask();
        activityLockInterestBatchTask.setOrgId(request.getOrgId());
        activityLockInterestBatchTask.setProjectId(request.getProjectId());
        activityLockInterestBatchTask.setProjectCode(activityLockInterest.getProjectCode());
        activityLockInterestBatchTask.setStatus(0);
        activityLockInterestBatchTask.setCreated(new Date());
        activityLockInterestBatchTask.setUpdated(new Date());
        activityLockInterestBatchTask.setUrl(request.getUrl());
        int row = activityLockInterestBatchTaskMapper.insert(activityLockInterestBatchTask);
        if (row == 1) {
            return CreateActivityOrderTaskResponse.newBuilder().setCode(0).setTaskId(activityLockInterestBatchTask.getId()).setProjectId(activityLockInterestBatchTask.getProjectId()).build();
        } else {
            return CreateActivityOrderTaskResponse.newBuilder().setTaskId(4).build();
        }
    }

    public static void main(String[] args) {

        BigDecimal i = new BigDecimal("11968.000000000000000000");
        BigDecimal j = new BigDecimal("7599.680000000000000000");

        BigDecimal userAmount = i.add(j);

        System.out.println(userAmount.toPlainString());

    }

    public ExecuteActivityOrderTaskResponse executeActivityOrderTask(ExecuteActivityOrderTaskRequest request) {
        long startTime = System.currentTimeMillis();
        ActivityLockInterestBatchTask task = activityLockInterestBatchTaskMapper.selectByPrimaryKey(request.getTaskId());
        if (task == null || task.getStatus() != 0) {
            return ExecuteActivityOrderTaskResponse.newBuilder().setBasicRet(BasicRet.newBuilder().setCode(1)).build();
        }
        int count = this.activityLockInterestMappingMapper.queryCountMappingCount(task.getOrgId(), task.getProjectId());
        //count = 0 数据未生成
        if (count == 0) {
            activityOrderTaskToFail(task.getOrgId(), task.getProjectId(), task.getId());
            return ExecuteActivityOrderTaskResponse.newBuilder().setBasicRet(BasicRet.newBuilder().setCode(2)).build();
        }
        try {
            Example example = new Example(ActivityLockInterestBatchDetail.class);
            Example.Criteria criteria = example.createCriteria();
            criteria.andEqualTo("orgId", task.getOrgId());
            criteria.andEqualTo("projectId", task.getProjectId());
            criteria.andEqualTo("taskId", request.getTaskId());
            criteria.andEqualTo("status", 0);
            List<ActivityLockInterestBatchDetail> batchDetailList = this.detailMapper.selectByExample(example);
            if (org.apache.commons.collections4.CollectionUtils.isEmpty(batchDetailList)) {
                log.info("executeActivityOrderTask fail orgId {} projectId {} taskId {} count {} size {} ",
                        task.getOrgId(), task.getProjectId(), task.getId(), count, 0);
                return ExecuteActivityOrderTaskResponse.newBuilder().setBasicRet(BasicRet.newBuilder().setCode(3)).build();
            }
            //上传条数不等于待处理条数
            if (count != batchDetailList.size()) {
                log.info("executeActivityOrderTask fail orgId {} projectId {} taskId {} count {} size {} ",
                        task.getOrgId(), task.getProjectId(), task.getId(), count, batchDetailList.size());
                activityOrderTaskToFail(task.getOrgId(), task.getProjectId(), task.getId());
                return ExecuteActivityOrderTaskResponse.newBuilder().setBasicRet(BasicRet.newBuilder().setCode(4)).build();
            }
            //校验是否有精度问题 如果有精度问题 提示具体的精度错误 任务设置为失败 重新上传
            //获取用户实际上花销
            BigDecimal amount = this.activityLockInterestMappingMapper.queryUserAmount(task.getOrgId(), task.getProjectId());
            BigDecimal backAmount = BigDecimal.ZERO;
            BigDecimal useAmount = BigDecimal.ZERO;
            for (ActivityLockInterestBatchDetail detail : batchDetailList) {
                backAmount = backAmount.add(detail.getBackAmount());
                useAmount = useAmount.add(detail.getUseAmount());
            }
            BigDecimal userAmount = backAmount.add(useAmount);
            if (userAmount.compareTo(amount) > 0) {
                log.info("executeActivityOrderTask fail orgId {} projectId {} taskId {} userAmount {} amount {} ",
                        task.getOrgId(), task.getProjectId(), task.getId(), userAmount, amount);
                //上传失败
                activityOrderTaskToFail(task.getOrgId(), task.getProjectId(), task.getId());
                //用户实际花销+解锁 > lock金额 会导致解锁失败
                return ExecuteActivityOrderTaskResponse.newBuilder().setBasicRet(BasicRet.newBuilder().setCode(5)).build();
            }
            if (userAmount.compareTo(amount) < 0) {
                log.info("executeActivityOrderTask fail orgId {} projectId {} taskId {} userAmount {} amount {} ",
                        task.getOrgId(), task.getProjectId(), task.getId(), userAmount, amount);
                //上传失败
                activityOrderTaskToFail(task.getOrgId(), task.getProjectId(), task.getId());
                //用户实际花销+解锁 < lock金额 会导致剩余一点不能解锁
                return ExecuteActivityOrderTaskResponse.newBuilder().setBasicRet(BasicRet.newBuilder().setCode(6)).build();
            }
            List<ActivityLockInterestMapping> activityLockInterestMappingList = batchDetailList.stream().map(detail -> {
                return ActivityLockInterestMapping
                        .builder()
                        .id(detail.getRecordId())
                        .useAmount(detail.getUseAmount())
                        .luckyAmount(detail.getLuckyAmount())
                        .backAmount(detail.getBackAmount())
                        .build();
            }).collect(Collectors.toList());
            if (CollectionUtils.isEmpty(activityLockInterestMappingList)) {
                return ExecuteActivityOrderTaskResponse.newBuilder().setBasicRet(BasicRet.newBuilder().setCode(3)).build();
            }
            activityLockInterestMappingMapper.batchUpdateActivityLockInterestMapping(activityLockInterestMappingList, task.getOrgId(), task.getProjectId());
        } catch (Exception ex) {
            log.info("Execute activity order task orgId {} projectId {} taskId {} error {}", task.getOrgId(), task.getProjectId(), task.getId(), ex);
            activityOrderTaskToFail(task.getOrgId(), task.getProjectId(), task.getId());
            return ExecuteActivityOrderTaskResponse.newBuilder().setBasicRet(BasicRet.newBuilder().setCode(7)).build();
        }
        activityOrderTaskToSuccess(task.getOrgId(), task.getProjectId(), task.getId());
        long endTime = System.currentTimeMillis();
        log.info("executeActivityOrderTask orgId {} projectId {} useTime {}", task.getOrgId(), task.getProjectId(), (endTime - startTime));
        return ExecuteActivityOrderTaskResponse.newBuilder().setBasicRet(BasicRet.newBuilder().setCode(0)).build();
    }

    public ModifyActivityOrderInfoResponse modifyActivityOrderInfo(ModifyActivityOrderInfoRequest request) {
        long orgId = request.getOrgId();
        long projectId = request.getProjectId();
        if (request.getOrderInfoCount() == 0) {
            return ModifyActivityOrderInfoResponse.newBuilder().setBasicRet(BasicRet.newBuilder().setCode(1)).build();
        }
        ActivityLockInterest activityLockInterest = this.activityLockInterestMapper.selectByPrimaryKey(request.getProjectId());
        if (activityLockInterest == null || activityLockInterest.getStatus().equals(0) || activityLockInterest.getStatus().equals(4)) {
            log.info("modifyActivityOrderInfo project not find orgId {} projectId {} status {}", orgId, projectId, activityLockInterest == null ? "null" : activityLockInterest.getStatus());
            return ModifyActivityOrderInfoResponse.newBuilder().setBasicRet(BasicRet.newBuilder().setCode(2)).build();
        }
        try {
            List<ActivityLockInterestBatchDetail> activityLockInterestBatchDetails = request.getOrderInfoList().stream().map(info -> {
                return ActivityLockInterestBatchDetail
                        .builder()
                        .orgId(request.getOrgId())
                        .projectId(request.getProjectId())
                        .projectCode(activityLockInterest.getProjectCode())
                        .taskId(request.getTaskId())
                        .recordId(info.getId())
                        .status(0)
                        .useAmount(new BigDecimal(info.getUseAmount()))
                        .luckyAmount(new BigDecimal(info.getLuckyAmount()))
                        .backAmount(new BigDecimal(info.getBackAmount()))
                        .created(new Date())
                        .updated(new Date())
                        .build();
            }).collect(Collectors.toList());
            detailMapper.insertList(activityLockInterestBatchDetails);
        } catch (Exception ex) {
            log.info("modifyActivityOrderInfo error {}", ex);
            return ModifyActivityOrderInfoResponse.newBuilder().setBasicRet(BasicRet.newBuilder().setCode(3)).build();
        }
        return ModifyActivityOrderInfoResponse.newBuilder().setBasicRet(BasicRet.newBuilder().setCode(0)).build();
    }

    public ActivityOrderTaskToFailResponse activityOrderTaskToFail(ActivityOrderTaskToFailRequest request) {
        this.detailMapper.updateStatusToFail(request.getOrgId(), request.getProjectId(), request.getTaskId());
        this.activityLockInterestBatchTaskMapper.updateStatusToFail(request.getTaskId());
        return ActivityOrderTaskToFailResponse.getDefaultInstance();
    }

    private void activityOrderTaskToSuccess(long orgId, long projectId, long taskId) {
        this.detailMapper.updateStatusToSuccess(orgId, projectId, taskId);
        this.activityLockInterestBatchTaskMapper.updateStatusToSuccess(taskId);
    }

    private void activityOrderTaskToFail(long orgId, long projectId, long taskId) {
        this.detailMapper.updateStatusToFail(orgId, projectId, taskId);
        this.activityLockInterestBatchTaskMapper.updateStatusToFail(taskId);
    }
}
