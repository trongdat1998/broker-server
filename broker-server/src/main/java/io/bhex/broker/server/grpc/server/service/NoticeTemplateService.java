/**********************************
 *@项目名称: server-parent
 *@文件名称: io.bhex.broker.server.grpc.server.service
 *@Date 2018/9/9
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
package io.bhex.broker.server.grpc.server.service;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.protobuf.TextFormat;
import io.bhex.base.common.*;
import io.bhex.base.idgen.api.ISequenceGenerator;
import io.bhex.broker.grpc.common.Header;
import io.bhex.broker.server.domain.BaseConfigConstants;
import io.bhex.broker.server.domain.NoticeBusinessType;
import io.bhex.broker.server.domain.SwitchStatus;
import io.bhex.broker.server.grpc.client.service.GrpcCommonServerService;
import io.bhex.broker.server.grpc.client.service.GrpcEmailService;
import io.bhex.broker.server.grpc.client.service.GrpcPushService;
import io.bhex.broker.server.grpc.client.service.GrpcSmsService;
import io.bhex.broker.server.model.*;
import io.bhex.broker.server.primary.mapper.AppBusinessPushRecordMapper;
import io.bhex.broker.server.primary.mapper.LoginLogMapper;
import io.bhex.broker.server.push.bo.ThirdPushTypeEnum;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import javax.annotation.Nullable;
import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.*;
import java.util.stream.Collectors;

import static io.bhex.broker.server.domain.NoticeBusinessType.*;

@Slf4j
@Service
public class NoticeTemplateService {

    @Resource
    private AppBusinessPushRecordMapper appBusinessPushRecordMapper;

    @Resource
    private GrpcSmsService grpcSmsService;

    @Resource
    private GrpcEmailService grpcEmailService;

    @Resource
    private GrpcPushService grpcPushService;

    @Resource
    private AppPushService appPushService;
    @Resource
    private BaseBizConfigService baseBizConfigService;
    @Resource
    private ISequenceGenerator sequenceGenerator;
    @Resource
    private LoginLogMapper loginLogMapper;
    @Resource
    private GrpcCommonServerService grpcCommonServerService;

//    @Resource(name = "stringRedisTemplate")
//    private StringRedisTemplate redisTemplate;

    private static final Map<String, List<NoticeBusinessType>> pushBizGroupMap = Maps.newHashMap();

    static {
        List<NoticeBusinessType> otcBizList = Lists.newArrayList(
                BUY_CREATE_MSG_TO_BUYER,
                BUY_CREATE_MSG_TO_SELLER,
                BUY_CREATE_MSG_TO_BUYER_TIME,
                SELL_CREATE_MSG_TO_BUYER_TIME,
                PAY_MSG_TO_BUYER,
                PAY_MSG_TO_SELLER,
                BUY_APPEAL_MSG_TO_BUYER,
                BUY_APPEAL_MSG_TO_SELLER,
                CANCEL_MSG_TO_BUYER,
                CANCEL_MSG_TO_SELLER,
                FINISH_MSG_TO_BUYER,
                FINISH_MSG_TO_SELLER, SELL_CREATE_MSG_TO_BUYER,
                SELL_CREATE_MSG_TO_SELLER,
                SELL_APPEAL_MSG_TO_BUYER,
                SELL_APPEAL_MSG_TO_SELLER,
                ORDER_AUTO_CANCEL,
                ORDER_AUTO_APPEAL_TO_BUYER,
                ORDER_AUTO_APPEAL_TO_SELLER,
                ITEM_AUTO_OFFLINE_SMALL_QUANTITY);
        pushBizGroupMap.put("kyc", Lists.newArrayList(ADMIN_KYC_VERIFY_SUC, ADMIN_KYC_VERIFY_FAIL,
                JOIN_HOBBIT_LEADER, QUIT_HOBBIT_LEADER, QUITING_HOBBIT_LEADER) //这几个类型只有hbtc用暂时就写在这吧
        );

        pushBizGroupMap.put("assetChange", Lists.newArrayList(AIRDROP_NOTICE, DEPOSIT_SUCCESS, WITHDRAW_SUCCESS_WITH_DETAIL,
                WITHDRAW_VERIFY_REJECTED, WITHDRAW_VERIFY_REJECTED_1, WITHDRAW_VERIFY_REJECTED_2, WITHDRAW_VERIFY_REJECTED_3,
                WITHDRAW_VERIFY_REJECTED_4, WITHDRAW_VERIFY_REJECTED_5, SPOT_BUY_TRADE_SUCCESS, SPOT_SELL_TRADE_SUCCESS, CONTRACT_BUY_OPEN_TRADE_SUCCESS,
                CONTRACT_BUY_CLOSE_TRADE_SUCCESS, CONTRACT_SELL_OPEN_TRADE_SUCCESS, CONTRACT_SELL_CLOSE_TRADE_SUCCESS));
        pushBizGroupMap.put("otc", otcBizList);
        pushBizGroupMap.put("liquidation", Lists.newArrayList(LIQUIDATION_ALERT, LIQUIDATED_NOTIFY, ADL_NOTIFY));
        pushBizGroupMap.put("notice", Lists.newArrayList(LOGIN_SUCCESS, REGISTER_SUCCESS));
        pushBizGroupMap.put("margin", Lists.newArrayList(MARGIN_WARN, MARGIN_APPEND, MARGIN_LIQUIDATION_ALERT, MARGIN_LIQUIDATED_NOTIFY,LIQUIDATED_TO_UPDATE_MARGIN));
        pushBizGroupMap.put("quote", Lists.newArrayList(REALTIME_UP_NOTIFY, REALTIME_DOWN_NOTIFY));
        pushBizGroupMap.put("compliance", Lists.newArrayList(COMPLIANCE_KYC_VERIFY_SUC, COMPLIANCE_KYC_VERIFY_FAIL, COMPLIANCE_INACTIVE_USER));

    }

    private static Map<Long, List<NoticeBusinessType>> orgBizTypeMap = Maps.newHashMap();
    private static Map<Long, Set<Long>> appPushBlackList = Maps.newHashMap();

    @PostConstruct
    @Scheduled(cron = "0 0/5 * * * ?")
    private void refreshBizType() {
        List<BaseConfigInfo> configs = baseBizConfigService.getConfigsByGroupsAndKey(0, Lists.newArrayList(BaseConfigConstants.APPPUSH_CONFIG_GROUP), "orgBizGroup");
        if (CollectionUtils.isEmpty(configs)) {
            return;
        }
        Map<Long, List<NoticeBusinessType>> _orgBizTypeMap = Maps.newHashMap();
        configs.forEach(c -> {
            List<NoticeBusinessType> bizTypes = Lists.newArrayList();
            String[] groupArr = c.getConfValue().split(",");
            for (String s : groupArr) {
                bizTypes.addAll(pushBizGroupMap.getOrDefault(s, Lists.newArrayList()));
            }
            _orgBizTypeMap.put(c.getOrgId(), bizTypes);
        });
        orgBizTypeMap = _orgBizTypeMap;
        List<BaseConfigInfo> blackUserConfigs = baseBizConfigService.getConfigsByGroupsAndKey(0,
                Lists.newArrayList(BaseConfigConstants.APPPUSH_CONFIG_GROUP), "black.user.list");
        if (!CollectionUtils.isEmpty(blackUserConfigs)) {
            for (BaseConfigInfo blackUserConfig : blackUserConfigs) {
                appPushBlackList.put(blackUserConfig.getOrgId(),
                        Arrays.stream(blackUserConfig.getConfValue().split(",")).map(Long::parseLong).collect(Collectors.toSet()));
            }
        }
    }

    public boolean canSendPush(long orgId, NoticeBusinessType businessType) {
        SwitchStatus switchStatus = baseBizConfigService.getConfigSwitchStatus(orgId, BaseConfigConstants.APPPUSH_CONFIG_GROUP, "ALL_SITE");
        if (!switchStatus.isOpen()) {
            return false;
        }
        if (!orgBizTypeMap.getOrDefault(orgId, Lists.newArrayList()).contains(businessType)) {
            return false;
        }
        return true;
    }

    public boolean canSendPush(long orgId, long userId, NoticeBusinessType businessType) {
        if (!canSendPush(orgId, businessType)) {
            return false;
        }
        if (appPushBlackList.getOrDefault(orgId, Sets.newHashSet()).contains(userId)) {
            return false;
        }
        return true;
    }

    /**
     * 发送系统业务push
     *
     * @param header
     * @param userId
     * @param businessType
     * @param reqOrderId       可以为空，自动生成一个,全局唯一 一个orderid只能发一次
     * @param contentParamJson kv对应模板中的key值
     * @param pushUrlData      url后面带的参数 可以为空
     */
    @Async
    public void sendBizPushNotice(Header header, Long userId, NoticeBusinessType businessType, String reqOrderId,
                                  @Nullable JsonObject contentParamJson, Map<String, String> pushUrlData) {
        if (!canSendPush(header.getOrgId(), userId, businessType)) {
            return;
        }

        AppPushDevice pushDevice = appPushService.getPushDevice(userId);
        if (pushDevice == null) {
            log.info("No pushDevice found{}", header.getUserId());
            return;
        }
        if (pushDevice.getThirdPushType().equalsIgnoreCase("JPUSH")) { //原有jpush无法发送push
            log.info("jpush channel {}", header.getUserId());
            return;
        }
//        if (System.currentTimeMillis() - pushDevice.getUpdated() > 30 * 86400_000L) { //超过30天没有更新就不发了
//            return;
//        }
        if (StringUtils.isEmpty(header.getLanguage()) && StringUtils.isEmpty(pushDevice.getLanguage())) {
            return;
        }

        Map<String, String> reqParam = Maps.newHashMap();
        if (contentParamJson != null && contentParamJson.entrySet().size() > 0) {
            for (Map.Entry<String, JsonElement> entry : contentParamJson.entrySet()) {
                reqParam.put(entry.getKey(), entry.getValue().getAsString());
            }
        }

        String orderId = !StringUtils.isEmpty(reqOrderId) ? reqOrderId : sequenceGenerator.getLong().toString();
        AppBusinessPushRecord pushRecord = AppBusinessPushRecord.builder()
                .orgId(header.getOrgId())
                .userId(userId)
                .pushChannel(pushDevice.getThirdPushType().toUpperCase())
                .pushToken(pushDevice.getDeviceToken())
                .reqOrderId(orderId)
                .businessType(businessType.name())
                .created(System.currentTimeMillis())
                .updated(System.currentTimeMillis())
                .build();
        appBusinessPushRecordMapper.insertSelective(pushRecord);

        SendBusinessPushRequest request = SendBusinessPushRequest.newBuilder()
                .setOrgId(header.getOrgId())
                .setAppChannel(Strings.nullToEmpty(pushDevice.getAppChannel()))
                .setPushChannel(pushDevice.getThirdPushType().toUpperCase())
                .addPushToken(pushDevice.getDeviceToken())
                .setBusinessType(businessType.name())
                .setLanguage(!StringUtils.isEmpty(pushDevice.getLanguage()) ? pushDevice.getLanguage() : header.getLanguage())
                .putAllReqParam(reqParam)
                .setAppId(pushDevice.getAppId())
                .putAllPushUrlData(!CollectionUtils.isEmpty(pushUrlData) ? pushUrlData : Maps.newHashMap())
                .setReqOrderId(orderId)
                .build();
        grpcPushService.sendBusinessPush(request);
    }

    @Async
    public void sendSmsNoticeAsync(Header header, long userId, NoticeBusinessType businessType, String nationalCode, String mobile) {
        sendSmsNotice(header, userId, businessType, nationalCode, mobile, null);
    }

    @Async
    public void sendSmsNoticeAsync(Header header, long userId, NoticeBusinessType businessType, String nationalCode, String mobile, @Nullable JsonObject contentParamJson) {
        sendSmsNotice(header, userId, businessType, nationalCode, mobile, contentParamJson);
    }

    @Async
    public void sendSmsNoticeAsync(Header header, long userId, NoticeBusinessType businessType, String nationalCode, String mobile, @Nullable JsonObject contentParamJson, String bizType) {
        sendSmsNotice(header, userId, businessType, nationalCode, mobile, contentParamJson, bizType);
    }

    public void sendSmsNotice(Header header, long userId, NoticeBusinessType businessType, String nationalCode, String mobile) {
        sendSmsNotice(header, userId, businessType, nationalCode, mobile, null);
    }

    public void sendSmsNotice(Header header, long userId, NoticeBusinessType businessType, String nationalCode, String mobile, @Nullable JsonObject contentParamJson) {
        sendSmsNotice(header, userId, businessType, nationalCode, mobile, contentParamJson, null);
    }

    public void sendApnsPushNotice(Header header, Long userId, NoticeBusinessType businessType, @Nullable JsonObject contentParamJson, String bizType) {
        log.info("sendPushNotice begin, uid: {}", userId);
        AppPushDevice pushDevice = appPushService.getPushDevice(userId);
        if (pushDevice != null) {
            List<String> params = Lists.newArrayList();
            if (contentParamJson != null && contentParamJson.entrySet().size() > 0) {
                for (Map.Entry<String, JsonElement> entry : contentParamJson.entrySet()) {
                    params.add(entry.getValue().getAsString());
                }
            }
            // 苹果推送
            if (pushDevice.getThirdPushType().equalsIgnoreCase(ThirdPushTypeEnum.APNS.name())) {
                try {
                    ApnsNotification apnsNotification = ApnsNotification.newBuilder()
                            .setDeviceToken(pushDevice.getDeviceToken())
                            .setOrgId(header.getOrgId())
                            .setBusinessType(businessType.name())
                            .setLanguage(header.getLanguage())
                            .addAllParams(params)
                            .setAppId(pushDevice.getAppId())
                            .setChannel(pushDevice.getAppChannel())
                            .build();
                    log.info(" send push request:{}", TextFormat.shortDebugString(apnsNotification));
                    grpcPushService.sendApnsNotification(apnsNotification);
                } catch (Exception e) {
                    log.error("send apns push notice has a error", e);
                }
            } else {
                log.info("No pushDevice config found.");
            }
        } else {
            log.info("No pushDevice found{}", header.getUserId());
        }
        log.info("sendPushNotice finish");
    }

    public void sendSmsNotice(Header header, long userId, NoticeBusinessType businessType, String nationalCode, String mobile, @Nullable JsonObject contentParamJson, String bizType) {
        try {
            List<String> params = Lists.newArrayList();
            Map<String, String> reqParam = Maps.newHashMap();
            if (contentParamJson != null && contentParamJson.entrySet().size() > 0) {
                for (Map.Entry<String, JsonElement> entry : contentParamJson.entrySet()) {
                    params.add(entry.getValue().getAsString());
                    reqParam.put(entry.getKey(), entry.getValue().getAsString());
                }
            }

            SimpleSMSRequest request = SimpleSMSRequest.newBuilder()
                    .setOrgId(header.getOrgId())
                    .setUserId(userId)
                    .setTelephone(Telephone.newBuilder().setNationCode(nationalCode).setMobile(mobile).build())
                    .setBusinessType(businessType.name())
                    .setLanguage(header.getLanguage())
                    .addAllParams(params)
                    .putAllReqParam(reqParam)
                    .setUserId(header.getUserId())
                    .setIp(header.getRemoteIp())
                    .build();
            grpcSmsService.sendSmsNotice(request);
        } catch (Exception e) {
            log.warn("send sms notice has a error", e);
        }
    }

    public void sendSmsNoticeSync(Header header, long userId, NoticeBusinessType businessType, String nationalCode, String mobile, List<String> params) {
        try {
            SimpleSMSRequest request = SimpleSMSRequest.newBuilder()
                    .setOrgId(header.getOrgId())
                    .setUserId(userId)
                    .setTelephone(Telephone.newBuilder().setNationCode(nationalCode).setMobile(mobile).build())
                    .setBusinessType(businessType.name())
                    .setLanguage(header.getLanguage())
                    .addAllParams(params)
                    .setSync(true)
                    .build();
            grpcSmsService.sendSmsNotice(request);
        } catch (Exception e) {
            log.warn("send sms:{} type:{} notice has a error", mobile, businessType, e);
        }
    }

    @Async
    public void sendEmailNoticeAsync(Header header, long userId, NoticeBusinessType businessType, String email) {
        sendEmailNotice(header, userId, businessType, email, null);
    }

    @Async
    public void sendEmailNoticeAsync(Header header, long userId, NoticeBusinessType businessType, String email, @Nullable JsonObject contentParamJson) {
        sendEmailNotice(header, userId, businessType, email, contentParamJson);
    }

    @Async
    public void sendEmailNoticeAsync(Header header, long userId, NoticeBusinessType businessType, String email, @Nullable JsonObject contentParamJson, String bizType) {
        sendEmailNotice(header, userId, businessType, email, contentParamJson, bizType);
    }

    public void sendEmailNotice(Header header, long userId, NoticeBusinessType businessType, String email) {
        sendEmailNotice(header, userId, businessType, email, null);
    }

    public void sendEmailNotice(Header header, long userId, NoticeBusinessType businessType, String email, @Nullable JsonObject contentParamJson) {
        sendEmailNotice(header, userId, businessType, email, contentParamJson, null);
    }

    /**
     * 发送主推提示邮件
     *
     * @param header           header
     * @param userId           用户id
     * @param email            邮箱地址
     * @param subject          主题名
     * @param contentParamJson 内容
     */
    public void sendSubjectEmailNotice(Header header, long userId, String email, String subject, @Nullable JsonObject contentParamJson) {
        sendEmailNotice(header, userId, null, email, subject, contentParamJson, null);
    }

//    public void sendEmailNoticeSync(Header header, long userId, NoticeBusinessType businessType, String email, List<String> params) {
//        try {
//
//            SimpleMailRequest request = SimpleMailRequest.newBuilder()
//                    .setOrgId(header.getOrgId())
//                    .setUserId(userId)
//                    .setMail(email)
//                    .setBusinessType(businessType.name())
//                    .setLanguage(header.getLanguage())
//                    .addAllParams(params)
//                    .setSync(true)
//                    .build();
//            grpcEmailService.sendEmailNotice(request);
//        } catch (Exception e) {
//            log.warn("send email:{} type:{} notice has a error", email, businessType, e);
//        }
//    }

    public void sendEmailNotice(Header header, long userId, NoticeBusinessType businessType, String email, @Nullable JsonObject contentParamJson, String bizType) {
        sendEmailNotice(header, userId, businessType, email, null, contentParamJson, bizType);
    }

    private void sendEmailNotice(Header header, long userId, NoticeBusinessType businessType, String email, String subject, @Nullable JsonObject contentParamJson, String bizType) {
        try {
            List<String> params = Lists.newArrayList();
            Map<String, String> reqParam = Maps.newHashMap();
            if (contentParamJson != null && contentParamJson.entrySet().size() > 0) {
                for (Map.Entry<String, JsonElement> entry : contentParamJson.entrySet()) {
                    params.add(entry.getValue().getAsString());
                    reqParam.put(entry.getKey(), entry.getValue().getAsString());
                }
            }

            SimpleMailRequest request = SimpleMailRequest.newBuilder()
                    .setOrgId(header.getOrgId())
                    .setUserId(userId)
                    .setMail(email)
                    .setEmailSubject(subject != null ? subject : "")
                    .setBusinessType(businessType != null ? businessType.name() : "")
                    .setLanguage(header.getLanguage())
                    .addAllParams(params)
                    .putAllReqParam(reqParam)
                    .setIp(header.getRemoteIp())
                    .build();
            grpcEmailService.sendEmailNotice(request);
        } catch (Exception e) {
            log.warn("send email notice has a error", e);
        }
    }

    //此方法会 邮件、短信、push 有哪个发哪个
    @Async
    public void sendTemplateMessage(User user, NoticeBusinessType businessType,  @Nullable JsonObject contentParamJson,
                                    Map<String, String> pushUrlData, String ip) {
        try {
            if (user == null) {
                return;
            }
            SendTemplateMessageRequest.Builder builder = SendTemplateMessageRequest.newBuilder()
                    .setOrgId(user.getOrgId())
                    .setBusinessType(businessType.name())
                    .setIp(Strings.nullToEmpty(ip));
            if (!Strings.isNullOrEmpty(user.getEmail())) {
                builder.setMail(user.getEmail());
            }
            if (!Strings.isNullOrEmpty(user.getMobile())) {
                builder.setTelephone(Telephone.newBuilder().setNationCode(user.getNationalCode()).setMobile(user.getMobile()).build());
            }

            String language = getUserLanguage(user);
            builder.setLanguage(language);

            Map<String, String> reqParam = Maps.newHashMap();
            if (contentParamJson != null && contentParamJson.entrySet().size() > 0) {
                for (Map.Entry<String, JsonElement> entry : contentParamJson.entrySet()) {
                    reqParam.put(entry.getKey(), entry.getValue().getAsString());
                }
            }
            builder.putAllReqParam(reqParam);

            if (!canSendPush(user.getOrgId(), user.getUserId(), businessType)) {
                grpcCommonServerService.sendTemplateMessage(builder.build());
                return;
            }
            AppPushDevice pushDevice = appPushService.getPushDevice(user.getUserId());
            if (pushDevice == null) {
                grpcCommonServerService.sendTemplateMessage(builder.build());
                return;
            }
            if (pushDevice.getThirdPushType().equalsIgnoreCase("JPUSH")) { //原有jpush无法发送push
                grpcCommonServerService.sendTemplateMessage(builder.build());
                return;
            }

            builder.setAppChannel(Strings.nullToEmpty(pushDevice.getAppChannel()))
                    .setPushChannel(pushDevice.getThirdPushType().toUpperCase())
                    .addPushToken(pushDevice.getDeviceToken())
                    .setAppId(pushDevice.getAppId())
                    .putAllPushUrlData(!CollectionUtils.isEmpty(pushUrlData) ? pushUrlData : Maps.newHashMap());
            grpcCommonServerService.sendTemplateMessage(builder.build());
        } catch (Exception e) {
            log.error("sendTemplateMessage error", e);
        }
    }
    @Async
    public void sendTemplateMessage(User user, NoticeBusinessType businessType,  @Nullable JsonObject contentParamJson,
                                    Map<String, String> pushUrlData) {
        sendTemplateMessage(user, businessType, contentParamJson, pushUrlData, null);
    }



    public String getUserLanguage(User user) {
        // 优先获取用户最近一次登录所使用的语言
        List<LoginLog> loginLogs = loginLogMapper.queryLastLoginLogs(user.getUserId(), 1);
        if (!CollectionUtils.isEmpty(loginLogs)) {
            String language = loginLogs.get(0).getLanguage();
            if (!Strings.isNullOrEmpty(language)) {
                return language;
            }
        }

        // 获取用户的默认语言
        if (!Strings.isNullOrEmpty(user.getLanguage())) {
            return user.getLanguage();
        }
        return Locale.US.toString();
    }
}
