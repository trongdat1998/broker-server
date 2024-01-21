package io.bhex.broker.server.grpc.server.service;

import com.github.pagehelper.Page;
import com.github.pagehelper.PageHelper;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.net.MediaType;
import com.google.gson.JsonObject;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.common.objectstorage.ObjectStorageUtil;
import io.bhex.broker.common.util.FileUtil;
import io.bhex.broker.grpc.admin.*;
import io.bhex.broker.grpc.common.Header;
import io.bhex.broker.server.domain.CommonIni;
import io.bhex.broker.server.domain.KycLevelGrade;
import io.bhex.broker.server.domain.NoticeBusinessType;
import io.bhex.broker.server.domain.UserVerifyStatus;
import io.bhex.broker.server.grpc.server.service.activity.ActivityConfig;
import io.bhex.broker.server.grpc.server.service.po.UserVerifyReasonPO;
import io.bhex.broker.server.model.*;
import io.bhex.broker.server.model.BrokerKycConfig;
import io.bhex.broker.server.primary.mapper.*;
import io.bhex.broker.server.util.BeanCopyUtils;
import io.bhex.broker.server.util.BrokerMessageProducer;
import io.bhex.broker.server.util.PageUtil;
import io.bhex.guild.grpc.gtp.GTPEventMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;
import tk.mybatis.mapper.entity.Example;

import javax.annotation.Resource;
import java.io.File;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.grpc.server.service
 * @Author: ming.xu
 * @CreateDate: 23/08/2018 9:21 PM
 * @Copyright（C）: 2018 BHEX Inc. All rights reserved.
 */
@Slf4j
@Service
public class UserVerifyService {

    private static final String KYC_PASS_ADD_GTP = "kyc_pass_add_gtp";

    @Resource
    UserMapper userMapper;
    @Resource
    UserVerifyMapper userVerifyMapper;
    @Resource
    LoginLogMapper loginLogMapper;

    @Resource
    UserVerifyHistoryMapper userVerifyHistoryMapper;

    @Resource
    UserKycApplyMapper userKycApplyMapper;

    @Resource
    UserVerifyReasonMapper userVerifyReasonMapper;

    @Resource
    BrokerKycConfigMapper brokerKycConfigMapper;

    @Resource
    private BasicService basicService;

    @Resource
    private BrokerMessageProducer brokerMessageProducer;

    @Resource
    private CommonIniService commonIniService;

    @Resource
    private PushDataService pushDataService;

    @Resource
    private FileStorageService fileStorageService;
    @Resource
    private NoticeTemplateService noticeTemplateService;

    public Integer count(Long userId, Long brokerId, int verifyStatus, int nationality, int cardType, long startTime, long endTime) {
        return userVerifyMapper.countByBrokerId(userId, brokerId, verifyStatus, nationality, cardType, startTime, endTime);
    }

    public ListUnverifiedUserReply queryUnverifiedUser(QueryUnverifiedUserRequest request) {
        Example example = Example.builder(UserVerify.class)
                .orderByDesc("id")
                .build();
        Page page = PageHelper.startPage(request.getCurrent(), request.getPageSize());

        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("orgId", request.getBrokerId())
                .andEqualTo("verifyStatus", UserVerifyStatus.UNDER_REVIEW.value());
        if (request.getUserId() > 0) {
            criteria.andEqualTo("userId", request.getUserId());
        }
        int level = request.getLevel();
        if (level > 0) {
            if (level == 2) {
                criteria.andIn("kycLevel", Arrays.asList(20, 25));
            } else if (level == 3) {
                criteria.andEqualTo("kycLevel", 30);
            }
        }
        List<UserVerify> userVerifies = userVerifyMapper.selectByExample(example);

        List<UserVerifyDetail> result = new ArrayList<>();
        for (UserVerify uf : userVerifies) {
            UserVerifyDetail.Builder builder = UserVerifyDetail.newBuilder();
            BeanCopyUtils.copyPropertiesIgnoreNull(uf, builder);
            builder.setCardTypeStr(basicService.getCardTypeTable().get(uf.getCardType(), request.getLocale()));
            result.add(builder.build());
        }

        ListUnverifiedUserReply reply = ListUnverifiedUserReply.newBuilder()
                .setCurrent(request.getCurrent())
                .setPageSize(request.getPageSize())
                .setTotal((int) page.getTotal())
                .addAllUserVerifyDetails(result)
                .build();
        return reply;

    }
    public ListUnverifiedUserReply queryUnverifiedUser(Integer current, Integer pageSize, Long userId, Long brokerId, String locale) {
        //count unverified user
        Integer total = count(userId, brokerId, UserVerifyStatus.UNDER_REVIEW.value(), 0, 0, 0L, 0L);
        PageUtil.Page page = PageUtil.pageCount(current, pageSize, total);
        List<UserVerify> userVerifies = userVerifyMapper.queryVerifyInfo(page.getStart(), page.getOffset(), userId, brokerId,
                UserVerifyStatus.UNDER_REVIEW.value(), 0, 0, 0L, 0L);
        List<UserVerifyDetail> result = new ArrayList<>();
        for (UserVerify uf : userVerifies) {
            UserVerifyDetail.Builder builder = UserVerifyDetail.newBuilder();
            BeanCopyUtils.copyPropertiesIgnoreNull(uf, builder);
            builder.setCardTypeStr(basicService.getCardTypeTable().get(uf.getCardType(), locale));
            result.add(builder.build());
        }

        ListUnverifiedUserReply reply = ListUnverifiedUserReply.newBuilder()
                .setCurrent(current)
                .setPageSize(pageSize)
                .setTotal(total)
                .addAllUserVerifyDetails(result)
                .build();
        return reply;
    }

    public QueryUserVerifyListReply queryUserVerifyList(QueryUserVerifyListRequest request) {

        Example example = Example.builder(UserVerify.class)
                .orderByAsc("updated")
                .build();
        Page page = PageHelper.startPage(request.getCurrent(), request.getPageSize());


        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("orgId", request.getBrokerId());

        if(!StringUtils.isEmpty(request.getCardNo())){
            String cardNoHash = UserVerify.hashCardNo(request.getCardNo());
            log.info("查询cardNo：{}  hash:{}  orgId:{}",request.getCardNo(),cardNoHash,request.getBrokerId());
            List<UserVerify> userVerifyList = userVerifyMapper.queryByOrgIdAndCardNoHash(request.getBrokerId(), cardNoHash);
            if(!userVerifyList.isEmpty()){
                List<Long> userIds = userVerifyList.stream().map(UserVerify::getUserId).collect(Collectors.toList());
                log.info("获取的userIds：{} ",userIds);
                criteria.andIn("userId",userIds);
            }else{
                return QueryUserVerifyListReply.getDefaultInstance();
            }
        }
        if (request.getUserId() > 0) {
            criteria.andEqualTo("userId", request.getUserId());
        }
        int level = request.getLevel();
        if (level > 0) {
            if (level == 2) {
                criteria.andIn("kycLevel", Arrays.asList(20, 25));
            } else if (level == 3) {
                criteria.andEqualTo("kycLevel", 30);
            }
        }
        if (request.getVerifyStatus() > 0) {
            criteria.andEqualTo("verifyStatus", request.getVerifyStatus());
        }
        if (request.getNationality() > 0) {
            criteria.andEqualTo("nationality", request.getNationality());
        }
        if (request.getCardType() > 0) {
            criteria.andEqualTo("cardType", request.getCardType());
        }
        if (request.getStartTime() > 0) {
            criteria.andGreaterThanOrEqualTo("created", request.getStartTime());
        }
        if (request.getEndTime() > 0) {
            criteria.andLessThanOrEqualTo("created", request.getEndTime());
        }


        List<UserVerify> userVerifies = userVerifyMapper.selectByExample(example);

        List<UserVerifyDetail> result = new ArrayList<>();
        for (UserVerify uf : userVerifies) {
            result.add(toUserVerifyDetail(uf, request.getLocale(), false));
        }

        QueryUserVerifyListReply reply = QueryUserVerifyListReply.newBuilder()
                .setCurrent(request.getCurrent())
                .setPageSize(request.getPageSize())
                .setTotal((int) page.getTotal())
                .addAllUserVerifyDetails(result)
                .build();
        return reply;
    }

//    public QueryUserVerifyListReply queryUserVerifyList(QueryUserVerifyListRequest request) {
//
//        Integer total = count(request.getUserId(), request.getBrokerId(), request.getVerifyStatus(), request.getNationality(),
//                request.getCardType(), request.getStartTime(), request.getEndTime());
//        PageUtil.Page page = PageUtil.pageCount(request.getCurrent(), request.getPageSize(), total);
//        List<UserVerify> userVerifies = userVerifyMapper.queryVerifyInfo(page.getStart(), page.getOffset(), request.getUserId(), request.getBrokerId(),
//                request.getVerifyStatus(), request.getNationality(), request.getCardType(), request.getStartTime(), request.getEndTime());
//        List<UserVerifyDetail> result = new ArrayList<>();
//        for (UserVerify uf : userVerifies) {
//            UserVerifyDetail.Builder builder = UserVerifyDetail.newBuilder();
//            BeanCopyUtils.copyPropertiesIgnoreNull(uf, builder);
//            builder.setCardTypeStr(Strings.nullToEmpty(basicService.getCardTypeTable().get(uf.getCardType(), request.getLocale())));
//            builder.setNationalityStr(Strings.nullToEmpty(basicService.getCountryName(uf.getNationality(), request.getLocale())));
//            result.add(builder.build());
//        }
//
//        QueryUserVerifyListReply reply = QueryUserVerifyListReply.newBuilder()
//                .setCurrent(request.getCurrent())
//                .setPageSize(request.getPageSize())
//                .setTotal(total)
//                .addAllUserVerifyDetails(result)
//                .build();
//        return reply;
//    }

    public UserVerifyDetail getVerifyUser(Long userVerifyId, Long brokerId, String locale, boolean decrypted) {
        UserVerify userVerify = userVerifyMapper.getById(userVerifyId);
        UserVerifyDetail.Builder builder = UserVerifyDetail.newBuilder();
        if (null != userVerify && userVerify.getOrgId().equals(brokerId)) {
            return toUserVerifyDetail(userVerify, locale, decrypted);
        }

        return builder.build();
    }

    public UpdateVerifyUserReply updateVerifyUser(Long userVerifyId, int verifyStatus, Long reasonId, Long adminUserId, Long brokerId, String remark) {
        UserVerify userVerify = userVerifyMapper.getById(userVerifyId);
        UpdateVerifyUserReply.Builder builder = UpdateVerifyUserReply.newBuilder();
        if (null != userVerify && userVerify.getKycLevel() > 0) {
            //update verify staus
            boolean isOk = updateVerifyResult(userVerify, verifyStatus, reasonId, null);
            //add verify history
            if (isOk) {
                UserVerifyHistory history = UserVerifyHistory.builder()
                        .adminUserId(adminUserId)
                        .userVerifyId(userVerifyId)
                        .createdAt(new Timestamp(System.currentTimeMillis()))
                        .infoUploadAt(new Timestamp(userVerify.getUpdated()))
                        .remark(remark)
                        .verifyReasonId(reasonId)
                        .status(verifyStatus)
                        .build();
                userVerifyHistoryMapper.insert(history);

                boolean addGtp = false;
                CommonIni commonIni = commonIniService.getCommonIni(userVerify.getOrgId(), KYC_PASS_ADD_GTP);
                if (commonIni != null && Boolean.getBoolean(commonIni.getIniValue())) {
                    addGtp = true;
                }

                if (verifyStatus == UserVerifyStatus.PASSED.value()) {
                    //第一次KYC通过数据推给需要的客户
                    pushDataService.userKycMessage(brokerId, userVerify.getUserId());
                }
                if (verifyStatus == UserVerifyStatus.PASSED.value() && addGtp) {
                    long count = userVerifyHistoryMapper.listAll(brokerId, userVerifyId).stream()
                            .filter(v -> v.getStatus() == UserVerifyStatus.PASSED.value()).count();
                    if (count == 1) {
                        //KYC 第一次认证通过增加算力值
                        GTPEventMessage gtpEventMessage = GTPEventMessage
                                .newBuilder()
                                .setUserId(userVerify.getUserId())
                                .setType(ActivityConfig.GtpType.KYC.getType())
                                .build();

                        brokerMessageProducer
                                .sendMessage(ActivityConfig.CARD_GTP_TOPIC, String.valueOf(ActivityConfig.KYC), gtpEventMessage);
                        log.info("kyc pass userId : {}", userVerify.getUserId());
                    }
                }
                sendVerifyPush(brokerId, userVerify.getUserId(), UserVerifyStatus.fromValue(verifyStatus), history.getId());
            }
            builder.setResult(true);
        } else {
            builder.setResult(false);
        }

        return builder.build();
    }

    private void sendVerifyPush(long orgId, long userId, UserVerifyStatus verifyStatus, Long verifyHistoryId) {
        if (verifyStatus != UserVerifyStatus.PASSED && verifyStatus != UserVerifyStatus.REFUSED) {
            return;
        }
        User user = userMapper.getByOrgAndUserId(orgId, userId);
        Header header = Header.newBuilder().setOrgId(orgId).setUserId(userId).build();
        List<LoginLog> loginLogs = loginLogMapper.queryLastLoginLogs(userId, 1);
        if (!CollectionUtils.isEmpty(loginLogs)) {
            header = header.toBuilder().setLanguage(Strings.nullToEmpty(loginLogs.get(0).getLanguage())).build();
        }
        NoticeBusinessType businessType = verifyStatus == UserVerifyStatus.PASSED
                ? NoticeBusinessType.ADMIN_KYC_VERIFY_SUC : NoticeBusinessType.ADMIN_KYC_VERIFY_FAIL;

        if (!StringUtils.isEmpty(user.getEmail())) {
            noticeTemplateService.sendEmailNotice(header, userId, businessType, user.getEmail(), null);
        }

        if (!StringUtils.isEmpty(user.getNationalCode()) && !StringUtils.isEmpty(user.getMobile())) {
            noticeTemplateService.sendSmsNotice(header, userId, businessType, user.getNationalCode(), user.getMobile(), null);
        }

        Map<String, String> pushUrlData = Maps.newHashMap();

        noticeTemplateService.sendBizPushNotice(Header.newBuilder().setOrgId(orgId).setUserId(userId).build(),
                userId, businessType, "verify_history_" + verifyHistoryId, null, pushUrlData);
    }

    public ListVerifyReasonReply listVerifyReason(String locale) {
        List<UserVerifyReasonPO> verifyReasons = userVerifyReasonMapper.listAll(locale);
        List<VerifyReasonDetail> result = new ArrayList<>();
        if (!CollectionUtils.isEmpty(verifyReasons)) {
            for (UserVerifyReasonPO r : verifyReasons) {
                VerifyReasonDetail.Builder builder = VerifyReasonDetail.newBuilder();
                BeanCopyUtils.copyPropertiesIgnoreNull(r, builder);
                result.add(builder.build());
            }
        }

        ListVerifyReasonReply reply = ListVerifyReasonReply.newBuilder()
                .addAllVerifyReasonDetails(result)
                .build();
        return reply;
    }

    public ListVerifyHistoryReply listVerifyHistory(Long userVerifyId, Long brokerId) {
        List<UserVerifyHistory> verifyHistories = userVerifyHistoryMapper.listAll(brokerId, userVerifyId);

        List<VerifyHistoryDetail> result = new ArrayList<>();
        for (UserVerifyHistory h : verifyHistories) {
            VerifyHistoryDetail.Builder builder = VerifyHistoryDetail.newBuilder();
            BeanCopyUtils.copyPropertiesIgnoreNull(h, builder);
            builder.setVerifyStatus(h.getStatus());
            result.add(builder.build());
        }

        ListVerifyHistoryReply reply = ListVerifyHistoryReply.newBuilder()
                .addAllVerifyHistoryDetails(result)
                .build();
        return reply;
    }

    public UserVerifyHistory getPassedVerify(Long userVerifyId) {
        return userVerifyHistoryMapper.getPassedVerify(userVerifyId);
    }

    public boolean openThirdKycAuth(OpenThirdKycAuthRequest request) {
        UserVerify userVerify = userVerifyMapper.getByUserId(request.getUserId());
        if (userVerify != null && userVerify.getVerifyStatus() == UserVerifyStatus.PASSED.value()
                && userVerify.getKycLevel() / 10 == 2) {
            userVerify.setKycLevel(30);
            userVerify.setVerifyStatus(UserVerifyStatus.NONE.value());
            userVerifyMapper.updateByPrimaryKey(userVerify);
            return true;
        }
        log.error("openThirdKycAuth req info error : {}", request);
        return false;
    }

    public boolean degradeBrokerKycLevel(DegradeBrokerKycLevelRequest request) {
        UserVerify userVerify = userVerifyMapper.getByUserId(request.getUserId());
        KycLevelGrade userGrade = KycLevelGrade.fromKycLevel(userVerify.getKycLevel());
        if (userGrade == null) {
            log.error("can not get KycLevelGrade for kycLevel: {}", userVerify.getKycLevel());
            throw new BrokerException(BrokerErrorCode.SYSTEM_ERROR);
        }

        switch (userGrade) {
            case VIP:
                BrokerKycConfig brokerKycConfig = basicService.getBrokerKycConfig(request.getBrokerId(), userVerify.getNationality());
                userVerify.setKycLevel(brokerKycConfig == null ? 20 : brokerKycConfig.getSecondKycLevel());
                userVerify.setVerifyStatus(UserVerifyStatus.PASSED.value());
                userVerify.setVideoUrl(""); // 三级降级到二级要清空视频认证url
                break;
            case SENIOR:
                userVerify.setKycLevel(10);
                // 二级降级到一级清空照片认证URL
                userVerify.setCardFrontUrl("");
                userVerify.setCardBackUrl("");
                userVerify.setCardHandUrl("");
                userVerify.setFacePhotoUrl("");
                userVerify.setFaceVideoUrl("");
                userVerify.setVerifyStatus(UserVerifyStatus.PASSED.value());
                break;
            case BASIC:
                // 一级重置清空姓名和证件号码
                userVerify.setFirstName("");
                userVerify.setSecondName("");
                userVerify.setCountryCode("");
                userVerify.setNationality(0l);
                userVerify.setCardNo("");
                userVerify.setCardNoHash("");
                userVerify.setVerifyStatus(UserVerifyStatus.NONE.value());
                userVerify.setKycLevel(0);
                break;
        }

        userVerifyMapper.updateByPrimaryKey(userVerify);
        return true;
    }

    public GetBrokerKycConfigsReply getBrokerKycConfigs(long brokerId) {
        List<BrokerKycConfig> configs = brokerKycConfigMapper.queryByBrokerId(brokerId);
        if (CollectionUtils.isEmpty(configs)) {
            return GetBrokerKycConfigsReply.newBuilder().build();
        }

        List<io.bhex.broker.grpc.admin.BrokerKycConfig> result = configs.stream().map(c -> {
            io.bhex.broker.grpc.admin.BrokerKycConfig.Builder builder = io.bhex.broker.grpc.admin.BrokerKycConfig.newBuilder();
            BeanCopyUtils.copyPropertiesIgnoreNull(c, builder);
            builder.setKycLevel(c.getSecondKycLevel());
            builder.setBrokerId(c.getOrgId());
            return builder.build();
        }).collect(Collectors.toList());
        return GetBrokerKycConfigsReply.newBuilder().addAllConfigs(result).build();
    }
    public void addBrokerKycConfig(AddBrokerKycConfigRequest request) {

        log.info("addBrokerKycConfig:{}", request);
        BrokerKycConfig kycConfig = basicService.getBrokerKycConfig(request.getBrokerId(), request.getCountryId(), request.getCountryId());
        if (kycConfig != null) {
            kycConfig.setSecondKycLevel(request.getKycLevel());
            kycConfig.setUpdated(System.currentTimeMillis());
            kycConfig.setWebankAndroidLicense(request.getWebankAndroidLicense());
            kycConfig.setWebankAppId(request.getWebankAppId());
            kycConfig.setWebankIosLicense(request.getWebankIosLicense());
            kycConfig.setAppName(request.getAppName());
            kycConfig.setCompanyName(request.getCompanyName());
            kycConfig.setWebankAppSecret(request.getWebankAppSecret());
            int r = brokerKycConfigMapper.updateByPrimaryKeySelective(kycConfig);
            log.info("updateBrokerKycConfig r:{}", r);
        } else {
            BrokerKycConfig brokerKycConfig = BrokerKycConfig.builder()
                    .countryId(request.getCountryId())
                    .created(System.currentTimeMillis())
                    .orgId(request.getBrokerId())
                    .secondKycLevel(request.getKycLevel())
                    .status(1)
                    .updated(System.currentTimeMillis())
                    .webankAndroidLicense(request.getWebankAndroidLicense())
                    .webankAppId(request.getWebankAppId())
                    .webankIosLicense(request.getWebankIosLicense())
                    .appName(request.getAppName())
                    .companyName(request.getCompanyName())
                    .webankAppSecret(request.getWebankAppSecret())
                    .build();
            int r = brokerKycConfigMapper.insertSelective(brokerKycConfig);
            log.info("addBrokerKycConfig r:{}", r);
        }


    }

    private UserVerifyDetail toUserVerifyDetail(UserVerify userVerify, String locale, boolean decrypted) {
        UserVerify decryptedUserVerify = UserVerify.decrypt(userVerify);
        UserVerifyDetail.Builder builder = UserVerifyDetail.newBuilder();
        BeanCopyUtils.copyPropertiesIgnoreNull(decryptedUserVerify, builder);

        // 尝试解密url对应的加密内容
        if (decrypted && !StringUtils.isEmpty(userVerify.getCardFrontUrl())) {
            builder.setCardFrontUrl(getPhotoPlainUrlKey(userVerify, userVerify.getCardFrontUrl()));
        }
        if (decrypted && !StringUtils.isEmpty(userVerify.getCardBackUrl())) {
            builder.setCardBackUrl(getPhotoPlainUrlKey(userVerify, userVerify.getCardBackUrl()));
        }
        if (decrypted && !StringUtils.isEmpty(userVerify.getCardHandUrl())) {
            builder.setCardHandUrl(getPhotoPlainUrlKey(userVerify, userVerify.getCardHandUrl()));
        }
        if (decrypted && !StringUtils.isEmpty(userVerify.getFacePhotoUrl())) {
            builder.setFacePhotoUrl(getPhotoPlainUrlKey(userVerify, userVerify.getFacePhotoUrl()));
        }
        if (decrypted && !StringUtils.isEmpty(userVerify.getFaceVideoUrl())) {
            builder.setFaceVideoUrl(getVideoPlainUrlKey(userVerify, userVerify.getFaceVideoUrl()));
        }

        if (decrypted && !StringUtils.isEmpty(userVerify.getVideoUrl())) {
            builder.setVideoUrl(getVideoPlainUrlKey(userVerify, userVerify.getVideoUrl()));
        }

        String cardTypeStr = StringUtils.isEmpty(basicService.getCardTypeTable().get(decryptedUserVerify.getCardType(), locale)) ? ""
                : basicService.getCardTypeTable().get(decryptedUserVerify.getCardType(), locale);
        String countryName = StringUtils.isEmpty(basicService.getCountryName(decryptedUserVerify.getNationality(), locale)) ? ""
                : basicService.getCountryName(decryptedUserVerify.getNationality(), locale);

        if (StringUtils.isEmpty(cardTypeStr)) {
            cardTypeStr = StringUtils.isEmpty(basicService.getCardTypeTable().get(decryptedUserVerify.getCardType(), "en_US")) ? ""
                    : basicService.getCardTypeTable().get(decryptedUserVerify.getCardType(), "en_US");
        }
        if (StringUtils.isEmpty(countryName)) {
            countryName = StringUtils.isEmpty(basicService.getCountryName(decryptedUserVerify.getNationality(), "en_US")) ? ""
                    : basicService.getCountryName(decryptedUserVerify.getNationality(), "en_US");
        }

        builder.setNationalityStr(countryName);
        builder.setCardTypeStr(cardTypeStr);
        builder.setCountryName(countryName);
        return builder.build();
    }

    @SuppressWarnings("UnstableApiUsage")
    public String getPhotoPlainUrlKey(UserVerify userVerify, String fileKey) {
        return getPlainUrlKey(userVerify, fileKey, MediaType.ANY_IMAGE_TYPE);
    }

    @SuppressWarnings("UnstableApiUsage")
    private String getVideoPlainUrlKey(UserVerify userVerify, String fileKey) {
        return getPlainUrlKey(userVerify, fileKey, MediaType.ANY_AUDIO_TYPE);
    }

    @SuppressWarnings("UnstableApiUsage")
    private String getPlainUrlKey(UserVerify userVerify, String fileKey, MediaType mediaType) {
        if (!fileKey.startsWith("encrypt")) {
            return fileKey;
        }
        File plainFile = fileStorageService.decryptFile(fileKey, userVerify.getDataSecret());

        String fileSuffix = FileUtil.getFileSuffix(fileKey, "");
        String plainFileKey;
        try {
            // 文件路经格式为 plain/{orgId}/{userId}/sha256_file_data.xxx
            plainFileKey = String.format("plain/%s/%s/%s", userVerify.getOrgId(), userVerify.getUserId(),
                    ObjectStorageUtil.sha256FileName(plainFile, fileSuffix));
            log.info("after decrypted, plainFileKey is: {}", plainFileKey);
        } catch (IOException e) {
            log.error("uploadEncryptedFile error.", e);
            throw new BrokerException(BrokerErrorCode.SYSTEM_ERROR);
        }
        fileStorageService.uploadFile(plainFileKey, mediaType, plainFile);
        log.info("uploadFile success, plainFileKey is: {}", plainFileKey);
        return plainFileKey;
    }

    public boolean updateVerifyResult(UserVerify userVerify, Integer verifyStatus,
                                      Long verifyReasonId, String verifyMessage) {
        if (userVerify == null) {
            return false;
        }

        try {
            int updated = userVerifyMapper.updateVerifyStatusByUserId(userVerify.getUserId(),
                    userVerify.getOrgId(), verifyStatus, verifyReasonId);
            Long kycApplyId = userVerify.getKycApplyId();
            if (kycApplyId != null && kycApplyId > 0) {
                userKycApplyMapper.updateVerifyStatus(kycApplyId, userVerify.getUserId(), userVerify.getOrgId(),
                        verifyReasonId, verifyStatus, verifyMessage, System.currentTimeMillis());
            }

            return updated == 1;
        } catch (Throwable e) {
            String errMsg = String.format("updateVerifyResult error, userId: %s, orgId: %s",
                    userVerify.getUserId(), userVerify.getOrgId());
            log.error(errMsg, e);
            return false;
        }
    }

    public boolean updateVerifyResultPassed(UserVerify userVerify) {
        return updateVerifyResult(userVerify, UserVerifyStatus.PASSED.value(), 0L, "success");
    }

    public List<UserVerify> listByUserIds(Set<Long> userIds) {
        Example exp = new Example(UserVerify.class);
        exp.createCriteria().andIn("userId", userIds);

        return userVerifyMapper.selectByExample(exp);
    }

    public String getRefusedReason(UserVerify userVerify, String language) {
        String refusedReason = "";
        if (userVerify.getVerifyStatus() == UserVerifyStatus.REFUSED.value()) {
            int refusedReasonId = userVerify.getVerifyReasonId() == null ? 0 : userVerify.getVerifyReasonId().intValue();
            if (refusedReasonId > 0) {
                UserVerifyReasonPO verifyReason = userVerifyReasonMapper.getVerifyReason(refusedReasonId, language);
                if (verifyReason == null) {
                    verifyReason = userVerifyReasonMapper.getVerifyReason(refusedReasonId, Locale.US.toString());
                }
                if (verifyReason != null) {
                    refusedReason = verifyReason.getReason();
                }
            } else { //管理自己写的拒绝原因
                UserVerifyHistory userVerifyHistory = userVerifyHistoryMapper.getRejectedVerify(userVerify.getId());
                if (userVerifyHistory != null) {
                    refusedReason = Strings.nullToEmpty(userVerifyHistory.getRemark());
                }
            }
        }
        return refusedReason;
    }
}
