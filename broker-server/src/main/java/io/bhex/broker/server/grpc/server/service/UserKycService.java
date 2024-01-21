package io.bhex.broker.server.grpc.server.service;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.Resource;

import io.bhex.base.idgen.api.ISequenceGenerator;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.common.util.JsonUtil;
import io.bhex.broker.grpc.common.Platform;
import io.bhex.broker.grpc.user.kyc.BasicKycVerifyRequest;
import io.bhex.broker.grpc.user.kyc.BasicKycVerifyResponse;
import io.bhex.broker.grpc.user.kyc.FaceKycVerifyRequest;
import io.bhex.broker.grpc.user.kyc.FaceKycVerifyResponse;
import io.bhex.broker.grpc.user.kyc.OrgBatchUpdateKycRequest;
import io.bhex.broker.grpc.user.kyc.OrgBatchUpdateKycResponse;
import io.bhex.broker.grpc.user.kyc.OrgBatchUpdateSeniorKycRequest;
import io.bhex.broker.grpc.user.kyc.OrgBatchUpdateSeniorKycResponse;
import io.bhex.broker.grpc.user.kyc.OrgUpdateKycInfo;
import io.bhex.broker.grpc.user.kyc.OrgUpdateKycResult;
import io.bhex.broker.grpc.user.kyc.OrgUpdateSeniorKycInfo;
import io.bhex.broker.grpc.user.kyc.PhotoKycVerifyRequest;
import io.bhex.broker.grpc.user.kyc.PhotoKycVerifyResponse;
import io.bhex.broker.grpc.user.kyc.SdkPrepareInfo;
import io.bhex.broker.grpc.user.kyc.VideoKycVerifyRequest;
import io.bhex.broker.grpc.user.kyc.VideoKycVerifyResponse;
import io.bhex.broker.grpc.user.kyc.WebankSdkLoginPrepareRequest;
import io.bhex.broker.grpc.user.kyc.WebankSdkLoginPrepareResponse;
import io.bhex.broker.server.domain.BaseConfigConstants;
import io.bhex.broker.server.domain.SwitchStatus;
import io.bhex.broker.server.domain.UserVerifyStatus;
import io.bhex.broker.server.domain.kyc.tencent.NonceTicket;
import io.bhex.broker.server.domain.kyc.tencent.SDKLoginPrepareResponse;
import io.bhex.broker.server.grpc.server.service.kyc.KycVerifyFactory;
import io.bhex.broker.server.grpc.server.service.kyc.tencent.WebankKycService;
import io.bhex.broker.server.grpc.server.service.notify.KycApplicationNotify;
import io.bhex.broker.server.model.BrokerKycConfig;
import io.bhex.broker.server.model.Country;
import io.bhex.broker.server.model.User;
import io.bhex.broker.server.model.UserVerify;
import io.bhex.broker.server.primary.mapper.UserVerifyMapper;
import io.bhex.broker.server.util.IDCardUtil;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class UserKycService {

    @Resource
    private ISequenceGenerator sequenceGenerator;

    @Resource
    private WebankKycService webankKycService;

    @Resource
    private KycVerifyFactory kycVerifyFactory;

    @Resource
    private UserVerifyMapper userVerifyMapper;

    @Resource
    private BasicService basicService;

    @Resource
    private UserService userService;

    @Resource
    private BaseBizConfigService baseBizConfigService;

    @Value("${verify-captcha:true}")
    private Boolean verifyCaptcha;

    @Value("${global-notify-type:1}")
    private Integer globalNotifyType;

    public WebankSdkLoginPrepareResponse webankSdkLoginPrepare(WebankSdkLoginPrepareRequest request) {
        Long userId = request.getHeader().getUserId();
        Long orgId = request.getHeader().getOrgId();
        NonceTicket nonceTicket = webankKycService.getNonceTicket(userId, orgId);
        String orderNo = String.valueOf(sequenceGenerator.getLong());
        SDKLoginPrepareResponse sdkResp = webankKycService.sdkLoginPrepare(request.getHeader(), request.getAppType(), orderNo, nonceTicket);

        return WebankSdkLoginPrepareResponse.newBuilder()
                .setSdkPrepareInfo(toSdkPrepareInfo(sdkResp))
                .build();
    }

    public BasicKycVerifyResponse basicKycVerify(BasicKycVerifyRequest request) {
        log.info("basicKycVerify: orgId:{} userId:{}, name:{}", request.getHeader().getOrgId(), request.getHeader().getUserId(), request.getName());
        return kycVerifyFactory.basicVerify(request);
    }

    @KycApplicationNotify
    public PhotoKycVerifyResponse photoKycVerify(PhotoKycVerifyRequest request) {
        return kycVerifyFactory.seniorVerify(request);
    }

    //@KycApplicationNotify
    public FaceKycVerifyResponse faceKycVerify(FaceKycVerifyRequest request) {
        return kycVerifyFactory.seniorFaceResultVerify(request);
    }

    @KycApplicationNotify
    public VideoKycVerifyResponse videoKycVerify(VideoKycVerifyRequest request) {
        if (!verifyCaptcha || globalNotifyType != 3) {
            //三级KYC必须绑定手机之后才能进行认证
            if (request.getHeader().getUserId() > 0) {
                User user = this.userService.getUser(request.getHeader().getUserId());
                if (StringUtils.isBlank(user.getMobile())) {
                    throw new BrokerException(BrokerErrorCode.KYC_VIDEO_VERIFY_NEED_MOBILE);
                }
            }
        }
        return kycVerifyFactory.vipVerify(request);
    }

    private SdkPrepareInfo toSdkPrepareInfo(SDKLoginPrepareResponse sdkResp) {
        return SdkPrepareInfo.newBuilder()
                .setAppId(sdkResp.getWebankAppId())
                .setLicense(sdkResp.getLicense())
                .setNonce(sdkResp.getNonce())
                .setVersion(sdkResp.getVersion())
                .setOrderNo(sdkResp.getOrderNo())
                .setSign(sdkResp.getSign())
                .setUserId(sdkResp.getUserId())
                .build();
    }

    public OrgBatchUpdateKycResponse orgBatchUpdateKyc(OrgBatchUpdateKycRequest request) {
        if (request.getHeader().getPlatform() != Platform.ORG_API) {
            throw new BrokerException(BrokerErrorCode.PARAM_INVALID);
        }
        List<OrgUpdateKycResult> resultList = request.getKycInfosList().stream()
                .map(k -> updateKycInfo(k, request.getHeader().getOrgId(), request.getForceUpdate()))
                .collect(Collectors.toList());
        return OrgBatchUpdateKycResponse.newBuilder().addAllUpdateResults(resultList).build();
    }

    public OrgBatchUpdateSeniorKycResponse orgBatchUpdateSeniorKyc(OrgBatchUpdateSeniorKycRequest request) {
        if (request.getHeader().getPlatform() != Platform.ORG_API) {
            throw new BrokerException(BrokerErrorCode.PARAM_INVALID);
        }
        List<OrgUpdateKycResult> resultList = request.getKycInfosList().stream()
                .map(k -> updateKycSeniorInfo(k, request.getHeader().getOrgId(), request.getForceUpdate()))
                .collect(Collectors.toList());
        return OrgBatchUpdateSeniorKycResponse.newBuilder().addAllUpdateResults(resultList).build();
    }

    /**
     * 更新用户kyc数据（仅供机构API调用） 暂时只允许二级认证状态更新(url修改三级认证等预留)
     */
    private OrgUpdateKycResult updateKycSeniorInfo(OrgUpdateSeniorKycInfo orgUpdateSeniorKycInfo, Long orgId, boolean forceUpdate) {
        if (orgUpdateSeniorKycInfo.getKycLevel() != 20) {
            return OrgUpdateKycResult.newBuilder()
                    .setUserId(orgUpdateSeniorKycInfo.getUserId())
                    .setResultCode(BrokerErrorCode.PARAM_INVALID.code())
                    .setResultMessage(BrokerErrorCode.PARAM_INVALID.msg())
                    .build();
        }
        //判断用户是否存在
        OrgUpdateKycResult orgUpdateKycResult = checkUserEffective(orgUpdateSeniorKycInfo.getUserId(), orgId);
        if (orgUpdateKycResult != null) {
            return orgUpdateKycResult;
        }
        //检查是否通过一级认证
        UserVerify existUserVerify = userVerifyMapper.getByUserId(orgUpdateSeniorKycInfo.getUserId());
        if (existUserVerify == null) {
            return OrgUpdateKycResult.newBuilder()
                    .setUserId(orgUpdateSeniorKycInfo.getUserId())
                    .setResultCode(BrokerErrorCode.KYC_BASIC_VERIFY_UNDONE.code())
                    .setResultMessage(BrokerErrorCode.KYC_BASIC_VERIFY_UNDONE.msg())
                    .build();
        }
        switch (existUserVerify.getGrade()) {
            case BASIC:
                if (!existUserVerify.isVerifyPassed()) {
                    return OrgUpdateKycResult.newBuilder()
                            .setUserId(orgUpdateSeniorKycInfo.getUserId())
                            .setResultCode(BrokerErrorCode.KYC_BASIC_VERIFY_UNDONE.code())
                            .setResultMessage(BrokerErrorCode.KYC_BASIC_VERIFY_UNDONE.msg())
                            .build();
                }
                break;
            case SENIOR:
                if (existUserVerify.isVerifyPassed() && UserVerifyStatus.PASSED.value() == orgUpdateSeniorKycInfo.getVerifyStatus()) {
                    return OrgUpdateKycResult.newBuilder()
                            .setUserId(orgUpdateSeniorKycInfo.getUserId())
                            .setResultCode(BrokerErrorCode.KYC_SENIOR_VERIFY_NONEED.code())
                            .setResultMessage(BrokerErrorCode.KYC_SENIOR_VERIFY_NONEED.msg())
                            .build();
                }
                break;
            default:
                return OrgUpdateKycResult.newBuilder()
                        .setUserId(orgUpdateSeniorKycInfo.getUserId())
                        .setResultCode(BrokerErrorCode.KYC_SENIOR_VERIFY_NONEED.code())
                        .setResultMessage(BrokerErrorCode.KYC_SENIOR_VERIFY_NONEED.msg())
                        .build();
        }
        if (!forceUpdate && "CN".equals(existUserVerify.getCountryCode())) {
            //不允许中国用户在false下更新二级状态
            return OrgUpdateKycResult.newBuilder()
                    .setUserId(orgUpdateSeniorKycInfo.getUserId())
                    .setResultCode(BrokerErrorCode.KYC_SENIOR_VERIFY_UNDONE.code())
                    .setResultMessage(BrokerErrorCode.KYC_SENIOR_VERIFY_NONEED.msg())
                    .build();
        }
        if (StringUtils.isEmpty(existUserVerify.getDataSecret())) {
            existUserVerify.setDataSecret(UserVerify.createUserAESKey(existUserVerify.getUserId()));
        }
        //更新高级认证等级
        existUserVerify.setKycLevel(orgUpdateSeniorKycInfo.getKycLevel());
        existUserVerify.setUpdated(System.currentTimeMillis());
        existUserVerify.setVerifyStatus(orgUpdateSeniorKycInfo.getVerifyStatus());
        try {
            userVerifyMapper.update(existUserVerify);
        } catch (Exception e) {
            log.error(String.format("updateSeniorKycInfo error. orgUpdateSeniorKycInfo: %s",
                    JsonUtil.defaultGson().toJson(orgUpdateSeniorKycInfo)), e);
            return OrgUpdateKycResult.newBuilder()
                    .setUserId(orgUpdateSeniorKycInfo.getUserId())
                    .setResultCode(BrokerErrorCode.SYSTEM_ERROR.code())
                    .setResultMessage(BrokerErrorCode.SYSTEM_ERROR.msg())
                    .build();
        }
        return OrgUpdateKycResult.newBuilder()
                .setUserId(orgUpdateSeniorKycInfo.getUserId())
                .setResultCode(BrokerErrorCode.SUCCESS.code())
                .setResultMessage(BrokerErrorCode.SUCCESS.msg())
                .build();
    }

    /**
     * 更新用户kyc数据（仅供机构API调用） 当前仅实现一级KYC数据更新
     */
    private OrgUpdateKycResult updateKycInfo(OrgUpdateKycInfo orgUpdateKycInfo, Long orgId, boolean forceUpdate) {
        if (orgUpdateKycInfo.getKycLevel() / 10 > 1 || orgUpdateKycInfo.getKycLevel() == 0) {
            return OrgUpdateKycResult.newBuilder()
                    .setUserId(orgUpdateKycInfo.getUserId())
                    .setResultCode(BrokerErrorCode.PARAM_INVALID.code())
                    .setResultMessage(BrokerErrorCode.PARAM_INVALID.msg())
                    .build();
        }

        OrgUpdateKycResult orgUpdateKycResult = checkUserEffective(orgUpdateKycInfo.getUserId(), orgId);
        if (orgUpdateKycResult != null) {
            return orgUpdateKycResult;
        }

        UserVerify existUserVerify = userVerifyMapper.getByUserId(orgUpdateKycInfo.getUserId());
        if (existUserVerify != null && !forceUpdate) {
            return OrgUpdateKycResult.newBuilder()
                    .setUserId(orgUpdateKycInfo.getUserId())
                    .setResultCode(BrokerErrorCode.KYC_EXIST.code())
                    .setResultMessage(BrokerErrorCode.KYC_EXIST.msg())
                    .build();
        }

        Country country = basicService.getCountryByCode(orgUpdateKycInfo.getCountryCode());
        if (country == null) {
            return OrgUpdateKycResult.newBuilder()
                    .setUserId(orgUpdateKycInfo.getUserId())
                    .setResultCode(BrokerErrorCode.KYC_INVALID_COUNTRY_CODE.code())
                    .setResultMessage(BrokerErrorCode.KYC_INVALID_COUNTRY_CODE.msg())
                    .build();
        }

        if (orgUpdateKycInfo.getGender() > 2 || orgUpdateKycInfo.getGender() < 0) {
            return OrgUpdateKycResult.newBuilder()
                    .setUserId(orgUpdateKycInfo.getUserId())
                    .setResultCode(BrokerErrorCode.GENDER_ERROR.code())
                    .setResultMessage(BrokerErrorCode.GENDER_ERROR.msg())
                    .build();
        }

        if (StringUtils.isEmpty(orgUpdateKycInfo.getFirstName())) {
            return OrgUpdateKycResult.newBuilder()
                    .setUserId(orgUpdateKycInfo.getUserId())
                    .setResultCode(BrokerErrorCode.FIRST_NAME_CANNOT_BE_NULL.code())
                    .setResultMessage(BrokerErrorCode.FIRST_NAME_CANNOT_BE_NULL.msg())
                    .build();
        }

        if (!basicService.getCardTypeTable().rowKeySet().contains(orgUpdateKycInfo.getCardType())) {
            return OrgUpdateKycResult.newBuilder()
                    .setUserId(orgUpdateKycInfo.getUserId())
                    .setResultCode(BrokerErrorCode.ID_CARD_TYPE_ERROR.code())
                    .setResultMessage(BrokerErrorCode.ID_CARD_TYPE_ERROR.msg())
                    .build();
        }

        if (StringUtils.isEmpty(orgUpdateKycInfo.getCardNo())) {
            return OrgUpdateKycResult.newBuilder()
                    .setUserId(orgUpdateKycInfo.getUserId())
                    .setResultCode(BrokerErrorCode.ID_CARD_NO_CANNOT_BE_NULL.code())
                    .setResultMessage(BrokerErrorCode.ID_CARD_NO_CANNOT_BE_NULL.msg())
                    .build();
        }
        //只校验 CN & 身份 & 号码符合格式
        if (orgUpdateKycInfo.getCountryCode().equalsIgnoreCase("CN")
                && orgUpdateKycInfo.getCardType() == 1
                && IDCardUtil.validateCard(orgUpdateKycInfo.getCardNo())) {
            //获取校验开关
            SwitchStatus openKycAgeCheckSwtich = baseBizConfigService.getConfigSwitchStatus(orgId,
                    BaseConfigConstants.WHOLE_SITE_CONTROL_SWITCH_GROUP, BaseConfigConstants.OPEN_KYC_AGE_CHECK_KEY);
            if (openKycAgeCheckSwtich.isOpen()) {
                int age = IDCardUtil.getAgeByCardAccurateToDay(orgUpdateKycInfo.getCardNo());
                if (age < 18 || age > 75) {
                    log.warn("orgId:{} userId:{}  no:{} age:{} less 18 or greater 75.", orgId, orgUpdateKycInfo.getUserId(), orgUpdateKycInfo.getCardNo(), age);
                    return OrgUpdateKycResult.newBuilder()
                            .setUserId(orgUpdateKycInfo.getUserId())
                            .setResultCode(BrokerErrorCode.AGE_LESS_18_ERROR.code())
                            .setResultMessage(BrokerErrorCode.AGE_LESS_18_ERROR.msg())
                            .build();
                }
            }
        }
        BrokerKycConfig brokerKycConfig = basicService.getBrokerKycConfig(orgId, country.getId());
        int secondKycLevel = brokerKycConfig == null ? 20 : brokerKycConfig.getSecondKycLevel();
        boolean useEncrypt = secondKycLevel == 25; // 新版KYC认证加密存储 老版KYC保持兼容

        Long timestamp = System.currentTimeMillis();
        UserVerify userVerify = UserVerify.builder()
                .orgId(orgId)
                .userId(orgUpdateKycInfo.getUserId())
                .nationality(country.getId())
                .countryCode(orgUpdateKycInfo.getCountryCode())
                .firstName(orgUpdateKycInfo.getFirstName())
                .secondName(orgUpdateKycInfo.getLastName())
                .gender(orgUpdateKycInfo.getGender())
                .cardType(orgUpdateKycInfo.getCardType())
                .cardNo(orgUpdateKycInfo.getCardNo())
                .cardNoHash(UserVerify.hashCardNo(orgUpdateKycInfo.getCardNo()))
                .dataEncrypt(0)
                .passedIdCheck(0)
                .verifyStatus(orgUpdateKycInfo.getVerifyStatus())
                .kycLevel(orgUpdateKycInfo.getKycLevel())
                .kycApplyId(0L)
                .updated(timestamp)
                .build();

        try {
            if (useEncrypt) {
                userVerify = UserVerify.encrypt(userVerify);
            }

            if (existUserVerify != null) {
                userVerifyMapper.update(userVerify);
            } else {
                userVerify.setCreated(timestamp);
                userVerifyMapper.insertRecord(userVerify);
            }
        } catch (Exception e) {
            log.error(String.format("updateKycInfo error. orgUpdateKycInfo: %s",
                    JsonUtil.defaultGson().toJson(orgUpdateKycInfo)), e);
            return OrgUpdateKycResult.newBuilder()
                    .setUserId(orgUpdateKycInfo.getUserId())
                    .setResultCode(BrokerErrorCode.SYSTEM_ERROR.code())
                    .setResultMessage(BrokerErrorCode.SYSTEM_ERROR.msg())
                    .build();
        }

        return OrgUpdateKycResult.newBuilder()
                .setUserId(orgUpdateKycInfo.getUserId())
                .setResultCode(BrokerErrorCode.SUCCESS.code())
                .setResultMessage(BrokerErrorCode.SUCCESS.msg())
                .build();
    }

    /**
     * 判断用户是否有效
     *
     * @param userId userId
     * @param orgId  orgId
     * @return OrgUpdateKycResult
     */
    private OrgUpdateKycResult checkUserEffective(long userId, Long orgId) {
        User user = userService.getUser(userId);
        if (user == null) {
            return OrgUpdateKycResult.newBuilder()
                    .setUserId(userId)
                    .setResultCode(BrokerErrorCode.USER_NOT_EXIST.code())
                    .setResultMessage(BrokerErrorCode.USER_NOT_EXIST.msg())
                    .build();
        }
        // 判断用户的orgId是否和请求的orgId一致
        if (!user.getOrgId().equals(orgId)) {
            return OrgUpdateKycResult.newBuilder()
                    .setUserId(userId)
                    .setResultCode(BrokerErrorCode.KYC_USER_ORG_NOT_MATCH.code())
                    .setResultMessage(BrokerErrorCode.KYC_USER_ORG_NOT_MATCH.msg())
                    .build();
        }
        return null;
    }
}
