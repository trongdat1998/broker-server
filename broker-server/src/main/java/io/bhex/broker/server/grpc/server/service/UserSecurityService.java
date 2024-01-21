/**********************************
 *@项目名称: broker-parent
 *@文件名称: io.bhex.broker.service.user
 *@Date 2018/8/2
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
package io.bhex.broker.server.grpc.server.service;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;

import io.bhex.broker.grpc.admin.AdminUpdateEmailResponse;
import io.bhex.broker.server.domain.*;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.Resource;

import io.bhex.base.account.ModifyOrgUserEmailRequest;
import io.bhex.base.account.ModifyOrgUserMobileRequest;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.common.util.CryptoUtil;
import io.bhex.broker.common.util.JsonUtil;
import io.bhex.broker.common.util.ValidateUtil;
import io.bhex.broker.core.domain.BaseResult;
import io.bhex.broker.grpc.account.Balance;
import io.bhex.broker.grpc.common.AccountTypeEnum;
import io.bhex.broker.grpc.common.Header;
import io.bhex.broker.grpc.common.Platform;
import io.bhex.broker.grpc.common.TwoStepAuth;
import io.bhex.broker.grpc.security.ApiKeyInfo;
import io.bhex.broker.grpc.security.ApiKeyStatus;
import io.bhex.broker.grpc.security.SecurityBeforeBindGARequest;
import io.bhex.broker.grpc.security.SecurityBeforeBindGAResponse;
import io.bhex.broker.grpc.security.SecurityBindGARequest;
import io.bhex.broker.grpc.security.SecurityBindGAResponse;
import io.bhex.broker.grpc.security.SecurityCreateApiKeyRequest;
import io.bhex.broker.grpc.security.SecurityCreateApiKeyResponse;
import io.bhex.broker.grpc.security.SecurityDeleteApiKeyRequest;
import io.bhex.broker.grpc.security.SecurityDeleteApiKeyResponse;
import io.bhex.broker.grpc.security.SecurityQueryOrgAuthorizedAccountApiKeysRequest;
import io.bhex.broker.grpc.security.SecurityQueryUserApiKeysRequest;
import io.bhex.broker.grpc.security.SecurityQueryUserApiKeysResponse;
import io.bhex.broker.grpc.security.SecurityResetPasswordRequest;
import io.bhex.broker.grpc.security.SecurityResetPasswordResponse;
import io.bhex.broker.grpc.security.SecuritySendEmailVerifyCodeRequest;
import io.bhex.broker.grpc.security.SecuritySendMobileVerifyCodeRequest;
import io.bhex.broker.grpc.security.SecuritySendVerifyCodeResponse;
import io.bhex.broker.grpc.security.SecuritySetTradePasswordRequest;
import io.bhex.broker.grpc.security.SecuritySetTradePasswordResponse;
import io.bhex.broker.grpc.security.SecurityUnBindGARequest;
import io.bhex.broker.grpc.security.SecurityUnBindGAResponse;
import io.bhex.broker.grpc.security.SecurityUpdateApiKeyRequest;
import io.bhex.broker.grpc.security.SecurityUpdateApiKeyResponse;
import io.bhex.broker.grpc.security.SecurityUpdatePasswordRequest;
import io.bhex.broker.grpc.security.SecurityUpdatePasswordResponse;
import io.bhex.broker.grpc.security.SecurityUpdateUserStatusRequest;
import io.bhex.broker.grpc.security.SecurityUpdateUserStatusResponse;
import io.bhex.broker.grpc.security.SecurityUser;
import io.bhex.broker.grpc.security.SecurityVerifyCodeType;
import io.bhex.broker.grpc.security.SecurityVerifyGARequest;
import io.bhex.broker.grpc.security.SecurityVerifyGAResponse;
import io.bhex.broker.grpc.security.SecurityVerifyTradePasswordRequest;
import io.bhex.broker.grpc.security.SecurityVerifyTradePasswordResponse;
import io.bhex.broker.grpc.user.AlterEmailResponse;
import io.bhex.broker.grpc.user.AlterGaResponse;
import io.bhex.broker.grpc.user.AlterMobileResponse;
import io.bhex.broker.grpc.user.ApiKey;
import io.bhex.broker.grpc.user.BeforeAlterGaResponse;
import io.bhex.broker.grpc.user.BeforeBindGAResponse;
import io.bhex.broker.grpc.user.BindEmailResponse;
import io.bhex.broker.grpc.user.BindGAResponse;
import io.bhex.broker.grpc.user.BindMobileResponse;
import io.bhex.broker.grpc.user.CreateApiKeyResponse;
import io.bhex.broker.grpc.user.DeleteApiKeyResponse;
import io.bhex.broker.grpc.user.FindPwdCheckResponse;
import io.bhex.broker.grpc.user.FindPwdResponse;
import io.bhex.broker.grpc.user.QueryApiKeyResponse;
import io.bhex.broker.grpc.user.SendFindPwdVerifyCodeResponse;
import io.bhex.broker.grpc.user.SetLoginPasswordResponse;
import io.bhex.broker.grpc.user.SetTradePasswordResponse;
import io.bhex.broker.grpc.user.UnbindEmailResponse;
import io.bhex.broker.grpc.user.UnbindGAResponse;
import io.bhex.broker.grpc.user.UnbindMobileResponse;
import io.bhex.broker.grpc.user.UpdateApiKeyResponse;
import io.bhex.broker.grpc.user.UpdatePasswordResponse;
import io.bhex.broker.grpc.user.UserUnbindEmailResponse;
import io.bhex.broker.grpc.user.UserUnbindGaResponse;
import io.bhex.broker.grpc.user.UserUnbindMobileResponse;
import io.bhex.broker.grpc.user.VerifyTradePasswordResponse;
import io.bhex.broker.server.grpc.client.service.GrpcAccountService;
import io.bhex.broker.server.grpc.client.service.GrpcSecurityService;
import io.bhex.broker.server.grpc.server.service.aspect.UserActionLogAnnotation;
import io.bhex.broker.server.model.Account;
import io.bhex.broker.server.model.Broker;
import io.bhex.broker.server.model.FindPwdRecord;
import io.bhex.broker.server.model.FrozenUserRecord;
import io.bhex.broker.server.model.LoginLog;
import io.bhex.broker.server.model.ThirdPartyAppUserAuthorize;
import io.bhex.broker.server.model.User;
import io.bhex.broker.server.model.UserVerify;
import io.bhex.broker.server.primary.mapper.AccountMapper;
import io.bhex.broker.server.primary.mapper.BrokerMapper;
import io.bhex.broker.server.primary.mapper.FindPwdRecordMapper;
import io.bhex.broker.server.primary.mapper.FrozenUserRecordMapper;
import io.bhex.broker.server.primary.mapper.LoginLogMapper;
import io.bhex.broker.server.primary.mapper.UserMapper;
import io.bhex.broker.server.primary.mapper.UserVerifyMapper;
import io.bhex.broker.server.util.BaseReqUtil;
import io.bhex.broker.server.util.EmailUtils;
import io.bhex.broker.server.util.GrpcHeaderUtil;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class UserSecurityService {

    private static final String CHECK_FIND_PWD_IP = "checkFindPwdIp";
    private static final String FROZEN_USER_LOGIN_AFTER_FIND_PWD = "frozenUserLoginAfterFindPwd";
    private static final String FROZEN_USER_WITHDRAW_AFTER_FIND_PWD = "frozenUserWithdrawAfterFindPwd";

    private static final String TRADE_PW_ERROR_COUNT_KEY = "BROKER:SERVER:TRADE_PWD_ERROR_COUNT:%s_%s";

    @Resource
    private GrpcSecurityService grpcSecurityService;

    @Resource
    private VerifyCodeService verifyCodeService;

    @Resource(name = "stringRedisTemplate")
    private StringRedisTemplate redisTemplate;

    @Resource
    private AccountService accountService;

    @Resource
    private AccountMapper accountMapper;

    @Resource
    private UserMapper userMapper;

    @Resource
    private UserVerifyMapper userVerifyMapper;

    @Resource
    private FindPwdRecordMapper findPwdRecordMapper;

    @Resource
    private LoginLogMapper loginLogMapper;

    @Resource
    private NoticeTemplateService noticeTemplateService;

    @Resource
    private BrokerMapper brokerMapper;

    @Resource
    private FrozenUserRecordMapper frozenUserRecordMapper;

    @Resource
    private GrpcAccountService grpcAccountService;

    @Resource
    private CommonIniService commonIniService;

    @Resource
    private OAuthService oAuthService;

    @Resource
    private ThirdPartyUserService thirdPartyUserService;
    @Resource
    private UserService userService;

    @Value("${verify-captcha:true}")
    private Boolean verifyCaptcha;

    @Value("${global-notify-type:1}")
    private Integer globalNotifyType;

    @UserActionLogAnnotation(actionType = BrokerServerConstants.ACTION_TYPE_BIND_EMAIL, action = "{#email}")
    public BindEmailResponse bindEmail(Header header, String email, Long emailVerifyCodeOrderId, String emailVerifyCode,
                                       Long mobileVerifyCodeOrderId, String mobileVerifyCode) {
        if (verifyCaptcha && globalNotifyType == 2) {
            //平台仅支持手机不允许绑定邮箱，但允许解绑
            throw new BrokerException(BrokerErrorCode.FEATURE_SUSPENDED);
        }

        ValidateUtil.validateEmail(email);
        User user = userMapper.getByUserId(header.getUserId());
        int registerType = user.getRegisterType();
        verifyCodeService.validVerifyCode(header, VerifyCodeType.BIND_EMAIL, emailVerifyCodeOrderId, emailVerifyCode, email, AuthType.EMAIL);
        boolean checkMobile = true;
        if (verifyCaptcha && globalNotifyType == 3) {
            //如果是仅邮箱，绑定后，切换注册类型为邮箱, 且不校验原手机验证码
            registerType = RegisterType.EMAIL.value();
            checkMobile = false;
        }
        if (checkMobile) {
            verifyCodeService.validVerifyCode(header, VerifyCodeType.BIND_EMAIL, mobileVerifyCodeOrderId, mobileVerifyCode,
                    user.getNationalCode() + user.getMobile(), AuthType.MOBILE);
        }
        if (userMapper.countEmailExists(header.getOrgId(), email, EmailUtils.emailAlias(email)) > 0) {
            throw new BrokerException(BrokerErrorCode.EMAIL_EXIST);
        }

//        if (email.toLowerCase().endsWith("@gmail.com")) {
//            String emailAlias = EmailUtil.emailAlias(email);
//            if (userMapper.getByEmailAlias(header.getOrgId(), emailAlias) != null) {
//                throw new BrokerException(BrokerErrorCode.EMAIL_EXIST);
//            }
//        }
        if (!Strings.isNullOrEmpty(user.getEmail())) {
            throw new BrokerException(BrokerErrorCode.EMAIL_UNBIND_FIRST);
        }
        User updateObj = User.builder()
                .userId(header.getUserId())
                .email(email)
                .registerType(registerType)
                .emailAlias(EmailUtils.emailAlias(email))
                .updated(System.currentTimeMillis())
                .build();
        userMapper.updateRecord(updateObj);
        try {
            verifyCodeService.invalidVerifyCode(header, VerifyCodeType.BIND_EMAIL, emailVerifyCodeOrderId, email);
            verifyCodeService.invalidVerifyCode(header, VerifyCodeType.BIND_EMAIL, mobileVerifyCodeOrderId,
                    user.getNationalCode() + user.getMobile());
        } catch (Exception e) {
            // ignore
        }
        try {
            ModifyOrgUserEmailRequest request = ModifyOrgUserEmailRequest.newBuilder()
                    .setBaseRequest(BaseReqUtil.getBaseRequest(header.getOrgId()))
                    .setOrgId(header.getOrgId())
                    .setUserId(header.getUserId())
                    .setEmail(email)
                    .build();
            grpcAccountService.modifyEmail(request);
        } catch (Exception e) {
            log.error("modify user email and notice bh-server error, orgId:{}, userId:{}", header.getOrgId(), header.getUserId(), e);
        }
        return BindEmailResponse.newBuilder().build();
    }

    public UnbindEmailResponse unbindEmail(Header header) {
        User user = userMapper.getByOrgAndUserId(header.getOrgId(), header.getUserId());
        if (user == null) {
            throw new BrokerException(BrokerErrorCode.USER_NOT_EXIST);
        }

        String originalEmail = user.getEmail();
        // 修改邮箱为空字符串
        userMapper.unbindUserEmail(header.getOrgId(), header.getUserId());
        log.info(" unbindEmail executed: userId:{} originalEmail:{}", user.getUserId(), originalEmail);
        try {
            // 同步修改bh那边的信息也为空字符串
            ModifyOrgUserEmailRequest request = ModifyOrgUserEmailRequest.newBuilder()
                    .setBaseRequest(BaseReqUtil.getBaseRequest(header.getOrgId()))
                    .setOrgId(header.getOrgId())
                    .setUserId(header.getUserId())
                    .setEmail("")
                    .build();
            grpcAccountService.modifyEmail(request);
        } catch (Exception e) {
            log.error("unbindEmail and notice bh-server error, orgId:{}, userId:{}", header.getOrgId(), header.getUserId(), e);
        }
        return UnbindEmailResponse.getDefaultInstance();
    }

    @UserActionLogAnnotation(actionType = BrokerServerConstants.ACTION_TYPE_BIND_MOBILE, action = "{#mobile}")
    public BindMobileResponse bindMobile(Header header, String nationalCode,
                                         String mobile, Long mobileVerifyCodeOrderId, String mobileVerifyCode,
                                         Long emailVerifyCodeOrderId, String emailVerifyCode) {
        if (verifyCaptcha && globalNotifyType == 3) {
            //平台仅支持邮箱不允许绑定手机，允许解绑
            throw new BrokerException(BrokerErrorCode.FEATURE_SUSPENDED);
        }
        User user = userMapper.getByUserId(header.getUserId());
        int registerType = user.getRegisterType();
        verifyCodeService.validVerifyCode(header, VerifyCodeType.BIND_MOBILE, mobileVerifyCodeOrderId, mobileVerifyCode,
                nationalCode + mobile, AuthType.MOBILE);
        boolean checkEmail = true;
        if (verifyCaptcha && globalNotifyType == 2) {
            //如果是仅邮箱，绑定后，切换注册类型为手机,且不校验原邮箱验证码
            registerType = RegisterType.MOBILE.value();
            checkEmail = false;
        }
        if (checkEmail) {
            verifyCodeService.validVerifyCode(header, VerifyCodeType.BIND_MOBILE, emailVerifyCodeOrderId, emailVerifyCode,
                    user.getEmail(), AuthType.EMAIL);
        }
        if (userMapper.getByMobile(header.getOrgId(), nationalCode, mobile) != null) {
            throw new BrokerException(BrokerErrorCode.MOBILE_EXIST);
        }
        if (!Strings.isNullOrEmpty(user.getMobile())) {
            throw new BrokerException(BrokerErrorCode.MOBILE_UNBIND_FIRST);
        }
        User updateObj = User.builder()
                .userId(header.getUserId())
                .nationalCode(nationalCode)
                .mobile(mobile)
                .registerType(registerType)
                .concatMobile(nationalCode + mobile)
                .updated(System.currentTimeMillis())
                .build();
        userMapper.updateRecord(updateObj);
        try {
            verifyCodeService.invalidVerifyCode(header, VerifyCodeType.BIND_MOBILE, mobileVerifyCodeOrderId, nationalCode + mobile);
            verifyCodeService.invalidVerifyCode(header, VerifyCodeType.BIND_MOBILE, emailVerifyCodeOrderId, user.getEmail());
        } catch (Exception e) {
            // ignore
        }
        try {
            ModifyOrgUserMobileRequest request = ModifyOrgUserMobileRequest.newBuilder()
                    .setBaseRequest(BaseReqUtil.getBaseRequest(header.getOrgId()))
                    .setOrgId(header.getOrgId())
                    .setUserId(header.getUserId())
                    .setCountryCode(nationalCode)
                    .setMobile(mobile)
                    .build();
            grpcAccountService.modifyMobile(request);
        } catch (Exception e) {
            log.error("modify user email and notice bh-server error, orgId:{}, userId:{}", header.getOrgId(), header.getUserId(), e);
        }
        return BindMobileResponse.newBuilder().build();
    }

    public UnbindMobileResponse unbindMobile(Header header) {
        User user = userMapper.getByOrgAndUserId(header.getOrgId(), header.getUserId());
        if (user == null) {
            throw new BrokerException(BrokerErrorCode.USER_NOT_EXIST);
        }

        // 用户类型不是邮箱注册的，不能解绑手机
//        if (!user.getRegisterType().equals(RegisterType.EMAIL.value())) {
//            throw new BrokerException(BrokerErrorCode.USER_REGISTER_TYPE_NOT_EMAIL);
//        }

        String originalNationalCode = user.getNationalCode();
        String originalMobile = user.getMobile();
        //// 修改手机号为空字符串
        userMapper.unbindUserMobile(header.getOrgId(), header.getUserId());
        log.info(" unbindMobile executed: userId:{} originalMobile:{}-{}", user.getUserId(), originalNationalCode, originalMobile);

        try {
            // 同步修改bh那边的信息也为空字符串
            ModifyOrgUserMobileRequest request = ModifyOrgUserMobileRequest.newBuilder()
                    .setBaseRequest(BaseReqUtil.getBaseRequest(header.getOrgId()))
                    .setOrgId(header.getOrgId())
                    .setUserId(header.getUserId())
                    .setCountryCode("")
                    .setMobile("")
                    .build();
            grpcAccountService.modifyMobile(request);
        } catch (Exception e) {
            log.error("unbindMobile and notice bh-server error, orgId:{}, userId:{}", header.getOrgId(), header.getUserId(), e);
        }
        return UnbindMobileResponse.getDefaultInstance();
    }

    /**
     * before bind GoogleAuthenticator, we should provide GoogleAuthenticator SecretKey or a qrcode
     */
    public BeforeBindGAResponse beforeBindGA(Header header) {
        User user = userMapper.getByUserId(header.getUserId());
        if (user.getBindGA() == 1) {
            throw new BrokerException(BrokerErrorCode.GA_UNBIND_FIRST);
        }
        Long time = System.currentTimeMillis();

        DateFormat sf = new SimpleDateFormat("MM-dd HH:mm:ss");
        String dateStr = sf.format(new Date());
        String accountName = user.getRegisterType() == 1
                ? user.getMobile().replaceAll(BrokerServerConstants.MASK_MOBILE_REG, "$1****$2")
                : user.getEmail().replaceAll(BrokerServerConstants.MASK_EMAIL_REG, "*");
        Broker broker = brokerMapper.getByOrgId(header.getOrgId());
        SecurityBeforeBindGARequest request = SecurityBeforeBindGARequest.newBuilder()
                .setHeader(header)
                .setGaIssuer(broker.getBrokerName())
                .setAccountName(accountName + "_" + dateStr)
                .setUserId(header.getUserId())
                .build();
        SecurityBeforeBindGAResponse securityBeforeBindGAResponse = grpcSecurityService.beforeBindGA(request);
        String otpAuthTotpUrl = securityBeforeBindGAResponse.getOtpAuthTotpUrl();
        return BeforeBindGAResponse.newBuilder()
                .setSecretKey(securityBeforeBindGAResponse.getKey())
                .setOtpAuthTotpUrl(otpAuthTotpUrl)
                .build();
    }

    @UserActionLogAnnotation(actionType = BrokerServerConstants.ACTION_TYPE_BINDGA, action = "{#gaCode}")
    public BindGAResponse bindGA(Header header, Integer gaCode, Long verifyCodeOrderId, String verifyCode) {
        User user = userMapper.getByUserId(header.getUserId());
        String receiver;
        if (user.getRegisterType().equals(RegisterType.MOBILE.value())) {
            receiver = user.getNationalCode() + user.getMobile();
            verifyCodeService.validVerifyCode(header, VerifyCodeType.BIND_GA, verifyCodeOrderId, verifyCode, receiver, AuthType.MOBILE);
        } else {
            receiver = user.getEmail();
            verifyCodeService.validVerifyCode(header, VerifyCodeType.BIND_GA, verifyCodeOrderId, verifyCode, receiver, AuthType.EMAIL);
        }
        SecurityBindGARequest request = SecurityBindGARequest.newBuilder()
                .setHeader(header)
                .setUserId(header.getUserId())
                .setGaCode(gaCode)
                .build();
        SecurityBindGAResponse securityBindGAResponse = grpcSecurityService.bindGA(request);
        User updateBindGAStatus = User.builder()
                .userId(header.getUserId())
                .bindGA(1)
                .updated(System.currentTimeMillis())
                .build();
        userMapper.updateRecord(updateBindGAStatus);
        verifyCodeService.invalidVerifyCode(header, VerifyCodeType.BIND_GA, verifyCodeOrderId, receiver);
        return BindGAResponse.newBuilder().build();
    }

    @UserActionLogAnnotation(actionType = BrokerServerConstants.ACTION_TYPE_BINDGA, action = "{#gaCode}")
    public BindGAResponse bindGADirect(Header header, String gaKey, Integer gaCode) {
        User user = userMapper.getByUserId(header.getUserId());
        if (user.getBindGA() == 1) {
            throw new BrokerException(BrokerErrorCode.GA_UNBIND_FIRST);
        }
//        SecurityBindGADirectRequest request = SecurityBindGADirectRequest.newBuilder()
//                .setHeader(header)
//                .setUserId(header.getUserId())
//                .setGaKey(gaKey)
//                .setGaCode(gaCode)
//                .build();
//        SecurityBindGAResponse securityBindGAResponse = grpcSecurityService.bindGADirect(request);
        SecurityBindGARequest request = SecurityBindGARequest.newBuilder()
                .setHeader(header)
                .setUserId(header.getUserId())
                .setGaCode(gaCode)
                .build();
        SecurityBindGAResponse securityBindGAResponse = grpcSecurityService.bindGA(request);
        User updateBindGAStatus = User.builder()
                .userId(header.getUserId())
                .bindGA(1)
                .updated(System.currentTimeMillis())
                .build();
        userMapper.updateRecord(updateBindGAStatus);
        return BindGAResponse.newBuilder().build();
    }

    //@UserActionLogAnnotation(actionType = "unbindGA")
    public UnbindGAResponse unbindGA(Header header, Long userId, boolean unbindByUser, TwoStepAuth twoStepAuth) {
        User user = userMapper.getByOrgAndUserId(header.getOrgId(), header.getUserId());
        if (user == null) {
            throw new BrokerException(BrokerErrorCode.USER_NOT_EXIST);
        }
        String receiver = "";
        try {
            if (unbindByUser) {
                if (user.getRegisterType().equals(RegisterType.MOBILE.value())) {
                    receiver = user.getNationalCode() + user.getMobile();
                    verifyCodeService.validVerifyCode(header, VerifyCodeType.UNBIND_GA, twoStepAuth.getOrderId(), twoStepAuth.getVerifyCode(), receiver, AuthType.MOBILE);
                } else {
                    receiver = user.getEmail();
                    verifyCodeService.validVerifyCode(header, VerifyCodeType.UNBIND_GA, twoStepAuth.getOrderId(), twoStepAuth.getVerifyCode(), receiver, AuthType.EMAIL);
                }
            }
            SecurityUnBindGAResponse response = grpcSecurityService.unBindGA(SecurityUnBindGARequest.newBuilder().setHeader(header).setUserId(userId).build());
            if (response.getRet() != 0) {
                return UnbindGAResponse.newBuilder().setRet(response.getRet()).build();
            }
            // 修改绑定ga的状态为0
            userMapper.unbindGA(header.getOrgId(), userId);
        } catch (Exception e) {
            log.error("unbindGA error, request:[{}-{}]", header.getOrgId(), userId, e);
        }
        //禁止出金
        forbidWithdraw24Hour(header, FrozenReason.UNBIND_GA);
        if (unbindByUser) {
            verifyCodeService.invalidVerifyCode(header, VerifyCodeType.UNBIND_GA, twoStepAuth.getOrderId(), receiver);
        }
        return UnbindGAResponse.getDefaultInstance();
    }

    /**
     * verify GoogleAuthenticator code
     */
    public void validGACode(Header header, Long userId, String gaCodeStr) {
        if (Strings.isNullOrEmpty(gaCodeStr) || Strings.isNullOrEmpty(gaCodeStr.trim())) {
            log.warn("give a null or empty gaCode");
            throw new BrokerException(BrokerErrorCode.GA_VALID_ERROR);
        }
        Integer gaCode;
        try {
            gaCode = Ints.tryParse(gaCodeStr);
        } catch (Exception e) {
            log.warn("valid ga: cannot parse gaCode {}", gaCodeStr);
            throw new BrokerException(BrokerErrorCode.GA_VALID_ERROR);
        }
        if (gaCode == null) {
            log.warn("valid ga: cannot parse gaCode {}", gaCodeStr);
            throw new BrokerException(BrokerErrorCode.GA_VALID_ERROR);
        }
        SecurityVerifyGARequest request = SecurityVerifyGARequest.newBuilder()
                .setHeader(header)
                .setUserId(userId)
                .setGaCode(gaCode)
                .build();
        SecurityVerifyGAResponse securityVerifyGAResponse = grpcSecurityService.verifyGA(request);
    }

    /**
     * This method is used for verify the verification code entered by the user after "Requesting
     * the verification code interface by userId".
     * <p>No matter email verification code or sms verification code.
     * <p>Just write here is because the method of "Verify the verification code method"
     * requires the verification code receiver, email or mobile.
     */
    public void validVerifyCode(Header header, AuthType authType,
                                Long verifyCodeOrderId, String verifyCode, VerifyCodeType verifyCodeType) {
        User user = userMapper.getByUserId(header.getUserId());
        if (authType == AuthType.EMAIL) {
            verifyCodeService.validVerifyCode(header, verifyCodeType, verifyCodeOrderId, verifyCode, user.getEmail(), authType);
        } else if (authType == AuthType.MOBILE) {
            verifyCodeService.validVerifyCode(header, verifyCodeType, verifyCodeOrderId, verifyCode,
                    user.getNationalCode() + user.getMobile(), authType);
        } else {
            throw new BrokerException(BrokerErrorCode.REQUEST_INVALID);
        }
    }

    public FindPwdCheckResponse findPwdByMobileCheck(Header header, String nationalCode, String mobile,
                                                     Long verifyCodeOrderId, String verifyCode, boolean isOldVersionRequest) {
        if (isOldVersionRequest) {
            checkOldVersionRequest(header);
        }
        checkFindPwdIpAction(header, nationalCode + mobile);
        if (redisTemplate.hasKey(String.format(BrokerServerConstants.FROZEN_LOGIN_USERNAME_KEY, header.getOrgId(), mobile))) {
            throw new BrokerException(BrokerErrorCode.LOGIN_INPUT_ERROR_6);
        }
        if (redisTemplate.hasKey(String.format(BrokerServerConstants.FROZEN_FIND_PWD_USERNAME_KEY, header.getOrgId(), mobile))) {
            throw new BrokerException(BrokerErrorCode.FIND_PWD_VERIFY_CODE_ERROR6);
        }
        try {
            verifyCodeService.validVerifyCode(header, VerifyCodeType.FIND_PASSWORD, verifyCodeOrderId, verifyCode,
                    nationalCode + mobile, AuthType.MOBILE);
        } catch (Exception e) {
            handleVerifyCodeError(header, nationalCode + mobile);
        }
        User user = userMapper.getByMobile(header.getOrgId(), nationalCode, mobile);
        FindPwdCheckResponse response = findPwdCheck(header, user, FindPwdType.MOBILE, isOldVersionRequest);
        verifyCodeService.invalidVerifyCode(header, VerifyCodeType.FIND_PASSWORD, verifyCodeOrderId, nationalCode + mobile);
        return response;
    }

    public FindPwdCheckResponse findPwdByEmailCheck(Header header, String email,
                                                    Long verifyCodeOrderId, String verifyCode, boolean isOldVersionRequest) {
        if (isOldVersionRequest) {
            checkOldVersionRequest(header);
        }
        checkFindPwdIpAction(header, email);
        // 因登录错误多次登录操作被冻结后禁止找回密码
        if (redisTemplate.hasKey(String.format(BrokerServerConstants.FROZEN_LOGIN_USERNAME_KEY, header.getOrgId(), email))) {
            throw new BrokerException(BrokerErrorCode.LOGIN_INPUT_ERROR_6);
        }
        // 因找回密码验证码错误多次禁止找回密码
        if (redisTemplate.hasKey(String.format(BrokerServerConstants.FROZEN_FIND_PWD_USERNAME_KEY, header.getOrgId(), email))) {
            throw new BrokerException(BrokerErrorCode.FIND_PWD_VERIFY_CODE_ERROR6);
        }
        try {
            verifyCodeService.validVerifyCode(header, VerifyCodeType.FIND_PASSWORD, verifyCodeOrderId, verifyCode, email, AuthType.EMAIL);
        } catch (Exception e) {
            handleVerifyCodeError(header, email);
        }
        User user = userMapper.getByEmail(header.getOrgId(), email);
        FindPwdCheckResponse response = findPwdCheck(header, user, FindPwdType.EMAIL, isOldVersionRequest);
        verifyCodeService.invalidVerifyCode(header, VerifyCodeType.FIND_PASSWORD, verifyCodeOrderId, email);
        return response;
    }

    private void checkOldVersionRequest(Header header) {
        // 如果需要兼容，继续，如果不需要兼容，提示不兼容信息
        if (commonIniService.getBooleanValueOrDefault(header.getOrgId(), "interceptOldVersionFindPwdRequest", true)) {
            throw new BrokerException(BrokerErrorCode.FIND_PWD_NOT_SUPPORT);
        }
    }

    private void checkFindPwdIpAction(Header header, String username) {
        if (commonIniService.getBooleanValueOrDefault(header.getOrgId(), CHECK_FIND_PWD_IP, true)) {
            String hours = new SimpleDateFormat("yyyyMMddHH").format(new Date());
            // 6001_limit_ip_find_pwd_192.168.0.1_2019080810
            String findPwdIpKey = header.getOrgId() + "_limit_ip_find_pwd_" + header.getRemoteIp() + "_" + hours;
            if (redisTemplate.opsForSet().size(findPwdIpKey) > 3) {
                log.error("ip:{} had operated 3 account 'Find Password', keys:{}", header.getRemoteIp(), findPwdIpKey);
                throw new BrokerException(BrokerErrorCode.FORBIDDEN_ACTION_FOR_SECURITY);
            }
            redisTemplate.expire(findPwdIpKey, 1, TimeUnit.HOURS); // 1小时失效
            redisTemplate.opsForSet().add(findPwdIpKey, username);
        }
    }

    private void handleVerifyCodeError(Header header, String username) {
        String error24HCountKey = String.format(BrokerServerConstants.FIND_PWD_24H_ERROR_USERNAME_COUNT, header.getOrgId(), username);
        redisTemplate.opsForValue().setIfAbsent(error24HCountKey, "0", Duration.ofHours(24));
        long errorTimes = redisTemplate.opsForValue().increment(error24HCountKey);
        if (errorTimes == 1) {
            throw new BrokerException(BrokerErrorCode.FIND_PWD_VERIFY_CODE_ERROR);
        } else if (errorTimes == 2) {
            throw new BrokerException(BrokerErrorCode.FIND_PWD_VERIFY_CODE_ERROR2);
        } else if (errorTimes == 3) {
            throw new BrokerException(BrokerErrorCode.FIND_PWD_VERIFY_CODE_ERROR3);
        } else if (errorTimes == 4) {
            throw new BrokerException(BrokerErrorCode.FIND_PWD_VERIFY_CODE_ERROR4);
        } else if (errorTimes == 5) {
            redisTemplate.opsForValue().set(String.format(BrokerServerConstants.FROZEN_FIND_PWD_USERNAME_KEY, header.getOrgId(), username),
                    "" + System.currentTimeMillis(), Duration.ofHours(24));
            log.error("ip:{} is trying to crack the {} verification code {} times", header.getRemoteIp(), username, errorTimes);
            throw new BrokerException(BrokerErrorCode.FIND_PWD_VERIFY_CODE_ERROR5);
        } else if (errorTimes > 5) {
            redisTemplate.opsForValue().setIfAbsent(String.format(BrokerServerConstants.FROZEN_FIND_PWD_USERNAME_KEY, header.getOrgId(), username),
                    "" + System.currentTimeMillis(), Duration.ofHours(24));
            log.error("ip:{} is trying to crack the {} verification code {} times", header.getRemoteIp(), username, errorTimes);
            throw new BrokerException(BrokerErrorCode.FIND_PWD_VERIFY_CODE_ERROR6);
        }
    }

    private FindPwdCheckResponse findPwdCheck(Header header, User user, FindPwdType findPwdType, boolean isOldVersionRequest) {
        if (user == null) {
            throw new BrokerException(BrokerErrorCode.USER_NOT_EXIST);
        }
        if (user.getUserStatus() != 1) {
            throw new BrokerException(BrokerErrorCode.USER_STATUS_FORBIDDEN);
        }

        // 判断是否处于登录冻结
        Long currentTimestamp = System.currentTimeMillis();
        FrozenUserRecord frozenUserRecord = frozenUserRecordMapper.getByUserIdAndFrozenType(header.getOrgId(), user.getUserId(), FrozenType.FROZEN_LOGIN.type());
        if (frozenUserRecord != null
                && (currentTimestamp <= frozenUserRecord.getEndTime() && currentTimestamp >= frozenUserRecord.getStartTime())) {
            log.info("user:{} is under login frozen, cannot do any action", header.getUserId());
            if (frozenUserRecord.getFrozenReason() == FrozenReason.LOGIN_INPUT_ERROR.reason()) {
                throw new BrokerException(BrokerErrorCode.LOGIN_INPUT_ERROR_6);
            } else {
                // 目前只有FrozenReason.SYSTEM_FROZEN
                throw new BrokerException(BrokerErrorCode.FROZEN_LOGIN_BY_SYSTEM);
            }
        }

        String requestId = CryptoUtil.getRandomCode(32);
        FindPwdRecord findPwdRecord = FindPwdRecord.builder()
                .requestId(requestId)
                .orgId(header.getOrgId())
                .userId(user.getUserId())
                .findType(findPwdType.value())
                .isOldVersionRequest(isOldVersionRequest ? 1 : 0)
                .ip(header.getRemoteIp())
                .platform(header.getPlatform().name())
                .userAgent(header.getUserAgent())
                .language(header.getLanguage())
                .appBaseHeader(GrpcHeaderUtil.getAppBaseInfo(header))
                .created(currentTimestamp)
                .updated(currentTimestamp)
                .build();

        FindPwdAuthType authType = null;
        boolean need2FA = false;
        if (!isOldVersionRequest) {
            /* 增加安全校验版本的请求
             * 1、如果用户使用手机找回
             *    如果用户是邮箱注册，提示用邮箱找回
             *    如果用户绑定了邮箱，2fa方式是邮箱，如果绑定了GA，2fa方式是GA，如果用户通过了实名认证，2fa方式是 ID_NO后六位
             *    否则不进行2fa，判定资产
             *    (如果全局仅手机，忽略已绑定邮箱)
             * 2、如果用户使用邮箱找回
             *    如果用户是手机号注册。提示用手机号找回
             *    如果用户绑定了手机，2fa方式是手机，如果绑定了GA，2fa方式是GA，如果用户通过了实名认证，2fa方式是 ID_NO后六位
             *    否则不进行2fa，判定资产
             *    （如果全局仅邮箱，忽略已绑定手机）
             * 3、判定资产
             *    如果用户有资产，判定UA和IP如果和24小时内成功登录过的记录中的UA和IP相同，则放过，否则冻结登录24小时
             *    如果用户没有资产，直接登录
             */
            if (user.getRegisterType() != findPwdType.value()) {
                findPwdRecord.setNeed2fa(0);
                findPwdRecord.setAuthType(FindPwdAuthType.NULL.value());
                findPwdRecord.setStatus(FindPwdStatus.CHANGE_FIND_TYPE.status());
                findPwdRecordMapper.insertSelective(findPwdRecord);
                if (findPwdType == FindPwdType.EMAIL) {
                    throw new BrokerException(BrokerErrorCode.FIND_PWD_BY_MOBILE);
                } else {
                    throw new BrokerException(BrokerErrorCode.FIND_PWD_BY_EMAIL);
                }
            }

            UserVerify userVerify = UserVerify.decrypt(userVerifyMapper.getByUserId(user.getUserId()));
            if (findPwdType == FindPwdType.MOBILE) {
                //如果仅手机，忽略用户的邮箱
                String email = verifyCaptcha && globalNotifyType == 2 ? "" : user.getEmail();
                if (user.getBindGA() == 1) {
                    authType = FindPwdAuthType.GA;
                    need2FA = true;
                } else if (!Strings.isNullOrEmpty(email)) {
                    authType = FindPwdAuthType.EMAIL;
                    need2FA = true;
                } else if (userVerify != null
                        && userVerify.isBasicVerifyPassed()
                        && !Strings.isNullOrEmpty(userVerify.getCardNo())
                        && userVerify.getCardNo().length() > 6) {
                    authType = FindPwdAuthType.ID_CARD;
                    need2FA = true;
                }
            } else {
                //如果仅邮箱，忽略用户的手机
                String mobile = verifyCaptcha && globalNotifyType == 3 ? "" : user.getMobile();
                if (user.getBindGA() == 1) {
                    authType = FindPwdAuthType.GA;
                    need2FA = true;
                } else if (!Strings.isNullOrEmpty(mobile)) {
                    authType = FindPwdAuthType.MOBILE;
                    need2FA = true;
                } else if (userVerify != null
                        && userVerify.getVerifyStatus() == UserVerifyStatus.PASSED.value()
                        && !Strings.isNullOrEmpty(userVerify.getCardNo())
                        && userVerify.getCardNo().length() > 6) {
                    authType = FindPwdAuthType.ID_CARD;
                    need2FA = true;
                }
            }
            findPwdRecord.setNeed2fa(need2FA ? 1 : 0);
            findPwdRecord.setAuthType(authType == null ? FindPwdAuthType.NULL.value() : authType.value());
            findPwdRecord.setStatus(authType == null ? FindPwdStatus.SKIP_2FA.status() : FindPwdStatus.WAIT_2FA.status());
            if (authType == null) {
                /*
                 * 应该都是需要2fa的，authType=NULL说明用户没有2fa，也没有实名。判定资产
                 */
                List<Balance> balanceList = accountService.queryBalance(header.toBuilder().setUserId(user.getUserId()).build(),
                        accountService.getAccountId(header.getOrgId(), user.getUserId()), Lists.newArrayList());
                boolean hasBalance = balanceList.stream().anyMatch(balance -> new BigDecimal(balance.getTotal()).compareTo(BigDecimal.ZERO) > 0);
                findPwdRecord.setHasBalance(hasBalance ? 1 : 0);
                findPwdRecord.setBalanceInfo(JsonUtil.defaultGson().toJson(balanceList));
                // 最后一次成功记录
                boolean frozenLogin = true;
                LoginLog loginLog = loginLogMapper.getLastSuccessLog(user.getOrgId(), user.getUserId(), header.getPlatform().name());
                if (loginLog != null
                        && loginLog.getStatus() == BrokerServerConstants.LOGIN_SUCCESS
                        && Strings.nullToEmpty(loginLog.getIp()).equals(header.getRemoteIp())
                        && Strings.nullToEmpty(loginLog.getUserAgent()).equals(header.getUserAgent())
                        && (System.currentTimeMillis() - loginLog.getUpdated()) < 24 * 3600 * 1000) {
                    frozenLogin = false;
                }
                // 如果没有资产直接放行，有资产，判断是否冻结登录
//                findPwdRecord.setFrozenLogin(!hasBalance ? 0 : (frozenLogin ? 1 : 0));
                findPwdRecord.setFrozenLogin(0);
            }
        } else {
            findPwdRecord.setNeed2fa(0);
            findPwdRecord.setAuthType(FindPwdAuthType.NULL.value());
            findPwdRecord.setStatus(FindPwdStatus.SKIP_2FA.status());
            findPwdRecord.setHasBalance(-1);
            findPwdRecord.setBalanceInfo("");
            findPwdRecord.setFrozenLogin(0);
        }
        findPwdRecordMapper.insertSelective(findPwdRecord);

        return FindPwdCheckResponse.newBuilder()
                .setRequestId(requestId)
                .setNeed2FaCheck(need2FA)
                .setAuthType(authType == null ? "" : authType.name())
                .build();
    }

    public SendFindPwdVerifyCodeResponse sendFindPwdVerifyCode(Header header, String requestId) {
        FindPwdRecord findPwdRecord = findPwdRecordMapper.selectOne(FindPwdRecord.builder().orgId(header.getOrgId()).requestId(requestId).build());
        if (findPwdRecord == null || findPwdRecord.getStatus() == FindPwdStatus.SUCCESS.status()) {
            throw new BrokerException(BrokerErrorCode.REQUEST_INVALID);
        }
        if (findPwdRecord.getAuthRequestCount() > 5) {
            throw new BrokerException(BrokerErrorCode.REPEATED_SUBMIT_REQUEST);
        }
        User user = userMapper.getByOrgAndUserId(header.getOrgId(), findPwdRecord.getUserId());
        if (findPwdRecord.getNeed2fa() == 1) {
            if (findPwdRecord.getAuthType() == FindPwdAuthType.MOBILE.value()) {
                SecuritySendMobileVerifyCodeRequest request = SecuritySendMobileVerifyCodeRequest.newBuilder()
                        .setHeader(header)
                        .setType(SecurityVerifyCodeType.FIND_PASSWORD)
                        .setUserId(user.getUserId())
                        .setNationalCode(user.getNationalCode())
                        .setMobile(user.getMobile())
                        .setLanguage(header.getLanguage())
                        .build();
                SecuritySendVerifyCodeResponse response = grpcSecurityService.sendMobileVerifyCode(request);
                findPwdRecordMapper.updateAuthRequestInfo(findPwdRecord.getId(), response.getOrderId());
                return SendFindPwdVerifyCodeResponse.newBuilder().setOrderId(response.getOrderId()).setIndex(response.getIndex()).build();
            } else if (findPwdRecord.getAuthType() == FindPwdAuthType.EMAIL.value()) {
                SecuritySendEmailVerifyCodeRequest request = SecuritySendEmailVerifyCodeRequest.newBuilder()
                        .setHeader(header)
                        .setType(SecurityVerifyCodeType.FIND_PASSWORD)
                        .setUserId(user.getUserId())
                        .setEmail(user.getEmail())
                        .setLanguage(header.getLanguage())
                        .build();
                SecuritySendVerifyCodeResponse response = grpcSecurityService.sendEmailVerifyCode(request);
                findPwdRecordMapper.updateAuthRequestInfo(findPwdRecord.getId(), response.getOrderId());
                return SendFindPwdVerifyCodeResponse.newBuilder().setOrderId(response.getOrderId()).setIndex(response.getIndex()).build();
            }
        }
        return SendFindPwdVerifyCodeResponse.getDefaultInstance();
    }

    public void findPwdCheck2(Header header, String requestId, Long orderId, String verifyCode) {
        FindPwdRecord findPwdRecord = findPwdRecordMapper.selectOne(FindPwdRecord.builder().orgId(header.getOrgId()).requestId(requestId).build());
        if (findPwdRecord == null || findPwdRecord.getStatus() == FindPwdStatus.SUCCESS.status()) {
            throw new BrokerException(BrokerErrorCode.REQUEST_INVALID);
        }
        User user = userMapper.getByOrgAndUserId(header.getOrgId(), findPwdRecord.getUserId());
        FindPwdRecord updateObj = FindPwdRecord.builder()
                .id(findPwdRecord.getId())
                .updated(System.currentTimeMillis())
                .build();
        try {
            if (findPwdRecord.getIsOldVersionRequest() == 0 && findPwdRecord.getNeed2fa() == 1) {
                header = header.toBuilder().setUserId(user.getUserId()).build();
                if (findPwdRecord.getAuthType() == FindPwdAuthType.MOBILE.value()) {
                    verifyCodeService.validVerifyCode(header, VerifyCodeType.FIND_PASSWORD, findPwdRecord.getAuthOrderId(), verifyCode,
                            user.getNationalCode() + user.getMobile(), AuthType.MOBILE);
                } else if (findPwdRecord.getAuthType() == FindPwdAuthType.EMAIL.value()) {
                    verifyCodeService.validVerifyCode(header, VerifyCodeType.FIND_PASSWORD, findPwdRecord.getAuthOrderId(), verifyCode,
                            user.getEmail(), AuthType.EMAIL);
                } else if (findPwdRecord.getAuthType() == FindPwdAuthType.GA.value()) {
                    validGACode(header, user.getUserId(), verifyCode);
                } else {
                    UserVerify userVerify = UserVerify.decrypt(userVerifyMapper.getByUserId(user.getUserId()));
                    if (userVerify == null || !userVerify.isBasicVerifyPassed()
                            || !verifyCode.equalsIgnoreCase(userVerify.getCardNo())) {
                        throw new BrokerException(BrokerErrorCode.ID_CARD_NO_ERROR);
                    }
                }
                updateObj.setStatus(FindPwdStatus.PASSED_2FA_CHECK.status());
                findPwdRecordMapper.updateByPrimaryKeySelective(updateObj);
            }
        } catch (Exception e) {
            log.error("findPwdCheck2 error", e);
            updateObj.setStatus(FindPwdStatus.ERROR_2FA_VERIFY_CODE.status());
            findPwdRecordMapper.updateByPrimaryKeySelective(updateObj);
            throw e;
        }
    }

    @UserActionLogAnnotation(actionType = BrokerServerConstants.ACTION_TYPE_FIND_PWD)
    public FindPwdResponse findPwd(Header header, String requestId, String password) {
        FindPwdRecord findPwdRecord = findPwdRecordMapper.selectOne(FindPwdRecord.builder().orgId(header.getOrgId()).requestId(requestId).build());
        if (findPwdRecord == null || findPwdRecord.getStatus() == FindPwdStatus.SUCCESS.status()) {
            throw new BrokerException(BrokerErrorCode.REQUEST_INVALID);
        }
        if (findPwdRecord.getIsOldVersionRequest() == 0
                && findPwdRecord.getNeed2fa() == 1
                && findPwdRecord.getStatus() != FindPwdStatus.PASSED_2FA_CHECK.status()) {
            /*
             * 校验请求是否通过了二次验证
             */
            throw new BrokerException(BrokerErrorCode.REQUEST_INVALID);
        }
        header = header.toBuilder().setUserId(findPwdRecord.getUserId()).build();
        User user = userMapper.getByOrgAndUserId(header.getOrgId(), findPwdRecord.getUserId());
        SecurityResetPasswordRequest request = SecurityResetPasswordRequest.newBuilder()
                .setHeader(header)
                .setUserId(user.getUserId())
                .setPassword(password)
                .build();
        SecurityResetPasswordResponse securityResetPasswordResponse = grpcSecurityService.resetPassword(request);
        findPwdRecordMapper.updateByPrimaryKeySelective(FindPwdRecord.builder().id(findPwdRecord.getId()).status(FindPwdStatus.SUCCESS.status()).updated(System.currentTimeMillis()).build());
        log.info("user({}) forgot password and retrieved password, platform prohibits user from withdrawing within 24 hours.", user.getUserId());
        redisTemplate.delete(BrokerServerConstants.FIND_PWD_USER_ID + requestId);
        Long currentTimestamp = System.currentTimeMillis();
        if (commonIniService.getBooleanValueOrDefault(header.getOrgId(), FROZEN_USER_WITHDRAW_AFTER_FIND_PWD, true)) {
            // 找回密码后冻结提币24小时
            forbidWithdraw24Hour(header, FrozenReason.FIND_LOGIN_PWD);
        }
        if (commonIniService.getBooleanValueOrDefault(header.getOrgId(), FROZEN_USER_LOGIN_AFTER_FIND_PWD, true) && findPwdRecord.getFrozenLogin() == 1) {
            FrozenUserRecord frozenRecord = FrozenUserRecord.builder()
                    .orgId(header.getOrgId())
                    .userId(user.getUserId())
                    .frozenType(FrozenType.FROZEN_LOGIN.type())
                    .frozenReason(FrozenReason.FIND_LOGIN_PWD.reason())
                    .startTime(System.currentTimeMillis())
                    .endTime(System.currentTimeMillis() + 24 * 3600 * 1000)
                    .status(FrozenStatus.UNDER_FROZEN.status())
                    .created(currentTimestamp)
                    .updated(currentTimestamp)
                    .build();
            frozenUserRecordMapper.insertRecord(frozenRecord);
        }

        //清理登录错误计数
        redisTemplate.delete(String.format(BrokerServerConstants.LOGIN_ERROR_USER_ID_COUNT, user.getOrgId(), user.getUserId()));
        // 找回密码后，将oAuth的授权清理一次
        oAuthService.invalidUserOAuthAccessToken(header.getOrgId(), user.getUserId(), ThirdPartyAppUserAuthorize.ACCESS_TOKEN_DISABLE_BY_CHANGE_PWD);
        if (!Strings.isNullOrEmpty(user.getMobile())) {
            noticeTemplateService.sendSmsNoticeAsync(header, user.getUserId(), NoticeBusinessType.FIND_PASSWORD_SUCCESS,
                    user.getNationalCode(), user.getMobile());
        }

        if (!Strings.isNullOrEmpty(user.getEmail())) {
            noticeTemplateService.sendEmailNoticeAsync(header, user.getUserId(), NoticeBusinessType.FIND_PASSWORD_SUCCESS, user.getEmail());
        }
        return FindPwdResponse.newBuilder().setUserId(user.getUserId())
                .setFrozenLogin(findPwdRecord.getFrozenLogin() == 1 && commonIniService.getBooleanValueOrDefault(header.getOrgId(), FROZEN_USER_LOGIN_AFTER_FIND_PWD, true)).build();
    }

    @UserActionLogAnnotation(actionType = BrokerServerConstants.ACTION_TYPE_UPDATE_PASSWORD)
    public UpdatePasswordResponse updatePassword(Header header, String oldPassword, String newPassword) {
        if (header.getPlatform() != Platform.ORG_API) {
            if (Strings.isNullOrEmpty(oldPassword)) {
                throw new BrokerException(BrokerErrorCode.OLD_PASSWORD_CANNOT_BE_NULL);
            }
        }
        if (Strings.isNullOrEmpty(newPassword)) {
            throw new BrokerException(BrokerErrorCode.NEW_PASSWORD_CANNOT_BE_NULL);
        }
        SecurityUpdatePasswordRequest request = SecurityUpdatePasswordRequest.newBuilder()
                .setHeader(header)
                .setUserId(header.getUserId())
                .setOldPassword(oldPassword)
                .setNewPassword(newPassword)
                .build();
        SecurityUpdatePasswordResponse securityUpdatePasswordResponse = grpcSecurityService.updatePassword(request);
        log.info("user({}) modify password, platform prohibits user from withdrawing within 24 hours.", header.getUserId());
        //修改密码冻结24小时出金
        forbidWithdraw24Hour(header, FrozenReason.MODIFY_LOGIN_PWD);
        oAuthService.invalidUserOAuthAccessToken(header.getOrgId(), header.getUserId(), ThirdPartyAppUserAuthorize.ACCESS_TOKEN_DISABLE_BY_CHANGE_PWD);
        return UpdatePasswordResponse.newBuilder().build();
    }

    @UserActionLogAnnotation(actionType = BrokerServerConstants.ACTION_TYPE_SET_PASSWORD)
    public SetLoginPasswordResponse setLoginPassword(Header header, String password, Integer authTypeValue, Long verifyCodeOrderId, String verifyCode) {
        User user = userMapper.getByUserId(header.getUserId());
        if (user.getBindPassword() == 1) {
            throw new BrokerException(BrokerErrorCode.OPERATION_HAS_NO_PERMISSION);
        }
        AuthType authType = AuthType.fromValue(authTypeValue);
        validVerifyCode(header, authType, verifyCodeOrderId, verifyCode, VerifyCodeType.SET_PASSWORD);
        SecurityUpdatePasswordRequest request = SecurityUpdatePasswordRequest.newBuilder()
                .setHeader(header)
                .setUserId(header.getUserId())
                .setOldPassword("") // 用户的旧密码为空字符串
                .setNewPassword(password)
                .setFirstSetPassword(true)
                .build();
        SecurityUpdatePasswordResponse securityUpdatePasswordResponse = grpcSecurityService.updatePassword(request);
        User updateObj = User.builder()
                .userId(header.getUserId())
                .bindPassword(1)
                .updated(System.currentTimeMillis())
                .build();
        userMapper.updateRecord(updateObj);
        return SetLoginPasswordResponse.newBuilder().build();
    }

    /**
     * Create ApiKey
     */
    @UserActionLogAnnotation(actionType = BrokerServerConstants.ACTION_TYPE_CREATE_API, action = "{#tag}")
    public CreateApiKeyResponse createApiKey(Header header, AccountTypeEnum accountTypeEnum, int index, String tag, int type,
                                             Integer authTypeValue, Long verifyCodeOrderId, String verifyCode) {
        AccountType accountType = AccountType.fromAccountTypeEnum(accountTypeEnum);

        Account account = accountMapper.getAccountByType(header.getOrgId(), header.getUserId(), accountType.value(), index);
        if (accountType != AccountType.MAIN && accountType != AccountType.FUTURES) {
            throw new BrokerException(BrokerErrorCode.CANNOT_CREATE_API_WITH_INVALID_ACCOUNT_TYPE);
        }
        if (Strings.isNullOrEmpty(verifyCode)) {
            throw new BrokerException(BrokerErrorCode.VERIFY_CODE_CANNOT_BE_NULL);
        }
        if (Strings.isNullOrEmpty(tag)) {
            throw new BrokerException(BrokerErrorCode.API_KEY_TAG_CANNOT_BE_NULL);
        }
        ApiKeyType apiKeyType = ApiKeyType.forType(type);
        if (apiKeyType == null || !apiKeyType.isUserAPiKey()) {
            throw new BrokerException(BrokerErrorCode.PARAM_INVALID);
        }
        AuthType authType = AuthType.fromValue(authTypeValue);
        if (authType == AuthType.GA) {
            validGACode(header, header.getUserId(), verifyCode);
        } else {
            validVerifyCode(header, authType, verifyCodeOrderId, verifyCode, VerifyCodeType.CREATE_API_KEY);
        }
        SecurityCreateApiKeyRequest request = SecurityCreateApiKeyRequest.newBuilder()
                .setHeader(header)
                .setUserId(header.getUserId())
                .setAccountType(accountTypeEnum)
                .setIndex(index)
                .setAccountName(Strings.nullToEmpty(account.getAccountName()))
                .setAccountId(account.getAccountId())
                .setTag(tag)
                .setType(type)
                .build();
        SecurityCreateApiKeyResponse securityCreateApiKeyResponse = grpcSecurityService.createApiKey(request);
        if (authType != AuthType.GA) {
            User user = userMapper.getByUserId(header.getUserId());
            if (authType == AuthType.MOBILE) {
                verifyCodeService.invalidVerifyCode(header, VerifyCodeType.CREATE_API_KEY, verifyCodeOrderId,
                        user.getNationalCode() + user.getMobile());
            } else {
                verifyCodeService.invalidVerifyCode(header, VerifyCodeType.CREATE_API_KEY, verifyCodeOrderId, user.getEmail());
            }
        }
        return CreateApiKeyResponse.newBuilder().setApiKey(getApiKey(securityCreateApiKeyResponse.getApiKey())).build();
    }

    public BaseResult<CreateApiKeyResponse> createThirdPartyUserApiKey(Header header, String thirdUserId, Long userId,
                                                                       AccountTypeEnum accountTypeEnum, int accountIndex, String tag, int type) {
        User user = userMapper.getByUserId(header.getUserId());
        if (user == null || user.getIsVip().equals(0)) {
            return BaseResult.fail(BrokerErrorCode.NO_PERMISSION_TO_CREATE_VIRTUAL_ACCOUNT);
        }
        User virtualUser = thirdPartyUserService.getVirtualUser(header.getOrgId(), header.getUserId(), thirdUserId, userId);
        if (virtualUser.getUserStatus() != 1) {
            return BaseResult.fail(BrokerErrorCode.USER_STATUS_FORBIDDEN);
        }
        Long accountId = accountService.getAccountId(header.getOrgId(), virtualUser.getUserId(), AccountType.fromAccountTypeEnum(accountTypeEnum), accountIndex);
        ApiKeyType apiKeyType = ApiKeyType.forType(type);
        if (apiKeyType == null || !apiKeyType.isUserAPiKey()) {
            return BaseResult.fail(BrokerErrorCode.PARAM_INVALID);
        }
        SecurityCreateApiKeyRequest request = SecurityCreateApiKeyRequest.newBuilder()
                .setHeader(header.toBuilder().setUserId(virtualUser.getUserId()).build())
                .setUserId(virtualUser.getUserId())
                .setAccountId(accountId)
                .setAccountType(accountTypeEnum)
                .setIndex(accountIndex)
                .setAccountName("")
                .setTag(tag)
                .setType(type)
                .build();
        SecurityCreateApiKeyResponse securityCreateApiKeyResponse = grpcSecurityService.createApiKey(request);
        return BaseResult.success(CreateApiKeyResponse.newBuilder().setApiKey(getApiKey(securityCreateApiKeyResponse.getApiKey())).build());
    }

    /**
     * Modify the user's ApiKey, you can modify ipWhiteList or enable/disable the ApiKey
     * <p>If you want to modify the tag、ipWhiteList and status synchronously, this method needs to
     * be modified.
     */
    public UpdateApiKeyResponse updateApiKeyIps(Header header, Long apiKeyId, String ipWhiteList,
                                                Integer authTypeValue, Long verifyCodeOrderId, String verifyCode) {
        Long accountId = accountService.getMainAccountId(header);
        if (Strings.isNullOrEmpty(verifyCode)) {
            throw new BrokerException(BrokerErrorCode.VERIFY_CODE_CANNOT_BE_NULL);
        }
        AuthType authType = AuthType.fromValue(authTypeValue);
        if (authType == AuthType.GA) {
            validGACode(header, header.getUserId(), verifyCode);
        } else {
            validVerifyCode(header, authType, verifyCodeOrderId, verifyCode, VerifyCodeType.MODIFY_API_KEY);
        }

        SecurityUpdateApiKeyRequest request = SecurityUpdateApiKeyRequest.newBuilder()
                .setHeader(header)
                .setUserId(header.getUserId())
                .setAccountId(accountId)
                .setApiKeyId(apiKeyId)
                .setIpWhiteList(ipWhiteList)
                .setUpdateType(SecurityUpdateApiKeyRequest.UpdateType.UPDATE_IP_WHITE_LIST)
                .build();

        SecurityUpdateApiKeyResponse securityUpdateApiKeyResponse = grpcSecurityService.updateApiKey(request);
        if (authType != AuthType.GA) {
            User user = userMapper.getByUserId(header.getUserId());
            if (authType == AuthType.MOBILE) {
                verifyCodeService.invalidVerifyCode(header, VerifyCodeType.MODIFY_API_KEY, verifyCodeOrderId,
                        user.getNationalCode() + user.getMobile());
            } else {
                verifyCodeService.invalidVerifyCode(header, VerifyCodeType.MODIFY_API_KEY, verifyCodeOrderId, user.getEmail());
            }
        }
        return UpdateApiKeyResponse.newBuilder().build();
    }

    public BaseResult<UpdateApiKeyResponse> updateThirdPartyUserApiKeyIps(Header header, String thirdUserId, Long userId,
                                                                          Long apiKeyId, String ipWhiteList) {
        User user = userMapper.getByUserId(header.getUserId());
        if (user == null || user.getIsVip().equals(0)) {
            return BaseResult.fail(BrokerErrorCode.NO_PERMISSION_TO_CREATE_VIRTUAL_ACCOUNT);
        }
        User virtualUser = thirdPartyUserService.getVirtualUser(header.getOrgId(), header.getUserId(), thirdUserId, userId);
        if (virtualUser.getUserStatus() != 1) {
            return BaseResult.fail(BrokerErrorCode.USER_STATUS_FORBIDDEN);
        }

        SecurityUpdateApiKeyRequest request = SecurityUpdateApiKeyRequest.newBuilder()
                .setHeader(header.toBuilder().setUserId(virtualUser.getUserId()).build())
                .setUserId(virtualUser.getUserId())
                .setApiKeyId(apiKeyId)
                .setIpWhiteList(ipWhiteList)
                .setUpdateType(SecurityUpdateApiKeyRequest.UpdateType.UPDATE_IP_WHITE_LIST)
                .build();

        SecurityUpdateApiKeyResponse securityUpdateApiKeyResponse = grpcSecurityService.updateApiKey(request);
        return BaseResult.success(UpdateApiKeyResponse.newBuilder().build());
    }

    /**
     * Modify the user's ApiKey, you can modify ipWhiteList or enable/disable the ApiKey
     * <p>If you want to modify the tag、ipWhiteList and status synchronously, this method needs to
     * be modified.
     */
    public UpdateApiKeyResponse updateApiKeyStatus(Header header, Long apiKeyId, Integer status,
                                                   Integer authTypeValue, Long verifyCodeOrderId, String verifyCode) {
        if (Strings.isNullOrEmpty(verifyCode)) {
            throw new BrokerException(BrokerErrorCode.VERIFY_CODE_CANNOT_BE_NULL);
        }
        AuthType authType = AuthType.fromValue(authTypeValue);
        if (authType == AuthType.GA) {
            validGACode(header, header.getUserId(), verifyCode);
        } else {
            validVerifyCode(header, authType, verifyCodeOrderId, verifyCode, VerifyCodeType.MODIFY_API_KEY);
        }

        SecurityUpdateApiKeyRequest request = SecurityUpdateApiKeyRequest.newBuilder()
                .setHeader(header)
                .setUserId(header.getUserId())
                .setAccountId(accountService.getMainAccountId(header))
                .setApiKeyId(apiKeyId)
                .setStatus(ApiKeyStatus.forNumber(status))
                .setUpdateType(SecurityUpdateApiKeyRequest.UpdateType.UPDATE_STATUS)
                .build();

        SecurityUpdateApiKeyResponse securityUpdateApiKeyResponse = grpcSecurityService.updateApiKey(request);
        if (authType != AuthType.GA) {
            User user = userMapper.getByUserId(header.getUserId());
            if (authType == AuthType.MOBILE) {
                verifyCodeService.invalidVerifyCode(header, VerifyCodeType.MODIFY_API_KEY, verifyCodeOrderId,
                        user.getNationalCode() + user.getMobile());
            } else {
                verifyCodeService.invalidVerifyCode(header, VerifyCodeType.MODIFY_API_KEY, verifyCodeOrderId, user.getEmail());
            }
        }
        return UpdateApiKeyResponse.newBuilder().build();
    }

    public BaseResult<UpdateApiKeyResponse> updateThirdPartyUserApiKeyStatus(Header header, String thirdUserId, Long userId,
                                                                             Long apiKeyId, Integer status) {
        User user = userMapper.getByUserId(header.getUserId());
        if (user == null || user.getIsVip().equals(0)) {
            return BaseResult.fail(BrokerErrorCode.NO_PERMISSION_TO_CREATE_VIRTUAL_ACCOUNT);
        }
        User virtualUser = thirdPartyUserService.getVirtualUser(header.getOrgId(), header.getUserId(), thirdUserId, userId);
        if (virtualUser.getUserStatus() != 1) {
            return BaseResult.fail(BrokerErrorCode.USER_STATUS_FORBIDDEN);
        }

        SecurityUpdateApiKeyRequest request = SecurityUpdateApiKeyRequest.newBuilder()
                .setHeader(header.toBuilder().setUserId(virtualUser.getUserId()).build())
                .setUserId(virtualUser.getUserId())
                .setApiKeyId(apiKeyId)
                .setStatus(ApiKeyStatus.forNumber(status))
                .setUpdateType(SecurityUpdateApiKeyRequest.UpdateType.UPDATE_STATUS)
                .build();

        SecurityUpdateApiKeyResponse securityUpdateApiKeyResponse = grpcSecurityService.updateApiKey(request);
        return BaseResult.success(UpdateApiKeyResponse.newBuilder().build());
    }

    /**
     * Delete user's ApiKey
     */
    @UserActionLogAnnotation(actionType = BrokerServerConstants.ACTION_TYPE_DELETE_API, action = "{#apiKeyId}")
    public DeleteApiKeyResponse deleteApiKey(Header header, Long apiKeyId,
                                             Integer authTypeValue, Long verifyCodeOrderId, String verifyCode) {
        if (Strings.isNullOrEmpty(verifyCode)) {
            throw new BrokerException(BrokerErrorCode.VERIFY_CODE_CANNOT_BE_NULL);
        }
        AuthType authType = AuthType.fromValue(authTypeValue);
        if (authType == AuthType.GA) {
            validGACode(header, header.getUserId(), verifyCode);
        } else {
            validVerifyCode(header, authType, verifyCodeOrderId, verifyCode, VerifyCodeType.DELETE_API_KEY);
        }
        SecurityDeleteApiKeyRequest request = SecurityDeleteApiKeyRequest.newBuilder()
                .setHeader(header)
                .setUserId(header.getUserId())
                .setAccountId(accountService.getMainAccountId(header))
                .setApiKeyId(apiKeyId)
                .build();
        SecurityDeleteApiKeyResponse securityDeleteApiKeyResponse = grpcSecurityService.deleteApiKey(request);
        if (authType != AuthType.GA) {
            User user = userMapper.getByUserId(header.getUserId());
            if (authType == AuthType.MOBILE) {
                verifyCodeService.invalidVerifyCode(header, VerifyCodeType.DELETE_API_KEY, verifyCodeOrderId,
                        user.getNationalCode() + user.getMobile());
            } else {
                verifyCodeService.invalidVerifyCode(header, VerifyCodeType.DELETE_API_KEY, verifyCodeOrderId, user.getEmail());
            }
        }
        return DeleteApiKeyResponse.newBuilder().build();
    }

    public BaseResult<DeleteApiKeyResponse> deleteThirdPartyUserApiKey(Header header, String thirdUserId, Long userId, Long apiKeyId) {
        User user = userMapper.getByUserId(header.getUserId());
        if (user == null || user.getIsVip().equals(0)) {
            return BaseResult.fail(BrokerErrorCode.NO_PERMISSION_TO_CREATE_VIRTUAL_ACCOUNT);
        }
        User virtualUser = thirdPartyUserService.getVirtualUser(header.getOrgId(), header.getUserId(), thirdUserId, userId);
        if (virtualUser.getUserStatus() != 1) {
            return BaseResult.fail(BrokerErrorCode.USER_STATUS_FORBIDDEN);
        }

        SecurityDeleteApiKeyRequest request = SecurityDeleteApiKeyRequest.newBuilder()
                .setHeader(header.toBuilder().setUserId(virtualUser.getUserId()).build())
                .setUserId(virtualUser.getUserId())
                .setApiKeyId(apiKeyId)
                .build();
        SecurityDeleteApiKeyResponse securityDeleteApiKeyResponse = grpcSecurityService.deleteApiKey(request);
        return BaseResult.success(DeleteApiKeyResponse.newBuilder().build());
    }

    /**
     * Query user's ApiKey records
     */
    public QueryApiKeyResponse queryOrgAuthorizedAccountApiKeys(Header header, long accountId) {
        boolean authorizedOrgOpen = commonIniService.getBooleanValueOrDefault(header.getOrgId(), BrokerServerConstants.SUB_ACCOUNT_AUTHORIZED_ORG_KEY, false);
        if (!authorizedOrgOpen) {
            log.error("org:{} not open SUB_ACCOUNT_AUTHORIZED_ORG", header.getOrgId());
            return QueryApiKeyResponse.newBuilder().setRet(BrokerErrorCode.NO_SUB_ACCOUNT_AUTHORIZED_ORG_CONFIG.code()).build();
        }
        boolean canGetKey = commonIniService.getBooleanValueOrDefault(header.getOrgId(), BrokerServerConstants.ORG_OBTAIN_OPENAPI_SECRETKEY, false);
        if (!canGetKey) {
            log.error("org:{} not open ORG_OBTAIN_OPENAPI_SECRETKEY", header.getOrgId());
            return QueryApiKeyResponse.newBuilder().setRet(BrokerErrorCode.OPERATION_HAS_NO_PERMISSION.code()).build();
        }


        Account account = accountMapper.getAccountByAccountId(accountId);
        if (account == null || !account.getUserId().equals(header.getUserId())) {
            return QueryApiKeyResponse.newBuilder().setRet(BrokerErrorCode.PARAM_INVALID.code()).build();
        }
        if (account.getIsAuthorizedOrg() == 0) {
            throw new BrokerException(BrokerErrorCode.NOT_AUTHORIZED_ACCOUNT);
        }

        SecurityQueryOrgAuthorizedAccountApiKeysRequest request = SecurityQueryOrgAuthorizedAccountApiKeysRequest.newBuilder()
                .setHeader(header)
                .setAccountId(accountId)
                .build();
        SecurityQueryUserApiKeysResponse securityQueryUserApiKeysResponse = grpcSecurityService.queryAuthorizedAccountApiKeys(request);
        List<ApiKey> apiKeyList = Lists.newArrayList();
        if (securityQueryUserApiKeysResponse.getApiKeyList() != null) {
            apiKeyList = securityQueryUserApiKeysResponse.getApiKeyList().stream().map(this::getApiKey).collect(Collectors.toList());
        }
        return QueryApiKeyResponse.newBuilder().addAllApiKeys(apiKeyList).build();
    }

    public QueryApiKeyResponse queryApiKeys(Header header) {
        SecurityQueryUserApiKeysRequest request = SecurityQueryUserApiKeysRequest.newBuilder()
                .setHeader(header)
                .setUserId(header.getUserId())
                .build();
        SecurityQueryUserApiKeysResponse securityQueryUserApiKeysResponse = grpcSecurityService.queryApiKeys(request);
        List<ApiKey> apiKeyList = Lists.newArrayList();
        if (securityQueryUserApiKeysResponse.getApiKeyList() != null) {
            apiKeyList = securityQueryUserApiKeysResponse.getApiKeyList().stream().map(this::getApiKey).collect(Collectors.toList());
        }
        return QueryApiKeyResponse.newBuilder().addAllApiKeys(apiKeyList).build();
    }

    public BaseResult<QueryApiKeyResponse> queryThirdPartyUserApiKeys(Header header, String thirdUserId, Long userId) {
        User user = userMapper.getByUserId(header.getUserId());
        if (user == null || user.getIsVip().equals(0)) {
            return BaseResult.fail(BrokerErrorCode.NO_PERMISSION_TO_CREATE_VIRTUAL_ACCOUNT);
        }
        User virtualUser = thirdPartyUserService.getVirtualUser(header.getOrgId(), header.getUserId(), thirdUserId, userId);
        if (virtualUser.getUserStatus() != 1) {
            return BaseResult.fail(BrokerErrorCode.USER_STATUS_FORBIDDEN);
        }
        SecurityQueryUserApiKeysRequest request = SecurityQueryUserApiKeysRequest.newBuilder()
                .setHeader(header.toBuilder().setUserId(virtualUser.getUserId()))
                .setUserId(virtualUser.getUserId())
                .build();
        SecurityQueryUserApiKeysResponse securityQueryUserApiKeysResponse = grpcSecurityService.queryApiKeys(request);
        List<ApiKey> apiKeyList = Lists.newArrayList();
        if (securityQueryUserApiKeysResponse.getApiKeyList() != null) {
            apiKeyList = securityQueryUserApiKeysResponse.getApiKeyList().stream().map(this::getApiKey).collect(Collectors.toList());
        }
        return BaseResult.success(QueryApiKeyResponse.newBuilder().addAllApiKeys(apiKeyList).build());
    }

    private ApiKey getApiKey(ApiKeyInfo apiKey) {
        return ApiKey.newBuilder()
                .setId(apiKey.getId())
                .setAccountId(apiKey.getAccountId())
                .setAccountType(apiKey.getAccountType())
                .setIndex(apiKey.getIndex())
                .setAccountName(apiKey.getAccountName())
                .setApiKey(apiKey.getApiKey())
                .setSecretKey(Strings.nullToEmpty(apiKey.getSecretKey()))
                .setTag(apiKey.getTag())
                .setIpWhiteList(apiKey.getIpWhiteList())
                .setStatus(apiKey.getStatusValue())
                .setCreated(apiKey.getCreated())
                .setUpdated(apiKey.getUpdated())
                .setType(apiKey.getType())
                .setLevel(apiKey.getLevel())
                .build();
    }

    /**
     * @param userId
     * @param enabled, true-enable false-disabled
     * @return
     */
    public boolean updateUserStatus(Long userId, boolean enabled) {
        User user = userMapper.getByUserId(userId);
        if (user == null) {
            return false;
        }
        int newStatus = enabled ? UserStatusEnum.ENABLE.getValue() : UserStatusEnum.DISABLED.getValue();
        if (user.getUserStatus() == newStatus) {
            return false;
        }
        SecurityUpdateUserStatusRequest request = SecurityUpdateUserStatusRequest.newBuilder()
                .setHeader(Header.newBuilder().setOrgId(user.getOrgId()).setUserId(userId).build())
                .setUserId(userId)
                .setUserStatus(SecurityUser.UserStatus.forNumber(newStatus))
                .build();
        SecurityUpdateUserStatusResponse response = grpcSecurityService.updateUserStatus(request);
        if (response != null && response.getRet() == 0) {
            Integer num = userMapper.updateUserStatus(userId, newStatus, user.getUserStatus());
            try {
                if (!enabled) {
                    oAuthService.invalidUserOAuthAccessToken(user.getOrgId(), userId, ThirdPartyAppUserAuthorize.ACCESS_TOKEN_DISABLE_BY_USER_STATUS_CHANGE);
                }
            } catch (Exception e) {
                log.error("invalidUserOAuthAccessToken error", e);
            }
            return num == 1;
        }

        return false;
    }

    @UserActionLogAnnotation(actionType = BrokerServerConstants.ACTION_TYPE_SET_TRADE_PWD)
    public SetTradePasswordResponse setTradePassword(Header header, Integer setType, String tradePwd, Long orderId, String verifyCode, Boolean frozenUser) {
        User user = userMapper.getByUserId(header.getUserId());
        if (user.getBindTradePwd() == 1 && setType == 1) {
            return SetTradePasswordResponse.newBuilder().setRet(BrokerErrorCode.REPEAT_SET_TRADE_PWD.code()).build();
        }
        if (header.getPlatform() == Platform.ORG_API) {
            boolean orgApiSetTradePasswordSwitch = commonIniService.getBooleanValueOrDefault(header.getOrgId(), "orgApiSetTradePassword", false);
            if (!orgApiSetTradePasswordSwitch) {
                throw new BrokerException(BrokerErrorCode.FEATURE_SUSPENDED);
            }
        }
        if (header.getPlatform() != Platform.ORG_API) {
            String receiver;
            AuthType authType;
            if (user.getRegisterType().equals(RegisterType.MOBILE.value())) {
                receiver = user.getNationalCode() + user.getMobile();
                authType = AuthType.MOBILE;
            } else {
                receiver = user.getEmail();
                authType = AuthType.EMAIL;
            }
            if (setType == 1) {
                verifyCodeService.validVerifyCode(header, VerifyCodeType.CREATE_FUND_PWD, orderId, verifyCode, receiver, authType);
            } else {
                verifyCodeService.validVerifyCode(header, VerifyCodeType.MODIFY_FUND_PWD, orderId, verifyCode, receiver, authType);
            }
        }
        SecuritySetTradePasswordRequest request = SecuritySetTradePasswordRequest.newBuilder()
                .setHeader(header)
                .setUserId(header.getUserId())
                .setTradePassword(tradePwd)
                .build();
        SecuritySetTradePasswordResponse response = grpcSecurityService.setTradePwd(request);
        // 资金密码设置成功，修改状态
        if (response.getRet() == 0) {
            User updater = User.builder()
                    .userId(header.getUserId())
                    .bindTradePwd(1)
                    .updated(System.currentTimeMillis())
                    .build();
            userMapper.updateRecord(updater);
            if (setType != 1 && (header.getPlatform() != Platform.ORG_API || frozenUser)) {
                //修改资金密码冻结24小时出金
                forbidWithdraw24Hour(header, FrozenReason.MODIFY_FUND_PWD);
            }
        }
        if (setType == 1) {
            verifyCodeService.invalidVerifyCode(header, VerifyCodeType.CREATE_FUND_PWD, orderId, "");
        } else {
            verifyCodeService.invalidVerifyCode(header, VerifyCodeType.MODIFY_FUND_PWD, orderId, "");
        }
        return SetTradePasswordResponse.newBuilder().setRet(response.getRet()).build();
    }

    public VerifyTradePasswordResponse verifyTradePassword(Header header, String tradePwd) {
        VerifyTradePasswordResponse.Builder result = VerifyTradePasswordResponse.newBuilder();
        String errorCountKey = String.format(TRADE_PW_ERROR_COUNT_KEY, header.getOrgId(), header.getUserId());
        String errorCount = redisTemplate.opsForValue().get(errorCountKey);
        // 资金密码错误次数达到最大，已被锁定
        if (StringUtils.isNotBlank(errorCount) && Integer.parseInt(errorCount) >= 5) {
            return result.setRet(BrokerErrorCode.TRADE_PWD_ERROR_6.code()).build();
        }
        SecurityVerifyTradePasswordRequest verifyRequest = SecurityVerifyTradePasswordRequest.newBuilder()
                .setHeader(header)
                .setUserId(header.getUserId())
                .setTradePassword(tradePwd)
                .build();
        SecurityVerifyTradePasswordResponse response = grpcSecurityService.verifyTradePwd(verifyRequest);
        // 验证成功
        if (response.getRet() == 0) {
            // 成功一次则清除错误次数
            redisTemplate.delete(errorCountKey);
            return result.setRet(0).build();
        }

        Boolean exist = redisTemplate.opsForValue().setIfAbsent(errorCountKey, "0");
        if (exist) {
            redisTemplate.expire(errorCountKey, 24, TimeUnit.HOURS);
        }
        Long errorTimes = redisTemplate.opsForValue().increment(errorCountKey, 1L);
        if (errorTimes == 1) {
            result.setRet(BrokerErrorCode.TRADE_PWD_ERROR_1.code());
        } else if (errorTimes == 2) {
            result.setRet(BrokerErrorCode.TRADE_PWD_ERROR_2.code());
        } else if (errorTimes == 3) {
            result.setRet(BrokerErrorCode.TRADE_PWD_ERROR_3.code());
        } else if (errorTimes == 4) {
            result.setRet(BrokerErrorCode.TRADE_PWD_ERROR_4.code());
        } else if (errorTimes == 5) {
            result.setRet(BrokerErrorCode.TRADE_PWD_ERROR_5.code());
        } else {
            result.setRet(BrokerErrorCode.TRADE_PWD_ERROR_6.code());
        }
        return result.build();
    }

    @UserActionLogAnnotation(actionType = BrokerServerConstants.ACTION_TYPE_UNBIND_GA)
    public UserUnbindGaResponse userUnbindGa(Header header, Long orderId, String verifyCode, String gaCode) {
        UserUnbindGaResponse.Builder result = UserUnbindGaResponse.newBuilder();
        User user = userMapper.getByOrgAndUserId(header.getOrgId(), header.getUserId());
        if (user == null) {
            return result.setRet(BrokerErrorCode.USER_NOT_EXIST.code()).build();
        }
        String receiver;
        AuthType authType;
        if (user.getRegisterType() == RegisterType.EMAIL.value()) {
            receiver = user.getEmail();
            authType = AuthType.EMAIL;
        } else {
            receiver = user.getNationalCode() + user.getMobile();
            authType = AuthType.MOBILE;
        }
        verifyCodeService.validVerifyCode(header, VerifyCodeType.UNBIND_GA, orderId, verifyCode, receiver, authType);
        validGACode(header, header.getUserId(), gaCode);
        SecurityUnBindGAResponse response = grpcSecurityService.unBindGA(SecurityUnBindGARequest.newBuilder().setHeader(header).setUserId(header.getUserId()).build());
        if (response.getRet() != 0) {
            return result.setRet(response.getRet()).build();
        }
        // 修改绑定ga的状态为0
        userMapper.unbindGA(header.getOrgId(), header.getUserId());
        //解绑GA冻结出金24小时
        forbidWithdraw24Hour(header, FrozenReason.UNBIND_GA);
        verifyCodeService.invalidVerifyCode(header, VerifyCodeType.UNBIND_GA, orderId, "");
        return result.build();
    }

    @UserActionLogAnnotation(actionType = BrokerServerConstants.ACTION_TYPE_UNBIND_EMAIL)
    public UserUnbindEmailResponse userUnbindEmail(Header header, Long emailOrderId, String emailVerifyCode, Long mobileOrderId, String mobileVerifyCode) {
        UserUnbindEmailResponse.Builder result = UserUnbindEmailResponse.newBuilder();
        User user = userMapper.getByOrgAndUserId(header.getOrgId(), header.getUserId());
        if (user == null) {
            return result.setRet(BrokerErrorCode.USER_NOT_EXIST.code()).build();
        }

        if (user.getRegisterType() == RegisterType.EMAIL.value()) {
            return result.setRet(BrokerErrorCode.UNBIND_EMAIL_FAILED.code()).build();
        }
        if (StringUtils.isBlank(user.getEmail())) {
            return result.setRet(BrokerErrorCode.EMAIL_NOT_BIND.code()).build();
        }
        //验证邮箱验证码
        verifyCodeService.validVerifyCode(header, VerifyCodeType.UNBIND_EMAIL, emailOrderId, emailVerifyCode, user.getEmail(), AuthType.EMAIL);
        //验证手机验证码
        verifyCodeService.validVerifyCode(header, VerifyCodeType.UNBIND_EMAIL, mobileOrderId, mobileVerifyCode, user.getNationalCode() + user.getMobile(), AuthType.MOBILE);
        String originalEmail = user.getEmail();
        // 修改邮箱为空字符串
        userMapper.unbindUserEmail(header.getOrgId(), header.getUserId());
        log.info(" unbindEmail executed: userId:{} originalEmail:{}", user.getUserId(), originalEmail);
        try {
            // 同步修改bh那边的信息也为空字符串
            ModifyOrgUserEmailRequest request = ModifyOrgUserEmailRequest.newBuilder()
                    .setBaseRequest(BaseReqUtil.getBaseRequest(header.getOrgId()))
                    .setOrgId(header.getOrgId())
                    .setUserId(header.getUserId())
                    .setEmail("")
                    .build();
            grpcAccountService.modifyEmail(request);
        } catch (Exception e) {
            log.error("unbindEmail and notice bh-server error, orgId:{}, userId:{}", header.getOrgId(), header.getUserId(), e);
        }
        //解绑邮箱 禁止提币24H
        forbidWithdraw24Hour(header, FrozenReason.UNBIND_EMAIL);
        verifyCodeService.invalidVerifyCode(header, VerifyCodeType.UNBIND_EMAIL, emailOrderId, "");
        verifyCodeService.invalidVerifyCode(header, VerifyCodeType.UNBIND_EMAIL, mobileOrderId, "");
        return result.build();
    }

    @UserActionLogAnnotation(actionType = BrokerServerConstants.ACTION_TYPE_UNBIND_MOBILE)
    public UserUnbindMobileResponse userUnbindMobile(Header header, Long emailOrderId, String emailVerifyCode, Long mobileOrderId, String mobileVerifyCode) {
        UserUnbindMobileResponse.Builder result = UserUnbindMobileResponse.newBuilder();
        User user = userMapper.getByOrgAndUserId(header.getOrgId(), header.getUserId());
        if (user == null) {
            return result.setRet(BrokerErrorCode.USER_NOT_EXIST.code()).build();
        }
        if (user.getRegisterType() == RegisterType.MOBILE.value()) {
            return result.setRet(BrokerErrorCode.UNBIND_MOBILE_FAILED.code()).build();
        }
        if (StringUtils.isBlank(user.getMobile())) {
            return result.setRet(BrokerErrorCode.MOBILE_NOT_BIND.code()).build();
        }
        //验证邮箱验证码
        verifyCodeService.validVerifyCode(header, VerifyCodeType.UNBIND_MOBILE, emailOrderId, emailVerifyCode, user.getEmail(), AuthType.EMAIL);
        //验证手机验证码
        verifyCodeService.validVerifyCode(header, VerifyCodeType.UNBIND_MOBILE, mobileOrderId, mobileVerifyCode, user.getNationalCode() + user.getMobile(), AuthType.MOBILE);
        String originalNationalCode = user.getNationalCode();
        String originalMobile = user.getMobile();
        //// 修改手机号为空字符串
        userMapper.unbindUserMobile(header.getOrgId(), header.getUserId());
        log.info(" unbindMobile executed: userId:{} originalMobile:{}-{}", user.getUserId(), originalNationalCode, originalMobile);
        try {
            // 同步修改bh那边的信息也为空字符串
            ModifyOrgUserMobileRequest request = ModifyOrgUserMobileRequest.newBuilder()
                    .setBaseRequest(BaseReqUtil.getBaseRequest(header.getOrgId()))
                    .setOrgId(header.getOrgId())
                    .setUserId(header.getUserId())
                    .setCountryCode("")
                    .setMobile("")
                    .build();
            grpcAccountService.modifyMobile(request);
        } catch (Exception e) {
            log.error("unbindMobile and notice bh-server error, orgId:{}, userId:{}", header.getOrgId(), header.getUserId(), e);
        }
        //解绑手机冻结24小时出金
        forbidWithdraw24Hour(header, FrozenReason.UNBIND_MOBILE);
        verifyCodeService.invalidVerifyCode(header, VerifyCodeType.UNBIND_MOBILE, emailOrderId, "");
        verifyCodeService.invalidVerifyCode(header, VerifyCodeType.UNBIND_MOBILE, mobileOrderId, "");
        return result.build();
    }

    @UserActionLogAnnotation(actionType = BrokerServerConstants.ACTION_TYPE_ALTER_EMAIL)
    public AlterEmailResponse alterEmail(Header header, Long originalEmailOrderId, String originalEmailVerifyCode, Long mobileOrderId, String mobileVerifyCode,
                                         String email, Long alterEmailOrderId, String alterEmailVerifyCode, String gaVerifyCode) {

        ValidateUtil.validateEmail(email);

        AlterEmailResponse.Builder result = AlterEmailResponse.newBuilder();
        User user = userMapper.getByOrgAndUserId(header.getOrgId(), header.getUserId());
        if (user == null) {
            return result.setRet(BrokerErrorCode.USER_NOT_EXIST.code()).build();
        }
        if (userMapper.countEmailExists(header.getOrgId(), email, EmailUtils.emailAlias(email)) > 0) {
            log.error("The email already exists orgId:{} uid:{}", header.getOrgId(), header.getUserId());
            throw new BrokerException(BrokerErrorCode.EMAIL_EXIST);
        }
        /*if (user.getRegisterType() == RegisterType.EMAIL.value()) {
            return result.setRet(BrokerErrorCode.UNBIND_EMAIL_FAILED.code()).build();
        }*/
        if (StringUtils.isBlank(user.getEmail())) {
            return result.setRet(BrokerErrorCode.EMAIL_NOT_BIND.code()).build();
        }
        if (verifyCaptcha && globalNotifyType == 3) {
            //当仅邮箱时，修改邮箱，要求必须有ga
            if (user.getBindGA() != 1) {
                return result.setRet(BrokerErrorCode.NEED_BIND_GA.code()).build();
            } else if (StringUtils.isBlank(gaVerifyCode)) {
                log.error("alter email error with ga verify code:{} is null ", gaVerifyCode);
                return result.setRet(BrokerErrorCode.REQUEST_INVALID.code()).build();
            }
            mobileVerifyCode = "";
        } else {
            //正常逻辑，手机Ga必须有一个
            if (StringUtils.isBlank(user.getMobile()) && user.getBindGA() != 1) {
                // 手机和GA都未绑定
                return result.setRet(BrokerErrorCode.NEED_BIND_MOBILE_OR_GA.code()).build();
            }
            if (StringUtils.isBlank(mobileVerifyCode) && StringUtils.isBlank(gaVerifyCode)) {
                //除了换绑邮箱验证码之外，至少传一个
                log.error("alter email error with ga verify code:{}  and mobile code:{} is null ", gaVerifyCode, mobileOrderId);
                return result.setRet(BrokerErrorCode.REQUEST_INVALID.code()).build();
            }
        }
        //验证旧邮箱验证码
        verifyCodeService.validVerifyCode(header, VerifyCodeType.REBIND_EMAIL, originalEmailOrderId, originalEmailVerifyCode, user.getEmail(), AuthType.EMAIL);
        //验证手机验证码
        if (!StringUtils.isBlank(user.getMobile()) && !StringUtils.isBlank(mobileVerifyCode)) {
            verifyCodeService.validVerifyCode(header, VerifyCodeType.REBIND_EMAIL, mobileOrderId, mobileVerifyCode, user.getNationalCode() + user.getMobile(), AuthType.MOBILE);
        }
        //验证GA
        if (user.getBindGA() == 1 && !StringUtils.isBlank(gaVerifyCode)) {
            validGACode(header, header.getUserId(), gaVerifyCode);
        }
        //验证新邮箱
        verifyCodeService.validVerifyCode(header, VerifyCodeType.REBIND_EMAIL, alterEmailOrderId, alterEmailVerifyCode, email, AuthType.EMAIL);

        User updateObj = User.builder()
                .userId(header.getUserId())
                .email(email)
                .emailAlias(EmailUtils.emailAlias(email))
                .updated(System.currentTimeMillis())
                .build();
        userMapper.updateRecord(updateObj);
        try {
            verifyCodeService.invalidVerifyCode(header, VerifyCodeType.REBIND_EMAIL, originalEmailOrderId, user.getEmail());
            if (!StringUtils.isBlank(user.getMobile()) && !StringUtils.isBlank(mobileVerifyCode)) {
                verifyCodeService.invalidVerifyCode(header, VerifyCodeType.REBIND_EMAIL, mobileOrderId, user.getNationalCode() + user.getMobile());
            }
            verifyCodeService.invalidVerifyCode(header, VerifyCodeType.REBIND_EMAIL, alterEmailOrderId, email);
        } catch (Exception e) {
            // ignore
        }
        try {
            ModifyOrgUserEmailRequest request = ModifyOrgUserEmailRequest.newBuilder()
                    .setBaseRequest(BaseReqUtil.getBaseRequest(header.getOrgId()))
                    .setOrgId(header.getOrgId())
                    .setUserId(header.getUserId())
                    .setEmail(email)
                    .build();
            grpcAccountService.modifyEmail(request);
            User newUser = userMapper.getByOrgAndUserId(header.getOrgId(), header.getUserId());
            userService.asyncUserInfo(newUser);

            sendAlterSuccessNotice(header, newUser, NoticeBusinessType.ALTER_EMAIL_SUCCESS);
        } catch (Exception e) {
            log.error("modify user email and notice bh-server error, orgId:{}, userId:{}", header.getOrgId(), header.getUserId(), e);
        }
        //重新绑定邮箱禁止提币24H
        forbidWithdraw24Hour(header, FrozenReason.REBIND_EMAIL);
        return result.build();
    }

    @UserActionLogAnnotation(actionType = BrokerServerConstants.ACTION_TYPE_ALTER_MOBILE)
    public AlterMobileResponse alterMobile(Header header, Long originalMobileOrderId, String originalMobileVerifyCode, Long emailOrderId, String emailVerifyCode,
                                           String nationalCode, String mobile, Long alterMobileOrderId, String alterMobileVerifyCode, String gaVerifyCode) {
        AlterMobileResponse.Builder result = AlterMobileResponse.newBuilder();
        User user = userMapper.getByUserId(header.getUserId());

        if (userMapper.getByMobile(header.getOrgId(), nationalCode, mobile) != null) {
            return result.setRet(BrokerErrorCode.MOBILE_EXIST.code()).build();
        }
        /*if (user.getRegisterType() == RegisterType.MOBILE.value()) {
            return result.setRet(BrokerErrorCode.UNBIND_MOBILE_FAILED.code()).build();
        }*/
        if (StringUtils.isBlank(user.getMobile())) {
            return result.setRet(BrokerErrorCode.MOBILE_NOT_BIND.code()).build();
        }
        if (verifyCaptcha && globalNotifyType == 2) {
            //当仅手机时，更换邮箱必须校验ga
            if (user.getBindGA() != 1) {
                return result.setRet(BrokerErrorCode.NEED_BIND_GA.code()).build();
            } else if (StringUtils.isBlank(gaVerifyCode)) {
                log.error("alter mobile error with ga verify code:{} is null ", gaVerifyCode);
                return result.setRet(BrokerErrorCode.REQUEST_INVALID.code()).build();
            }
            emailVerifyCode = "";
        } else {
            //正常逻辑，邮箱Ga必须有一个
            if (StringUtils.isBlank(user.getEmail()) && user.getBindGA() != 1) {
                // 邮箱和GA都未绑定
                return result.setRet(BrokerErrorCode.NEED_BIND_EMAIL_OR_GA.code()).build();
            }
            if (StringUtils.isBlank(emailVerifyCode) && StringUtils.isBlank(gaVerifyCode)) {
                //除了换绑手机验证码之外，至少传一个
                log.error("alter mobile error with ga verify code:{}  and email code:{} is null ", gaVerifyCode, emailVerifyCode);
                return result.setRet(BrokerErrorCode.REQUEST_INVALID.code()).build();
            }
        }
        //验证旧手机
        verifyCodeService.validVerifyCode(header, VerifyCodeType.REBIND_MOBILE, originalMobileOrderId, originalMobileVerifyCode,
                user.getNationalCode() + user.getMobile(), AuthType.MOBILE);

        if (!StringUtils.isBlank(user.getEmail()) && !StringUtils.isBlank(emailVerifyCode)) {
            verifyCodeService.validVerifyCode(header, VerifyCodeType.REBIND_MOBILE, emailOrderId, emailVerifyCode,
                    user.getEmail(), AuthType.EMAIL);
        }
        //验证GA
        if (user.getBindGA() == 1 && !StringUtils.isBlank(gaVerifyCode)) {
            validGACode(header, header.getUserId(), gaVerifyCode);
        }
        //验证新手机
        verifyCodeService.validVerifyCode(header, VerifyCodeType.REBIND_MOBILE, alterMobileOrderId, alterMobileVerifyCode,
                nationalCode + mobile, AuthType.MOBILE);
        User updateObj = User.builder()
                .userId(header.getUserId())
                .nationalCode(nationalCode)
                .mobile(mobile)
                .concatMobile(nationalCode + mobile)
                .updated(System.currentTimeMillis())
                .build();
        userMapper.updateRecord(updateObj);
        try {
            verifyCodeService.invalidVerifyCode(header, VerifyCodeType.REBIND_MOBILE, originalMobileOrderId, user.getNationalCode() + user.getMobile());
            //邮箱验证码未传
            if (!StringUtils.isBlank(user.getEmail()) && !StringUtils.isBlank(emailVerifyCode)) {
                verifyCodeService.invalidVerifyCode(header, VerifyCodeType.REBIND_MOBILE, emailOrderId, user.getEmail());
            }
            verifyCodeService.invalidVerifyCode(header, VerifyCodeType.REBIND_MOBILE, alterMobileOrderId, nationalCode + mobile);
        } catch (Exception e) {
            // ignore
        }
        try {
            ModifyOrgUserMobileRequest request = ModifyOrgUserMobileRequest.newBuilder()
                    .setBaseRequest(BaseReqUtil.getBaseRequest(header.getOrgId()))
                    .setOrgId(header.getOrgId())
                    .setUserId(header.getUserId())
                    .setCountryCode(nationalCode)
                    .setMobile(mobile)
                    .build();
            grpcAccountService.modifyMobile(request);
            User newUser = userMapper.getByOrgAndUserId(header.getOrgId(), header.getUserId());
            userService.asyncUserInfo(newUser);
            sendAlterSuccessNotice(header, newUser, NoticeBusinessType.ALTER_MOBIL_SUCCESS);
        } catch (Exception e) {
            log.error("modify user email and notice bh-server error, orgId:{}, userId:{}", header.getOrgId(), header.getUserId(), e);
        }
        //禁止提币24H
        forbidWithdraw24Hour(header, FrozenReason.REBIND_MOBILE);
        return result.build();
    }

    @UserActionLogAnnotation(actionType = BrokerServerConstants.ACTION_TYPE_ALTER_GA)
    public AlterGaResponse alterGa(Header header, Long orderId, String verifyCode, String originalGaCode, String alterGaCode) {
        AlterGaResponse.Builder result = AlterGaResponse.newBuilder();
        User user = userMapper.getByUserId(header.getUserId());
        String receiver;
        AuthType authType;
        if (user.getRegisterType() == RegisterType.EMAIL.value()) {
            receiver = user.getEmail();
            authType = AuthType.EMAIL;
        } else {
            receiver = user.getNationalCode() + user.getMobile();
            authType = AuthType.MOBILE;
        }
        verifyCodeService.validVerifyCode(header, VerifyCodeType.REBIND_GA, orderId, verifyCode, receiver, authType);
        //验证旧GA
        validGACode(header, header.getUserId(), originalGaCode);

        Integer gaCode;
        try {
            gaCode = Ints.tryParse(alterGaCode);
        } catch (Exception e) {
            log.warn("valid ga: cannot parse gaCode {}", alterGaCode);
            throw new BrokerException(BrokerErrorCode.GA_VALID_ERROR);
        }
        if (gaCode == null) {
            log.warn("valid ga: cannot parse gaCode {}", alterGaCode);
            throw new BrokerException(BrokerErrorCode.GA_VALID_ERROR);
        }
        SecurityBindGARequest request = SecurityBindGARequest.newBuilder()
                .setHeader(header)
                .setUserId(header.getUserId())
                .setGaCode(gaCode)
                .build();
        SecurityBindGAResponse securityBindGAResponse = grpcSecurityService.alterBindGA(request);
        User updateBindGAStatus = User.builder()
                .userId(header.getUserId())
                .bindGA(1)
                .updated(System.currentTimeMillis())
                .build();
        userMapper.updateRecord(updateBindGAStatus);
        sendAlterSuccessNotice(header, user, NoticeBusinessType.ALTER_GA_SUCCESS);
        verifyCodeService.invalidVerifyCode(header, VerifyCodeType.REBIND_GA, orderId, receiver);

        //重新绑定GA禁止提币24H
        forbidWithdraw24Hour(header, FrozenReason.REBIND_GA);
        return result.build();
    }

    /**
     * before alter GoogleAuthenticator, we should provide GoogleAuthenticator SecretKey or a
     * qrcode
     */
    public BeforeAlterGaResponse beforeAlterGA(Header header) {
        User user = userMapper.getByUserId(header.getUserId());
        Long time = System.currentTimeMillis();

        DateFormat sf = new SimpleDateFormat("MM-dd HH:mm:ss");
        String dateStr = sf.format(new Date());
        String accountName = user.getRegisterType() == 1
                ? user.getMobile().replaceAll(BrokerServerConstants.MASK_MOBILE_REG, "$1****$2")
                : user.getEmail().replaceAll(BrokerServerConstants.MASK_EMAIL_REG, "*");
        Broker broker = brokerMapper.getByOrgId(header.getOrgId());
        SecurityBeforeBindGARequest request = SecurityBeforeBindGARequest.newBuilder()
                .setHeader(header)
                .setGaIssuer(broker.getBrokerName())
                .setAccountName(accountName + "_" + dateStr)
                .setUserId(header.getUserId())
                .build();
        SecurityBeforeBindGAResponse securityBeforeBindGAResponse = grpcSecurityService.beforeBindGA(request);
        String otpAuthTotpUrl = securityBeforeBindGAResponse.getOtpAuthTotpUrl();
        return BeforeAlterGaResponse.newBuilder()
                .setSecretKey(securityBeforeBindGAResponse.getKey())
                .setOtpAuthTotpUrl(otpAuthTotpUrl)
                .build();
    }

    /**
     * 禁止出金冻结24小时操作 管理员操作解绑GA 找回密码 修改资金密码 用户解绑GA 用户解绑邮箱 用户解绑手机 重新绑定邮箱 重新绑定手机 重新绑定GA
     *
     * @param header
     */
    public void forbidWithdraw24Hour(Header header, FrozenReason reason) {
        Long currentTimestamp = System.currentTimeMillis();

        List<FrozenType> frozenTypeList = FrozenType.forbidWithdrawList();
        List<FrozenUserRecord> frozenUserRecordList = frozenTypeList.stream().map(frozen -> {
            long startTime = System.currentTimeMillis();
            long endTime = startTime + 24 * 3600 * 1000;
            if (reason.equals(FrozenReason.MARGIN_BLACK_LIST)) {
                endTime = startTime + 24L * 3600 * 1000 * 365 * 10;
            }
            return FrozenUserRecord.builder()
                    .orgId(header.getOrgId())
                    .userId(header.getUserId())
                    .frozenType(frozen.type())
                    .frozenReason(reason.reason())
                    .startTime(startTime)
                    .endTime(endTime)
                    .status(FrozenStatus.UNDER_FROZEN.status())
                    .created(currentTimestamp)
                    .updated(currentTimestamp)
                    .build();
        }).collect(Collectors.toList());
        if (CollectionUtils.isNotEmpty(frozenUserRecordList)) {
            frozenUserRecordMapper.insertList(frozenUserRecordList);
        } else {
            log.info("forbidWithdraw24Hour error frozenUserRecordList is null orgId {} userId {}", header.getOrgId(), header.getUserId(), reason);
        }
    }

    /**
     * 券商禁止用户出金内部接口 默认lockDay=100 仅手工解锁
     *
     * @param orgId
     * @param lockDay
     * @param userId
     */
    public void foreverFrozenUser(long orgId, int lockDay, List<Long> userId) {
        Long currentTimestamp = System.currentTimeMillis();
        List<FrozenUserRecord> frozenUserRecordList = userId.stream().map(uid -> {
            return FrozenUserRecord.builder()
                    .orgId(orgId)
                    .userId(uid)
                    .frozenType(FrozenType.FROZEN.type())
                    .frozenReason(FrozenReason.SYSTEM_FROZEN.reason())
                    .startTime(System.currentTimeMillis())
                    .endTime(DateTime.now().plusYears(lockDay <= 0 ? 100 : lockDay).toDateTime().getMillis())
                    .status(FrozenStatus.UNDER_FROZEN.status())
                    .created(currentTimestamp)
                    .updated(currentTimestamp)
                    .build();
        }).collect(Collectors.toList());
        if (CollectionUtils.isNotEmpty(frozenUserRecordList)) {
            frozenUserRecordMapper.insertList(frozenUserRecordList);
        } else {
            log.info("foreverFrozenUser error is null orgId {} lockDay {} userId {}", orgId, lockDay, userId);
        }
    }

    /**
     * 发送换绑成功短信通知
     *
     * @param header
     * @param user
     * @param type
     */
    @Async
    public void sendAlterSuccessNotice(Header header, User user, NoticeBusinessType type) {
        try{
            if (type == null) {
                return;
            }
            if (!StringUtils.isBlank(user.getEmail())) {
                noticeTemplateService.sendEmailNotice(header, user.getUserId(), type, user.getEmail());
            }
            if (!StringUtils.isBlank(user.getMobile())) {
                noticeTemplateService.sendSmsNotice(header, user.getUserId(), type, user.getNationalCode(), user.getMobile());
            }
        }catch (Exception e){
            log.warn("send alter notice error orgId:{} uid:{} type:{}  {}",header.getOrgId(),user.getUserId(),type.name(),e.getMessage());
        }
    }

    public AdminUpdateEmailResponse adminUpdateEmail(Long orgId, Long userId, String email) {
        //判断需要设置的邮箱是否已存在
        String emailAlias = EmailUtils.emailAlias(email);
        int exitNum = userMapper.countEmailExists(orgId, email, emailAlias);
        if (exitNum > 0) {
            throw new BrokerException(BrokerErrorCode.EMAIL_EXIST);
        }
        //判断当前用户是否存在
        User user = userMapper.getByOrgAndUserId(orgId, userId);
        if (user == null) {
            throw new BrokerException(BrokerErrorCode.USER_NOT_EXIST);
        }
        user.setEmail(email);
        user.setEmailAlias(emailAlias);
        user.setUpdated(System.currentTimeMillis());
        if (verifyCaptcha && globalNotifyType == 3) {
            user.setRegisterType(RegisterType.EMAIL.value());
        }
        userMapper.updateRecord(user);
        ModifyOrgUserEmailRequest request = ModifyOrgUserEmailRequest.newBuilder()
                .setBaseRequest(BaseReqUtil.getBaseRequest(orgId))
                .setOrgId(orgId)
                .setUserId(userId)
                .setEmail(email)
                .build();
        grpcAccountService.modifyEmail(request);
        User newUser = userMapper.getByOrgAndUserId(orgId, userId);
        userService.asyncUserInfo(newUser);
        return AdminUpdateEmailResponse.getDefaultInstance();
    }

}
