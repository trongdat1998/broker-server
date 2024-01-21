/*
 ************************************
 * @项目名称: broker
 * @文件名称: UserServiceimpl
 * @Date 2018/05/22
 * @Author will.zhao@bhex.io
 * @Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 * 注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 **************************************
 */
package io.bhex.broker.server.grpc.server.service;

import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.hash.Hashing;
import com.google.common.primitives.Ints;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;
import com.google.protobuf.TextFormat;

import com.github.pagehelper.PageHelper;

import io.bhex.broker.server.BrokerServerProperties;
import io.bhex.broker.server.model.*;
import io.bhex.broker.server.primary.mapper.*;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.core.task.TaskExecutor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;

import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.Resource;

import io.bhex.base.account.BusinessSubject;
import io.bhex.base.account.CreateFuturesAccountReply;
import io.bhex.base.account.CreateOptionAccountReply;
import io.bhex.base.account.CreateSpecialUserAccountReply;
import io.bhex.base.account.CreateSpecialUserAccountRequest;
import io.bhex.base.account.ModifyOrgUserRequest;
import io.bhex.base.account.SimpleCreateAccountReply;
import io.bhex.base.account.SimpleCreateAccountRequest;
import io.bhex.base.account.SyncTransferRequest;
import io.bhex.base.account.SyncTransferResponse;
import io.bhex.base.idgen.api.ISequenceGenerator;
import io.bhex.base.proto.ErrorCode;
import io.bhex.broker.common.api.client.jiean.IDCardVerifyRequest;
import io.bhex.broker.common.api.client.jiean.IDCardVerifyResponse;
import io.bhex.broker.common.api.client.jiean.JieanApi;
import io.bhex.broker.common.api.client.tencent.TencentApi;
import io.bhex.broker.common.api.client.tencent.TencentIDCardVerifyRequest;
import io.bhex.broker.common.api.client.tencent.TencentIDCardVerifyResponse;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.common.util.CryptoUtil;
import io.bhex.broker.common.util.JsonUtil;
import io.bhex.broker.common.util.ValidateUtil;
import io.bhex.broker.grpc.admin.AccountInfo;
import io.bhex.broker.grpc.admin.GetAccountInfosResponse;
import io.bhex.broker.grpc.admin.ListUpdateUserByDateReply;
import io.bhex.broker.grpc.admin.ListUpdateUserByDateRequest;
import io.bhex.broker.grpc.admin.ListUserAccountRequest;
import io.bhex.broker.grpc.admin.ListUserAccountResponse;
import io.bhex.broker.grpc.admin.QueryLoginLogsResponse;
import io.bhex.broker.grpc.admin.UserAccountMap;
import io.bhex.broker.grpc.admin.UserLoginLog;
import io.bhex.broker.grpc.basic.KycLevel;
import io.bhex.broker.grpc.common.AccountTypeEnum;
import io.bhex.broker.grpc.common.Header;
import io.bhex.broker.grpc.common.Platform;
import io.bhex.broker.grpc.security.QueryUserTokenRequest;
import io.bhex.broker.grpc.security.QueryUserTokenResponse;
import io.bhex.broker.grpc.security.SecurityLoginRequest;
import io.bhex.broker.grpc.security.SecurityLoginResponse;
import io.bhex.broker.grpc.security.SecurityRegisterRequest;
import io.bhex.broker.grpc.security.SecurityRegisterResponse;
import io.bhex.broker.grpc.user.CancelFavoriteResponse;
import io.bhex.broker.grpc.user.CreateFavoriteResponse;
import io.bhex.broker.grpc.user.EditAntiPhishingCodeRequest;
import io.bhex.broker.grpc.user.EditAntiPhishingCodeResponse;
import io.bhex.broker.grpc.user.ExploreApplyRequest;
import io.bhex.broker.grpc.user.Favorite;
import io.bhex.broker.grpc.user.GetInviteCodeResponse;
import io.bhex.broker.grpc.user.GetPersonalVerifyInfoResponse;
import io.bhex.broker.grpc.user.GetScanLoginQrCodeResultResponse;
import io.bhex.broker.grpc.user.GetUserContactResponse;
import io.bhex.broker.grpc.user.GetUserContractResponse;
import io.bhex.broker.grpc.user.GetUserInfoResponse;
import io.bhex.broker.grpc.user.Index0AccountIds;
import io.bhex.broker.grpc.user.InviteCodeUseLimit;
import io.bhex.broker.grpc.user.LoginAdvanceResponse;
import io.bhex.broker.grpc.user.LoginResponse;
import io.bhex.broker.grpc.user.PersonalVerifyInfo;
import io.bhex.broker.grpc.user.PersonalVerifyResponse;
import io.bhex.broker.grpc.user.QrCodeAuthorizeLoginResponse;
import io.bhex.broker.grpc.user.QueryFavoritesResponse;
import io.bhex.broker.grpc.user.QueryLoginLogResponse;
import io.bhex.broker.grpc.user.QuickLoginCheckPasswordResponse;
import io.bhex.broker.grpc.user.QuickLoginResponse;
import io.bhex.broker.grpc.user.RegisterResponse;
import io.bhex.broker.grpc.user.SaveUserContractRequest;
import io.bhex.broker.grpc.user.SaveUserContractResponse;
import io.bhex.broker.grpc.user.ScanLoginQrCodeResponse;
import io.bhex.broker.grpc.user.SortFavoritesResponse;
import io.bhex.broker.server.domain.AccountType;
import io.bhex.broker.server.domain.AirdropOpsAccountInfo;
import io.bhex.broker.server.domain.AuthType;
import io.bhex.broker.server.domain.BaseConfigConstants;
import io.bhex.broker.server.domain.BrokerServerConstants;
import io.bhex.broker.server.domain.CommonIni;
import io.bhex.broker.server.domain.FrozenReason;
import io.bhex.broker.server.domain.FrozenStatus;
import io.bhex.broker.server.domain.FrozenType;
import io.bhex.broker.server.domain.LoginType;
import io.bhex.broker.server.domain.NoticeBusinessType;
import io.bhex.broker.server.domain.RegisterType;
import io.bhex.broker.server.domain.UserType;
import io.bhex.broker.server.domain.UserVerifyStatus;
import io.bhex.broker.server.domain.VerifyCodeType;
import io.bhex.broker.server.grpc.client.service.GrpcAccountService;
import io.bhex.broker.server.grpc.client.service.GrpcBatchTransferService;
import io.bhex.broker.server.grpc.client.service.GrpcEmailService;
import io.bhex.broker.server.grpc.client.service.GrpcSecurityService;
import io.bhex.broker.server.grpc.server.service.aspect.SiteFunctionLimitSwitchAnnotation;
import io.bhex.broker.server.grpc.server.service.listener.InviteRelationEvent;
import io.bhex.broker.server.grpc.server.service.notify.KycApplicationNotify;
import io.bhex.broker.server.util.BaseReqUtil;
import io.bhex.broker.server.util.EmailUtils;
import io.bhex.broker.server.util.GrpcHeaderUtil;
import io.bhex.broker.server.util.IDCardUtil;
import lombok.extern.slf4j.Slf4j;
import tk.mybatis.mapper.entity.Example;
import tk.mybatis.mapper.util.StringUtil;

/**
 * User Service
 *
 * @author will.zhao@bhex.io
 */
@Service("userService")
@Slf4j
public class UserService {

    private static final int MAX_LIMIT = 500;

    private static final String LOGIN_EMAIL_WHITE_LIST = "login_email_white_list";
    private static final String LOGIN_MOBILE_WHITE_LIST = "login_mobile_white_list";

    private static final String CHECK_INVITE_CODE = "custom_checkInviteCode";
    private static final String DEFAULT_INVITE_CODE = "BHEX88";
    private static final String UNUSED_INVITE_CODE = "UNUSED_INVITE_CODE";
    private static final String USED_INVITE_CODE = "USED_INVITE_CODE";

    private static final String USER_KYC_ONE_HOUR_LIMIT = "USER::KYC::ONE::HOUR::NUM::%s";

    private static final String USER_KYC_ONE_DAY_LIMIT = "USER::KYC::ONE::DAY::NUM::%s";

    private static final long ONE_DAY_EXPIRE_TIME = 24 * 60 * 60 * 1000;

    private static final long ONE_HOUR_EXPIRE_TIME = 1 * 60 * 60 * 1000;

    private static final String DEFAULT_FUTURES_AIRDROP_OPS_ACCOUNT_INFO = "futures_airdrop_ops_account_info";
    private static final String FUTURES_AIRDROP_SIMU_SYMBOLS = "futures_airdrop_simu_symbols";

    private static final String JIEAN_SUCCESS_CODE = "000";
    private static final List<String> JIEAN_AUTHENTICATION_NAME_INVALID_CODES = Lists.newArrayList("215", "226", "301");
    private static final List<String> JIEAN_AUTHENTICATION_CARD_NO_INVALID_CODES = Lists.newArrayList("216", "227", "228", "302");

    private static final int TENCENT_SUCCESS_CODE = 0;

    @Resource
    private ISequenceGenerator sequenceGenerator;

    @Resource
    private UserSecurityService userSecurityService;

    @Resource
    private GrpcSecurityService grpcSecurityService;
	
	@Resource
    private BrokerServerProperties brokerServerProperties;

    @Resource
    private GrpcAccountService grpcAccountService;

    @Resource(name = "stringRedisTemplate")
    private StringRedisTemplate redisTemplate;

    @Resource
    private UserMapper userMapper;

    @Resource
    private AccountMapper accountMapper;

    @Resource
    private UserVerifyMapper userVerifyMapper;

    @Resource
    private LoginLogMapper loginLogMapper;

    @Resource
    private FavoriteMapper favoriteMapper;

    @Resource
    private BasicService basicService;

    @Resource
    private VerifyCodeService verifyCodeService;

    @Resource
    private NoticeTemplateService noticeTemplateService;

    @Resource
    private JieanApi jieanApi;

    @Autowired
    ApplicationContext applicationContext;

    @Resource
    private AccountService accountService;

    @Resource
    private FrozenUserRecordMapper frozenUserRecordMapper;

    @Resource
    private UserContractMapper userContractMapper;

    @Resource
    private GrpcBatchTransferService grpcBatchTransferService;

    @Resource
    private InviteRelationMapper inviteRelationMapper;
    @Resource
    private InviteInfoMapper inviteInfoMapper;
    @Resource
    private CommonIniService commonIniService;

    @Resource
    private BrokerDictConfigMapper brokerDictConfigMapper;

    @Resource
    private PushDataService pushDataService;

    @Resource
    private AgentUserMapper agentUserMapper;

    @Resource
    private UserVerifyService userVerifyService;

    @Resource
    private BrokerService brokerService;

    @Resource
    private ThirdPartyUserMapper thirdPartyUserMapper;

    @Resource
    private ThirdPartyUserService thirdPartyUserService;

    @Resource
    private BalanceProofMapper balanceProofMapper;

    @Resource
    protected UserKycApplyMapper userKycApplyMapper;

    @Resource
    private GrpcEmailService grpcEmailService;

    @Resource
    private UserSettingsMapper userSettingsMapper;
    @Resource(name = "asyncTaskExecutor")
    private TaskExecutor taskExecutor;

    @Resource
    private LockBalanceLogMapper lockBalanceLogMapper;

    @Resource
    BalanceService balanceService;

    @Resource
    MarginService marginService;
	
	@Resource
    private TencentApi tencentApi;

    @Value("${verify-captcha:true}")
    private Boolean verifyCaptcha;

    @Value("${global-notify-type:1}")
    private Integer globalNotifyType;

    @SiteFunctionLimitSwitchAnnotation(wholeSiteSwitchKey = BaseConfigConstants.STOP_REGISTER_KEY)
    public RegisterResponse mobileRegister(Header header, String nationalCode, String mobile, String password,
                                           Long verifyCodeOrderId, String verifyCode, String inviteCode) {
        if (Strings.isNullOrEmpty(verifyCode)) {
            throw new BrokerException(BrokerErrorCode.VERIFY_CODE_CANNOT_BE_NULL);
        }
        basicService.validNationalCode(nationalCode);
        if (Strings.isNullOrEmpty(mobile)) {
            throw new BrokerException(BrokerErrorCode.MOBILE_CANNOT_BE_NULL);
        }
        verifyCodeService.validVerifyCode(header, VerifyCodeType.REGISTER, verifyCodeOrderId, verifyCode, nationalCode + mobile, AuthType.MOBILE);
        // 验证后即刻失效
        verifyCodeService.invalidVerifyCode(header, VerifyCodeType.REGISTER, verifyCodeOrderId, nationalCode + mobile);
        Long inviteUserId = checkInviteCode(header, inviteCode);
        if (userMapper.getByMobile(header.getOrgId(), nationalCode, mobile) != null) {
            throw new BrokerException(BrokerErrorCode.MOBILE_EXIST);
        }
        return register(header, nationalCode, mobile, "", password, inviteUserId, inviteCode, RegisterType.MOBILE, true, false);
    }

    @SiteFunctionLimitSwitchAnnotation(wholeSiteSwitchKey = BaseConfigConstants.STOP_REGISTER_KEY)
    public RegisterResponse emailRegister(Header header, String email, String password,
                                          Long verifyCodeOrderId, String verifyCode, String inviteCode) {
        if (Strings.isNullOrEmpty(verifyCode)) {
            throw new BrokerException(BrokerErrorCode.VERIFY_CODE_CANNOT_BE_NULL);
        }
        ValidateUtil.validateEmail(email);
        verifyCodeService.validVerifyCode(header, VerifyCodeType.REGISTER, verifyCodeOrderId, verifyCode, email, AuthType.EMAIL);
        verifyCodeService.invalidVerifyCode(header, VerifyCodeType.REGISTER, verifyCodeOrderId, email);
        Long inviteUserId = checkInviteCode(header, inviteCode);
        if (userMapper.countEmailExists(header.getOrgId(), email, EmailUtils.emailAlias(email)) > 0) {
            throw new BrokerException(BrokerErrorCode.EMAIL_EXIST);
        }
//        if (email.toLowerCase().endsWith("@gmail.com")) {
//            String emailAlias = EmailUtil.emailAlias(email);
//            if (userMapper.getByEmailAlias(header.getOrgId(), emailAlias) != null) {
//                throw new BrokerException(BrokerErrorCode.EMAIL_EXIST);
//            }
//        }
        return register(header, "", "", email, password, inviteUserId, inviteCode, RegisterType.EMAIL, true, false);
    }

    @SiteFunctionLimitSwitchAnnotation(wholeSiteSwitchKey = BaseConfigConstants.STOP_REGISTER_KEY)
    public RegisterResponse quickRegister(Header header, String nationalCode, String mobile, String email,
                                          Long verifyCodeOrderId, String verifyCode, String inviteCode) {
        Long inviteUserId = 0L;
        RegisterType registerType;
        if (Strings.isNullOrEmpty(email)) {
            if (Strings.isNullOrEmpty(verifyCode)) {
                throw new BrokerException(BrokerErrorCode.VERIFY_CODE_CANNOT_BE_NULL);
            }
            basicService.validNationalCode(nationalCode);
            if (Strings.isNullOrEmpty(mobile)) {
                throw new BrokerException(BrokerErrorCode.MOBILE_CANNOT_BE_NULL);
            }
            verifyCodeService.validVerifyCode(header, VerifyCodeType.QUICK_REGISTER, verifyCodeOrderId, verifyCode, nationalCode + mobile, AuthType.MOBILE);
            // 验证后即刻失效
            verifyCodeService.invalidVerifyCode(header, VerifyCodeType.QUICK_REGISTER, verifyCodeOrderId, nationalCode + mobile);
            inviteUserId = checkInviteCode(header, inviteCode);
            if (userMapper.getByMobile(header.getOrgId(), nationalCode, mobile) != null) {
                throw new BrokerException(BrokerErrorCode.MOBILE_EXIST);
            }
            registerType = RegisterType.MOBILE;
        } else {
            if (Strings.isNullOrEmpty(verifyCode)) {
                throw new BrokerException(BrokerErrorCode.VERIFY_CODE_CANNOT_BE_NULL);
            }
            ValidateUtil.validateEmail(email);
            verifyCodeService.validVerifyCode(header, VerifyCodeType.QUICK_REGISTER, verifyCodeOrderId, verifyCode, email, AuthType.EMAIL);
            verifyCodeService.invalidVerifyCode(header, VerifyCodeType.QUICK_REGISTER, verifyCodeOrderId, email);
            inviteUserId = checkInviteCode(header, inviteCode);
            if (userMapper.countEmailExists(header.getOrgId(), email, EmailUtils.emailAlias(email)) > 0) {
                throw new BrokerException(BrokerErrorCode.EMAIL_EXIST);
            }
            registerType = RegisterType.EMAIL;
        }
        return register(header, nationalCode, mobile, email, "", inviteUserId, inviteCode, registerType, true, true);
    }

    public RegisterResponse registerWithThirdUserIdAndNoneUserInfo(Header header, String thirdUserId) {
        ThirdPartyUser thirdPartyUserInfo = this.thirdPartyUserMapper.getByMasterUserIdAndThirdUserId(header.getOrgId(), header.getUserId(), thirdUserId);
        if (thirdPartyUserInfo != null) {
            log.info("Third user {}  is exist", thirdUserId);
            LoginResponse loginResponse = thirdPartyUserLogin(header, thirdUserId, thirdPartyUserInfo.getUserId());
            return RegisterResponse.newBuilder()
                    .setToken(loginResponse.getToken())
                    .setUser(loginResponse.getUser())
                    .build();
        }
        RegisterResponse registerResponse;
        try {
            registerResponse = thirdPartyUserRegister(header, false);
            if (registerResponse == null || registerResponse.getUser() == null) {
                log.info("Virtual user register fail thirdUserId {}", thirdUserId);
                throw new BrokerException(BrokerErrorCode.REGISTER_FAILED);
            }
        } catch (Exception e) {
            log.error("registerWithThirdUserIdAndNoneUserInfo error", e);
            throw new BrokerException(BrokerErrorCode.REGISTER_FAILED);
        }

        ThirdPartyUser thirdPartyUser = ThirdPartyUser
                .builder()
                .masterUserId(header.getUserId())
                .thirdUserId(thirdUserId)
                .userId(registerResponse.getUser().getUserId())
                .orgId(header.getOrgId())
                .created(new Date())
                .updated(new Date())
                .build();
        this.thirdPartyUserMapper.insert(thirdPartyUser);
        return registerResponse;
    }

    public RegisterResponse importRegister(Header header, String nationalCode, String mobile, String email, String password,
                                           String inviteCode, Long inviteUserId, Boolean sendSuccessNotice, Boolean checkInviteInfo, Boolean forceCheckEmail) {
        RegisterType registerType = null;
        if (!Strings.isNullOrEmpty(mobile)) {
            basicService.validNationalCode(nationalCode);
            if (userMapper.getByMobile(header.getOrgId(), nationalCode, mobile) != null) {
                throw new BrokerException(BrokerErrorCode.MOBILE_EXIST);
            }
            registerType = RegisterType.MOBILE;
        }
        if (!Strings.isNullOrEmpty(email)) {
            if (registerType == null) {
                registerType = RegisterType.EMAIL;
            }
            // 校验email是否有重复，
            if (userMapper.getByEmail(header.getOrgId(), email) != null
                    || userMapper.getByEmailAlias(header.getOrgId(), email) != null) {
                throw new BrokerException(BrokerErrorCode.EMAIL_EXIST);
            }
            if (forceCheckEmail) {
                ValidateUtil.validateEmail(email);
                // 校验emailAlias是否有重复
                if (userMapper.getByEmail(header.getOrgId(), EmailUtils.emailAlias(email)) != null
                        || userMapper.getByEmailAlias(header.getOrgId(), EmailUtils.emailAlias(email)) != null) {
                    throw new BrokerException(BrokerErrorCode.EMAIL_EXIST);
                }
            }
        }
        if (registerType == null) {
            throw new BrokerException(BrokerErrorCode.MOBILE_CANNOT_BE_NULL);
        }

        if (!Strings.isNullOrEmpty(inviteCode)) {
            inviteUserId = checkInviteCode(header, inviteCode);
        } else {
            User user = userMapper.getByOrgAndUserId(header.getOrgId(), inviteUserId);
            if (user == null) {
                log.warn("importRegister: cannot find user with inviteUserId:{}", inviteUserId);
                inviteUserId = 0L;
            }
        }
        // FIXME: Importing users through interfaces does not force verification of invitation relationships.
        if (checkInviteInfo) {
            boolean checkInviteCode = commonIniService.getBooleanValueOrDefault(header.getOrgId(), CHECK_INVITE_CODE, false);
            if (checkInviteCode && inviteUserId == 0) {
                log.error("{} import register invite data error, inviteCode:{}, inviteUserId:{}", header.getOrgId(), inviteCode, inviteUserId);
                throw new BrokerException(BrokerErrorCode.INVITE_CODE_ERROR);
            }
        }
        return register(header, nationalCode, mobile, email, password, inviteUserId, inviteCode, registerType, sendSuccessNotice, false);
    }

    /**
     * check Invite info
     *
     * @return inviteUserId
     */
    private Long checkInviteCode(Header header, String inviteCode) {
        if (Strings.nullToEmpty(inviteCode).equalsIgnoreCase(DEFAULT_INVITE_CODE)) {
            return 0L;
        }
        boolean checkInviteCode = commonIniService.getBooleanValueOrDefault(header.getOrgId(), CHECK_INVITE_CODE, false);
        Long inviteUserId = 0L;
        if (!Strings.isNullOrEmpty(inviteCode)) {
            User user = userMapper.getByInviteCode(header.getOrgId(), inviteCode);
            if (user != null) {
                inviteUserId = user.getUserId();
            }
        }
        if (checkInviteCode && inviteUserId == 0) {
            log.error("{} register invite code \"{}\" is incorrect", header.getOrgId(), inviteCode);
            throw new BrokerException(BrokerErrorCode.INVITE_CODE_ERROR);
        }
        return inviteUserId;
    }

    public RegisterResponse register(Header header, String nationalCode, String mobile, String email, String password, Long inviteUserId, String inputInviteCode,
                                     RegisterType registerType, Boolean sendSuccessNotice, Boolean isQuickRegister) {
//        boolean checkInviteCode = CommonIniService.getBooleanValueOrDefault(header.getOrgId(), CHECK_INVITE_CODE, false);
//        String unusedInviteCodeKey = header.getOrgId() + "_" + UNUSED_INVITE_CODE;
//        String usedInviteCodeKey = header.getOrgId() + "_" + USED_INVITE_CODE;
//        if (checkInviteCode) {
//            if (Strings.isNullOrEmpty(inviteCode) || !redisTemplate.opsForHash().hasKey(unusedInviteCodeKey, inviteCode)) {
//                log.warn("register invite code '{}' is null or invalid, redis has key? {}", inviteCode, redisTemplate.opsForHash().hasKey(unusedInviteCodeKey, inviteCode));
//                throw new BrokerException(BrokerErrorCode.INVITE_CODE_ERROR);
//            }
//            if ("1".equals(redisTemplate.opsForHash().get(unusedInviteCodeKey, inviteCode))
//                    && !redisTemplate.opsForHash().putIfAbsent(usedInviteCodeKey, inviteCode, "1")) {
//                log.warn("register cannot get the invite code '{}' lock, maybe invite code is used", inviteCode);
//                throw new BrokerException(BrokerErrorCode.INVITE_CODE_ERROR);
//            }
//            if ("2".equals(redisTemplate.opsForHash().get(unusedInviteCodeKey, inviteCode))) {
//                inviteCode = getInviteCode(header);
//            }
//        } else {
//            inviteCode = getInviteCode(header);
//        }

        String inviteCode = getInviteCode(header);
        User user;
        Long mainAccountId;
        String loginToken;
//        Long userId = sequenceGenerator.getLong();
        Long userId = 0L;
        try {
            SimpleCreateAccountRequest request = SimpleCreateAccountRequest.newBuilder()
                    .setBaseRequest(BaseReqUtil.getBaseRequest(header.getOrgId()))
                    .setOrgId(header.getOrgId())
                    .setUserId(userId.toString())
                    .setCountryCode(Strings.nullToEmpty(nationalCode))
                    .setMobile(Strings.nullToEmpty(mobile))
                    .setEmail(Strings.nullToEmpty(email))
                    .build();
            SimpleCreateAccountReply reply = grpcAccountService.createAccount(request);
            if (reply.getErrCode() != ErrorCode.SUCCESS_VALUE) {
                log.error("register invoke GrpcAccountService.createAccount error:{}", reply.getErrCode());
                throw new BrokerException(BrokerErrorCode.SIGN_UP_ERROR);
            }

            Long bhUserId = reply.getBhUserId();
            userId = bhUserId;

            SecurityRegisterRequest registerRequest = SecurityRegisterRequest.newBuilder()
                    .setHeader(header)
                    .setUserId(userId)
                    .setPassword(password)
                    .build();
            SecurityRegisterResponse securityRegisterResponse = grpcSecurityService.register(registerRequest);
            loginToken = securityRegisterResponse.getToken();

            mainAccountId = reply.getAccountId();
            Long optionAccountId = reply.getOptionAccountId();
            Long futuresAccountId = reply.getFuturesAccountId();
            // Long marginAccountId = reply.getMarginAccountId();

            // String inviteCode = getInviteCode(header);
            Long timestamp = System.currentTimeMillis();

            log.info("create account success. userId{} mainAccountId:{}, optionAccountId:{} futuresAccountId:{}",
                    userId, mainAccountId, optionAccountId, futuresAccountId);
            createAccount(header, mainAccountId, AccountType.MAIN.value(), userId, timestamp);
            createAccount(header, optionAccountId, AccountType.OPTION.value(), userId, timestamp);
            createAccount(header, futuresAccountId, AccountType.FUTURES.value(), userId, timestamp);
            //createAccount(header, marginAccountId, AccountType.MARGIN.value(), userId, timestamp);


            Long secondLevelInviteUserId = 0L;
            if (inviteUserId != null && inviteUserId > 0) {
                User inviteUser = userMapper.getByOrgAndUserId(header.getOrgId(), inviteUserId);
                if (inviteUser != null) {
                    secondLevelInviteUserId = inviteUser.getInviteUserId();
                }
            }
            user = User.builder()
                    .orgId(header.getOrgId())
                    .userId(userId)
                    .bhUserId(bhUserId)
                    .nationalCode(nationalCode)
                    .mobile(mobile)
                    .concatMobile(nationalCode + mobile)
                    .email(email)
//                    .emailAlias(registerType == RegisterType.MOBILE ? "" : EmailUtils.emailAlias(email))
                    .emailAlias(EmailUtils.emailAlias(email))
                    .registerType(registerType.value())
                    .ip(header.getRemoteIp())
                    .inputInviteCode(Strings.nullToEmpty(inputInviteCode))
                    .inviteUserId(inviteUserId)
                    .secondLevelInviteUserId(secondLevelInviteUserId)
                    .inviteCode(inviteCode)
                    .bindGA(0)
                    .bindTradePwd(0)
                    .bindPassword(isQuickRegister ? 0 : 1)
                    .userType(UserType.GENERAL_USER.value())
                    .channel(header.getChannel())
                    .source(header.getSource())
                    .platform(header.getPlatform().name())
                    .userAgent(header.getUserAgent())
                    .language(header.getLanguage())
                    .appBaseHeader(GrpcHeaderUtil.getAppBaseInfo(header))
                    .created(timestamp)
                    .updated(timestamp)
                    .build();
            userMapper.insertRecord(user);

//            if (checkInviteCode && "1".equals(redisTemplate.opsForHash().get(unusedInviteCodeKey, inviteCode))) {
//                redisTemplate.opsForHash().delete(unusedInviteCodeKey, inviteCode);
//            } else if (checkInviteCode && "2".equals(redisTemplate.opsForHash().get(unusedInviteCodeKey, inviteCode))) {
//                redisTemplate.opsForHash().increment(usedInviteCodeKey, inviteCode, 1L);
//            }
        } catch (Exception e) {
            // release the invite code
//            if (checkInviteCode && "1".equals(redisTemplate.opsForHash().get(unusedInviteCodeKey, inviteCode))) {
//                redisTemplate.opsForHash().delete(usedInviteCodeKey, inviteCode);
//            }
            throw e;
        }
        LoginType loginType = LoginType.fromType(registerType.value());
        Long logId = addLoginLog(header, userId, loginType, BrokerServerConstants.LOGIN_SUCCESS);
        //异步推送给需要登录信息的客户
        this.pushDataService.userLoginMessage(header, user.getOrgId(), user.getUserId(), loginType, logId);


        if (!Strings.isNullOrEmpty(mobile)) {
            redisTemplate.delete(String.format(BrokerServerConstants.FROZEN_LOGIN_USERNAME_KEY, header.getOrgId(), mobile));
            redisTemplate.delete(String.format(BrokerServerConstants.LOGIN_ERROR_USERNAME_COUNT, header.getOrgId(), mobile));
        } else if (!Strings.isNullOrEmpty(email)) {
            redisTemplate.delete(String.format(BrokerServerConstants.FROZEN_LOGIN_USERNAME_KEY, header.getOrgId(), email));
            redisTemplate.delete(String.format(BrokerServerConstants.LOGIN_ERROR_USERNAME_COUNT, header.getOrgId(), email));
        }

        if (sendSuccessNotice) {
            if (Strings.isNullOrEmpty(email)) {
                noticeTemplateService.sendSmsNoticeAsync(header, userId, NoticeBusinessType.REGISTER_SUCCESS, nationalCode, mobile);
            } else {
                noticeTemplateService.sendEmailNoticeAsync(header, userId, NoticeBusinessType.REGISTER_SUCCESS, email);
            }
        }

        // 添加监听，异步构建用户邀请关系
        InviteRelationEvent event = InviteRelationEvent.builder()
                .orgId(header.getOrgId())
                .userId(userId)
                .accountId(mainAccountId)
                .name(StringUtils.isEmpty(mobile) ? email : mobile)
                .inviteId(inviteUserId)
                .build();
        applicationContext.publishEvent(event);

        //异步推送给需要注册信息的客户
        this.pushDataService.userRegisterMessage(header.getOrgId(), user.getUserId());

        return RegisterResponse.newBuilder()
                .setToken(loginToken)
                .setUser(getUserInfo(user))
                .build();
    }

    private void createAccount(Header header, Long accountId, Integer accountType, Long userId, Long timestamp) {
        Account account = Account.builder()
                .orgId(header.getOrgId() > 0 ? header.getOrgId() : 9001L)
                .userId(userId)
                .accountId(accountId)
                .accountName("")
                .accountType(accountType)
                .created(timestamp)
                .updated(timestamp)
                .build();
        accountMapper.insertRecord(account);
    }

    private String getInviteCode(Header header) {
        String inviteCode = CryptoUtil.getRandomCode(6);
        if (userMapper.getByInviteCode(header.getOrgId(), inviteCode) != null) {
            return getInviteCode(header);
        }
        return inviteCode;
    }

    @SiteFunctionLimitSwitchAnnotation(wholeSiteSwitchKey = BaseConfigConstants.STOP_LOGIN_KEY)
    public LoginResponse mobileLogin(Header header, String nationalCode, String mobile, String password, boolean need2FACheck, boolean requestSuccessNotice) {
        if (Strings.isNullOrEmpty(mobile)) {
            throw new BrokerException(BrokerErrorCode.MOBILE_CANNOT_BE_NULL);
        }
        if (redisTemplate.hasKey(String.format(BrokerServerConstants.FROZEN_LOGIN_USERNAME_KEY, header.getOrgId(), mobile))) {
            throw new BrokerException(BrokerErrorCode.LOGIN_INPUT_ERROR_6);
        }
        basicService.validNationalCode(nationalCode);
        if (Strings.isNullOrEmpty(password)) {
            throw new BrokerException(BrokerErrorCode.PASSWORD_CANNOT_BE_NULL);
        }
        User user = userMapper.getByMobile(header.getOrgId(), nationalCode, mobile);
        if (user == null) {
            handleNotExistUserLogin(header, mobile);
            throw new BrokerException(BrokerErrorCode.USER_NOT_EXIST);
        }
        return login(header, user, password, LoginType.MOBILE, need2FACheck, requestSuccessNotice);
    }

    @SiteFunctionLimitSwitchAnnotation(wholeSiteSwitchKey = BaseConfigConstants.STOP_LOGIN_KEY)
    public LoginResponse emailLogin(Header header, String email, String password, boolean need2FACheck, boolean requestSuccessNotice) {
        if (redisTemplate.hasKey(String.format(BrokerServerConstants.FROZEN_LOGIN_USERNAME_KEY, header.getOrgId(), email))) {
            throw new BrokerException(BrokerErrorCode.LOGIN_INPUT_ERROR_6);
        }
        ValidateUtil.validateEmail(email);
        if (Strings.isNullOrEmpty(password)) {
            throw new BrokerException(BrokerErrorCode.PASSWORD_CANNOT_BE_NULL);
        }
        User user = userMapper.getByEmail(header.getOrgId(), email);
        if (user == null) {
            user = userMapper.getByEmailAlias(header.getOrgId(), EmailUtils.emailAlias(email));
            if (user == null) {
                handleNotExistUserLogin(header, email);
                throw new BrokerException(BrokerErrorCode.USER_NOT_EXIST);
            }
        }
        return login(header, user, password, LoginType.EMAIL, need2FACheck, requestSuccessNotice);
    }

    public LoginResponse thirdPartyUserLogin(Header header, String thirdUserId, Long userId) {
        User user = thirdPartyUserService.getVirtualUser(header.getOrgId(), header.getUserId(), thirdUserId, userId);
        SecurityLoginRequest request = SecurityLoginRequest.newBuilder()
                .setHeader(header)
                .setUserId(user.getUserId())
                .setPassword("")
                .setIsQuickLogin(true)
                .build();
        SecurityLoginResponse loginResponse = grpcSecurityService.login(request);
        Long logId = addLoginLog(header, user.getUserId(), LoginType.ORG_API, BrokerServerConstants.LOGIN_SUCCESS);
        //异步推送给需要登录信息的客户
        this.pushDataService.userLoginMessage(header, user.getOrgId(), user.getUserId(), LoginType.ORG_API, logId);
        return LoginResponse.newBuilder()
                .setToken(loginResponse.getToken())
                .setUser(getUserInfo(user))
                .build();
    }

    public QuickLoginResponse quickLogin(Header header, String nationalCode, String mobile, String email,
                                         Long verifyCodeOrderId, String verifyCode) {
        User user;
        LoginType loginType;
        if (Strings.isNullOrEmpty(email)) {
            if (Strings.isNullOrEmpty(mobile)) {
                throw new BrokerException(BrokerErrorCode.MOBILE_CANNOT_BE_NULL);
            }
            if (redisTemplate.hasKey(String.format(BrokerServerConstants.FROZEN_LOGIN_USERNAME_KEY, header.getOrgId(), mobile))) {
                throw new BrokerException(BrokerErrorCode.LOGIN_INPUT_ERROR_6);
            }
            basicService.validNationalCode(nationalCode);
            verifyCodeService.validVerifyCode(header, VerifyCodeType.QUICK_LOGIN, verifyCodeOrderId, verifyCode, nationalCode + mobile, AuthType.MOBILE);
            // 验证后即刻失效
            verifyCodeService.invalidVerifyCode(header, VerifyCodeType.QUICK_LOGIN, verifyCodeOrderId, nationalCode + mobile);
            user = userMapper.getByMobile(header.getOrgId(), nationalCode, mobile);
            if (user == null) {
                //handleNotExistUserLogin(header, mobile);
                throw new BrokerException(BrokerErrorCode.USER_NOT_EXIST);
            }
            loginType = LoginType.MOBILE_QUICK_LOGIN;
        } else {
            if (redisTemplate.hasKey(String.format(BrokerServerConstants.FROZEN_LOGIN_USERNAME_KEY, header.getOrgId(), email))) {
                throw new BrokerException(BrokerErrorCode.LOGIN_INPUT_ERROR_6);
            }
            ValidateUtil.validateEmail(email);
            verifyCodeService.validVerifyCode(header, VerifyCodeType.QUICK_LOGIN, verifyCodeOrderId, verifyCode, email, AuthType.EMAIL);
            // 验证后即刻失效
            verifyCodeService.invalidVerifyCode(header, VerifyCodeType.QUICK_LOGIN, verifyCodeOrderId, email);

            user = userMapper.getByEmail(header.getOrgId(), email);
            if (user == null) {
                user = userMapper.getByEmailAlias(header.getOrgId(), EmailUtils.emailAlias(email));
                if (user == null) {
                    //handleNotExistUserLogin(header, email);
                    throw new BrokerException(BrokerErrorCode.USER_NOT_EXIST);
                }
            }
            loginType = LoginType.EMAIL_QUICK_LOGIN;
        }
        return quickLogin(header, user, loginType);
    }

    @SiteFunctionLimitSwitchAnnotation(wholeSiteSwitchKey = BaseConfigConstants.STOP_LOGIN_KEY)
    public LoginResponse usernameLogin(Header header, String username, String password, boolean need2FACheck, boolean requestSuccessNotice) {
        if (Strings.isNullOrEmpty(username)) {
            throw new BrokerException(BrokerErrorCode.LOGIN_INPUTS_ERROR);
        }
        if (redisTemplate.hasKey(String.format(BrokerServerConstants.FROZEN_LOGIN_USERNAME_KEY, header.getOrgId(), username))) {
            throw new BrokerException(BrokerErrorCode.LOGIN_INPUT_ERROR_6);
        }
        if (Strings.isNullOrEmpty(password)) {
            throw new BrokerException(BrokerErrorCode.PASSWORD_CANNOT_BE_NULL);
        }
        User user;
        if (username.contains("@")) {
            if (username.startsWith("@") || username.endsWith("@")) {
                user = null;
            }
            user = userMapper.getByEmail(header.getOrgId(), username);
            // TODO 如果user == null，查询一下username
        } else {
            // TODO 如果Longs.tryParse是null，查询username

            // 带 "." 的是兼容了选择区号的这个版本 national_code.mobile
            if (username.contains(".") && !username.startsWith(".") && !username.endsWith(".")) {
                String nationalCode = username.split("\\.")[0];
                if (!Strings.isNullOrEmpty(nationalCode) && nationalCode.startsWith("0")) {
                    Integer boxNationalCode = Ints.tryParse(nationalCode);
                    nationalCode = boxNationalCode == null ? null : boxNationalCode.toString();
                }
                String mobile = username.split("\\.")[1];
                user = userMapper.getByMobile(header.getOrgId(), nationalCode, mobile);
            } else if (username.startsWith("+")) {
                username = StringUtils.stripStart(username, "+0");
                user = userMapper.getByConcatMobile(header.getOrgId(), username);
            } else {
                user = userMapper.getByMobileOrConcatMobile(header.getOrgId(), username);
                if (user == null && username.startsWith("0")) {
                    username = StringUtils.stripStart(username, "0");
                    user = userMapper.getByMobileOrConcatMobile(header.getOrgId(), username);
                }
            }
        }
        if (user == null) {
            handleNotExistUserLogin(header, username);
            throw new BrokerException(BrokerErrorCode.USER_NOT_EXIST);
        }
        LoginType loginType = username.contains("@") ? LoginType.EMAIL : LoginType.MOBILE;
        return login(header, user, password, loginType, need2FACheck, requestSuccessNotice);
    }

    private void handleNotExistUserLogin(Header header, String username) {
        String errorCountKey = String.format(BrokerServerConstants.LOGIN_ERROR_USERNAME_COUNT, header.getOrgId(), username);
        redisTemplate.opsForValue()
                .setIfAbsent(errorCountKey, "0", Duration.ofHours(24));
        long errorTimes = redisTemplate.opsForValue().increment(errorCountKey);

        if (errorTimes == 1) {
            throw new BrokerException(BrokerErrorCode.LOGIN_INPUTS_ERROR);
        } else if (errorTimes == 2) {
            throw new BrokerException(BrokerErrorCode.LOGIN_INPUT_ERROR_2);
        } else if (errorTimes == 3) {
            throw new BrokerException(BrokerErrorCode.LOGIN_INPUT_ERROR_3);
        } else if (errorTimes == 4) {
            throw new BrokerException(BrokerErrorCode.LOGIN_INPUT_ERROR_4);
        } else if (errorTimes == 5) {
            redisTemplate.opsForValue().set(String.format(BrokerServerConstants.FROZEN_LOGIN_USERNAME_KEY, header.getOrgId(), username),
                    "" + System.currentTimeMillis(), Duration.ofHours(24));
            throw new BrokerException(BrokerErrorCode.LOGIN_INPUT_ERROR_5);
        } else {
            throw new BrokerException(BrokerErrorCode.LOGIN_INPUT_ERROR_6);
        }
    }

    public LoginResponse login(Header header, User user, String password, LoginType loginType, boolean need2FACheck, boolean requestSuccessNotice) {
        Long currentTimestamp = System.currentTimeMillis();
        // 使用数据库的记录来判断冻结状态
        boolean userUnderFrozen = false;
        FrozenUserRecord frozenUserRecord = frozenUserRecordMapper.getByUserIdAndFrozenType(header.getOrgId(), user.getUserId(), FrozenType.FROZEN_LOGIN.type());
        if (frozenUserRecord != null
                && (currentTimestamp <= frozenUserRecord.getEndTime() && currentTimestamp >= frozenUserRecord.getStartTime())) {
            log.info("user:{} is under login frozen, cannot login", header.getUserId());
            userUnderFrozen = true;
        }
        if (userUnderFrozen) {
            if (frozenUserRecord.getFrozenReason() == FrozenReason.LOGIN_INPUT_ERROR.reason()) {
                return LoginResponse.newBuilder().setRet(BrokerErrorCode.LOGIN_INPUT_ERROR_6.code())
                        .setUser(io.bhex.broker.grpc.user.User.newBuilder().setUserId(user.getUserId()).build()).build();
            } else if (frozenUserRecord.getFrozenReason() == FrozenReason.SYSTEM_FROZEN.reason()) {
                return LoginResponse.newBuilder().setRet(BrokerErrorCode.FROZEN_LOGIN_BY_SYSTEM.code())
                        .setUser(io.bhex.broker.grpc.user.User.newBuilder().setUserId(user.getUserId()).build()).build();
            } else {
                // 目前只有FrozenReason.SYSTEM_FROZEN
                return LoginResponse.newBuilder().setRet(BrokerErrorCode.FROZEN_LOGIN_BY_ILLEGAL_OPERATION.code())
                        .setUser(io.bhex.broker.grpc.user.User.newBuilder().setUserId(user.getUserId()).build()).build();
            }
        }

        SecurityLoginRequest request = SecurityLoginRequest.newBuilder()
                .setHeader(header)
                .setUserId(user.getUserId())
                .setPassword(password)
                .build();
        SecurityLoginResponse securityLoginResponse;
        try {
            securityLoginResponse = grpcSecurityService.login(request);
        } catch (BrokerException e) {
            if (e.getCode() != BrokerErrorCode.LOGIN_INPUTS_ERROR.code()) {
                throw e;
            }
            addLoginLog(header, user.getUserId(), loginType, BrokerServerConstants.LOGIN_FAILED);

            redisTemplate.opsForValue().setIfAbsent(String.format(BrokerServerConstants.LOGIN_ERROR_USER_ID_COUNT, user.getOrgId(), user.getUserId()), "0", Duration.ofHours(24));
            long errorTimes = redisTemplate.opsForValue().increment(String.format(BrokerServerConstants.LOGIN_ERROR_USER_ID_COUNT, user.getOrgId(), user.getUserId()), 1L);
            if (errorTimes == 1) {
                return LoginResponse.newBuilder().setRet(BrokerErrorCode.LOGIN_INPUTS_ERROR.code())
                        .setUser(io.bhex.broker.grpc.user.User.newBuilder().setUserId(user.getUserId()).build()).build();
                // throw new BrokerException(BrokerErrorCode.LOGIN_INPUTS_ERROR);
            } else if (errorTimes == 2) {
                return LoginResponse.newBuilder().setRet(BrokerErrorCode.LOGIN_INPUT_ERROR_2.code())
                        .setUser(io.bhex.broker.grpc.user.User.newBuilder().setUserId(user.getUserId()).build()).build();
                // throw new BrokerException(BrokerErrorCode.LOGIN_INPUT_ERROR_2);
            } else if (errorTimes == 3) {
                return LoginResponse.newBuilder().setRet(BrokerErrorCode.LOGIN_INPUT_ERROR_3.code())
                        .setUser(io.bhex.broker.grpc.user.User.newBuilder().setUserId(user.getUserId()).build()).build();
                // throw new BrokerException(BrokerErrorCode.LOGIN_INPUT_ERROR_3);
            } else if (errorTimes == 4) {
                return LoginResponse.newBuilder().setRet(BrokerErrorCode.LOGIN_INPUT_ERROR_4.code())
                        .setUser(io.bhex.broker.grpc.user.User.newBuilder().setUserId(user.getUserId()).build()).build();
                // throw new BrokerException(BrokerErrorCode.LOGIN_INPUT_ERROR_4);
            } else if (errorTimes == 5) {
                forzenUserLogin(user.getOrgId(), user.getUserId());
                return LoginResponse.newBuilder().setRet(BrokerErrorCode.LOGIN_INPUT_ERROR_5.code())
                        .setUser(io.bhex.broker.grpc.user.User.newBuilder().setUserId(user.getUserId()).build()).build();
            } else {
                frozenUserRecord = frozenUserRecordMapper.getByUserIdAndFrozenType(header.getOrgId(), user.getUserId(), FrozenType.FROZEN_LOGIN.type());
                if (frozenUserRecord == null || currentTimestamp > frozenUserRecord.getEndTime()) {
                    forzenUserLogin(user.getOrgId(), user.getUserId());
                }
                return LoginResponse.newBuilder().setRet(BrokerErrorCode.LOGIN_INPUT_ERROR_6.code())
                        .setUser(io.bhex.broker.grpc.user.User.newBuilder().setUserId(user.getUserId()).build()).build();
                // throw new BrokerException(BrokerErrorCode.LOGIN_INPUT_ERROR_6);
            }
        }
        if (user.getUserStatus() != 1) {
            throw new BrokerException(BrokerErrorCode.USER_STATUS_FORBIDDEN);
        }

        boolean bindMobile = false, bindEmail = false, bindGA = false, skipSendLoginSuccessNotice = false;
        String authType = "";
        boolean hasBalance = true;
        if (need2FACheck) {
            try {
                Header balanceHeader = header.toBuilder().setUserId(user.getUserId()).build();
                List<io.bhex.broker.grpc.account.Account> mainAccountList = accountService.queryBalanceByAccountType(balanceHeader, AccountType.MAIN, false, true);
                List<io.bhex.broker.grpc.account.Account> optionAccountList = accountService.queryBalanceByAccountType(balanceHeader, AccountType.OPTION, false, true);
                List<io.bhex.broker.grpc.account.Account> futuresAccountList = accountService.queryBalanceByAccountType(balanceHeader, AccountType.FUTURES, false, true);

                BigDecimal btcValue = mainAccountList.stream().map(account -> new BigDecimal(account.getBtcValue())).reduce(BigDecimal.ZERO, BigDecimal::add)
                        .add(optionAccountList.stream().map(account -> new BigDecimal(account.getBtcValue())).reduce(BigDecimal.ZERO, BigDecimal::add))
                        .add(futuresAccountList.stream().map(account -> new BigDecimal(account.getBtcValue())).reduce(BigDecimal.ZERO, BigDecimal::add));
                log.info("loginHandle: orgId:{} platform:{} userId:{} loginType:{} balanceBtcAsset:{}", header.getOrgId(), header.getPlatform(), user.getUserId(), loginType, btcValue.toPlainString());
                if (btcValue.compareTo(new BigDecimal(commonIniService.getStringValueOrDefault(header.getOrgId(), "loginSkipSecondCheckBalanceValue", "0.001"))) < 0) {
                    hasBalance = false;
                }
            } catch (Exception e) {
                log.warn("login checkBalance error:", e);
                hasBalance = true;
            }

            bindMobile = !Strings.isNullOrEmpty(user.getMobile());
            bindEmail = !Strings.isNullOrEmpty(user.getEmail());
            bindGA = user.getBindGA() == BrokerServerConstants.BIND_GA_SUCCESS.intValue();
            Broker broker = brokerService.getBrokerByOrgId(header.getOrgId());
            if (hasBalance || broker.getLoginNeed2fa() == 1 || bindGA) {
                // check white list
                CommonIni loginEmailWhiteCommonIni = commonIniService.getCommonIniFromCache(header.getOrgId(), LOGIN_EMAIL_WHITE_LIST);
                CommonIni loginMobileWhiteCommonIni = commonIniService.getCommonIniFromCache(header.getOrgId(), LOGIN_MOBILE_WHITE_LIST);
                String loginEmailWhiteList = loginEmailWhiteCommonIni == null ? "" : loginEmailWhiteCommonIni.getIniValue();
                String loginMobileWhiteList = loginMobileWhiteCommonIni == null ? "" : loginMobileWhiteCommonIni.getIniValue();
                if ((!Strings.isNullOrEmpty(user.getEmail()) && !Strings.isNullOrEmpty(loginEmailWhiteList) && loginEmailWhiteList.contains(user.getEmail()))
                        || (!Strings.isNullOrEmpty(user.getMobile()) && !Strings.isNullOrEmpty(loginMobileWhiteList) && loginMobileWhiteList.contains(user.getMobile()))) {
                    bindMobile = false;
                    bindEmail = false;
                    bindGA = false;
                    need2FACheck = false;
                    skipSendLoginSuccessNotice = true;
                } else {
                    /*
                     * 20200816 需求
                     * 只要有GA，就必须验证GA
                     */
                    if (bindGA) {
                        bindMobile = false;
                        bindEmail = false;
                        authType = AuthType.GA.name();
                    } else {
                        // check previousLoginSuccess;
                        LoginLog loginLog = loginLogMapper.getLastLoginLog(user.getOrgId(), user.getUserId(), header.getPlatform().name());
                        if (loginLog != null
                                && loginLog.getStatus() == BrokerServerConstants.LOGIN_SUCCESS
                                && Strings.nullToEmpty(loginLog.getIp()).equals(header.getRemoteIp())
                                && header.getAppBaseHeader().getImei().equals(JsonUtil.getString(JsonParser.parseString(loginLog.getAppBaseHeader()), ".imei", ""))
                                && Strings.nullToEmpty(loginLog.getUserAgent()).equals(header.getUserAgent())
                                && (System.currentTimeMillis() - loginLog.getUpdated()) < 24 * 3600 * 1000
                                && broker.getLoginNeed2fa() == 0) {
                            bindMobile = false;
                            bindEmail = false;
                            bindGA = false;
                            need2FACheck = false;
                            log.info("loginHandle: orgId:{} platform:{} userId:{} loginType:{} skip2FACheck", header.getOrgId(), header.getPlatform(), user.getUserId(), loginType);
                        } else {
//                            if (bindGA) {
//                                bindMobile = false;
//                                bindEmail = false;
//                                authType = AuthType.GA.name();
//                            } else {
//                                if (loginType == LoginType.MOBILE) {
//                                    bindMobile = true;
//                                    bindEmail = false;
//                                    authType = AuthType.MOBILE.name();
//                                } else {
//                                    bindEmail = true;
//                                    bindMobile = false;
//                                    authType = AuthType.EMAIL.name();
//                                }
//                            }
                            if (loginType == LoginType.MOBILE) {
                                bindMobile = true;
                                bindEmail = false;
                                authType = AuthType.MOBILE.name();
                            } else {
                                bindEmail = true;
                                bindMobile = false;
                                authType = AuthType.EMAIL.name();
                            }
                        }
                    }
                }
            } else {
                need2FACheck = false;
                bindGA = false;
                bindMobile = false;
                bindEmail = false;
                skipSendLoginSuccessNotice = true;
            }
        }

        LoginResponse.Builder builder = LoginResponse.newBuilder()
                .setBindMobile(bindMobile)
                .setBindEmail(bindEmail)
                .setBindGa(bindGA)
                .setNeed2FaCheck(need2FACheck)
                .setAuthType(authType)
                .setUser(getUserInfo(user));

        if (need2FACheck) {
            long loginId = addLoginLog(header, user.getUserId(), loginType, BrokerServerConstants.LOGIN_INTERRUPT);
            String requestId = CryptoUtil.getRandomCode(32);
            builder.setRequestId(requestId);
            redisTemplate.opsForValue().set(BrokerServerConstants.LOGIN_USER_ID + requestId, user.getUserId().toString(),
                    BrokerServerConstants.PRE_REQUEST_EFFECTIVE_MINUTES, TimeUnit.MINUTES);
            redisTemplate.opsForValue().set(BrokerServerConstants.LOGIN_RECORD_ID + requestId, Long.toString(loginId),
                    BrokerServerConstants.PRE_REQUEST_EFFECTIVE_MINUTES, TimeUnit.MINUTES);
            redisTemplate.opsForValue().set(BrokerServerConstants.LOGIN_SUCCESS_TOKEN + requestId, securityLoginResponse.getToken(),
                    BrokerServerConstants.PRE_REQUEST_EFFECTIVE_MINUTES, TimeUnit.MINUTES);
        } else {
            builder.setToken(securityLoginResponse.getToken());
            Long logId = addLoginLog(header, user.getUserId(), loginType, BrokerServerConstants.LOGIN_SUCCESS);
            asyncUserInfo(user);
            if (!skipSendLoginSuccessNotice && requestSuccessNotice) {
                sendLoginSuccessNotice(header, user, loginType);
            }
            //异步推送给需要登录信息的客户
            this.pushDataService.userLoginMessage(header, user.getOrgId(), user.getUserId(), loginType, logId);
        }
        createUserOptionalAccountAndFuturesAccountIfNoeExist(header.getOrgId(), user.getUserId());
        redisTemplate.delete(String.format(BrokerServerConstants.LOGIN_ERROR_USER_ID_COUNT, user.getOrgId(), user.getUserId()));
        return builder.build();
    }

    public QuickLoginResponse quickLogin(Header header, User user, LoginType loginType) {
        Long currentTimestamp = System.currentTimeMillis();
        // 使用数据库的记录来判断冻结状态
        boolean userUnderFrozen = false;
        FrozenUserRecord frozenUserRecord = frozenUserRecordMapper.getByUserIdAndFrozenType(header.getOrgId(), user.getUserId(), FrozenType.FROZEN_LOGIN.type());
        if (frozenUserRecord != null
                && (currentTimestamp <= frozenUserRecord.getEndTime() && currentTimestamp >= frozenUserRecord.getStartTime())) {
            log.info("user:{} is under login frozen, cannot login", header.getUserId());
            userUnderFrozen = true;
        }
        if (userUnderFrozen) {
            if (frozenUserRecord.getFrozenReason() == FrozenReason.LOGIN_INPUT_ERROR.reason()) {
                return QuickLoginResponse.newBuilder().setRet(BrokerErrorCode.LOGIN_INPUT_ERROR_6.code())
                        .setUser(io.bhex.broker.grpc.user.User.newBuilder().setUserId(user.getUserId()).build()).build();
            } else if (frozenUserRecord.getFrozenReason() == FrozenReason.SYSTEM_FROZEN.reason()) {
                return QuickLoginResponse.newBuilder().setRet(BrokerErrorCode.FROZEN_LOGIN_BY_SYSTEM.code())
                        .setUser(io.bhex.broker.grpc.user.User.newBuilder().setUserId(user.getUserId()).build()).build();
            } else {
                // 目前只有FrozenReason.SYSTEM_FROZEN
                return QuickLoginResponse.newBuilder().setRet(BrokerErrorCode.FROZEN_LOGIN_BY_ILLEGAL_OPERATION.code())
                        .setUser(io.bhex.broker.grpc.user.User.newBuilder().setUserId(user.getUserId()).build()).build();
            }
        }

        if (user.getUserStatus() != 1) {
            throw new BrokerException(BrokerErrorCode.USER_STATUS_FORBIDDEN);
        }
        SecurityLoginRequest request = SecurityLoginRequest.newBuilder()
                .setHeader(header)
                .setUserId(user.getUserId())
                .setPassword("")
                .setIsQuickLogin(true)
                .build();
        SecurityLoginResponse securityLoginResponse = grpcSecurityService.login(request);
        boolean needCheckPassword = false, hasBalance = true, skipSendLoginSuccessNotice = false;
        if (user.getBindPassword() == 1) {
            needCheckPassword = true;
            try {
                Header balanceHeader = header.toBuilder().setUserId(user.getUserId()).build();
                List<io.bhex.broker.grpc.account.Account> mainAccountList = accountService.queryBalanceByAccountType(balanceHeader, AccountType.MAIN, false, true);
                List<io.bhex.broker.grpc.account.Account> optionAccountList = accountService.queryBalanceByAccountType(balanceHeader, AccountType.OPTION, false, true);
                List<io.bhex.broker.grpc.account.Account> futuresAccountList = accountService.queryBalanceByAccountType(balanceHeader, AccountType.FUTURES, false, true);

                BigDecimal btcValue = mainAccountList.stream().map(account -> new BigDecimal(account.getBtcValue())).reduce(BigDecimal.ZERO, BigDecimal::add)
                        .add(optionAccountList.stream().map(account -> new BigDecimal(account.getBtcValue())).reduce(BigDecimal.ZERO, BigDecimal::add))
                        .add(futuresAccountList.stream().map(account -> new BigDecimal(account.getBtcValue())).reduce(BigDecimal.ZERO, BigDecimal::add));
                log.info("quickLoginHandle: orgId:{} platform:{} userId:{} loginType:{} balanceBtcAsset:{}", header.getOrgId(), header.getPlatform(), user.getUserId(), loginType, btcValue.toPlainString());
                if (btcValue.compareTo(new BigDecimal(commonIniService.getStringValueOrDefault(header.getOrgId(), "loginSkipSecondCheckBalanceValue", "0.001"))) < 0) {
                    hasBalance = false;
                }
            } catch (Exception e) {
                // ignore
            }
            Broker broker = brokerService.getBrokerByOrgId(header.getOrgId());
            if (hasBalance || broker.getLoginNeed2fa() == 1) {
                // check white list
                CommonIni loginEmailWhiteCommonIni = commonIniService.getCommonIniFromCache(header.getOrgId(), LOGIN_EMAIL_WHITE_LIST);
                CommonIni loginMobileWhiteCommonIni = commonIniService.getCommonIniFromCache(header.getOrgId(), LOGIN_MOBILE_WHITE_LIST);
                String loginEmailWhiteList = loginEmailWhiteCommonIni == null ? "" : loginEmailWhiteCommonIni.getIniValue();
                String loginMobileWhiteList = loginMobileWhiteCommonIni == null ? "" : loginMobileWhiteCommonIni.getIniValue();
                if ((!Strings.isNullOrEmpty(user.getEmail()) && !Strings.isNullOrEmpty(loginEmailWhiteList) && loginEmailWhiteList.contains(user.getEmail()))
                        || (!Strings.isNullOrEmpty(user.getMobile()) && !Strings.isNullOrEmpty(loginMobileWhiteList) && loginMobileWhiteList.contains(user.getMobile()))) {
                    needCheckPassword = false;
                } else {
                    // check previousLoginSuccess;
                    LoginLog loginLog = loginLogMapper.getLastLoginLog(user.getOrgId(), user.getUserId(), header.getPlatform().name());
                    if (loginLog != null
                            && loginLog.getStatus() == BrokerServerConstants.LOGIN_SUCCESS
                            && Strings.nullToEmpty(loginLog.getIp()).equals(header.getRemoteIp())
                            && header.getAppBaseHeader().getImei().equals(JsonUtil.getString(JsonParser.parseString(loginLog.getAppBaseHeader()), ".imei", ""))
                            && Strings.nullToEmpty(loginLog.getUserAgent()).equals(header.getUserAgent())
                            && (System.currentTimeMillis() - loginLog.getUpdated()) < 24 * 3600 * 1000
                            && broker.getLoginNeed2fa() == 0) {
                        needCheckPassword = false;
                        log.info("quickLoginHandle: orgId:{} platform:{} userId:{} loginType:{} skip2FACheck", header.getOrgId(), header.getPlatform(), user.getUserId(), loginType);
                    } else {
                        needCheckPassword = true;
                    }
                }
                skipSendLoginSuccessNotice = false;
            } else {
                needCheckPassword = false;
                skipSendLoginSuccessNotice = true;
            }
        }

        QuickLoginResponse.Builder builder = QuickLoginResponse.newBuilder()
                .setNeedCheckPassword(needCheckPassword)
                .setUser(getUserInfo(user));
        if (needCheckPassword) {
            long loginId = addLoginLog(header, user.getUserId(), loginType, BrokerServerConstants.LOGIN_INTERRUPT);
            String requestId = CryptoUtil.getRandomCode(32);
            builder.setRequestId(requestId);
            redisTemplate.opsForValue().set(BrokerServerConstants.LOGIN_USER_ID + requestId, user.getUserId().toString(),
                    BrokerServerConstants.PRE_REQUEST_EFFECTIVE_MINUTES, TimeUnit.MINUTES);
            redisTemplate.opsForValue().set(BrokerServerConstants.LOGIN_RECORD_ID + requestId, Long.toString(loginId),
                    BrokerServerConstants.PRE_REQUEST_EFFECTIVE_MINUTES, TimeUnit.MINUTES);
            redisTemplate.opsForValue().set(BrokerServerConstants.LOGIN_SUCCESS_TOKEN + requestId, securityLoginResponse.getToken(),
                    BrokerServerConstants.PRE_REQUEST_EFFECTIVE_MINUTES, TimeUnit.MINUTES);
        } else {
            builder.setToken(securityLoginResponse.getToken());
            Long logId = addLoginLog(header, user.getUserId(), loginType, BrokerServerConstants.LOGIN_SUCCESS);
            asyncUserInfo(user);
            //异步推送给需要登录信息的客户
            this.pushDataService.userLoginMessage(header, user.getOrgId(), user.getUserId(), loginType, logId);
            if (!skipSendLoginSuccessNotice) {
                sendLoginSuccessNotice(header, user, loginType);
            }
        }
        createUserOptionalAccountAndFuturesAccountIfNoeExist(header.getOrgId(), user.getUserId());
        redisTemplate.delete(String.format(BrokerServerConstants.LOGIN_ERROR_USER_ID_COUNT, user.getOrgId(), user.getUserId()));
        return builder.build();
    }

    public QuickLoginCheckPasswordResponse quickLoginCheckPassword(Header header, String requestId, String password) {
        Long currentTimestamp = System.currentTimeMillis();
        String cachedUserId = redisTemplate.opsForValue().get(BrokerServerConstants.LOGIN_USER_ID + requestId);
        String cachedLoginId = redisTemplate.opsForValue().get(BrokerServerConstants.LOGIN_RECORD_ID + requestId);
        String token = redisTemplate.opsForValue().get(BrokerServerConstants.LOGIN_SUCCESS_TOKEN + requestId);
        if (Strings.isNullOrEmpty(cachedUserId) || Strings.isNullOrEmpty(cachedLoginId) || Strings.isNullOrEmpty(token)) {
            throw new BrokerException(BrokerErrorCode.REQUEST_INVALID);
        }
        Long userId = Long.valueOf(cachedUserId), loginId = Long.valueOf(cachedLoginId);
        User user = userMapper.getByUserId(userId);

        SecurityLoginRequest request = SecurityLoginRequest.newBuilder()
                .setHeader(header)
                .setUserId(user.getUserId())
                .setPassword(password)
                .build();
        SecurityLoginResponse securityLoginResponse;
        try {
            securityLoginResponse = grpcSecurityService.login(request);
        } catch (BrokerException e) {
            if (e.getCode() != BrokerErrorCode.LOGIN_INPUTS_ERROR.code()) {
                throw e;
            }
            updateLoginLog(loginId, BrokerServerConstants.LOGIN_FAILED);

            redisTemplate.opsForValue().setIfAbsent(String.format(BrokerServerConstants.LOGIN_ERROR_USER_ID_COUNT, user.getOrgId(), user.getUserId()), "0", Duration.ofHours(24));
            long errorTimes = redisTemplate.opsForValue().increment(String.format(BrokerServerConstants.LOGIN_ERROR_USER_ID_COUNT, user.getOrgId(), user.getUserId()), 1L);
            if (errorTimes == 1) {
                return QuickLoginCheckPasswordResponse.newBuilder().setRet(BrokerErrorCode.LOGIN_INPUTS_ERROR.code())
                        .setUser(io.bhex.broker.grpc.user.User.newBuilder().setUserId(user.getUserId()).build()).build();
                // throw new BrokerException(BrokerErrorCode.LOGIN_INPUTS_ERROR);
            } else if (errorTimes == 2) {
                return QuickLoginCheckPasswordResponse.newBuilder().setRet(BrokerErrorCode.LOGIN_INPUT_ERROR_2.code())
                        .setUser(io.bhex.broker.grpc.user.User.newBuilder().setUserId(user.getUserId()).build()).build();
                // throw new BrokerException(BrokerErrorCode.LOGIN_INPUT_ERROR_2);
            } else if (errorTimes == 3) {
                return QuickLoginCheckPasswordResponse.newBuilder().setRet(BrokerErrorCode.LOGIN_INPUT_ERROR_3.code())
                        .setUser(io.bhex.broker.grpc.user.User.newBuilder().setUserId(user.getUserId()).build()).build();
                // throw new BrokerException(BrokerErrorCode.LOGIN_INPUT_ERROR_3);
            } else if (errorTimes == 4) {
                return QuickLoginCheckPasswordResponse.newBuilder().setRet(BrokerErrorCode.LOGIN_INPUT_ERROR_4.code())
                        .setUser(io.bhex.broker.grpc.user.User.newBuilder().setUserId(user.getUserId()).build()).build();
                // throw new BrokerException(BrokerErrorCode.LOGIN_INPUT_ERROR_4);
            } else if (errorTimes == 5) {
                forzenUserLogin(user.getOrgId(), user.getUserId());
                return QuickLoginCheckPasswordResponse.newBuilder().setRet(BrokerErrorCode.LOGIN_INPUT_ERROR_5.code())
                        .setUser(io.bhex.broker.grpc.user.User.newBuilder().setUserId(user.getUserId()).build()).build();
            } else {
                FrozenUserRecord frozenUserRecord = frozenUserRecordMapper.getByUserIdAndFrozenType(header.getOrgId(), user.getUserId(), FrozenType.FROZEN_LOGIN.type());
                if (frozenUserRecord == null || currentTimestamp > frozenUserRecord.getEndTime()) {
                    forzenUserLogin(user.getOrgId(), user.getUserId());
                }
                return QuickLoginCheckPasswordResponse.newBuilder().setRet(BrokerErrorCode.LOGIN_INPUT_ERROR_6.code())
                        .setUser(io.bhex.broker.grpc.user.User.newBuilder().setUserId(user.getUserId()).build()).build();
                // throw new BrokerException(BrokerErrorCode.LOGIN_INPUT_ERROR_6);
            }
        }
        if (user.getUserStatus() != 1) {
            throw new BrokerException(BrokerErrorCode.USER_STATUS_FORBIDDEN);
        }
        updateLoginLog(loginId, BrokerServerConstants.LOGIN_SUCCESS);

        asyncUserInfo(user);
        LoginLog loginLog = loginLogMapper.getById(loginId);
        if (loginLog != null && loginLog.getLoginType() != null) {
            sendLoginSuccessNotice(header, user, LoginType.fromType(loginLog.getLoginType()));
            //异步推送给需要登录信息的客户
            this.pushDataService.userLoginMessage(header, user.getOrgId(), user.getUserId(), LoginType.fromType(loginLog.getLoginType()), loginId);
        }
        return QuickLoginCheckPasswordResponse.newBuilder()
                .setUser(getUserInfo(user))
                .setToken(securityLoginResponse.getToken())
                .build();
    }

    public ScanLoginQrCodeResponse scanLoginQrCode(Header header, String ticket, Header webRequestHeader) {
        header = header.toBuilder().setUserAgent(webRequestHeader.getUserAgent()).setRemoteIp(webRequestHeader.getRemoteIp()).setPlatform(Platform.PC).build();
        long loginId = addLoginLog(header, header.getUserId(), LoginType.SCAN_QRCODE, BrokerServerConstants.SCAN_LOGIN_QRCODE_SUCCESS);
        redisTemplate.opsForValue().set(BrokerServerConstants.LOGIN_RECORD_ID + ticket, Long.toString(loginId),
                BrokerServerConstants.PRE_REQUEST_EFFECTIVE_MINUTES, TimeUnit.MINUTES);
        redisTemplate.opsForValue().set(BrokerServerConstants.LOGIN_USER_ID + ticket, String.valueOf(header.getUserId()),
                BrokerServerConstants.PRE_REQUEST_EFFECTIVE_MINUTES, TimeUnit.MINUTES);
        return ScanLoginQrCodeResponse.getDefaultInstance();
    }

    public QrCodeAuthorizeLoginResponse qrCodeAuthorizeLogin(Header header, String ticket, boolean authorizeLogin) {
        String cachedLoginId = redisTemplate.opsForValue().get(BrokerServerConstants.LOGIN_RECORD_ID + ticket);
        if (Strings.isNullOrEmpty(cachedLoginId)) {
            return QrCodeAuthorizeLoginResponse.newBuilder().setRet(BrokerErrorCode.LOGIN_QRCODE_EXPIRED_OR_NOT_EXIST.code()).build();
        }
        Long loginId = Long.valueOf(cachedLoginId);
        LoginLog loginLog = loginLogMapper.getById(loginId);
        if (loginLog.getUserId() != header.getUserId()) {
            return QrCodeAuthorizeLoginResponse.newBuilder().setRet(BrokerErrorCode.SCAN_LOGIN_QRCODE_WITH_ANOTHER_USER.code()).build();
        }
        updateLoginLog(loginId, authorizeLogin ? BrokerServerConstants.QRCODE_AUTHORIZED_LOGIN : BrokerServerConstants.QRCODE_UNAUTHORIZED_LOGIN);
        return QrCodeAuthorizeLoginResponse.getDefaultInstance();
    }

    public GetScanLoginQrCodeResultResponse getScanLoginQrCodeResult(Header header, String ticket) {
        String cachedUserId = redisTemplate.opsForValue().get(BrokerServerConstants.LOGIN_USER_ID + ticket);
        String cachedLoginId = redisTemplate.opsForValue().get(BrokerServerConstants.LOGIN_RECORD_ID + ticket);
        if (Strings.isNullOrEmpty(cachedLoginId) || Strings.isNullOrEmpty(cachedUserId)) {
            return GetScanLoginQrCodeResultResponse.newBuilder().setRet(BrokerErrorCode.LOGIN_QRCODE_EXPIRED_OR_NOT_EXIST.code()).build();
        }
        Long loginId = Long.valueOf(cachedLoginId);
        LoginLog loginLog = loginLogMapper.getById(loginId);
        if (loginLog.getStatus() != BrokerServerConstants.QRCODE_AUTHORIZED_LOGIN) {
            return GetScanLoginQrCodeResultResponse.newBuilder().setRet(BrokerErrorCode.GET_SCAN_LOGIN_QRCODE_RESULT_ERROR.code()).build();
        }
        User user = userMapper.getByOrgAndUserId(header.getOrgId(), Long.valueOf(cachedUserId));
        QueryUserTokenResponse response
                = grpcSecurityService.getAuthorizeToken(QueryUserTokenRequest.newBuilder().setHeader(header).setUserId(Long.parseLong(cachedUserId)).build());
        return GetScanLoginQrCodeResultResponse.newBuilder()
                .setUser(getUserInfo(user))
                .setToken(response.getToken())
                .build();
    }

    @Async
    public void createUserOptionalAccountAndFuturesAccountIfNoeExist(Long orgId, Long userId) {
        try {
            if (orgId > 0 && userId > 0) {
                createOptionAccount(orgId, userId);
                createFuturesAccount(orgId, userId);
                //createMarginAccount(orgId, userId);
            }
        } catch (Exception e) {
            // ignore
        }
    }

    private void forzenUserLogin(Long orgId, Long userId) {
        Long currentTimestamp = System.currentTimeMillis();
        FrozenUserRecord frozenRecord = FrozenUserRecord.builder()
                .orgId(orgId)
                .userId(userId)
                .frozenType(FrozenType.FROZEN_LOGIN.type())
                .frozenReason(FrozenReason.LOGIN_INPUT_ERROR.reason())
                .startTime(System.currentTimeMillis())
                .endTime(System.currentTimeMillis() + 2 * 3600 * 1000)
                .status(FrozenStatus.UNDER_FROZEN.status())
                .created(currentTimestamp)
                .updated(currentTimestamp)
                .build();
        frozenUserRecordMapper.insertRecord(frozenRecord);
    }

    @SiteFunctionLimitSwitchAnnotation(wholeSiteSwitchKey = BaseConfigConstants.STOP_LOGIN_KEY)
    public LoginAdvanceResponse loginAdvance(Header header, String requestId,
                                             Integer authTypeValue, Long verifyCodeOrderId, String verifyCode) {
        String cachedUserId = redisTemplate.opsForValue().get(BrokerServerConstants.LOGIN_USER_ID + requestId);
        String cachedLoginId = redisTemplate.opsForValue().get(BrokerServerConstants.LOGIN_RECORD_ID + requestId);
        String token = redisTemplate.opsForValue().get(BrokerServerConstants.LOGIN_SUCCESS_TOKEN + requestId);
        if (Strings.isNullOrEmpty(cachedUserId) || Strings.isNullOrEmpty(cachedLoginId) || Strings.isNullOrEmpty(token)) {
            throw new BrokerException(BrokerErrorCode.REQUEST_INVALID);
        }
        Long userId = Long.valueOf(cachedUserId), loginId = Long.valueOf(cachedLoginId);
        User user = userMapper.getByUserId(userId);
        AuthType authType = AuthType.fromValue(authTypeValue);
        if (authType == AuthType.GA) {
            userSecurityService.validGACode(header, user.getUserId(), verifyCode);
        } else {
            header = header.toBuilder().setUserId(user.getUserId()).build();
            boolean isCheck = true;
            if (authType == AuthType.EMAIL && verifyCaptcha && globalNotifyType == 2 && user.getRegisterType() == 2) {
                //平台仅支持手机,对于历史注册类型为邮箱的登陆验证，可以忽略校验邮箱验证码
                isCheck = false;
            } else if (authType == AuthType.MOBILE && verifyCaptcha && globalNotifyType == 3 && user.getRegisterType() == 1) {
                //平台仅支持邮箱,对于历史注册类型为手机的登陆验证，可以忽略校验手机验证码
                isCheck = false;
            }
            if (isCheck) {
                userSecurityService.validVerifyCode(header, authType, verifyCodeOrderId, verifyCode, VerifyCodeType.LOGIN_ADVANCE);
            }
        }
        updateLoginLog(loginId, BrokerServerConstants.LOGIN_SUCCESS);
        if (authType != AuthType.GA) {
            if (authType == AuthType.MOBILE) {
                verifyCodeService.invalidVerifyCode(header, VerifyCodeType.LOGIN_ADVANCE, verifyCodeOrderId, user.getNationalCode() + user.getMobile());
            } else {
                verifyCodeService.invalidVerifyCode(header, VerifyCodeType.LOGIN_ADVANCE, verifyCodeOrderId, user.getEmail());
            }
        }
        asyncUserInfo(user);
        LoginLog loginLog = loginLogMapper.getById(loginId);
        if (loginLog != null && loginLog.getLoginType() != null) {
            sendLoginSuccessNotice(header, user, LoginType.fromType(loginLog.getLoginType()));
            //异步推送给需要登录信息的客户
            this.pushDataService.userLoginMessage(header, user.getOrgId(), user.getUserId(), LoginType.fromType(loginLog.getLoginType()), loginId);
        }
        return LoginAdvanceResponse.newBuilder()
                .setToken(token)
                .setUser(getUserInfo(user))
                .build();
    }

    @Async
    public void asyncUserInfo(User user) {
        try {
            ModifyOrgUserRequest request = ModifyOrgUserRequest.newBuilder()
                    .setBaseRequest(BaseReqUtil.getBaseRequest(user.getOrgId()))
                    .setOrgId(user.getOrgId())
                    .setUserId(user.getUserId().toString())
                    .setCountryCode(Strings.nullToEmpty(user.getNationalCode()))
                    .setMobile(Strings.nullToEmpty(user.getMobile()))
                    .setEmail(Strings.nullToEmpty(user.getEmail()))
                    .build();
            grpcAccountService.asyncUserInfo(request);
        } catch (Exception e) {
            log.warn("async user info to bh-server has error", e);
        }
    }

    @Async
    public void sendLoginSuccessNotice(Header header, User user, LoginType loginType) {
        try {
            if (loginType == null) {
                return;
            }
            Broker broker = brokerService.getBrokerById(header.getOrgId());
            String brokerTimeZone = broker.getTimeZone();
            if (Strings.isNullOrEmpty(brokerTimeZone)) {
                brokerTimeZone = "GMT+8";
            }
            TimeZone timeZone;
            try {
                timeZone = TimeZone.getTimeZone(brokerTimeZone);
            } catch (Exception e) {
                timeZone = TimeZone.getTimeZone("GMT+8");
            }
            String timeZoneID = timeZone.getID();
            Date current = new Date();
            JsonObject jsonObject = new JsonObject();
//            DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss('" + timeZoneID + "')");
            DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss(z)");
            dateFormat.setTimeZone(timeZone);
            if (loginType == LoginType.MOBILE) {
                jsonObject.addProperty("username", user.getMobile().replaceAll("\\d*(\\d{4})", "**$1"));
                jsonObject.addProperty("datetime", dateFormat.format(current));
                noticeTemplateService.sendSmsNotice(header, user.getUserId(), NoticeBusinessType.LOGIN_SUCCESS, user.getNationalCode(), user.getMobile(), jsonObject);
            } else {
                // jsonObject.addProperty("username", user.getEmail().replaceAll("(?<=.).(?=[^@]*?.@)", "*"));
                jsonObject.addProperty("datetime", dateFormat.format(current));
                noticeTemplateService.sendEmailNotice(header, user.getUserId(), NoticeBusinessType.LOGIN_SUCCESS, user.getEmail(), jsonObject);
            }
            //noticeTemplateService.sendBizPushNotice(header, user.getUserId(), NoticeBusinessType.LOGIN_SUCCESS, null, jsonObject, null);
        } catch (Exception e) {
            // ignore exception
        }
    }

    public long addLoginLog(Header header, Long userId, LoginType loginType, Integer status) {
        Long currentTimestamp = System.currentTimeMillis();
        try {
            LoginLog loginLog = LoginLog.builder()
                    .orgId(header.getOrgId())
                    .userId(userId)
                    .ip(header.getRemoteIp())
                    .status(status)
                    .loginType(loginType.value())
                    .platform(header.getPlatform().name())
                    .userAgent(header.getUserAgent())
                    .language(header.getLanguage())
                    .appBaseHeader(GrpcHeaderUtil.getAppBaseInfo(header))
                    .channel(header.getChannel())
                    .source(header.getSource())
                    .created(currentTimestamp)
                    .updated(currentTimestamp)
                    .build();
            loginLogMapper.insert(loginLog);
            return loginLog.getId();
        } catch (Exception e) {
            log.warn("add login log error", e);
            throw e;
        }
    }

    public void updateLoginLog(Long loginId, Integer status) {
        try {
            LoginLog loginLog = LoginLog.builder()
                    .id(loginId)
                    .status(status)
                    .updated(System.currentTimeMillis())
                    .build();
            loginLogMapper.update(loginLog);
        } catch (Exception e) {
            log.warn("update login log error", e);
        }
    }

    public QueryLoginLogResponse queryLoginLog(Header header, Long startTime, Long endTime, Long fromId, Integer limit) {
        List<LoginLog> loginLogList = loginLogMapper.queryLog(header.getUserId(), startTime, endTime, fromId, limit);
        List<io.bhex.broker.grpc.user.LoginLog> logList = Lists.newArrayList();
        if (loginLogList != null && loginLogList.size() > 0) {
            logList = loginLogList.stream().map(log -> {
                String ip = Strings.nullToEmpty(log.getIp());
//                if (Strings.isNullOrEmpty(log.getIp()) && log.getIp().indexOf(".") > 0) {
//                    ip = log.getIp().replaceAll(BrokerServerConstants.MASK_IP_REG, "$1.***.***$4");
//                } else if (Strings.isNullOrEmpty(log.getIp()) && log.getIp().indexOf(":") > 0) {
//
//                }
                return io.bhex.broker.grpc.user.LoginLog.newBuilder()
                        .setId(log.getId())
                        .setIp(ip)
                        .setStatus(log.getStatus())
                        .setPlatform(log.getPlatform())
                        .setUserAgent(Strings.nullToEmpty(log.getUserAgent()))
                        .setLanguage(log.getLanguage())
                        .setAppBaseHeader(Strings.nullToEmpty(log.getAppBaseHeader()))
                        .setCreated(log.getCreated())
                        .build();
            }).collect(Collectors.toList());
        }
        return QueryLoginLogResponse.newBuilder().addAllLoginLogs(logList).build();
    }

    public QueryLoginLogsResponse queryLoginLog4Admin(Long userId, Long startTime, Long endTime, Integer current, Integer pageSize) {
        int start = (current - 1) * pageSize;
        List<LoginLog> loginLogList = loginLogMapper.getLogs(userId, startTime, endTime, start, pageSize);
        if (CollectionUtils.isEmpty(loginLogList)) {
            return QueryLoginLogsResponse.newBuilder().build();
        }
        List<UserLoginLog> logList = loginLogList.stream().map(log ->
                UserLoginLog.newBuilder()
                        .setId(log.getId())
                        .setIp(log.getIp())
                        .setStatus(log.getStatus())
                        .setCreated(log.getCreated())
                        .setLanguage(log.getLanguage())
                        .setRegion(Strings.nullToEmpty(log.getRegion()))
                        .setPlatform(Strings.nullToEmpty(log.getPlatform()))
                        .setAppBaseHeader(Strings.nullToEmpty(log.getAppBaseHeader()))
                        .setUserAgent(Strings.nullToEmpty(log.getUserAgent()))
                        .setLoginType(log.getLoginType())
                        .build())
                .collect(Collectors.toList());
        int count = loginLogMapper.countLogs(userId, startTime, endTime);
        return QueryLoginLogsResponse.newBuilder().addAllLoginLogs(logList).setTotal(count).build();
    }

    public GetUserInfoResponse getUserInfo(Long orgId, Long userId, String mobile, String email) {
        User user = userMapper.findUser(orgId, userId, mobile, email);
        if (user == null) {
            throw new BrokerException(BrokerErrorCode.USER_NOT_EXIST);
        }
        return GetUserInfoResponse.newBuilder()
                .setUser(getUserInfo(user))
                .build();
    }

    public EditAntiPhishingCodeResponse editAntiPhishingCode(EditAntiPhishingCodeRequest request) {
        User user = userMapper.getByOrgAndUserId(request.getHeader().getOrgId(), request.getHeader().getUserId());
        if (user == null) {
            throw new BrokerException(BrokerErrorCode.USER_NOT_EXIST);
        }
        if (StringUtil.isEmpty(user.getEmail())) {
            throw new BrokerException(BrokerErrorCode.REQUEST_INVALID);
        }

        userSecurityService.validVerifyCode(request.getHeader(), AuthType.fromValue(request.getAuthType()),
                request.getOrderId(), request.getVerifyCode(), VerifyCodeType.EDIT_ANTI_PHISHING_CODE);


        if (AuthType.fromValue(request.getAuthType()) == AuthType.MOBILE) {
            verifyCodeService.invalidVerifyCode(request.getHeader(), VerifyCodeType.EDIT_ANTI_PHISHING_CODE, request.getOrderId(), user.getNationalCode() + user.getMobile());
        } else {
            verifyCodeService.invalidVerifyCode(request.getHeader(), VerifyCodeType.EDIT_ANTI_PHISHING_CODE, request.getOrderId(), user.getEmail());
        }


        io.bhex.base.common.EditAntiPhishingCodeRequest emailRequest = io.bhex.base.common.EditAntiPhishingCodeRequest.newBuilder()
                .setOrgId(request.getHeader().getOrgId())
                .setUserId(request.getHeader().getUserId())
                .setAntiPhishingCode(request.getAntiPhishingCode())
                .build();
        boolean suc = grpcEmailService.editAntiPhishingCode(emailRequest);
        if (!suc) {
            throw new BrokerException(BrokerErrorCode.SYSTEM_ERROR);
        }

        user.setAntiPhishingCode(request.getAntiPhishingCode().replaceAll("(.{1})(.*)(.{1})", "$1****$3"));
        userMapper.updateByPrimaryKeySelective(user);
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("email", user.getEmail().replaceAll(BrokerServerConstants.MASK_EMAIL_REG, "*"));
        noticeTemplateService.sendEmailNoticeAsync(request.getHeader(), user.getUserId(), NoticeBusinessType.ANTI_PHISHING_CODE_SUCCESS, user.getEmail(), jsonObject);
        return EditAntiPhishingCodeResponse.getDefaultInstance();
    }


    private Integer isAgentUser(Long orgId, Long userId) {
        AgentUser agentUser = agentUserMapper.queryAgentUserByUserId(orgId, userId);
        if (Objects.nonNull(agentUser) && agentUser.getStatus().equals(0)) {
            return 0;
        }
        if (Objects.nonNull(agentUser) && agentUser.getIsAgent().equals(1)) {
            return 1;
        }
        return 0;
    }

    public GetUserInfoResponse getUserInfo(Header header) {
        User user = userMapper.getByUserId(header.getUserId());
        if (user == null) {
            throw new BrokerException(BrokerErrorCode.USER_NOT_EXIST);
        }
        return GetUserInfoResponse.newBuilder()
                .setUser(getUserInfo(user))
                .build();
    }

    public List<SimpleUserInfo> querySimpleUserInfoList(Long orgId, String source, Long fromId, Long endId,
                                                        Long startTime, Long endTime, Integer limit) {
        boolean orderDesc = true;
        if (fromId == 0 && endId > 0) {
            orderDesc = false;
        }
        List<SimpleUserInfo> userInfoList = userMapper.queryUserInfo(orgId, source, fromId, endId, startTime, endTime,
                limit > MAX_LIMIT ? MAX_LIMIT : limit, orderDesc);
        if (!userInfoList.isEmpty()) {
            Example example = Example.builder(UserVerify.class).build();
            example.createCriteria().andEqualTo("orgId", orgId).andIn("userId", userInfoList.stream().map(SimpleUserInfo::getUserId).collect(Collectors.toList()));
            List<UserVerify> list = userVerifyMapper.selectByExample(example);
            Map<Long, Integer> userVerifies = Maps.newHashMap();
            list.stream().forEach(userVerify -> {
                userVerifies.put(userVerify.getUserId(), userVerify.getVerifyStatus());
            });
            userInfoList.stream().forEach(userInfo -> {
                userInfo.setVerifyStatus(userVerifies.getOrDefault(userInfo.getUserId(), 0));
            });
        }
        if (!orderDesc) {
            Collections.reverse(userInfoList);
        }
        return userInfoList;
    }

    //
    public List<SimpleUserInfo> queryChangedSimpleUserInfoList(Long orgId, String source, Long fromId, Long endId,
                                                               Long startTime, Long endTime, Integer limit) {
        boolean orderDesc = true;
        if (fromId == 0 && endId > 0) {
            orderDesc = false;
        }
        List<SimpleUserInfo> userInfoList = userMapper.queryChangedUserInfo(orgId, source, fromId, endId, startTime, endTime,
                limit > MAX_LIMIT ? MAX_LIMIT : limit, orderDesc);
        if (!orderDesc) {
            Collections.reverse(userInfoList);
        }
        return userInfoList;
    }

    public List<UserInviteInfo> queryUserInviteInfo(Long orgId, Long userId, Integer inviteType, Boolean invitedLeader, Long fromId, Long endId,
                                                    Long startTime, Long endTime, String source, Integer limit) {
        boolean orderDesc = true;
        if (fromId == 0 && endId > 0) {
            orderDesc = false;
        }
        List<UserInviteInfo> userInviteInfoList = inviteRelationMapper.queryUserInviteInfo(orgId, userId, inviteType, invitedLeader, fromId, endId,
                startTime, endTime, source, limit > MAX_LIMIT ? MAX_LIMIT : limit, orderDesc);
        if (CollectionUtils.isEmpty(userInviteInfoList)) {
            return Lists.newArrayList();
        }
        if (!orderDesc) {
            Collections.reverse(userInviteInfoList);
        }

        return userInviteInfoList;
    }

    public User getUser(Long userId) {
        return userMapper.getByUserId(userId);
    }

    public List<User> getUsers(List<Long> userIds) {

        Example exp = new Example(User.class);
        exp.createCriteria().andIn("userId", userIds);

        return userMapper.selectByExample(exp);
    }

    public List<User> getUsersByStatus(long orgId, Long lastId, int userStatus, Integer limit, long userId) {
        Example example = Example.builder(User.class)
                .orderByDesc("id")
                .build();
        PageHelper.startPage(0, limit);
        Example.Criteria criteria = example.createCriteria()
                .andEqualTo("orgId", orgId)
                .andEqualTo("userStatus", userStatus);
        if (lastId > 0) {
            criteria.andLessThan("id", lastId);
        }
        if (userId > 0) {
            criteria.andEqualTo("userId", userId);
        }

        return userMapper.selectByExample(example);
    }

    public List<UserVerify> queryUserKycInfoList(Long orgId, Long fromId, Long lastId, Long startTime, Long endTime, Integer limit) {
        boolean orderDesc = true;
        if (fromId == 0 && lastId > 0) {
            orderDesc = false;
        }
        List<UserVerify> userVerifyList = userVerifyMapper.queryUserKycList(orgId, fromId, lastId, startTime, endTime, limit, orderDesc);
        if (!orderDesc) {
            Collections.reverse(userVerifyList);
        }
        return userVerifyList;
    }

    public void rebuildUserInviteRelation(Long orgId, Long userId, Long inviteUserId) {
        User user = userMapper.getByOrgAndUserId(orgId, userId);
        if (user == null) {
            throw new BrokerException(BrokerErrorCode.USER_NOT_EXIST);
        }

        if (user.getInviteUserId() != null && user.getInviteUserId() > 0 && user.getInviteUserId() != inviteUserId.longValue()) {
            throw new BrokerException(BrokerErrorCode.INVITE_RELATION_HAS_BEEN_ESTABLISHED);
        }
        User inviteUser = userMapper.getByOrgAndUserId(orgId, inviteUserId);
        if (inviteUser == null) {
            throw new BrokerException(BrokerErrorCode.INVITE_USER_NOT_EXIST);
        }
        if (userId.equals(inviteUser.getInviteUserId()) || userId.equals(inviteUser.getSecondLevelInviteUserId())) {
            log.warn("userId:{} is the invitation of  inviteUserId:{} ", userId, inviteUserId);
            throw new BrokerException(BrokerErrorCode.WRONG_INVITE_RELATION);
        }
        User updateObj = User.builder()
                .userId(user.getUserId())
                .inviteUserId(inviteUserId)
                .secondLevelInviteUserId(inviteUser.getInviteUserId() == null ? 0 : inviteUser.getInviteUserId())
                .updated(System.currentTimeMillis())
                .build();
        int rows = userMapper.updateRecord(updateObj);
        if (rows <= 0) {
            log.error("rebuild user:{} invite relation error", userId);
        }
        // 添加监听，异步构建用户邀请关系
        InviteRelationEvent event = InviteRelationEvent.builder()
                .orgId(orgId)
                .userId(userId)
                .accountId(accountService.getAccountId(orgId, userId))
                .name(StringUtils.isEmpty(user.getEmail()) ? user.getMobile() : user.getEmail())
                .inviteId(inviteUserId)
                .build();
        applicationContext.publishEvent(event);
    }

    /**
     * 按照不同条件查询用户
     *
     * @param orgId
     * @param userId
     * @param nationalCode
     * @param mobile
     * @param email
     * @return
     */
    public User getUserInfo(Long orgId, Long accountId, Long userId, String nationalCode, String mobile, String email) {
        User user = null;
        if (accountId > 0) {
            Account account = accountMapper.getAccountByAccountId(accountId);
            userId = account != null && account.getOrgId().equals(orgId) ? account.getUserId() : userId;
        }
        if (userId > 0) {
            user = userMapper.getByOrgAndUserId(orgId, userId);
        } else if (!StringUtils.isEmpty(mobile)) {
            user = userMapper.getByMobile(orgId, nationalCode, mobile);
        } else if (!StringUtils.isEmpty(email)) {
            user = userMapper.getByEmail(orgId, email);
        }
        if (user == null) {
            return null;
        }
        return user;
    }

    public GetInviteCodeResponse getInviteCode(Header header, InviteCodeUseLimit useLimit) {
        if (header.getUserId() <= 0 && header.getOrgId() <= 0) {
            return GetInviteCodeResponse.newBuilder().setRet(-1).build();
        }
        String unusedInviteCodeKey = header.getOrgId() + "_" + UNUSED_INVITE_CODE;
        if (useLimit == InviteCodeUseLimit.ONCE) {
            if (header.getOrgId() <= 0) {
                return GetInviteCodeResponse.newBuilder().setRet(-1).build();
            }
            String inviteCode = getInviteCode(header);
            redisTemplate.opsForHash().put(unusedInviteCodeKey, inviteCode, "1");
            return GetInviteCodeResponse.newBuilder().setInviteCode(inviteCode).build();
        } else {
            if (header.getUserId() <= 0) {
                return GetInviteCodeResponse.newBuilder().setRet(-1).build();
            }
            User user = userMapper.getByUserId(header.getUserId());
            String inviteCode = user.getInviteCode();
            redisTemplate.opsForHash().put(unusedInviteCodeKey, inviteCode, "2");
            return GetInviteCodeResponse.newBuilder().setInviteCode(inviteCode).build();
        }
    }

    public GetUserContactResponse getUserContact(Long accountId) {

        Account account = accountMapper.getAccountByAccountId(accountId);
        if (account == null) {
            return GetUserContactResponse.getDefaultInstance();
        }

        User user = userMapper.getByUserId(account.getUserId());
        if (user == null) {
            return GetUserContactResponse.getDefaultInstance();
        }

        if (StringUtils.isNotBlank(user.getMobile())) {
            String contract = user.getNationalCode() + " " + user.getMobile();
            return GetUserContactResponse.newBuilder()
                    .setContact(contract)
                    .build();
        }

        if (StringUtils.isNotBlank(user.getEmail())) {
            return GetUserContactResponse.newBuilder()
                    .setContact(user.getEmail())
                    .build();
        }

        return GetUserContactResponse.getDefaultInstance();
    }

    private io.bhex.broker.grpc.user.User getUserInfo(User user) {
        UserVerify userVerify = UserVerify.decrypt(userVerifyMapper.getByUserId(user.getUserId()));
        int verifyStatus = 0;
        int kycLevel = 0;
        String displayLevel = "";
        if (userVerify != null) {
            verifyStatus = userVerify.getVerifyStatus();
            kycLevel = userVerify.getKycLevel();
            KycLevel kycLevelObj = basicService.getKycLevel(kycLevel);
            displayLevel = (kycLevelObj == null) ? "0" : kycLevelObj.getDisplayLevel();
        }
        List<Long> labelIds = new ArrayList<>();
        try {
            labelIds = user.getCustomLabelIdList();
        } catch (NumberFormatException e) {
            log.error("getUserInfo error: custom label id parse error. id str => {}.", user.getCustomLabelIdList());
        }
        Long marginAid = accountService.getMarginAccountIdNoThrow(user.getOrgId(), user.getUserId(), 0);
        Index0AccountIds index0AccountIds = Index0AccountIds.newBuilder()
                .setCoinIndex0AccountId(getDefaultIndex0AccountId(user.getOrgId(), user.getUserId(), AccountType.MAIN))
                .setOptionalIndex0AccountId(getDefaultIndex0AccountId(user.getOrgId(), user.getUserId(), AccountType.OPTION))
                .setFuturesIndex0AccountId(getDefaultIndex0AccountId(user.getOrgId(), user.getUserId(), AccountType.FUTURES))
                .setMarginIndex0AccountId(marginAid == null ? 0L : marginAid)
                .build();
        //获取最后登录成功记录
        Long lastLoginDate = 0L;
        String lastLoginIp = "";
        String platfrom = "";
        LoginLog loginLog = loginLogMapper.getAllPlatfromLastSuccessLog(user.getOrgId(), user.getUserId());
        if (loginLog != null) {
            lastLoginDate = loginLog.getCreated();
            lastLoginIp = loginLog.getIp();
            platfrom = loginLog.getPlatform();
        }
        return io.bhex.broker.grpc.user.User.newBuilder()
                .setUserId(user.getUserId())
                .setNationalCode(Strings.nullToEmpty(user.getNationalCode()))
                .setMobile(Strings.nullToEmpty(user.getMobile()))
                .setEmail(Strings.nullToEmpty(user.getEmail()))
                .setBindGa(user.getBindGA() == BrokerServerConstants.BIND_GA_SUCCESS.intValue())
                .setBindTradePwd(user.getBindTradePwd().equals(BrokerServerConstants.BIND_TRADE_PWD_SUCCESS))
                .setRegisterType(user.getRegisterType())
                .setUserType(user.getUserType())
                .setVerifyStatus(verifyStatus)
                .setDefaultAccountId(accountService.getAccountId(user.getOrgId(), user.getUserId()))
                .setRegisterDate(user.getCreated())
                .setSource(Strings.nullToEmpty(user.getSource()))
                .setInviteUserId(user.getInviteUserId() == null ? 0 : user.getInviteUserId())
                .setSecondLevelInviteUserId(user.getSecondLevelInviteUserId() == null ? 0 : user.getSecondLevelInviteUserId())
                .setDefaultIndex0AccountIds(index0AccountIds)
                .setKycLevel(kycLevel)
                .setDisplayLevel(displayLevel)
                .setIsAgent(isAgentUser(user.getOrgId(), user.getUserId()))
                .addAllCustomLabelIds(labelIds)
                .setHasLoginPassword(user.getBindPassword() == 1)
                .setAntiPhishingCode(Strings.nullToEmpty(user.getAntiPhishingCode()))
                .setIsComplianceVerify(false)
                .setLastLoginDate(lastLoginDate)
                .setLastLoginIp(lastLoginIp)
                .setPlatform(platfrom)
                .build();
    }

    private Long getDefaultIndex0AccountId(Long orgId, Long userId, AccountType accountType) {
        try {
            return accountService.getAccountId(orgId, userId, accountType);
        } catch (Exception e) {
            return 0L;
        }
    }

    public CreateFavoriteResponse createFavorite(Header header, Long exchangeId, String symbolId) {
        if (favoriteMapper.countIfExists(header.getUserId(), exchangeId, symbolId) <= 0) {
            Integer maxOrder = favoriteMapper.getMaxCustomerOrder(header.getOrgId(), header.getUserId());
            if (maxOrder == null) {
                maxOrder = 0;
            }
            io.bhex.broker.server.model.Favorite favorite = io.bhex.broker.server.model.Favorite.builder()
                    .orgId(header.getOrgId())
                    .userId(header.getUserId())
                    .exchangeId(exchangeId)
                    .symbolId(symbolId)
                    .created(System.currentTimeMillis())
                    .customOrder(maxOrder + 1)
                    .build();
            favoriteMapper.insert(favorite);
        }
        return CreateFavoriteResponse.getDefaultInstance();
    }

    public CancelFavoriteResponse cancelFavorite(Header header, Long exchangeId, String symbolId) {
        favoriteMapper.delete(header.getUserId(), exchangeId, symbolId);
        return CancelFavoriteResponse.getDefaultInstance();
    }

    public QueryFavoritesResponse queryFavorites(Header header) {
        List<io.bhex.broker.server.model.Favorite> favoriteList = favoriteMapper.queryByUserId(header.getOrgId(), header.getUserId());
        return QueryFavoritesResponse.newBuilder()
                .addAllFavorites(favoriteList.stream().map(this::getFavorite).collect(Collectors.toList())
                ).build();
    }

    @Transactional
    public SortFavoritesResponse sortFavorites(Header header, List<Favorite> favorites) {
        favoriteMapper.deleteMyFavorites(header.getOrgId(), header.getUserId());
        if (!CollectionUtils.isEmpty(favorites)) {
            List<io.bhex.broker.server.model.Favorite> favoriteList = Lists.newArrayList();
            for (int i = favorites.size() - 1; i >= 0; i--) {
                Favorite f = favorites.get(i);
                favoriteList.add(io.bhex.broker.server.model.Favorite.builder()
                        .orgId(header.getOrgId())
                        .userId(header.getUserId())
                        .exchangeId(f.getExchangeId())
                        .symbolId(f.getSymbolId())
                        .created(System.currentTimeMillis())
                        .customOrder(favorites.size() - i)
                        .build());
            }
            favoriteMapper.insertList(favoriteList);
        }

        return SortFavoritesResponse.getDefaultInstance();
    }

    private Favorite getFavorite(io.bhex.broker.server.model.Favorite favorite) {
        return Favorite.newBuilder()
                .setId(favorite.getId())
                .setExchangeId(favorite.getExchangeId())
                .setSymbolId(favorite.getSymbolId())
                .build();
    }

    /**
     * todo: reKYC need refactor
     */
    @KycApplicationNotify
    public PersonalVerifyResponse userVerify(Header header, Long nationality, String firstName, String secondName, Integer gender,
                                             Integer cardType, String cardNo, String cardFrontUrl, String cardHandUrl,
                                             Integer authTypeValue, Long verifyCodeOrderId, String verifyCode) {
        basicService.validCountryId(nationality);
        if (Strings.isNullOrEmpty(firstName)) {
            throw new BrokerException(BrokerErrorCode.FIRST_NAME_CANNOT_BE_NULL);
        }
        if (Strings.isNullOrEmpty(secondName)) {
            throw new BrokerException(BrokerErrorCode.SECOND_NAME_CANNOT_BE_NULL);
        }
        if (gender == null) {
            throw new BrokerException(BrokerErrorCode.GENDER_ERROR);
        }
        if (cardType == null) {
            throw new BrokerException(BrokerErrorCode.ID_CARD_TYPE_ERROR);
        }
        if (Strings.isNullOrEmpty(cardNo)) {
            throw new BrokerException(BrokerErrorCode.ID_CARD_NO_CANNOT_BE_NULL);
        }
        if (Strings.isNullOrEmpty(cardFrontUrl)) {
            throw new BrokerException(BrokerErrorCode.ID_CARD_FRONT_URL_ERROR);
        }
        if (Strings.isNullOrEmpty(cardHandUrl)) {
            throw new BrokerException(BrokerErrorCode.ID_CARD_HAND_URL_ERROR);
        }

        String countryCode = basicService.getCountryCode(nationality);
        if (countryCode.equalsIgnoreCase("CN") && cardType != 1) {
            throw new BrokerException(BrokerErrorCode.CARD_TYPE_NOT_MATCH_WITH_NATIONALITY);
        }

        //一小时鉴权次数+1
        checkUserKycLimit(header.getUserId(), USER_KYC_ONE_HOUR_LIMIT);

        //一天鉴权次数+1
        checkUserKycLimit(header.getUserId(), USER_KYC_ONE_DAY_LIMIT);

        User user = userMapper.getByOrgAndUserId(header.getOrgId(), header.getUserId());
        AuthType authType = AuthType.fromValue(authTypeValue);
        if (authType != null) {
            if (Strings.isNullOrEmpty(verifyCode)) {
                throw new BrokerException(BrokerErrorCode.VERIFY_CODE_CANNOT_BE_NULL);
            }
            if (authType == AuthType.GA) {
                userSecurityService.validGACode(header, user.getUserId(), verifyCode);
            } else {
                userSecurityService.validVerifyCode(header, authType, verifyCodeOrderId, verifyCode, VerifyCodeType.KYC);
            }
        }

        String cardNoHash = UserVerify.hashCardNo(cardNo);
        List<UserVerify> userVerifyList = userVerifyMapper.queryByOrgIdAndCardNoHash(header.getOrgId(), cardNoHash);
        for (UserVerify userVerify : userVerifyList) {
            if (userVerify != null && userVerify.getUserId() != header.getUserId() && userVerify.getVerifyStatus() != UserVerifyStatus.REFUSED.value()) {
                throw new BrokerException(BrokerErrorCode.IDENTITY_CARD_NO_BEING_USED);
            }
        }

        boolean existVerifyInfo = false;
        UserVerify userVerify = userVerifyMapper.getByUserId(header.getUserId());
        if (userVerify != null && (userVerify.getVerifyStatus() != UserVerifyStatus.REFUSED.value()
                && userVerify.getVerifyStatus() != UserVerifyStatus.NONE.value())) {
            throw new BrokerException(BrokerErrorCode.IDENTITY_SUBMIT_REPEATED);
        }
        if (userVerify != null) {
            existVerifyInfo = true;
        }

        boolean passedIdCheck = false;
        if (basicService.getCountryCode(nationality).equalsIgnoreCase("CN")) {
            if (!IDCardUtil.validateCard(cardNo)) {
                log.info("orgId:{} userId:{} enter a invalid IDCardNO:{}", header.getOrgId(), header.getUserId(), cardNo);
                throw new BrokerException(BrokerErrorCode.IDENTITY_AUTHENTICATION_CARD_NO_INVALID);
            }
            String checkSwitch = redisTemplate.opsForValue().get("cnUserIDCardNoCheck");
            if (Strings.isNullOrEmpty(checkSwitch)) {
                checkSwitch = "true";
            }
            boolean cnUserIDCardNoCheck = Boolean.parseBoolean(checkSwitch);
            if (!cnUserIDCardNoCheck) {
                log.warn("NO_PASSED_CARD_NO_INFO: orgId:{}-userId:{} submit info cardNo:{} will nor passed check",
                        header.getOrgId(), header.getUserId(), Hashing.md5().hashString(Strings.nullToEmpty(cardNo), Charsets.UTF_8).toString());
            }
            if (!cardNo.substring(0, 2).equals("65") && !cardNo.substring(0, 2).equals("54") && cnUserIDCardNoCheck) {
                String idCardName = firstName + secondName;
                Long checkOrderId = sequenceGenerator.getLong();
                if (brokerServerProperties.isTencentKyc()) {
                    TencentIDCardVerifyRequest request = TencentIDCardVerifyRequest.builder()
                            .certcode(cardNo)
                            .name(idCardName)
                            .build();
                    String checkResponse = "";
                    TencentIDCardVerifyResponse response = null;
                    Long startTime = System.currentTimeMillis();
                    try {
                        response = tencentApi.idCardVerify(request);
                        checkResponse = JsonUtil.defaultGson().toJson(response);
                        insertUserIdCheckLog(checkOrderId, header.getOrgId(), header.getUserId(), idCardName, cardNo, checkResponse);

                    } catch (BrokerException e) {
                        log.error("orgId:{}, userId:{}, name:{} kyc catch BrokerException, code:{}", header.getOrgId(), header.getUserId(), idCardName, e.getCode());
                        insertUserIdCheckLog(checkOrderId, header.getOrgId(), header.getUserId(), idCardName, cardNo, "BrokerException, code:" + e.getCode());
                        if (e.getCode() == BrokerErrorCode.IDENTITY_AUTHENTICATION_TIMEOUT.code()) {
                            log.warn("user:{} kyc timeout, please pay attention", header.getUserId());
                        } else {
                            throw e;
                        }
                    } catch (Exception e) {
                        log.error("user:{} kyc exception", header.getUserId(), e);
                        insertUserIdCheckLog(checkOrderId, header.getOrgId(), header.getUserId(), idCardName, cardNo, "Exception, message:" + Strings.nullToEmpty(e.getMessage()));
                        throw new BrokerException(BrokerErrorCode.IDENTITY_AUTHENTICATION_FAILED);
                    }
                    Long endTime = System.currentTimeMillis();
                    log.info("TencentApi-invoke-log, orderId:{}, startTime:{}, endTime:{}, duration:{}", checkOrderId, startTime, endTime, (endTime - startTime));
                    if (response != null) {
                        // code为0&& result为 1 代表身份信息数据一致
                        if (response.getCode() != TENCENT_SUCCESS_CODE || response.getResult() != 1) {
                            log.warn("user:{} kyc chinese user identity authentication failed, errorCode:{}, errorMsg:{}", header.getUserId(), response.getCode(), response.getMessage());
                            throw new BrokerException(BrokerErrorCode.INCONSISTENT_IDENTITY_INFO);
                        }
                        passedIdCheck = true;
                    }
                } else {
                    IDCardVerifyRequest request = IDCardVerifyRequest.builder()
                            .orderId(checkOrderId.toString())
                            .idCardName(idCardName)
                            .idCardId(cardNo)
                            .build();
                    String checkResponse = "";
                    IDCardVerifyResponse response = null;
                    Long startTime = System.currentTimeMillis();
                    try {
                        response = jieanApi.idCardVerify(request);
                        checkResponse = JsonUtil.defaultGson().toJson(response);
                        insertUserIdCheckLog(checkOrderId, header.getOrgId(), header.getUserId(), idCardName, cardNo, checkResponse);
                    } catch (BrokerException e) {
                        log.error("user:{} kyc catch BrokerException, code:{}", header.getUserId(), e.getCode());
                        insertUserIdCheckLog(checkOrderId, header.getOrgId(), header.getUserId(), idCardName, cardNo, "BrokerException, code:" + e.getCode());
                        if (e.getCode() == BrokerErrorCode.IDENTITY_AUTHENTICATION_TIMEOUT.code()) {
                            log.warn("user:{} kyc timeout, please pay attention", header.getUserId());
                        } else {
                            throw e;
                        }
                    } catch (Exception e) {
                        log.error("user:{} kyc exception", header.getUserId(), e);
                        insertUserIdCheckLog(checkOrderId, header.getOrgId(), header.getUserId(), idCardName, cardNo, "Exception, message:" + Strings.nullToEmpty(e.getMessage()));
                        throw new BrokerException(BrokerErrorCode.IDENTITY_AUTHENTICATION_FAILED);
                    }
                    Long endTime = System.currentTimeMillis();
                    log.info("JieanApi-invoke-log, orderId:{}, startTime:{}, endTime:{}, duration:{}", checkOrderId, startTime, endTime, (endTime - startTime));
                    if (response != null) {
                        // 捷安的000代表身份信息数据一致
                        if (!response.getRespCode().equals(JIEAN_SUCCESS_CODE)) {
                            log.warn("user:{} kyc chinese user identity authentication failed, errorCode:{}, errorMsg:{}", header.getUserId(), response.getRespCode(), response.getRespDesc());
                            if (JIEAN_AUTHENTICATION_NAME_INVALID_CODES.contains(response.getRespCode())) {
                                throw new BrokerException(BrokerErrorCode.IDENTITY_AUTHENTICATION_NAME_INVALID);
                            }
                            if (JIEAN_AUTHENTICATION_CARD_NO_INVALID_CODES.contains(response.getRespCode())) {
                                throw new BrokerException(BrokerErrorCode.IDENTITY_AUTHENTICATION_CARD_NO_INVALID);
                            }
                            throw new BrokerException(BrokerErrorCode.INCONSISTENT_IDENTITY_INFO);
                        }
                        passedIdCheck = true;
                    }
                }
            }
        }

        Long timestamp = System.currentTimeMillis();

        // 插入申请记录
        UserKycApply userKycApply = UserKycApply.builder()
                .id(sequenceGenerator.getLong())
                .kycLevel(20)
                .userId(header.getUserId())
                .orgId(header.getOrgId())
                .cardFrontUrl(cardFrontUrl)
                .cardBackUrl("")
                .cardHandUrl(cardHandUrl)
                .dataSecret("")
                .created(System.currentTimeMillis())
                .build();
        userKycApplyMapper.insertSelective(userKycApply);

        userVerify = UserVerify.builder()
                .orgId(header.getOrgId())
                .userId(header.getUserId())
                .nationality(nationality)
                .countryCode(countryCode)
                .firstName(firstName)
                .secondName(secondName)
                .gender(gender)
                .cardType(cardType)
                .cardNo(cardNo)
                .cardNoHash(cardNoHash)
                .cardFrontUrl(cardFrontUrl)
                .cardHandUrl(cardHandUrl)
                .dataEncrypt(0)
                .passedIdCheck(passedIdCheck ? 1 : 0)
                .verifyStatus(UserVerifyStatus.UNDER_REVIEW.value())
                .kycLevel(20) // 旧版KYC审核申请默认是二级审核
                .kycApplyId(userKycApply.getId())
                .build();
        if (existVerifyInfo) {
            userVerify.setUpdated(timestamp);
            userVerifyMapper.update(userVerify);
        } else {
            userVerify.setUpdated(timestamp);
            userVerify.setCreated(timestamp);
            userVerifyMapper.insertRecord(userVerify);
        }
        if (authType != null && authType != AuthType.GA) {
            if (authType == AuthType.MOBILE) {
                verifyCodeService.invalidVerifyCode(header, VerifyCodeType.KYC, verifyCodeOrderId, user.getNationalCode() + user.getMobile());
            } else {
                verifyCodeService.invalidVerifyCode(header, VerifyCodeType.KYC, verifyCodeOrderId, user.getEmail());
            }
        }
        return PersonalVerifyResponse.newBuilder().build();
    }

    private void insertUserIdCheckLog(Long id, Long orgId, Long userId, String name, String idCardNo, String jsonResponse) {
        try {
            UserIdCheckLog userIdCheckLog = UserIdCheckLog.builder()
                    .id(id)
                    .orgId(orgId)
                    .userId(userId)
                    .name(name)
                    .idNo(idCardNo)
                    .response(jsonResponse)
                    .created(System.currentTimeMillis())
                    .build();
            userVerifyMapper.insertUserIdCheckLog(userIdCheckLog);
        } catch (Exception e) {
            log.error("save id card check log exception", e);
        }
    }

    public GetPersonalVerifyInfoResponse getUserVerifyInfo(Header header) {
        return getUserVerifyInfo(header, false, false);
    }

    public GetPersonalVerifyInfoResponse getUserVerifyInfo(Header header, boolean needDecryptIdNumber, boolean needDecryptPhotoUrl) {
        UserVerify dbUserVerify = userVerifyMapper.getByUserId(header.getUserId());
        if (dbUserVerify == null) {
            return GetPersonalVerifyInfoResponse.newBuilder().build();
        }
        // 如果数据库存储的字段是加密的话，需要解密
        UserVerify userVerify = UserVerify.decrypt(dbUserVerify);
        String cardNo = needDecryptIdNumber ? userVerify.getCardNo() : userVerify.getCardNo().replaceAll("(.{5})(.*)(.{3})", "$1******$3");
        String countryName = Strings.nullToEmpty(basicService.getCountryName(userVerify.getNationality(), header.getLanguage()));
        if (StringUtils.isEmpty(countryName)) {
            countryName = Strings.nullToEmpty(basicService.getCountryName(userVerify.getNationality(), Locale.US.toString()));
        }
        String cardFrontUrl = Strings.nullToEmpty(userVerify.getCardFrontUrl());
        String cardBackUrl = Strings.nullToEmpty(userVerify.getCardBackUrl());
        String cardHandUrl = Strings.nullToEmpty(userVerify.getCardHandUrl());

        String refusedReason = userVerifyService.getRefusedReason(userVerify, header.getLanguage());
        return GetPersonalVerifyInfoResponse.newBuilder()
                .setVerifyInfo(PersonalVerifyInfo.newBuilder()
                        .setUserVerifyId(userVerify.getId())
                        .setNationality(countryName)
                        .setCountryCode(Strings.nullToEmpty(userVerify.getCountryCode()))
                        .setFirstName(userVerify.getFirstName())
                        .setSecondName(Strings.nullToEmpty(userVerify.getSecondName()))
                        .setGender(userVerify.getGender() == null ? 0 : userVerify.getGender())
                        .setCardType(Strings.nullToEmpty(basicService.getCardTypeTable().get(userVerify.getCardType(), header.getLanguage())))
                        .setCardNo(cardNo)
                        .setCardFrontUrl(needDecryptPhotoUrl ? userVerifyService.getPhotoPlainUrlKey(userVerify, cardFrontUrl) : cardFrontUrl)
                        .setCardBackUrl(needDecryptPhotoUrl ? userVerifyService.getPhotoPlainUrlKey(userVerify, cardBackUrl) : cardBackUrl)
                        .setCardHandUrl(needDecryptPhotoUrl ? userVerifyService.getPhotoPlainUrlKey(userVerify, cardHandUrl) : cardHandUrl)
                        .setKycLevel(userVerify.getKycLevel())
                        .setVerifyStatus(userVerify.getVerifyStatus())
                        //.setRefusedId(refusedReasonId)
                        .setRefusedReason(refusedReason)
                        .build())
                .build();
    }


    public GetAccountInfosResponse getAccountByUserId(Long orgId, Long userId) {
        List<Account> accounts = accountMapper.queryByUserId(orgId, userId);
        GetAccountInfosResponse.Builder builder = GetAccountInfosResponse.newBuilder();
        if (!CollectionUtils.isEmpty(accounts)) {
            List<AccountInfo> accountInfoList = accounts.stream().map(account ->
                    AccountInfo.newBuilder()
                            .setAccountId(account.getAccountId())
                            .setAccountName(account.getAccountName())
                            .setAccountStatus(account.getAccountStatus())
                            .setAccountType(account.getAccountType())
                            .setOrgId(account.getOrgId())
                            .setId(account.getId())
                            .build()).collect(Collectors.toList());
            builder.addAllAccountInfos(accountInfoList);
        }
        return builder.build();
    }

    public boolean createOptionAccount(Long orgId, Long userId) {
        Account account = accountMapper.getAccountByType(orgId, userId, AccountType.OPTION.value(), 0);
        if (account == null) {
            CreateOptionAccountReply reply = grpcAccountService.createOptionAccount(orgId, userId);
            if (reply == null) {
                return false;
            }
            Long timestamp = System.currentTimeMillis();
            account = Account.builder()
                    .orgId(orgId)
                    .userId(userId)
                    .accountId(reply.getOptionAccountId())
                    .accountName("")
                    .accountType(AccountType.OPTION.value())
                    .created(timestamp)
                    .updated(timestamp)
                    .build();
            accountMapper.insertRecord(account);
        }
        return true;
    }

    public Long createFuturesAccount(Long orgId, Long userId) {
        Account account = accountMapper.getAccountByType(orgId, userId, AccountType.FUTURES.value(), 0);
        if (account == null) {
            CreateFuturesAccountReply reply = grpcAccountService.createFuturesAccount(orgId, userId);
            if (reply != null && reply.getFuturesAccountId() > 0) {
                Long accountId = reply.getFuturesAccountId();
                Long timestamp = System.currentTimeMillis();
                account = Account.builder()
                        .orgId(orgId)
                        .userId(userId)
                        .accountId(accountId)
                        .accountName("")
                        .accountType(AccountType.FUTURES.value())
                        .created(timestamp)
                        .updated(timestamp)
                        .build();
                accountMapper.insertRecord(account);
            }
            return reply.getFuturesAccountId();
        }
        return null;
    }

    public Long createMarginAccount(Long orgId, Long userId) {
        Account account = accountMapper.getAccountByType(orgId, userId, AccountType.MARGIN.value(), 0);
        if (account == null) {
            CreateSpecialUserAccountRequest request = CreateSpecialUserAccountRequest.newBuilder()
                    .setBaseRequest(BaseReqUtil.getBaseRequest(orgId))
                    .setOrgId(orgId)
                    .setBrokerUserId(String.valueOf(userId))
                    .setAccountType(io.bhex.base.account.AccountType.MARGIN_CROSS_ACCOUNT)
                    .setAccountIndex(0)
                    .build();
            CreateSpecialUserAccountReply reply = grpcAccountService.createSubAccount(request);
            if (reply != null && reply.getAccountId() > 0) {
                Long timestamp = System.currentTimeMillis();
                account = Account.builder()
                        .orgId(orgId)
                        .userId(userId)
                        .accountId(reply.getAccountId())
                        .accountName("")
                        .accountType(AccountType.MARGIN.value())
                        .accountIndex(0)
                        .created(timestamp)
                        .updated(timestamp)
                        .build();
                accountMapper.insertSelective(account);
            }
            return reply != null ? reply.getAccountId() : 0;
        }
        return account.getId();
    }

    public GetUserContractResponse getUserContract(Header header) {
        List<UserContract> userContracts = listUserContract(header.getOrgId(), header.getUserId());
        List<GetUserContractResponse.UserContract> responseList = new ArrayList<>();
        if (!CollectionUtils.isEmpty(userContracts)) {
            userContracts.forEach(c -> {
                responseList.add(GetUserContractResponse.UserContract.newBuilder()
                        .setOrgId(c.getOrgId())
                        .setUserId(c.getUserId())
                        .setName(c.getName())
                        .setOpen(c.getOpen())
                        .setCreated(c.getCreated())
                        .setUpdated(c.getUpdated())
                        .build());
            });
        }
        GetUserContractResponse response = GetUserContractResponse.newBuilder()
                .addAllUserContract(responseList)
                .build();
        return response;
    }

    private List<UserContract> listUserContract(Long orgId, Long userId) {
        Example example = new Example(UserContract.class);
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("orgId", orgId);
        criteria.andEqualTo("userId", userId);
        return userContractMapper.selectByExample(example);
    }

    private UserContract getUserContract(Long orgId, Long userId, String name) {
        if (StringUtils.isEmpty(name)) {
            return null;
        }
        List<UserContract> userContracts = listUserContract(orgId, userId);
        UserContract userContract = null;
        if (!CollectionUtils.isEmpty(userContracts)) {
            for (UserContract c : userContracts) {
                if (c.getName().equals(name)) {
                    userContract = c;
                    break;
                }
            }
        }
        return userContract;
    }

    public SaveUserContractResponse saveUserContract(SaveUserContractRequest request) {
        if (request.getName().equals(UserContract.MARGIN_NAME)) {//创建杠杆子账户
            Long accountId = createMarginAccount(request.getOrgId(), request.getUserId());
            if (accountId == 0L) {
                throw new BrokerException(BrokerErrorCode.MARGIN_CREATE_ACCOUNT_FAILED);
            }
        }
        UserContract userContract = getUserContract(request.getOrgId(), request.getUserId(), request.getName());
        if (Objects.isNull(userContract)) {
            userContract = UserContract.builder()
                    .orgId(request.getOrgId())
                    .userId(request.getUserId())
                    .name(request.getName())
                    .open(request.getOpen())
                    .updated(System.currentTimeMillis())
                    .created(System.currentTimeMillis())
                    .build();
            int success = userContractMapper.insert(userContract);
            if (success > 0) {
                if (userContract.getName().equals(UserContract.OPTION_NAME)) {
                    syncTransferOptionAirDrop(userContract);
                } else if (userContract.getName().equals(UserContract.FUTURES_NAME)) {
                    log.info("saveUserContract begin syncTransferFuturesAirDrop. userContract: {}",
                            JsonUtil.defaultGson().toJson(userContract));
                    syncTransferFuturesAirDrop(userContract);
                }
            } else {
                log.warn("saveUserContract error. request:{}", TextFormat.shortDebugString(request));
            }
        } else {
            userContract.setOrgId(request.getOrgId());
            userContract.setUserId(request.getUserId());
            userContract.setName(request.getName());
            userContract.setOpen(request.getOpen());
            userContract.setUpdated(System.currentTimeMillis());
            userContractMapper.updateByPrimaryKey(userContract);
        }
        return SaveUserContractResponse.newBuilder().build();
    }

    public ListUserAccountResponse listUserAccount(ListUserAccountRequest request) {
        Long orgId = request.getOrgId();
        AccountTypeEnum ate = request.getAccountType();
        List<Long> userIds = request.getUserIdList();

        if (orgId < 1L) {
            return ListUserAccountResponse.getDefaultInstance();
        }

        if (AccountTypeEnum.UNRECOGNIZED == ate) {
            log.error("Unrecongnized AccountTypeEnum ");
            throw new BrokerException(BrokerErrorCode.PARAM_INVALID);
        }

        if (CollectionUtils.isEmpty(userIds)) {
            log.error("UserIds is empty ");
            throw new BrokerException(BrokerErrorCode.PARAM_INVALID);
        }
        int accountType = AccountType.fromAccountTypeEnum(ate).value();
        Example exp = new Example(Account.class);
        exp.createCriteria()
                .andEqualTo("orgId", orgId)
                .andIn("userId", userIds)
                .andEqualTo("accountType", accountType)
                .andEqualTo("accountStatus", 1);

        List<Account> list = accountMapper.selectByExample(exp);
        List<UserAccountMap> accountMaps = list.stream().map(i -> {
            return UserAccountMap.newBuilder()
                    .setAccountId(i.getAccountId())
                    .setUserId(i.getUserId())
                    .setAccountType(AccountType.fromAccountType(AccountType.fromValue(i.getAccountType())))
                    .setAccountIndex(i.getAccountIndex())
                    .build();
        }).collect(Collectors.toList());

        return ListUserAccountResponse.newBuilder().addAllAccountInfo(accountMaps).build();
    }

    /**
     * 开通期权协议 - 运营数据定义
     */
    static class userContractOps {
        static Long opsAccountId = 220229299463102720L;
        static Long opsAccountOrgId = 6002L;
        static String opsTokenId = "BUSDT";
        static BigDecimal opsAirdropAmount = BigDecimal.valueOf(10000);
    }

    /**
     * 开通期权协议 - 运营规则：每个用户答完题后，赠送BUSDT
     * <p>
     * 处理： 1 每个用户答完题后，给转 10000 BUSDT 2 运营账户: orgId = 6002， accountId = 220229299463102720
     *
     * @param userContract 开通期权协议
     */
    private void syncTransferOptionAirDrop(UserContract userContract) {
//        try {
//            Long targetUserId = userContract.getUserId();
//            Long targetOrgId = userContract.getOrgId();
//            Long targetAccountId = accountService.getAccountId(userContract.getOrgId(), targetUserId, AccountType.OPTION);
//
//            SyncTransferRequest transferRequest = SyncTransferRequest.newBuilder()
//                    .setClientTransferId(userContract.getId()) //from account
//                    .setSourceAccountId(userContractOps.opsAccountId)
//                    .setSourceOrgId(userContractOps.opsAccountOrgId)
//                    .setSourceFlowSubject(BusinessSubject.AIRDROP)
//                    .setTokenId(userContractOps.opsTokenId)
//                    .setAmount(DecimalUtil.toTrimString(userContractOps.opsAirdropAmount))
//                    .setTargetAccountId(targetAccountId) //to account
//                    .setTargetOrgId(targetOrgId)
//                    .setSourceFlowSubject(BusinessSubject.AIRDROP)
//                    .build();
//            SyncTransferResponse transferResponse = grpcBatchTransferService.syncTransfer(transferRequest);
//            if (transferResponse.getCodeValue() != 200) {
//                log.error("syncTransfer. transferRequest:{}", TextFormat.shortDebugString(transferRequest));
//            }
//        } catch (Exception e) {
//            log.error("syncTransferOptionAirDrop error. userContract:{}", userContract);
//        }

    }

    private List<AirdropOpsAccountInfo> getFuturesAirdropOpsAccountInfo(Long orgId) {
        BrokerDictConfig brokerDictConfig = brokerDictConfigMapper.getBrokerDictConfigByKey(
                orgId, BrokerDictConfig.KEY_FUTURES_AIRDROP_OPS_ACCOUNT_INFO);
        if (brokerDictConfig == null || !brokerDictConfig.getStatus().equals(BrokerDictConfig.STATUS_ENABLE)) {
            // 如果没有配置合约空投体验配置，则从commonIni获取默认配置
            return getDefaultFuturesAirdropOpsAccountInfo(orgId);
        }

        List<AirdropOpsAccountInfo> infos;
        Gson gson = new Gson();
        try {
            AirdropOpsAccountInfo info = gson.fromJson(brokerDictConfig.getConfigValue(), AirdropOpsAccountInfo.class);
            infos = Lists.newArrayList(info);
        } catch (Exception e) {
            try {
                // 尝试按照JSON ARRAY解析
                infos = gson.fromJson(brokerDictConfig.getConfigValue(),
                        new TypeToken<List<AirdropOpsAccountInfo>>() {
                        }.getType());
            } catch (Exception e1) {
                log.error(String.format("parse AirdropOpsAccountInfo list from orgId: %s json: %s error",
                        orgId, brokerDictConfig.getConfigValue()));
                infos = Lists.newArrayList();
            }
        }

        if (!infos.isEmpty()) {
            BrokerDictConfig sysOpsAccountConfig = brokerDictConfigMapper.getBrokerDictConfigByKey(
                    orgId, BrokerDictConfig.KEY_FUTURES_AIRDROP_OPS_SYSTEM_ACCOUNT);
            if (sysOpsAccountConfig == null || !sysOpsAccountConfig.getStatus().equals(BrokerDictConfig.STATUS_ENABLE)) {
                return infos;
            }

            if (StringUtils.isEmpty(sysOpsAccountConfig.getConfigValue())) {
                return infos;
            }

            try {
                String[] opsAccoutValues = StringUtils.split(sysOpsAccountConfig.getConfigValue(), "|");
                Long sysOpsAccountId = Long.parseLong(opsAccoutValues[0]);
                Long sysOpsOrgId = Long.parseLong(opsAccoutValues[1]);
                // 将券商的运营账户替换为系统运营账户
                infos.forEach(info -> {
                    log.info("before replace the AirdropOpsAccountInfo is: {}", info);
                    info.setOpsAccountId(sysOpsAccountId);
                    info.setOpsAccountOrgId(sysOpsOrgId);
                    log.info("after replace the AirdropOpsAccountInfo is: {}", info);
                });
                return infos;
            } catch (Exception e) {
                log.error(String.format("Get sysOpsAccountId from: %s error", sysOpsAccountConfig.getConfigValue()), e);
                return infos;
            }
        } else {
            return infos;
        }
    }

    private List<AirdropOpsAccountInfo> getDefaultFuturesAirdropOpsAccountInfo(Long orgId) {
        List<AirdropOpsAccountInfo> accountInfos = Lists.newArrayList();

        try {
            CommonIni opsAccountInfoIni = commonIniService.getCommonIniFromCache(0L, DEFAULT_FUTURES_AIRDROP_OPS_ACCOUNT_INFO);
            if (opsAccountInfoIni == null || StringUtils.isEmpty(opsAccountInfoIni.getIniValue())) {
                log.warn("can not get key: {} from commonIni", DEFAULT_FUTURES_AIRDROP_OPS_ACCOUNT_INFO);
                return accountInfos;
            }

            Gson gson = new Gson();
            List<AirdropOpsAccountInfo> defaultAccountInfos = gson.fromJson(opsAccountInfoIni.getIniValue(),
                    new TypeToken<List<AirdropOpsAccountInfo>>() {
                    }.getType());

            CommonIni simuSymbolsIni = commonIniService.getCommonIniFromCache(0L, FUTURES_AIRDROP_SIMU_SYMBOLS);
            if (simuSymbolsIni == null || StringUtils.isEmpty(simuSymbolsIni.getIniValue())) {
                log.warn("can not get key: {} from commonIni", FUTURES_AIRDROP_SIMU_SYMBOLS);
                return accountInfos;
            }

            // 从配置获取体验合约的币对，检查org是否配置了这些体验合约
            String[] simuSymbolIds = StringUtils.split(simuSymbolsIni.getIniValue(), ",");
            for (String symbolId : simuSymbolIds) {
                Symbol symbol = basicService.getBrokerFuturesSymbol(orgId, symbolId);
                if (symbol == null) {
                    continue;
                }

                for (AirdropOpsAccountInfo defaultAccountInfo : defaultAccountInfos) {
                    if (defaultAccountInfo.getOpsTokenId().equals(symbol.getQuoteTokenId())) {
                        accountInfos.add(defaultAccountInfo);
                    }
                }
            }

            log.info("orgId: {} get default AirdropOpsAccountInfos: {}", orgId, accountInfos);
            return accountInfos;
        } catch (Exception e) {
            log.error("getDefaultFuturesAirdropOpsAccountInfo error", e);
            return accountInfos;
        }
    }

    /**
     * 用户新开通期货协议之后，给用户空投体验币资产 当用户的期货账户不存在时，先创建期货账户，再空投
     *
     * @param userContract 开通期货协议
     */
    private void syncTransferFuturesAirDrop(UserContract userContract) {
        try {
            List<AirdropOpsAccountInfo> opsAccountInfos = getFuturesAirdropOpsAccountInfo(userContract.getOrgId());
            if (CollectionUtils.isEmpty(opsAccountInfos)) {
                log.error("can not find opsAccountInfo. so airdrop stop. userContract: {}", JsonUtil.defaultGson().toJson(userContract));
                return;
            }

            Long targetOrgId = userContract.getOrgId();
            Long targetUserId = userContract.getUserId();
            Long targetAccountId;

            try {
                targetAccountId = accountService.getFuturesAccountId(Header.newBuilder().setOrgId(targetOrgId).setUserId(targetUserId).build());
            } catch (BrokerException e) {
                if (BrokerErrorCode.fromCode(e.getCode()).equals(BrokerErrorCode.ACCOUNT_NOT_EXIST)) {
                    // 创建期货账户
                    targetAccountId = createFuturesAccount(targetOrgId, targetUserId);
                } else {
                    throw e;
                }
            }

            long clientTransferIdSuffix = 0L;
            for (AirdropOpsAccountInfo opsAccountInfo : opsAccountInfos) {
                long clientTransferId = new BigDecimal(String.format("%d%d%02d",
                        userContract.getUserId(), userContract.getId(), clientTransferIdSuffix++)).longValue() & 0x7fffffffffffffffl;
                SyncTransferRequest transferRequest = SyncTransferRequest.newBuilder()
                        .setBaseRequest(BaseReqUtil.getBaseRequest(opsAccountInfo.getOpsAccountOrgId()))
                        .setClientTransferId(clientTransferId) //from account
                        .setSourceAccountId(opsAccountInfo.getOpsAccountId())
                        .setSourceOrgId(opsAccountInfo.getOpsAccountOrgId())
                        .setSourceFlowSubject(BusinessSubject.AIRDROP)
                        .setTokenId(opsAccountInfo.getOpsTokenId())
                        .setAmount(opsAccountInfo.getOpsAirdropAmount())
                        .setTargetAccountId(targetAccountId) //to account
                        .setTargetAccountType(io.bhex.base.account.AccountType.FUTURES_ACCOUNT)
                        .setTargetOrgId(targetOrgId)
                        .setTargetFlowSubject(BusinessSubject.AIRDROP)
                        .build();
                log.info("syncTransferFuturesAirDrop {}", TextFormat.shortDebugString(transferRequest));
                SyncTransferResponse transferResponse = grpcBatchTransferService.syncTransfer(transferRequest);
                if (transferResponse.getCodeValue() != 200) {
                    log.error("syncTransfer. transferRequest:{}", TextFormat.shortDebugString(transferResponse));
                }
            }
        } catch (Exception e) {
            log.error("syncTransferOptionAirDrop error. userContract:{}", userContract);
        }
    }

    public ListUpdateUserByDateReply listUpdateUserByDate(ListUpdateUserByDateRequest request) {
        ListUpdateUserByDateReply.Builder replyBulider = ListUpdateUserByDateReply.newBuilder();
        List<User> users = userMapper.listUpdateUserByDate(request.getOrgId(), request.getFromId(), request.getEndId(), request.getStartTime(), request.getEndTime(), request.getLimit());
        List<io.bhex.broker.grpc.admin.UserInfo> userInfos = new ArrayList<>();
        users.forEach(u -> {
            io.bhex.broker.grpc.admin.UserInfo.Builder userInfoBuilder = io.bhex.broker.grpc.admin.UserInfo.newBuilder();
            userInfoBuilder.setId(u.getId());
            userInfoBuilder.setUserId(u.getUserId());
            userInfoBuilder.setNationalCode(u.getNationalCode());
            userInfoBuilder.setMobile(u.getMobile());
            userInfoBuilder.setEmail(u.getEmail());
            userInfoBuilder.setBindGa(u.getBindGA() == BrokerServerConstants.BIND_GA_SUCCESS.intValue());
            userInfoBuilder.setRegisterType(u.getRegisterType());
            userInfoBuilder.setUserType(u.getUserType());
            userInfoBuilder.setCreated(u.getCreated());
            userInfoBuilder.setUserStatus(u.getUserStatus());
            userInfoBuilder.setUpdated(u.getUpdated());
            Header header = Header.newBuilder()
                    .setUserId(u.getUserId())
                    .build();
            GetPersonalVerifyInfoResponse kyc = getUserVerifyInfo(header);
            PersonalVerifyInfo kycinfo = kyc.getVerifyInfo();
            if (kycinfo != null) {
                userInfoBuilder.setVerifyStatus(kycinfo.getVerifyStatus());
                userInfoBuilder.setFirstName(kycinfo.getFirstName());
                userInfoBuilder.setSecondName(kycinfo.getSecondName());
            }
            userInfos.add(userInfoBuilder.build());
        });
        replyBulider.addAllUserInfo(userInfos);
        return replyBulider.build();
    }

    public void unfreezeUsers(Long orgId, Long userId, Integer type) {
        FrozenUserRecord frozenUserRecord
                = frozenUserRecordMapper.getByUserIdAndFrozenType(orgId, userId, type);
        if (frozenUserRecord != null && frozenUserRecord.getId() != null) {
            //若为杠杆黑名单导致的禁止出金，解除时先判断是否还位于黑名单中
            if (frozenUserRecord.getFrozenReason() == FrozenReason.MARGIN_BLACK_LIST.reason() && !marginService.checkAccountNotInMarginBlack(orgId, userId)) {
                // 存在于黑名单中，禁止解冻
                throw new BrokerException(BrokerErrorCode.IN_MARGIN_RISK_BLACK);
            }
            frozenUserRecordMapper.unfreezeUsers(frozenUserRecord.getId());
        }

        if (type != null && type.equals(1)) {
            redisTemplate.delete(String.format(BrokerServerConstants.LOGIN_ERROR_USER_ID_COUNT, orgId, userId));
        }
    }

    public List<FrozenUserRecord> queryFrozenUserRecords(Long orgId, Long userId) {
        return frozenUserRecordMapper.listByUser(orgId, userId, new Date().getTime());
    }

    public void checkUserKycLimit(long userId, String limitKey) {
        String key = String.format(limitKey, userId);
        if (limitKey.equals(USER_KYC_ONE_HOUR_LIMIT)) {
            redisTemplate.opsForValue().setIfAbsent(key, "0", 1, TimeUnit.HOURS);
            long limitNum = redisTemplate.opsForValue().increment(key, 1);
            log.info("Check user kyc limit userId {} num {} limitKey {}", userId, limitNum, limitKey);
            if (limitNum > 3) {
                throw new BrokerException(BrokerErrorCode.REPEATED_SUBMIT_REQUEST);
            }
        }

        if (limitKey.equals(USER_KYC_ONE_DAY_LIMIT)) {
            redisTemplate.opsForValue().setIfAbsent(key, "0", 1, TimeUnit.DAYS);
            long limitNum = redisTemplate.opsForValue().increment(key, 1);
            log.info("Check user kyc limit userId {} num {} limitKey {}", userId, limitNum, limitKey);
            if (limitNum > 10) {
                throw new BrokerException(BrokerErrorCode.REPEATED_SUBMIT_REQUEST);
            }
        }
    }

    public BalanceProof getUserBalanceProof(Header header, String tokenId) {
        BalanceProof balanceProof = balanceProofMapper.selectOne(BalanceProof.builder()
                .orgId(header.getOrgId())
                .userId(header.getUserId())
                .tokenId(tokenId)
                .build());
        if (balanceProof == null) {
            throw new BrokerException(BrokerErrorCode.DB_RECORD_NOT_EXISTS);
        }
        return balanceProof;
    }

    public Long updateBalanceProof(Long orgId, Long userId, String tokenId,
                                   String nonce, String amount, String proofJson, Long createdTime) {
        BalanceProof balanceProof = balanceProofMapper.selectOne(BalanceProof.builder()
                .orgId(orgId)
                .userId(userId)
                .tokenId(tokenId)
                .build());
        if (balanceProof == null) {
            balanceProof = balanceProof.toBuilder()
                    .nonce(nonce)
                    .amount(new BigDecimal(amount))
                    .proofJson(proofJson)
                    .created(createdTime)
                    .build();
            balanceProofMapper.insertSelective(balanceProof);
        } else {
            balanceProof = balanceProof.toBuilder()
                    .nonce(nonce)
                    .amount(new BigDecimal(amount))
                    .proofJson(proofJson)
                    .created(createdTime)
                    .build();
            balanceProofMapper.updateByPrimaryKey(balanceProof);
        }
        return balanceProof.getId();
    }

    /**
     * neither mobile nor email exists, only insert user record into tb_user
     *
     * @param header
     * @param isVirtual is_virtual is 1 or 0, if user.is_virtual is 1, user can withdraw, and if
     *                  user.is+virtual is 0, user cannot withdraw
     * @return
     */
    public RegisterResponse thirdPartyUserRegister(Header header, boolean isVirtual) {
        User user;
        Long mainAccountId;
        String loginToken;
//        Long userId = sequenceGenerator.getLong();
        Long userId = 0L;
        try {
            SimpleCreateAccountRequest request = SimpleCreateAccountRequest.newBuilder()
                    .setBaseRequest(BaseReqUtil.getBaseRequest(header.getOrgId()))
                    .setOrgId(header.getOrgId())
                    .setUserId(userId.toString())
                    .build();
            SimpleCreateAccountReply reply = grpcAccountService.createAccount(request);
            if (reply.getErrCode() != ErrorCode.SUCCESS_VALUE) {
                log.error("register invoke GrpcAccountService.createAccount error:{}", reply.getErrCode());
                throw new BrokerException(BrokerErrorCode.SIGN_UP_ERROR);
            }

            Long bhUserId = reply.getBhUserId();
            userId = bhUserId;

            SecurityRegisterRequest registerRequest = SecurityRegisterRequest.newBuilder()
                    .setHeader(header)
                    .setUserId(userId)
                    .setPassword("")
                    .build();
            SecurityRegisterResponse securityRegisterResponse = grpcSecurityService.register(registerRequest);
            loginToken = securityRegisterResponse.getToken();

            mainAccountId = reply.getAccountId();
            Long optionAccountId = reply.getOptionAccountId();
            Long futuresAccountId = reply.getFuturesAccountId();

            Long timestamp = System.currentTimeMillis();
            log.info("create account success. userId{} mainAccountId:{}, optionAccountId:{} futuresAccountId:{}",
                    userId, mainAccountId, optionAccountId, futuresAccountId);
            createAccount(header, mainAccountId, AccountType.MAIN.value(), userId, timestamp);
            createAccount(header, optionAccountId, AccountType.OPTION.value(), userId, timestamp);
            createAccount(header, futuresAccountId, AccountType.FUTURES.value(), userId, timestamp);

            Long secondLevelInviteUserId = 0L;
            user = User.builder()
                    .orgId(header.getOrgId())
                    .userId(userId)
                    .bhUserId(bhUserId)
                    .nationalCode("")
                    .mobile("")
                    .concatMobile("")
                    .email("")
                    .emailAlias("")
                    .registerType(0)
                    .ip(header.getRemoteIp())
                    .inputInviteCode("")
                    .inviteUserId(0L)
                    .secondLevelInviteUserId(secondLevelInviteUserId)
                    .bindGA(0)
                    .bindTradePwd(0)
                    .userType(UserType.GENERAL_USER.value())
                    .channel(header.getChannel())
                    .source(header.getSource())
                    .platform(header.getPlatform().name())
                    .userAgent(header.getUserAgent())
                    .language(header.getLanguage())
                    .appBaseHeader(GrpcHeaderUtil.getAppBaseInfo(header))
                    .created(timestamp)
                    .updated(timestamp)
                    .isVip(0)
                    .isVirtual(isVirtual ? 1 : 0)
                    .bindPassword(1)
                    .build();
            userMapper.insertRecord(user);
        } catch (Exception e) {
            throw e;
        }
        return RegisterResponse.newBuilder()
                .setToken(loginToken)
                .setUser(getUserInfo(user))
                .build();
    }

    public UserVerify queryUserVerifyInfoByAccountId(Long accountId) {
        Account account = this.accountMapper.getAccountByAccountId(accountId);
        if (account == null) {
            return null;
        }
        UserVerify userVerify = UserVerify.decrypt(userVerifyMapper.getByUserId(account.getUserId()));
        if (userVerify == null) {
            return null;
        }
        return userVerify;
    }

    public UserSettings getUserSettings(Header header) {
        UserSettings settings = userSettingsMapper.getByUserId(header.getOrgId(), header.getUserId());
        if (settings == null) {
            try {
                settings = UserSettings.builder()
                        .orgId(header.getOrgId())
                        .userId(header.getUserId())
                        .created(System.currentTimeMillis())
                        .updated(System.currentTimeMillis())
                        .commonConfig("")
                        .build();
                userSettingsMapper.insert(settings);
            } catch (Exception e) {
                log.warn("settings create failed " + e.getMessage(), e);
            }
        }
        return settings;
    }

    public int setUserSettings(Header header, String commonConfig) {
        //判断是否存在配置
        UserSettings settings = getUserSettings(header);
        if (settings == null) {
            try {
                settings = UserSettings.builder()
                        .orgId(header.getOrgId())
                        .userId(header.getUserId())
                        .created(System.currentTimeMillis())
                        .updated(System.currentTimeMillis())
                        .commonConfig(commonConfig)
                        .build();
                return userSettingsMapper.insert(settings);
            } catch (Exception e) {
                return 0;
            }
        }
        return userSettingsMapper.updateCommonConfig(header.getOrgId(), header.getUserId(), commonConfig, System.currentTimeMillis());
    }


    public void exploreApply(ExploreApplyRequest request) {
        //判断是否存在配置
        String htmlContent = "<div style=\"border: 1px solid #e5e5e5; padding: 15px 20px; max-width: 600px; margin: auto;\">\n" +
                "<p>Service, 你好</p>\n" +
                "<p>收到一个新的演示申请 </p>\n" +
                "<p>&nbsp;</p>\n" +
                "<p>公司: ${company}</p>\n" +
                "<p>姓名: ${name}</p>\n" +
                "<p>手机: ${phone}</p>\n" +
                "<p>邮箱: ${email}&nbsp;</p>\n" +
                "<p>社交媒体: ${contact}&nbsp;</p>\n" +
                "<p>内容: </p>\n" +
                "<p style=\"padding: 12px; border-left: 6px solid #eee; font-style: italic;\">\n" +
                "${content}\n" +
                "</p>\n" +
                "<p>&nbsp;</p>\n" +
                "<p>&nbsp;<br />Best wishes</p>\n" +
                "</div>";

        htmlContent = htmlContent.replace("${company}", request.getCompany())
                .replace("${name}", request.getName())
                .replace("${phone}", request.getPhone())
                .replace("${email}", request.getEmail())
                .replace("${content}", request.getContent())
                .replace("${contact}", request.getContact());

        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("content", htmlContent);

        noticeTemplateService.sendSubjectEmailNotice(request.getHeader(), 0L, "Service@bluehelix.com", "演示申请", jsonObject);

    }

    public void batchUserUnlockAirDrop(Long orgId, Integer unlockType, String tokenId, String userIds, String mark) {
        CompletableFuture.runAsync(() -> {
            try {
                Example example = new Example(LockBalanceLog.class);
                Example.Criteria criteria = example.createCriteria();
                criteria.andEqualTo("brokerId", orgId);
                //空投锁仓
                criteria.andEqualTo("type", 4);
                criteria.andEqualTo("tokenId", tokenId);
                criteria.andEqualTo("status", 1);
                if (unlockType != 1) {
                    List<Long> uids = Arrays.stream(userIds.split(",")).map(Long::parseLong).collect(Collectors.toList());
                    criteria.andIn("userId", uids);
                }
                List<LockBalanceLog> lockInfos = lockBalanceLogMapper.selectByExample(example);
                for (LockBalanceLog lock : lockInfos) {
                    try {
                        if (lock.getLastAmount().compareTo(BigDecimal.ZERO) > 0) {
                            balanceService.userUnLockBalanceForAdmin(lock.getId(), lock.getBrokerId(), lock.getUserId(), lock.getLastAmount().stripTrailingZeros().toPlainString(), mark, 0L);
                        }
                    } catch (Exception e) {
                        log.warn("batchUserUnlockAirDrop foreach unlock  {} amount :{} error {} ", lock.getId(), lock.getLastAmount(), e.getMessage(), e);
                    }
                }
            } catch (Exception e) {
                log.warn("batchUserUnlock {}", e.getMessage(), e);
            }
        }, taskExecutor);
    }
}


