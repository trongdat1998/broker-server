package io.bhex.broker.server.grpc.server.service;

import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableTable;
import com.google.common.hash.Hashing;
import com.google.gson.JsonElement;
import io.bhex.base.account.BusinessSubject;
import io.bhex.base.account.SyncTransferRequest;
import io.bhex.base.account.SyncTransferResponse;
import io.bhex.base.idgen.api.ISequenceGenerator;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.common.util.CryptoUtil;
import io.bhex.broker.common.util.JsonUtil;
import io.bhex.broker.grpc.common.Header;
import io.bhex.broker.server.grpc.client.service.GrpcBatchTransferService;
import io.bhex.broker.server.model.*;
import io.bhex.broker.server.primary.mapper.*;
import io.bhex.broker.server.util.BaseReqUtil;
import io.bhex.broker.server.util.GrpcHeaderUtil;
import io.bhex.broker.server.util.SignUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.math.BigDecimal;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
@Service
public class OAuthService {

    private static final long OAUTH_CODE_EFFECTIVE_TIME = 10 * 60 * 1000L;
    private static final long ACCESS_TOKEN_EFFECTIVE_TIME = 90 * 24 * 60 * 60 * 1000L;

    private static final int TRANSFER_PERMISSION_OPENED = 1;

    private static ImmutableTable<String, String, ThirdPartyAppOpenFunction> OPEN_FUNCTION_TABLE = ImmutableTable.of();

    @Resource
    private ThirdPartyAppMapper thirdPartyAppMapper;

    @Resource
    private ThirdPartyAppOpenFunctionMapper thirdPartyAppOpenFunctionMapper;

    @Resource
    private OAuthAuthorizeRecordMapper oAuthAuthorizeRecordMapper;

    @Resource
    private ThirdPartyAppUserAuthorizeMapper thirdPartyAppUserAuthorizeMapper;

    @Resource
    private ThirdPartyAppTransferMapper thirdPartyAppTransferMapper;

    @Resource
    private AccountService accountService;

    @Resource
    private GrpcBatchTransferService grpcBatchTransferService;

    @Resource
    private ISequenceGenerator sequenceGenerator;

    @PostConstruct
    @Scheduled(cron = "0 0/1 * * * ?")
    public void initOpenFunctions() {
        List<ThirdPartyAppOpenFunction> functions = thirdPartyAppOpenFunctionMapper.selectAll();
        HashBasedTable<String, String, ThirdPartyAppOpenFunction> tmpTable = HashBasedTable.create();
        for (ThirdPartyAppOpenFunction function : functions) {
            tmpTable.put(function.getFunction(), function.getLanguage(), function);
        }
        OPEN_FUNCTION_TABLE = ImmutableTable.copyOf(tmpTable);
    }

    /**
     * 查询券商签约的第三方APP
     *
     * @param orgId 券商id
     * @return ThirdPartyAppList
     */
    public List<ThirdPartyApp> queryThirdPartyApp(Long orgId) {
        ThirdPartyApp queryObj = ThirdPartyApp.builder().orgId(orgId).build();
        return thirdPartyAppMapper.select(queryObj);
    }

    /**
     * 保存ThirdPartyApp,
     */
    public ThirdPartyApp saveThirdPartyApp(Long orgId, Integer appType, String appId, String appName, String callback, String functions) throws Exception {
        if (Stream.of(appName, callback, functions).anyMatch(Strings::isNullOrEmpty)) {
            throw new BrokerException(BrokerErrorCode.PARAM_INVALID);
        }
        String clientId = CryptoUtil.getRandomCode(64);
        String clientSecret = CryptoUtil.getRandomCode(64);
        String snow = CryptoUtil.getRandomCode(16);
        Long currentTimestamp = System.currentTimeMillis();
        ThirdPartyApp thirdPartyApp = ThirdPartyApp.builder()
                .orgId(orgId)
                .appType(appType)
                .appId(appId)
                .appName(appName)
                .clientId(clientId)
                .clientSecret(clientSecret)
                .snow(snow)
                .status(ThirdPartyApp.ENABLE_STATUS)
                .callback(callback)
                .randomCode(CryptoUtil.getRandomCode(6))
                .functions(functions)
                .created(currentTimestamp)
                .updated(currentTimestamp)
                .build();
        thirdPartyApp.setClientSecret(SignUtils.encryptDataWithAES(getKey(thirdPartyApp), clientSecret));
        thirdPartyAppMapper.insertSelective(thirdPartyApp);
        thirdPartyApp.setClientSecret(clientSecret);
        return thirdPartyApp;
    }

    public void updateThirdPartyAppStatus(Long orgId, Long thirdPartyAppId, int status) {
        ThirdPartyApp updateObj = ThirdPartyApp.builder()
                .id(thirdPartyAppId)
                .status(status)
                .updated(System.currentTimeMillis())
                .build();
        thirdPartyAppMapper.updateByPrimaryKeySelective(updateObj);
        if (status == ThirdPartyApp.DISABLE_STATUS) {
            thirdPartyAppUserAuthorizeMapper.disableAppTokenStatus(orgId, thirdPartyAppId, ThirdPartyAppUserAuthorize.ACCESS_TOKEN_DISABLE_BY_APP_STATUS_CHANGE);
        }
    }

    private String getKey(ThirdPartyApp thirdPartyApp) {
        return thirdPartyApp.getOrgId() + thirdPartyApp.getClientId() + thirdPartyApp.getSnow();
    }

    /**
     * oauth step1: get oauth authorize code
     * request user authorization
     *
     * @param header      Global param
     * @param clientId    == appId
     * @param redirectUrl 授权结束跳转url
     * @param state       == 原样返回第三方
     * @param scope       == 请求授权列表, 逗号分隔
     * @return 返回登录页面who请求授权，以及请求授权的功能列表
     */
    public OAuthAuthorizeResponse authorize(Header header, String clientId, String redirectUrl, String state, String scope) throws Exception {
        ThirdPartyApp thirdPartyApp = getThirdPartyApp(header.getOrgId(), clientId);
        if (!URI.create(redirectUrl).getHost().equals(thirdPartyApp.getCallback())) {
            throw new BrokerException(BrokerErrorCode.OAUTH_REDIRECT_URL_ILLEGAL);
        }
        List<String> functionList = Arrays.stream(scope.split(",")).map(String::trim).distinct().collect(Collectors.toList());
//        Example example = Example.builder(ThirdPartyAppOpenFunction.class).build();
//        Example.Criteria criteria = example.createCriteria();
//        criteria.andIn("function",
//                Arrays.stream(scope.split(",")).map(String::trim).collect(Collectors.toList()));
//        List<ThirdPartyAppOpenFunction> functions = thirdPartyAppOpenFunctionMapper.selectByExample(example);
//        if (functions.size() != functionList.size()) {
//            throw new BrokerException(BrokerErrorCode.OAUTH_GET_APP_OPEN_FUNCTION_ERROR);
//        }
//        functions.sort(Comparator.comparing(ThirdPartyAppOpenFunction::getSort));
//        functions = functions.stream().peek(function -> {
//            function.setShowName(getMultiLanguageValue(function.getShowName(), header.getLanguage()));
//            function.setShowDesc(getMultiLanguageValue(function.getShowDesc(), header.getLanguage()));
//        }).collect(Collectors.toList());
        List<ThirdPartyAppOpenFunction> functions = functionList.stream()
                .map(function -> OPEN_FUNCTION_TABLE.get(function, header.getLanguage()))
                .collect(Collectors.toList());
        String requestId = CryptoUtil.getRandomCode(64);
        long currentTimestamp = System.currentTimeMillis();
        OAuthAuthorizeRecord record = OAuthAuthorizeRecord.builder()
                .orgId(header.getOrgId())
                .thirdPartyAppId(thirdPartyApp.getId())
                .requestId(requestId)
                .clientId(clientId)
                .redirectUrl(redirectUrl)
                .state(state)
                .scope(scope)
                .status(OAuthAuthorizeRecord.UNUSED_STATUS) // oauthCode暂未兑换accessToken
                .platform(header.getPlatform().name())
                .userAgent(header.getUserAgent())
                .language(header.getLanguage())
                .appBaseHeader(GrpcHeaderUtil.getAppBaseInfo(header))
                .created(currentTimestamp)
                .updated(currentTimestamp)
                .build();
        oAuthAuthorizeRecordMapper.insertSelective(record);
        return OAuthAuthorizeResponse.builder()
                .requestId(requestId)
                .appName(getMultiLanguageValue(thirdPartyApp.getAppName(), header.getLanguage()))
                .functions(functions)
                .build();
    }

    /**
     * oauth step1 response
     * request user authorization
     * <p>
     * 为了防止对同一requestId重复请求oauthCode，进行加锁处理。
     *
     * @return 返回跟用户关联的一个code码，保证有效期
     */
    @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.READ_COMMITTED)
    public GetOAuthCodeResponse getAuthorizeCode(Header header, Long userId, String authorizeRequestId) throws Exception {
        OAuthAuthorizeRecord authorizeRecord = oAuthAuthorizeRecordMapper.getByOAuthRequestIdForUpdate(header.getOrgId(), authorizeRequestId);
        if (authorizeRecord == null) {
            throw new BrokerException(BrokerErrorCode.PARAM_INVALID);
        }
        if (authorizeRecord.getStatus() == OAuthAuthorizeRecord.USED_STATUS) {
            ThirdPartyAppUserAuthorize queryObj = ThirdPartyAppUserAuthorize.builder()
                    .orgId(header.getOrgId())
                    .authorizeId(authorizeRecord.getId())
                    .build();
            ThirdPartyAppUserAuthorize userAuthorize = thirdPartyAppUserAuthorizeMapper.selectOne(queryObj);
            if (userAuthorize == null) {
                log.error("userId:{} oauthCode requestId:{} has used, but cannot find user authorize record", userId, authorizeRequestId);
                throw new BrokerException(BrokerErrorCode.OAUTH_GET_CODE_ERROR);
            }
            log.warn("userId:{} request getOAuthCode with requestId:{} repeated", userId, authorizeRequestId);
            return GetOAuthCodeResponse.builder()
                    .oauthCode(userAuthorize.getOauthCode())
                    .expired(userAuthorize.getCodeExpired())
                    .state(authorizeRecord.getState())
                    .redirectUrl(authorizeRecord.getRedirectUrl())
                    .build();
        }
        ThirdPartyApp thirdPartyApp = getThirdPartyApp(header.getOrgId(), authorizeRecord.getClientId()); // 校验ThirdPartyApp.status
        String oauthCode = CryptoUtil.getRandomCode(32);
        String accessToken = CryptoUtil.getRandomCode(64);
        String openId = Hashing.md5().hashString(authorizeRecord.getThirdPartyAppId() + thirdPartyApp.getRandomCode() + userId, Charsets.UTF_8).toString();
        long currentTimestamp = System.currentTimeMillis();
        ThirdPartyAppUserAuthorize userAuthorize = ThirdPartyAppUserAuthorize.builder()
                .orgId(header.getOrgId())
                .userId(userId)
                .openId(openId)
                .thirdPartyAppId(authorizeRecord.getThirdPartyAppId())
                .authorizeId(authorizeRecord.getId())
                .requestId(authorizeRequestId)
                .authorizeFunctions(authorizeRecord.getScope())
                .status(ThirdPartyAppUserAuthorize.STATUS_NORMAL)
                .oauthCode(oauthCode)
                .codeExpired(currentTimestamp + OAUTH_CODE_EFFECTIVE_TIME)
                .codeStatus(ThirdPartyAppUserAuthorize.CODE_UNUSED_STATUS)
                .accessToken(accessToken)
                .tokenExpired(currentTimestamp + ACCESS_TOKEN_EFFECTIVE_TIME)
                .tokenStatus(ThirdPartyAppUserAuthorize.ACCESS_TOKEN_NOT_ACQUIRED)
                .platform(header.getPlatform().name())
                .userAgent(header.getUserAgent())
                .language(header.getLanguage())
                .appBaseHeader(GrpcHeaderUtil.getAppBaseInfo(header))
                .created(currentTimestamp)
                .updated(currentTimestamp)
                .build();
        thirdPartyAppUserAuthorizeMapper.insertSelective(userAuthorize);
        OAuthAuthorizeRecord updateObj = OAuthAuthorizeRecord.builder()
                .id(authorizeRecord.getId())
                .status(OAuthAuthorizeRecord.USED_STATUS)
                .updated(currentTimestamp)
                .build();
        oAuthAuthorizeRecordMapper.updateByPrimaryKeySelective(updateObj);
        return GetOAuthCodeResponse.builder()
                .oauthCode(oauthCode)
                .expired(userAuthorize.getCodeExpired())
                .state(authorizeRecord.getState())
                .redirectUrl(authorizeRecord.getRedirectUrl())
                .build();
    }

    /**
     * oauth step2: get oauth access token
     *
     * @param header    Global header param
     * @param oauthCode code in step1 response
     * @return
     */
    @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.READ_COMMITTED)
    public GetOAuthAccessTokenResponse getAccessToken(Header header, String clientId, String clientSecret, String oauthCode, String redirectUrl) throws Exception {
        ThirdPartyApp thirdPartyApp = getThirdPartyApp(header.getOrgId(), clientId); // 校验ThirdPartyApp.status，这里不用加锁了。
        // 校验一下 clientSecret是否正确
        if (!Hashing.md5().hashString(thirdPartyApp.getClientSecret(), Charsets.UTF_8).toString().equalsIgnoreCase(clientSecret)) {
            log.error("clientId:{} getAccessToken terminated by error clientSecret", clientId);
            throw new BrokerException(BrokerErrorCode.OAUTH_PARAM_INVALID);
        }
        if (!Strings.isNullOrEmpty(redirectUrl) && !URI.create(redirectUrl).getHost().equalsIgnoreCase(thirdPartyApp.getCallback())) {
            throw new BrokerException(BrokerErrorCode.OAUTH_REDIRECT_URL_ILLEGAL);
        }
        ThirdPartyAppUserAuthorize userAuthorize = thirdPartyAppUserAuthorizeMapper.getByOrgIdAndOAuthCodeForUpdate(header.getOrgId(), oauthCode);
        if (userAuthorize == null) {
            throw new BrokerException(BrokerErrorCode.OAUTH_CODE_ILLEGAL);
        }
        if (userAuthorize.getCodeStatus() == ThirdPartyAppUserAuthorize.CODE_USED_STATUS) {
            throw new BrokerException(BrokerErrorCode.OAUTH_CODE_REUSE);
        }
        if (System.currentTimeMillis() > userAuthorize.getCodeExpired()) {
            throw new BrokerException(BrokerErrorCode.OAUTH_CODE_EXPIRED);
        }
        long currentTimestamp = System.currentTimeMillis();
        ThirdPartyAppUserAuthorize updateObj = ThirdPartyAppUserAuthorize.builder()
                .id(userAuthorize.getId())
                .codeStatus(ThirdPartyAppUserAuthorize.CODE_USED_STATUS)
                .tokenStatus(ThirdPartyAppUserAuthorize.ACCESS_TOKEN_ACQUIRED)
                .updated(currentTimestamp)
                .build();
        thirdPartyAppUserAuthorizeMapper.updateByPrimaryKeySelective(updateObj);
        return GetOAuthAccessTokenResponse.builder()
                .accessToken(userAuthorize.getAccessToken())
                .expired(userAuthorize.getTokenExpired())
                .openId(userAuthorize.getOpenId())
                .userId(userAuthorize.getUserId())
                .build();
    }

    public void invalidUserOAuthAccessToken(Long orgId, Long userId, int tokenStatus) {
        thirdPartyAppUserAuthorizeMapper.disableUserTokenStatus(orgId, userId, tokenStatus);
    }

    public void oauthTransfer(Header header, String clientId, String clientOrderId, Long userId, String tokenId, String amount, Boolean fromLock, Boolean toLock, Integer businessSubject) throws Exception {
        ThirdPartyApp thirdPartyApp = getThirdPartyApp(header.getOrgId(), clientId); // 校验ThirdPartyApp.status
        if (thirdPartyApp.getTransferPermission() != TRANSFER_PERMISSION_OPENED) {
            throw new BrokerException(BrokerErrorCode.OPERATION_NOT_SUPPORT_IN_VERSION);
        }
        ThirdPartyAppTransfer transfer = thirdPartyAppTransferMapper.getByAppIdAndClientOrderId(header.getOrgId(), thirdPartyApp.getId(), clientOrderId);
        if (transfer == null) {
            transfer = ThirdPartyAppTransfer.builder()
                    .orgId(header.getOrgId())
                    .thirdPartyAppId(thirdPartyApp.getId())
                    .clientOrderId(clientOrderId)
                    .transferId(sequenceGenerator.getLong())
                    .sourceUserId(thirdPartyApp.getBindUserId())
                    .sourceAccountId(accountService.getAccountId(header.getOrgId(), thirdPartyApp.getBindUserId()))
                    .targetUserId(userId)
                    .targetAccountId(accountService.getAccountId(header.getOrgId(), userId))
                    .tokenId(tokenId)
                    .amount(new BigDecimal(amount))
                    .fromSourceLock(fromLock ? 1 : 0)
                    .toTargetLock(toLock ? 1 : 0)
                    .businessSubject(businessSubject)
                    .status(0)
                    .response("")
                    .build();
            thirdPartyAppTransferMapper.insertSelective(transfer);
        }
        if (transfer.getStatus() == 200) {
            return;
        }
        SyncTransferRequest transferRequest = SyncTransferRequest.newBuilder()
                .setBaseRequest(BaseReqUtil.getBaseRequest(header.getOrgId()))
                .setClientTransferId(transfer.getTransferId())
                .setSourceOrgId(header.getOrgId())
                .setSourceFlowSubject(BusinessSubject.forNumber(transfer.getBusinessSubject()))
                .setSourceAccountId(transfer.getSourceAccountId())
                .setTokenId(transfer.getTokenId())
                .setAmount(transfer.getAmount().stripTrailingZeros().toPlainString())
                .setTargetOrgId(transfer.getOrgId())
                .setTargetAccountId(transfer.getTargetAccountId())
                .setTargetFlowSubject(BusinessSubject.forNumber(transfer.getBusinessSubject()))
                .setFromPosition(transfer.getFromSourceLock() == 1)
                .setToPosition(transfer.getToTargetLock() == 1)
                .build();
        SyncTransferResponse response = grpcBatchTransferService.syncTransfer(transferRequest);
        ThirdPartyAppTransfer updateObj = ThirdPartyAppTransfer.builder()
                .id(transfer.getId())
                .status(response.getCodeValue())
                .response(JsonUtil.defaultGson().toJson(response))
                .updated(System.currentTimeMillis())
                .build();
        thirdPartyAppTransferMapper.updateByPrimaryKeySelective(updateObj);
        if (response.getCode() == SyncTransferResponse.ResponseCode.SUCCESS) {
            return;
        }
        handleTransferResponse(response);
    }

    private void handleTransferResponse(SyncTransferResponse response) {
        if (response.getCode() == SyncTransferResponse.ResponseCode.BALANCE_INSUFFICIENT
                || response.getCode() == SyncTransferResponse.ResponseCode.LOCK_BALANCE_INSUFFICIENT) {
            throw new BrokerException(BrokerErrorCode.TRANSFER_INSUFFICIENT_BALANCE);
        } else if (response.getCode() == SyncTransferResponse.ResponseCode.FROM_ACCOUNT_NOT_EXIST
                || response.getCode() == SyncTransferResponse.ResponseCode.TO_ACCOUNT_NOT_EXIST
                || response.getCode() == SyncTransferResponse.ResponseCode.NO_POSITION
                || response.getCode() == SyncTransferResponse.ResponseCode.REQUEST_PARA_ERROR
                || response.getCode() == SyncTransferResponse.ResponseCode.ACCOUNT_NOT_EXIST
                || response.getCode() == SyncTransferResponse.ResponseCode.ACCOUNT_NOT_SAME_SHARD) {
            throw new BrokerException(BrokerErrorCode.PARAM_INVALID);
        } else {
            throw new BrokerException(BrokerErrorCode.MAPPING_TRANSFER_FAILED);
        }
    }

    /**
     * check oauth access_token
     * openId没什么用
     *
     * @return orgId and userId
     * @throws Exception
     */
    public CheckOAuthResponse checkOAuthRequest(Header header, String clientId, String accessToken, String openId, String redirectUrl, String requiredPermission) throws Exception {
        ThirdPartyApp thirdPartyApp = getThirdPartyApp(header.getOrgId(), clientId); // 校验ThirdPartyApp.status
        if (!Strings.isNullOrEmpty(redirectUrl) && !URI.create(redirectUrl).getHost().equals(thirdPartyApp.getCallback())) {
            throw new BrokerException(BrokerErrorCode.OAUTH_REDIRECT_URL_ILLEGAL);
        }
        ThirdPartyAppUserAuthorize userAuthorize = thirdPartyAppUserAuthorizeMapper.getByOrgIdAndAccessToken(header.getOrgId(), accessToken);
        if (userAuthorize == null) {
            throw new BrokerException(BrokerErrorCode.OAUTH_ACCESS_TOKEN_ILLEGAL);
        }
        if (System.currentTimeMillis() > userAuthorize.getTokenExpired()) {
            throw new BrokerException(BrokerErrorCode.OAUTH_ACCESS_TOKEN_EXPIRED);
        }
        if (userAuthorize.getTokenStatus() == ThirdPartyAppUserAuthorize.ACCESS_TOKEN_NOT_ACQUIRED) {
            throw new BrokerException(BrokerErrorCode.OAUTH_ACCESS_TOKEN_ILLEGAL);
        }
        if (userAuthorize.getTokenStatus() == ThirdPartyAppUserAuthorize.ACCESS_TOKEN_DISABLE_BY_USER) {
            throw new BrokerException(BrokerErrorCode.OAUTH_ACCESS_TOKEN_DISABLED_BY_USER);
        }
        if (userAuthorize.getTokenStatus() == ThirdPartyAppUserAuthorize.ACCESS_TOKEN_DISABLE_BY_CHANGE_PWD) {
            throw new BrokerException(BrokerErrorCode.OAUTH_ACCESS_TOKEN_DISABLED_BY_CHANGE_PWD);
        }
        if (userAuthorize.getTokenStatus() == ThirdPartyAppUserAuthorize.ACCESS_TOKEN_DISABLE_BY_USER_STATUS_CHANGE) {
            throw new BrokerException(BrokerErrorCode.OAUTH_ACCESS_TOKEN_DISABLED_BY_USER_STATUS_CHANGE);
        }
        if (userAuthorize.getTokenStatus() == ThirdPartyAppUserAuthorize.ACCESS_TOKEN_DISABLE_BY_APP_STATUS_CHANGE) {
            throw new BrokerException(BrokerErrorCode.OAUTH_ACCESS_TOKEN_DISABLED_BY_APP_STATUS_CHANGE);
        }
        if (Arrays.stream(userAuthorize.getAuthorizeFunctions().split(",")).noneMatch(requiredPermission::equals)) {
            throw new BrokerException(BrokerErrorCode.OAUTH_REQUEST_HAS_NO_PERMISSION);
        }
        return CheckOAuthResponse.builder()
                .orgId(header.getOrgId()).userId(userAuthorize.getUserId()).build();
    }

    private ThirdPartyApp getThirdPartyApp(Long orgId, String clientId) throws Exception {
        ThirdPartyApp queryObj = ThirdPartyApp.builder().orgId(orgId).clientId(clientId).build();
        ThirdPartyApp thirdPartyApp = thirdPartyAppMapper.selectOne(queryObj);
        if (thirdPartyApp != null) {
            thirdPartyApp.setClientSecret(SignUtils.decryptDataWithAES(getKey(thirdPartyApp), thirdPartyApp.getClientSecret()));
        }
        if (thirdPartyApp == null) {
            throw new BrokerException(BrokerErrorCode.OAUTH_APP_NOT_FOUNT);
        }
        if (thirdPartyApp.getStatus() != ThirdPartyApp.ENABLE_STATUS) {
            throw new BrokerException(BrokerErrorCode.OAUTH_APP_OFFLINE);
        }
        return thirdPartyApp;
    }

    private String getMultiLanguageValue(String value, String language) {
        JsonElement element = JsonUtil.defaultJsonParser().parse(value);
        if (element.isJsonPrimitive()) {
            return value;
        }
        String jsonValue = JsonUtil.tryGetString(element, "." + language);
        return Strings.isNullOrEmpty(jsonValue) ? JsonUtil.getString(element, "." + Locale.US.toString(), "") : jsonValue;
    }

}
