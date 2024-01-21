package io.bhex.broker.server.grpc.server.service.kyc.tencent;

import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import com.google.common.hash.Hashing;
import io.bhex.base.idgen.api.ISequenceGenerator;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.common.objectstorage.ObjectStorage;
import io.bhex.broker.common.util.JsonUtil;
import io.bhex.broker.grpc.common.Header;
import io.bhex.broker.grpc.user.kyc.AppType;
import io.bhex.broker.server.domain.kyc.tencent.*;
import io.bhex.broker.server.grpc.server.service.FileStorageService;
import io.bhex.broker.server.model.BrokerKycConfig;
import io.bhex.broker.server.model.UserTencentKycLog;
import io.bhex.broker.server.primary.mapper.UserTencentKycLogMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@Slf4j
@Service
public class WebankKycService {

    @Resource
    private TencentOAuthService tencentOAuthService;

    @Resource
    private ObjectStorage awsObjectStorage;

    @Resource
    private FileStorageService fileStorageService;

    @Resource
    private UserTencentKycLogMapper userTencentKycLogMapper;

    @Resource
    private ISequenceGenerator sequenceGenerator;

    @Resource
    private IdascSAO idascSAO;

    private String computeSign(List<String> values, String ticket) {
        if (values == null) {
            throw new NullPointerException("values is null");
        }
        values.removeAll(Collections.singleton(null));// remove null
        values.add(ticket);
        java.util.Collections.sort(values);
        StringBuilder sb = new StringBuilder();
        for (String s : values) {
            sb.append(s);
        }
        String sign = Hashing.sha1().hashString(sb, Charsets.UTF_8).toString().toUpperCase();
        log.debug("compute sign, values: {}, sign: {}", values, sign);
        return sign;
    }

    public NonceTicket getNonceTicket(Long userId, Long orgId) {
        String ticket = tencentOAuthService.getNonceTicket(userId, orgId);
        String nonce = TencentOAuthService.createNonce();
        return new NonceTicket(userId, orgId, nonce, ticket);
    }

    public GetFaceIdResponse getFaceId(Long kycApplyId, Long userId, Long orgId, String idNo, String name, String photoFileKey, NonceTicket nonceTicket) {
        String sourcePhotoBase64;
        if (StringUtils.isEmpty(photoFileKey)) {
            sourcePhotoBase64 = "";
        } else {
            sourcePhotoBase64 = getAwsFileBase64(photoFileKey);
        }
        Long userTencentKycLogId = sequenceGenerator.getLong();

        GetFaceIdRequest request = new GetFaceIdRequest();
        request.setWebankAppId(tencentOAuthService.getBrokerKycConfig(orgId).getWebankAppId());
        request.setIdNo(idNo);
        request.setName(name);
        request.setOrderNo(String.valueOf(userTencentKycLogId)); // 使用生成的唯一ID作为订单号
        request.setSourcePhotoStr(sourcePhotoBase64);
        request.setSourcePhotoType("2");
        request.setUserId(generateRequestUserId(userId));
        request.setNonce(nonceTicket.getNonce());
        request.setVersion(tencentOAuthService.getVersion());
        List<String> signParams = new ArrayList<>(
                Arrays.asList(request.getWebankAppId(), request.getUserId(), request.getNonce(), request.getUserId()));
        String sign = computeSign(signParams, nonceTicket.getTicket());
        request.setSign(sign);

        UserTencentKycLog userTencentKycLog = prepareUserTencentKycLog(
                userTencentKycLogId, userId, orgId, kycApplyId,"getFaceId");
        try {
            userTencentKycLog.setReqParam(IdascSAO.toPrintJson(request));
            GetFaceIdResponse faceIdResponse = idascSAO.getFaceId(request);
            updateUserTencentKycLogFromResponse(userTencentKycLog, faceIdResponse);
            return faceIdResponse;
        } catch (Exception e) {
            updateUserTencentKycLogFromException(userTencentKycLog, e);

            throw e;
        } finally {
            userTencentKycLog.setUpdated(System.currentTimeMillis());
            userTencentKycLogMapper.insertSelective(userTencentKycLog);
        }
    }

    public IDCardOcrResponse idCardOcr(Long kycApplyId, Long userId, Long orgId, String idCardBase64) {
        Long userTencentKycLogId = sequenceGenerator.getLong();
        String signTicket  = tencentOAuthService.getSignTicket(orgId);

        IDCardOcrRequest request = new IDCardOcrRequest();
        request.setCardType("0"); // 0：人像面 1：国徽面
        request.setOrderNo(String.valueOf(userTencentKycLogId));
        request.setUserId(generateRequestUserId(userId));
        request.setVersion(tencentOAuthService.getVersion());
        request.setNonce(TencentOAuthService.createNonce());
        request.setWebankAppId(tencentOAuthService.getBrokerKycConfig(orgId).getWebankAppId());
        request.setIdcardStr(idCardBase64);
        List<String> signParams = new ArrayList<>(Arrays.asList(
                request.getWebankAppId(), request.getOrderNo(), request.getVersion(), request.getNonce()));
        String sign = computeSign(signParams, signTicket);
        request.setSign(sign);

        UserTencentKycLog userTencentKycLog = prepareUserTencentKycLog(
                userTencentKycLogId, userId, orgId, kycApplyId,"idCardOcr");
        try {
            userTencentKycLog.setReqParam(IdascSAO.toPrintJson(request));
            IDCardOcrResponse ocrResponse = idascSAO.idCardOcr(request);
            updateUserTencentKycLogFromResponse(userTencentKycLog, ocrResponse);
            return ocrResponse;
        } catch (Exception e) {
            updateUserTencentKycLogFromException(userTencentKycLog, e);

            throw e;
        } finally {
            userTencentKycLog.setUpdated(System.currentTimeMillis());
            userTencentKycLogMapper.insertSelective(userTencentKycLog);
        }
    }

    private String getAwsFileBase64(String fileKey) {
        byte[] fileBytes = awsObjectStorage.downloadObject(fileKey);
        return Base64.encodeBase64String(fileBytes);
    }

    public SDKLoginPrepareResponse sdkLoginPrepare(Header header, AppType appType, String orderNo, NonceTicket nonceTicket) {
        log.info("sdk login prepare start, userId: {}", header.getUserId());
        BrokerKycConfig config = tencentOAuthService.getBrokerKycConfig(header.getOrgId());
        SDKLoginPrepareResponse response = new SDKLoginPrepareResponse();
        response.setWebankAppId(config.getWebankAppId());
        response.setVersion(tencentOAuthService.getVersion());
        response.setNonce(nonceTicket.getNonce());
        response.setUserId(String.valueOf(header.getUserId()));
        List<String> signParams = new ArrayList<>(
                Arrays.asList(response.getWebankAppId(), response.getVersion(), response.getNonce(), response.getUserId()));
        String sign = computeSign(signParams, nonceTicket.getTicket());
        response.setSign(sign);
        response.setOrderNo(orderNo);
        response.setLicense(getWebankAppLicense(header, appType, config));
        log.info("sdk login prepare success, response: {}", response);
        return response;
    }

    private String getWebankAppLicense(Header header, AppType appType, BrokerKycConfig config) {
        String packageName = header.getAppBaseHeader() == null ? "" : header.getAppBaseHeader().getAppId();
        if (StringUtils.isEmpty(packageName)) {
            log.warn("getWebankAppLicense: can not get packageName from header: {}", header);
            return null;
        }

        switch (appType) {
            case ANDROID_APP:
                return config.getAndroidLicenseByPackageName(packageName);
            case IOS_APP:
                return config.getIosLicenseByPackageName(packageName);
            default:
                return null;
        }
    }

    public QueryResultResponse queryFaceCompareResult(Long userId, Long orgId, String orderNo, String getFile) {
        // 从之前getFaceId的请求中查询kycApplyId
        // 将getFaceId和queryFaceCompareResult两个操作通过kycApplyId来进行关联
        UserTencentKycLog prevLog = userTencentKycLogMapper.selectByPrimaryKey(Long.parseLong(orderNo));
        if (prevLog == null) {
            throw new BrokerException(BrokerErrorCode.TENCENT_KYC_BUSINESS_ERROR);
        }
        Long kycApplyId = prevLog.getKycApplyId();

        QueryResultRequest request = new QueryResultRequest();
        request.setAppId(tencentOAuthService.getBrokerKycConfig(orgId).getWebankAppId());
        request.setVersion(tencentOAuthService.getVersion());
        String nonce = TencentOAuthService.createNonce();
        request.setNonce(nonce);
        request.setOrderNo(orderNo);
        request.setGetFile(getFile);
        List<String> signParams = new ArrayList<>(
                Arrays.asList(request.getAppId(), request.getNonce(), request.getVersion(), request.getOrderNo()));
        request.setSign(computeSign(signParams, tencentOAuthService.getSignTicket(orgId)));

        UserTencentKycLog userTencentKycLog = prepareUserTencentKycLog(sequenceGenerator.getLong(),
                userId, orgId, kycApplyId,"queryFaceCompareResult");
        try {
            userTencentKycLog.setReqParam(IdascSAO.toPrintJson(request));
            QueryResultResponse result = idascSAO.queryFaceCompareResult(request);
            updateUserTencentKycLogFromResponse(userTencentKycLog, result);
            return result;
        } catch (Exception e) {
            updateUserTencentKycLogFromException(userTencentKycLog, e);
            throw e;
        } finally {
            saveUserTencentKycLogToDB(userTencentKycLog);
        }
    }

    private UserTencentKycLog prepareUserTencentKycLog(Long id, Long userId, Long orgId, Long kycApplyId, String action) {

        UserTencentKycLog userTencentKycLog = new UserTencentKycLog();
        userTencentKycLog.setId(id);
        userTencentKycLog.setUserId(userId);
        userTencentKycLog.setKycApplyId(kycApplyId);
        userTencentKycLog.setOrgId(orgId);
        userTencentKycLog.setCreated(System.currentTimeMillis());
        userTencentKycLog.setAction(action);
        return userTencentKycLog;
    }

    private void updateUserTencentKycLogFromException(UserTencentKycLog userTencentKycLog, Exception e) {
        if (e instanceof WebankKycException) {
            userTencentKycLog.setRespCode(((WebankKycException) e).getBizCode());
            userTencentKycLog.setRespMsg(((WebankKycException) e).getBizMsg());
        } else {
            userTencentKycLog.setRespCode(UserTencentKycLog.RESP_CODE_UNKNOWN);
            userTencentKycLog.setRespMsg(e.getMessage());
        }
    }

    private void updateUserTencentKycLogFromResponse(UserTencentKycLog userTencentKycLog, WebankBaseResponse response) {
        userTencentKycLog.setRespCode(response.getCode());
        userTencentKycLog.setRespMsg(response.getMsg());
    }

    private void saveUserTencentKycLogToDB(UserTencentKycLog userTencentKycLog) {
        Long timestamp = System.currentTimeMillis();

        if (userTencentKycLog.getCreated() == null) {
            userTencentKycLog.setCreated(timestamp);
        }

        if (userTencentKycLog.getUpdated() == null) {
            userTencentKycLog.setUpdated(timestamp);
        }

        userTencentKycLogMapper.insertSelective(userTencentKycLog);
    }

    private String generateRequestUserId(Long userId) {
        return String.valueOf(userId);
    }
}
