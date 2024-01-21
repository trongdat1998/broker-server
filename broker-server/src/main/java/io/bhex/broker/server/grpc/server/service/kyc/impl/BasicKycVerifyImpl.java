package io.bhex.broker.server.grpc.server.service.kyc.impl;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import io.bhex.broker.common.api.client.jiean.IDCardVerifyRequest;
import io.bhex.broker.common.api.client.jiean.IDCardVerifyResponse;
import io.bhex.broker.common.api.client.jiean.JieanApi;
import io.bhex.broker.common.api.client.tencent.TencentApi;
import io.bhex.broker.common.api.client.tencent.TencentIDCardVerifyRequest;
import io.bhex.broker.common.api.client.tencent.TencentIDCardVerifyResponse;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.common.util.JsonUtil;
import io.bhex.broker.grpc.user.kyc.BasicKycVerifyRequest;
import io.bhex.broker.grpc.user.kyc.BasicKycVerifyResponse;
import io.bhex.broker.server.BrokerServerProperties;
import io.bhex.broker.server.domain.BaseConfigConstants;
import io.bhex.broker.server.domain.SwitchStatus;
import io.bhex.broker.server.domain.UserVerifyStatus;
import io.bhex.broker.server.domain.kyc.KycLevelEnum;
import io.bhex.broker.server.grpc.server.service.BaseBizConfigService;
import io.bhex.broker.server.grpc.server.service.kyc.AbstractKycVerify;
import io.bhex.broker.server.model.Country;
import io.bhex.broker.server.model.UserIdCheckLog;
import io.bhex.broker.server.model.UserKycApply;
import io.bhex.broker.server.model.UserVerify;
import io.bhex.broker.server.util.IDCardUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;

@Slf4j
@Service
public class BasicKycVerifyImpl extends AbstractKycVerify<BasicKycVerifyRequest, BasicKycVerifyResponse> {

    private static final String JIEAN_SUCCESS_CODE = "000";
    private static final List<String> JIEAN_AUTHENTICATION_NAME_INVALID_CODES = Lists.newArrayList("215", "226", "301");
    private static final List<String> JIEAN_AUTHENTICATION_CARD_NO_INVALID_CODES = Lists.newArrayList("216", "227", "228", "302");

    private static final int TENCENT_SUCCESS_CODE = 0;
    @Resource
    private JieanApi jieanApi;

    @Resource
    private TencentApi tencentApi;

    @Resource(name = "stringRedisTemplate")
    private StringRedisTemplate redisTemplate;

    @Resource
    private BaseBizConfigService baseBizConfigService;
	
	@Resource
    private BrokerServerProperties brokerServerProperties;

    @Override
    public int getLevel() {
        return 10;
    }

    @Override
    protected UserVerify preVerify(BasicKycVerifyRequest request) {
        Long orgId = request.getHeader().getOrgId();
        Long userId = request.getHeader().getUserId();

        Country country = basicService.getCountryByCode(request.getCountryCode());
        if (country == null) {
            throw new BrokerException(BrokerErrorCode.NATIONALITY_ERROR);
        }

        if (isVerifyChina(request) && request.getCardType() != 1) {
            throw new BrokerException(BrokerErrorCode.CARD_TYPE_NOT_MATCH_WITH_NATIONALITY);
        }

        // 如果是国内身份证，将身份证号里的小写x转换成大写X
        String cardNo = isVerifyChina(request) ? request.getCardNo().toUpperCase() : request.getCardNo();
        String cardNoHash = UserVerify.hashCardNo(cardNo);
        List<UserVerify> userVerifyList = userVerifyMapper.queryByOrgIdAndCardNoHash(orgId, cardNoHash);
        for (UserVerify verifyEntity : userVerifyList) {
            if (verifyEntity != null && !verifyEntity.getUserId().equals(userId) && !verifyEntity.isVerifyRefused()) {
                throw new BrokerException(BrokerErrorCode.IDENTITY_CARD_NO_BEING_USED);
            }
        }

        // 姓名处理
        String firstName;
        String secondName;
        if (isVerifyChina(request)) {
            firstName = request.getName();
            secondName = "";

            if (Strings.isNullOrEmpty(firstName)) {
                throw new BrokerException(BrokerErrorCode.FIRST_NAME_CANNOT_BE_NULL);
            }
        } else {
            firstName = request.getFirstName();
            secondName = request.getSecondName();

            if (Strings.isNullOrEmpty(firstName)) {
                throw new BrokerException(BrokerErrorCode.FIRST_NAME_CANNOT_BE_NULL);
            }
            if (Strings.isNullOrEmpty(secondName)) {
                throw new BrokerException(BrokerErrorCode.SECOND_NAME_CANNOT_BE_NULL);
            }
        }

        UserVerify userVerify;

        // 查询是否已经存在认证记录
        UserVerify existUserVerify = getUserVerify(request.getHeader().getUserId(), true);
        if (existUserVerify != null) {
            // 判断是否满足基础认证的条件
            if (existUserVerify.isSeniorVerifyPassed()) {
                // 高级认证已经通过。不需要进行基础认证
                throw new BrokerException(BrokerErrorCode.KYC_BASIC_VERIFY_NONEED);
            }

            userVerify = UserVerify.copyFrom(existUserVerify);
        } else {
            userVerify = UserVerify.builder()
                    .orgId(orgId)
                    .userId(userId)
                    .nationality(country.getId())
                    .kycLevel(KycLevelEnum.BASIC.getValue())
                    .countryCode(request.getCountryCode())
                    .cardType(request.getCardType())
                    .cardNo(request.getCardNo())
                    .cardNoHash(cardNoHash)
                    .build();
        }

        userVerify.setOrgId(orgId);
        userVerify.setUserId(userId);
        userVerify.setNationality(country.getId());
        userVerify.setCountryCode(request.getCountryCode());
        userVerify.setKycLevel(KycLevelEnum.BASIC.getValue());
        userVerify.setCardType(request.getCardType());
        userVerify.setCardNo(cardNo);
        userVerify.setCardNoHash(cardNoHash);
        userVerify.setFirstName(firstName);
        userVerify.setSecondName(secondName);

        return userVerify;
    }

    @Override
    protected BasicKycVerifyResponse doVerify(BasicKycVerifyRequest request, UserVerify userVerify) {
        // 针对中国用户校验身份证二要素
        // 设置passedIdCheck（身份证校验是否通过）
        userVerify.setPassedIdCheck(0);
        // 保存用户的申请记录，初始化userVerify信息到数据库
        initApplyAndVerify(userVerify);

        // 设置审核状态为审核通过
        userVerify.setVerifyStatus(UserVerifyStatus.PASSED.value());

        // 更新数据库状态
        userVerifyService.updateVerifyResultPassed(userVerify);

        return BasicKycVerifyResponse.newBuilder().setBizCode(OK_BIZ_CODE).setBizMsg(OK_BIZ_MSG).build();
    }

    @Override
    protected void postVerify(BasicKycVerifyRequest request, BasicKycVerifyResponse response, UserVerify userVerify) {

    }

    private boolean basicKycVerifyCN(Long orgId, Long userId, String cardNo, String idCardName) {
        log.info("basicKycVerifyCN check: orgId:{}, userId:{}, name:{} invoke CN IDCheck", orgId, userId, idCardName);
        try {
            if (!IDCardUtil.validateCard(cardNo)) {
                log.info("orgId:{} userId:{} enter a invalid IDCardNO:{}", orgId, userId, cardNo);
                throw new BrokerException(BrokerErrorCode.IDENTITY_AUTHENTICATION_CARD_NO_INVALID);
            }

            if (cardNo.substring(0, 2).equals("65") || cardNo.substring(0, 2).equals("54")) {
                return false;
            }
            //todo 校验年龄
            //获取校验开关
            SwitchStatus openKycAgeCheckSwtich = baseBizConfigService.getConfigSwitchStatus(orgId,
                    BaseConfigConstants.WHOLE_SITE_CONTROL_SWITCH_GROUP, BaseConfigConstants.OPEN_KYC_AGE_CHECK_KEY);
            if (openKycAgeCheckSwtich.isOpen()) {
                int age = IDCardUtil.getAgeByCardAccurateToDay(cardNo);
                if (age < 18 || age > 75) {
                    log.warn("orgId:{} userId:{}  no:{} age:{} less 18 or greater 75.", orgId, userId, cardNo, age);
                    throw new BrokerException(BrokerErrorCode.AGE_LESS_18_ERROR);
                }
            }
            String checkSwitch = redisTemplate.opsForValue().get("cnUserIDCardNoCheck");
            if (Strings.isNullOrEmpty(checkSwitch)) {
                checkSwitch = "true";
            }
            boolean cnUserIDCardNoCheck = Boolean.parseBoolean(checkSwitch);
            if (!cnUserIDCardNoCheck) {
                return false;
            }
            boolean passedIdCheck = false;
            Long checkOrderId = sequenceGenerator.getLong();
            if (brokerServerProperties.isTencentKyc()) {
                TencentIDCardVerifyRequest request = TencentIDCardVerifyRequest.builder()
                        .certcode(cardNo)
                        .name(idCardName)
                        .build();
                String checkResponse;
                TencentIDCardVerifyResponse response = null;
                Long startTime = System.currentTimeMillis();
                try {
                    response = tencentApi.idCardVerify(request);
                    checkResponse = JsonUtil.defaultGson().toJson(response);
                    insertUserIdCheckLog(checkOrderId, orgId, userId, idCardName, cardNo, checkResponse);
                } catch (BrokerException e) {
                    log.error("orgId:{}, userId:{}, name:{} kyc catch BrokerException, code:{}", orgId, userId, idCardName, e.getCode());
                    insertUserIdCheckLog(checkOrderId, orgId, userId, idCardName, cardNo, "BrokerException, code:" + e.getCode());
                    if (e.getCode() == BrokerErrorCode.IDENTITY_AUTHENTICATION_TIMEOUT.code()) {
                        log.warn("user:{} kyc timeout, please pay attention", userId);
                    } else {
                        throw e;
                    }
                } catch (Exception e) {
                    log.error("user:{} kyc exception", userId, e);
                    insertUserIdCheckLog(checkOrderId, orgId, userId, idCardName, cardNo, "Exception, message:" + Strings.nullToEmpty(e.getMessage()));
                    throw new BrokerException(BrokerErrorCode.IDENTITY_AUTHENTICATION_FAILED);
                }
                Long endTime = System.currentTimeMillis();
                log.info("TencentApi-invoke-log, orderId:{}, startTime:{}, endTime:{}, duration:{}", checkOrderId, startTime, endTime, (endTime - startTime));
                if (response != null) {
                    // code为0&& result为 1 代表身份信息数据一致
                    if (response.getCode() != TENCENT_SUCCESS_CODE || response.getResult() != 1) {
                        log.warn("user:{} kyc chinese user identity authentication failed, errorCode:{}, errorMsg:{}", userId, response.getCode(), response.getMessage());
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
                    insertUserIdCheckLog(checkOrderId, orgId, userId, idCardName, cardNo, checkResponse);
                } catch (BrokerException e) {
                    log.error("orgId:{}, userId:{}, name:{} kyc catch BrokerException, code:{}", orgId, userId, idCardName, e.getCode());
                    insertUserIdCheckLog(checkOrderId, orgId, userId, idCardName, cardNo, "BrokerException, code:" + e.getCode());
                    if (e.getCode() == BrokerErrorCode.IDENTITY_AUTHENTICATION_TIMEOUT.code()) {
                        log.warn("user:{} kyc timeout, please pay attention", userId);
                    } else {
                        throw e;
                    }
                } catch (Exception e) {
                    log.error("user:{} kyc exception", userId, e);
                    insertUserIdCheckLog(checkOrderId, orgId, userId, idCardName, cardNo, "Exception, message:" + Strings.nullToEmpty(e.getMessage()));
                    throw new BrokerException(BrokerErrorCode.IDENTITY_AUTHENTICATION_FAILED);
                }
                Long endTime = System.currentTimeMillis();
                log.info("JieanApi-invoke-log, orderId:{}, startTime:{}, endTime:{}, duration:{}", checkOrderId, startTime, endTime, (endTime - startTime));
                if (response != null) {
                    // 捷安的000代表身份信息数据一致
                    if (!response.getRespCode().equals(JIEAN_SUCCESS_CODE)) {
                        log.warn("user:{} kyc chinese user identity authentication failed, errorCode:{}, errorMsg:{}", userId, response.getRespCode(), response.getRespDesc());
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
            return passedIdCheck;
        } catch (Throwable e) {
            log.info("basicKycVerifyCN check has error: orgId:{}, userId:{}, name:{} invoke CN IDCheck", orgId, userId, idCardName, e);
            throw e;
        }
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

    private static boolean isVerifyChina(BasicKycVerifyRequest request) {
        return request.getCountryCode().equalsIgnoreCase("CN");
    }

    private void initApplyAndVerify(UserVerify userVerify) {
        // 如果preVerify返回的UserVerify对象有id，则说明数据库已经存在KYC信息
        boolean existVerifyInfo = userVerify.getId() != null && userVerify.getId() > 0;

        Long userKycApplyId = sequenceGenerator.getLong();

        // 设置当前用户KYC申请记录ID
        userVerify.setKycApplyId(userKycApplyId);

        // 初始化审核状态
        userVerify.setVerifyStatus(UserVerifyStatus.UNDER_REVIEW.value());

        UserVerify dbUserVerify = UserVerify.encrypt(userVerify);

        // 插入申请记录
        UserKycApply userKycApply = UserKycApply.builder()
                .id(userKycApplyId)
                .kycLevel(dbUserVerify.getKycLevel())
                .userId(dbUserVerify.getUserId())
                .orgId(dbUserVerify.getOrgId())
                .countryCode(dbUserVerify.getCountryCode())
                .firstName(dbUserVerify.getFirstName())
                .secondName(dbUserVerify.getSecondName())
                .cardType(dbUserVerify.getCardType())
                .cardNo(dbUserVerify.getCardNo())
                .dataSecret(dbUserVerify.getDataSecret())
                .created(System.currentTimeMillis())
                .build();
        userKycApplyMapper.insertSelective(userKycApply);

        // 保存UserVerify（关键字段需要加密）
        Long timestamp = System.currentTimeMillis();
        dbUserVerify.setUpdated(timestamp);
        if (existVerifyInfo) {
            userVerifyMapper.update(dbUserVerify);
        } else {
            dbUserVerify.setCreated(timestamp);
            userVerifyMapper.insertRecord(dbUserVerify);
        }
    }
}
