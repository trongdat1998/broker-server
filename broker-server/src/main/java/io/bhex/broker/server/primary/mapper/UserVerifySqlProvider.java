/**********************************
 *@项目名称: broker-parent
 *@文件名称: io.bhex.broker.mapper
 *@Date 2018/7/8
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
package io.bhex.broker.server.primary.mapper;

import com.google.common.base.Strings;
import io.bhex.broker.server.model.UserVerify;
import org.apache.ibatis.jdbc.SQL;

import java.util.Map;

public class UserVerifySqlProvider {

    private static final String TABLE_NAME = "tb_user_verify";

    private static final String COLUMNS = "id, user_id, org_id, kyc_level, nationality, country_code, first_name, second_name, gender, "
            + "card_type, card_no, card_no_hash, card_front_url, card_back_url, card_hand_url, "
            + "face_photo_url, face_video_url, video_url, data_encrypt, data_secret, "
            + "passed_id_check, verify_status, verify_reason_id, kyc_apply_id, created, updated";

    public String insert(UserVerify userVerify) {
        return new SQL() {
            {
                INSERT_INTO(TABLE_NAME);
                if (userVerify.getOrgId() != null) {
                    VALUES("org_id", "#{orgId}");
                }
                if (userVerify.getUserId() != null) {
                    VALUES("user_id", "#{userId}");
                }
                if (userVerify.getKycLevel() != null) {
                    VALUES("kyc_level", "#{kycLevel}");
                }
                if (userVerify.getNationality() != null) {
                    VALUES("nationality", "#{nationality}");
                }
                if (!Strings.isNullOrEmpty(userVerify.getCountryCode())) {
                    VALUES("country_code", "#{countryCode}");
                }
                if (!Strings.isNullOrEmpty(userVerify.getFirstName())) {
                    VALUES("first_name", "#{firstName}");
                }
                if (!Strings.isNullOrEmpty(userVerify.getSecondName())) {
                    VALUES("second_name", "#{secondName}");
                }
                if (userVerify.getGender() != null) {
                    VALUES("gender", "#{gender}");
                }
                if (userVerify.getCardType() != null) {
                    VALUES("card_type", "#{cardType}");
                }
                if (!Strings.isNullOrEmpty(userVerify.getCardNo())) {
                    VALUES("card_no", "#{cardNo}");
                }
                if (!Strings.isNullOrEmpty(userVerify.getCardNoHash())) {
                    VALUES("card_no_hash", "#{cardNoHash}");
                }
                if (!Strings.isNullOrEmpty(userVerify.getCardFrontUrl())) {
                    VALUES("card_front_url", "#{cardFrontUrl}");
                }
                if (!Strings.isNullOrEmpty(userVerify.getCardBackUrl())) {
                    VALUES("card_back_url", "#{cardBackUrl}");
                }
                if (!Strings.isNullOrEmpty(userVerify.getCardHandUrl())) {
                    VALUES("card_hand_url", "#{cardHandUrl}");
                }
                if (!Strings.isNullOrEmpty(userVerify.getFacePhotoUrl())) {
                    VALUES("face_photo_url", "#{facePhotoUrl}");
                }
                if (!Strings.isNullOrEmpty(userVerify.getFaceVideoUrl())) {
                    VALUES("face_video_url", "#{faceVideoUrl}");
                }
                if (!Strings.isNullOrEmpty(userVerify.getVideoUrl())) {
                    VALUES("video_url", "#{videoUrl}");
                }
                if (userVerify.getDataEncrypt() != null) {
                    VALUES("data_encrypt", "#{dataEncrypt}");
                }
                if (!Strings.isNullOrEmpty(userVerify.getDataSecret())) {
                    VALUES("data_secret", "#{dataSecret}");
                }
                if (userVerify.getPassedIdCheck() != null) {
                    VALUES("passed_id_check", "#{passedIdCheck}");
                }
                if (userVerify.getVerifyStatus() != null) {
                    VALUES("verify_status", "#{verifyStatus}");
                }
                if (userVerify.getKycApplyId() != null) {
                    VALUES("kyc_apply_id", "#{kycApplyId}");
                }
                VALUES("created", "#{created}");
                VALUES("updated", "#{updated}");
            }
        }.toString();
    }

    public String update(UserVerify userVerify) {
        return new SQL() {
            {
                UPDATE(TABLE_NAME);
                if (userVerify.getNationality() != null) {
                    SET("nationality = #{nationality}");
                }
                if (!Strings.isNullOrEmpty(userVerify.getCountryCode())) {
                    SET("country_code = #{countryCode}");
                }
                if (userVerify.getKycLevel() != null) {
                    SET("kyc_level = #{kycLevel}");
                }
                if (!Strings.isNullOrEmpty(userVerify.getFirstName())) {
                    SET("first_name = #{firstName}");
                }
                if (userVerify.getSecondName() != null) {
                    SET("second_name = #{secondName}");
                }
                if (userVerify.getGender() != null) {
                    SET("gender = #{gender}");
                }
                if (userVerify.getCardType() != null) {
                    SET("card_type = #{cardType}");
                }
                if (!Strings.isNullOrEmpty(userVerify.getCardNo())) {
                    SET("card_no = #{cardNo}");
                }
                if (!Strings.isNullOrEmpty(userVerify.getCardNoHash())) {
                    SET("card_no_hash = #{cardNoHash}");
                }
                if (userVerify.getCardFrontUrl() != null) {
                    SET("card_front_url = #{cardFrontUrl}");
                }
                if (userVerify.getCardBackUrl() != null) {
                    SET("card_back_url = #{cardBackUrl}");
                }
                if (userVerify.getCardHandUrl() != null) {
                    SET("card_hand_url = #{cardHandUrl}");
                }
                if (userVerify.getFacePhotoUrl() != null) {
                    SET("face_photo_url = #{facePhotoUrl}");
                }
                if (userVerify.getFaceVideoUrl() != null) {
                    SET("face_video_url = #{faceVideoUrl}");
                }
                if (userVerify.getVideoUrl() != null) {
                    SET("video_url = #{videoUrl}");
                }
                if (userVerify.getDataEncrypt() != null) {
                    SET("data_encrypt = #{dataEncrypt}");
                }
                if (!Strings.isNullOrEmpty(userVerify.getDataSecret())) {
                    SET("data_secret = #{dataSecret}");
                }
                if (userVerify.getPassedIdCheck() != null) {
                    SET("passed_id_check = #{passedIdCheck}");
                }
                if (userVerify.getVerifyStatus() != null) {
                    SET("verify_status = #{verifyStatus}");
                }
                if (userVerify.getKycApplyId() != null) {
                    SET("kyc_apply_id = #{kycApplyId}");
                }
                SET("updated = #{updated}");
                WHERE("user_id = #{userId}");
            }
        }.toString();
    }

    public String getByUserId() {
        return new SQL() {
            {
                SELECT(COLUMNS).FROM(TABLE_NAME);
                WHERE("user_id = #{userId}");
            }
        }.toString();
    }

    public String getByOrgIdAndCardNo() {
        return new SQL() {
            {
                SELECT(COLUMNS).FROM(TABLE_NAME);
                WHERE("org_id = #{orgId}").WHERE("card_no = #{cardNo}");
            }
        }.toString();
    }

    public String getByOrgIdAndCardNoHash() {
        return new SQL() {
            {
                SELECT(COLUMNS).FROM(TABLE_NAME);
                WHERE("org_id = #{orgId}").WHERE("card_no_hash = #{cardNoHash}");
            }
        }.toString();
    }

    public String queryUserVerify(Map<String, Object> parameter) {

        Long userId = (Long) parameter.get("userId");
        Integer verifyStatus = (Integer) parameter.get("verifyStatus");
        Integer nationality = (Integer) parameter.get("nationality");
        Integer cardType = (Integer) parameter.get("cardType");
        Long startTime = (Long) parameter.get("startTime");
        Long endTime = (Long) parameter.get("endTime");

        return new SQL() {
            {
                SELECT(UserVerifyMapper.COLUMNS).FROM(UserVerifyMapper.TABLE_NAME);
                WHERE("org_id = #{brokerId}");
                if (userId != null && userId != 0L) {
                    WHERE("user_id = #{userId}");
                }
                if (verifyStatus != null && verifyStatus != 0L) {
                    WHERE("verify_status = #{verifyStatus}");
                }
                if (nationality != null && nationality != 0L) {
                    WHERE("nationality = #{nationality}");
                }
                if (cardType != null && cardType != 0L) {
                    WHERE("card_type = #{cardType}");
                }
                if (startTime != null && startTime != 0L) {
                    WHERE("created >= #{startTime}");
                }
                if (endTime != null && endTime != 0L) {
                    WHERE("created <= #{endTime}");
                }
                ORDER_BY("id desc limit #{fromindex}, #{endindex}");
            }
        }.toString();
    }

    public String countByBrokerId(Map<String, Object> parameter) {
        Long userId = (Long) parameter.get("userId");
        Integer verifyStatus = (Integer) parameter.get("verifyStatus");
        Integer nationality = (Integer) parameter.get("nationality");
        Integer cardType = (Integer) parameter.get("cardType");
        Long startTime = (Long) parameter.get("startTime");
        Long endTime = (Long) parameter.get("endTime");

        return new SQL() {
            {
                SELECT(" count(id) ").FROM(UserVerifyMapper.TABLE_NAME);
                WHERE("org_id = #{brokerId}");
                if (userId != null && userId != 0L) {
                    WHERE("user_id = #{userId}");
                }
                if (verifyStatus != null && verifyStatus != 0L) {
                    WHERE("verify_status = #{verifyStatus}");
                }
                if (nationality != null && nationality != 0L) {
                    WHERE("nationality = #{nationality}");
                }
                if (cardType != null && cardType != 0L) {
                    WHERE("card_type = #{cardType}");
                }
                if (startTime != null && startTime != 0L) {
                    WHERE("created >= #{startTime}");
                }
                if (endTime != null && endTime != 0L) {
                    WHERE("created <= #{endTime}");
                }
            }
        }.toString();
    }

}
