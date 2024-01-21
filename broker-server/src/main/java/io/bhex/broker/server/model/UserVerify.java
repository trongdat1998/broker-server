/**********************************
 *@项目名称: broker-parent
 *@文件名称: io.bhex.broker.domain.entity
 *@Date 2018/7/8
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
package io.bhex.broker.server.model;

import com.google.common.base.Strings;
import io.bhex.broker.common.util.AESCipher;
import io.bhex.broker.server.domain.KycLevelGrade;
import io.bhex.broker.server.domain.UserVerifyStatus;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.BeanUtils;

import javax.persistence.Id;
import javax.persistence.Table;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "tb_user_verify")
public class UserVerify {

    public static final Integer DATA_ENCRYPTED = 1;

    @Id
    private Long id;
    private Long orgId;
    private Long userId;
    private Long nationality;
    private String countryCode;
    private Integer kycLevel;
    private String firstName;
    private String secondName;
    private Integer gender;
    private Integer cardType;
    private String cardNo;
    private String cardNoHash;
    private String cardFrontUrl;
    private String cardBackUrl;
    private String cardHandUrl;
    private String facePhotoUrl;
    private String faceVideoUrl;
    private String videoUrl;
    private Integer dataEncrypt;
    private String dataSecret;
    private Integer passedIdCheck;
    private Integer verifyStatus;
    private Long created;
    private Long updated;

    private Long verifyReasonId;
    private Long kycApplyId;

    public KycLevelGrade getGrade() {
        return KycLevelGrade.fromKycLevel(kycLevel);
    }

    public boolean isBasicVerifyPassed() {
        return isGradeVerifyPassed(KycLevelGrade.BASIC);
    }

    public boolean isSeniorVerifyPassed() {
        return isGradeVerifyPassed(KycLevelGrade.SENIOR);
    }

    public boolean isVipVerifyPassed() {
        return isGradeVerifyPassed(KycLevelGrade.VIP);
    }

    /**
     * 判断指定的KYC等级是否已经认证通过
     *
     * @param grade 指定的KYC等级
     * @return 如果等级通过，则返回true，否则返回false
     */
    private boolean isGradeVerifyPassed(KycLevelGrade grade) {
        KycLevelGrade userGrade = getGrade();
        return (userGrade == grade && isVerifyPassed()) || (userGrade.getValue() > grade.getValue());
    }

    public boolean isVerifyPassed() {
        return verifyStatus != null && verifyStatus.equals(UserVerifyStatus.PASSED.value());
    }

    public boolean isVerifyRefused() {
        return verifyStatus != null && verifyStatus.equals(UserVerifyStatus.REFUSED.value());
    }

    /**
     * 对敏感信息加密（主要用于基础认证）
     * 目前数据库里需要加密的字段包括：firstName, secondName, cardNo
     * 注：文件数据目前在数据库中只存url地址，因此用户的数据文件加密不在这里做
     *
     * @param userVerify 需要加密的实体对象
     * @return 返回新的加密过敏感字段的实体对象
     */
    public static UserVerify encrypt(UserVerify userVerify) {
        if (DATA_ENCRYPTED.equals(userVerify.getDataEncrypt())) {
            return copyFrom(userVerify);
        }

        UserVerify encryptedUserVerify = new UserVerify();
        BeanUtils.copyProperties(userVerify, encryptedUserVerify);

        String dataSecret = encryptedUserVerify.getDataSecret();
        if (StringUtils.isEmpty(dataSecret)) {
            dataSecret = createUserAESKey(encryptedUserVerify.getUserId());
        }

        if (StringUtils.isNotEmpty(encryptedUserVerify.getFirstName())) {
            encryptedUserVerify.setFirstName(AESCipher.encryptString(encryptedUserVerify.getFirstName(), dataSecret));
        }

        if (StringUtils.isNotEmpty(encryptedUserVerify.getSecondName())) {
            encryptedUserVerify.setSecondName(AESCipher.encryptString(encryptedUserVerify.getSecondName(), dataSecret));
        }

        if (StringUtils.isNotEmpty(encryptedUserVerify.getCardNo())) {
            encryptedUserVerify.setCardNo(AESCipher.encryptString(encryptedUserVerify.getCardNo(), dataSecret));
        }

        encryptedUserVerify.setDataEncrypt(1);
        encryptedUserVerify.setDataSecret(dataSecret);
        return encryptedUserVerify;
    }

    /**
     * 对UserVerify的关键字段信息解密
     *
     * @param userVerify 需要解密的实体
     * @return 解密过的实体对象
     */
    public static UserVerify decrypt(UserVerify userVerify) {
        if (userVerify == null) {
            return null;
        }

        String dataSecret = userVerify.getDataSecret();
        if (!DATA_ENCRYPTED.equals(userVerify.getDataEncrypt()) || StringUtils.isEmpty(dataSecret)) {
            return copyFrom(userVerify);
        }

        UserVerify decryptedUserVerify = new UserVerify();
        BeanUtils.copyProperties(userVerify, decryptedUserVerify);

        if (StringUtils.isNotEmpty(decryptedUserVerify.getFirstName())) {
            decryptedUserVerify.setFirstName(AESCipher.decryptString(decryptedUserVerify.getFirstName(), dataSecret));
        }

        if (StringUtils.isNotEmpty(decryptedUserVerify.getSecondName())) {
            decryptedUserVerify.setSecondName(AESCipher.decryptString(decryptedUserVerify.getSecondName(), dataSecret));
        }

        if (StringUtils.isNotEmpty(decryptedUserVerify.getCardNo())) {
            decryptedUserVerify.setCardNo(AESCipher.decryptString(decryptedUserVerify.getCardNo(), dataSecret));
        }

        decryptedUserVerify.setDataEncrypt(0);
        return decryptedUserVerify;
    }

    public static String getKycName(Integer dataEncrypt, String dataSecret, String firstName, String secondName, boolean cnUser) {
        String fn;
        String sn;
        if (!DATA_ENCRYPTED.equals(dataEncrypt) || StringUtils.isEmpty(dataSecret)) {
            fn = Strings.nullToEmpty(firstName);
            sn = Strings.nullToEmpty(secondName);
        } else {
            fn = Strings.nullToEmpty(AESCipher.decryptString(firstName, dataSecret));
            sn = Strings.nullToEmpty(AESCipher.decryptString(secondName, dataSecret));
        }
        if (!cnUser) { //firstname打码
            return "* " + sn;
        }
        if ((fn + sn).length() > 1) { //姓打码
            return "*" + (fn + sn).substring(1);
        }
        return fn + sn;
    }

    public static UserVerify copyFrom(UserVerify other) {
        UserVerify me = new UserVerify();
        BeanUtils.copyProperties(other, me);
        return me;
    }

    public static String createUserAESKey(Long userId) {
        // 使用用户userId的MD5作为加密密钥
        // 注意：这个密钥不是最终加解密的最终密钥
        // 最终密钥需要用这个用户密钥加上给定的特殊的加盐值二次MD5
        return DigestUtils.md5Hex(String.valueOf(userId));
    }

    public static String hashCardNo(String cardNo) {
        return DigestUtils.sha256Hex(cardNo);
    }

}
