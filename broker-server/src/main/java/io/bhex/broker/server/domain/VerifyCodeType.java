package io.bhex.broker.server.domain;

import io.bhex.broker.grpc.security.SecurityVerifyCodeType;

/**********************************
 *@项目名称: broker-parent
 *@文件名称: io.bhex.broker.domain
 *@Date 2018/7/23
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
public enum VerifyCodeType {

    REGISTER(1, SecurityVerifyCodeType.REGISTER, true),
    LOGIN_ADVANCE(2, SecurityVerifyCodeType.LOGIN, false),
    FIND_PASSWORD(3, SecurityVerifyCodeType.FIND_PASSWORD, true),
    RESET_PASSWORD(4, SecurityVerifyCodeType.RESET_PASSWORD, false),
    BIND_EMAIL(6, SecurityVerifyCodeType.BIND_EMAIL, false),
    BIND_MOBILE(5, SecurityVerifyCodeType.BIND_MOBILE, false),
    BIND_GA(7, SecurityVerifyCodeType.BIND_GA, false),
    ADD_WITHDRAW_ADDRESS(8, SecurityVerifyCodeType.ADD_WITHDRAW_ADDRESS, false),
    DELETE_WITHDRAW_ADDRESS(9, SecurityVerifyCodeType.DELETE_WITHDRAW_ADDRESS, false),
    KYC(10, SecurityVerifyCodeType.KYC, false),
    CREATE_API_KEY(11, SecurityVerifyCodeType.CREATE_API_KEY, false),
    MODIFY_API_KEY(12, SecurityVerifyCodeType.MODIFY_API_KEY, false),
    DELETE_API_KEY(13, SecurityVerifyCodeType.DELETE_API_KEY, false),
    WITHDRAW(14, SecurityVerifyCodeType.WITHDRAW, false),
    CREATE_FUND_PWD(15, SecurityVerifyCodeType.CREATE_FUND_PWD, false),
    MODIFY_FUND_PWD(16, SecurityVerifyCodeType.MODIFY_FUND_PWD, false),
    UNBIND_EMAIL(17, SecurityVerifyCodeType.UNBIND_EMAIL, false),
    UNBIND_MOBILE(18, SecurityVerifyCodeType.UNBIND_MOBILE, false),
    UNBIND_GA(19, SecurityVerifyCodeType.UNBIND_GA, false),
    ORDER_PAY(20, SecurityVerifyCodeType.ORDER_PAY, false),
    QUICK_REGISTER(21, SecurityVerifyCodeType.QUICK_REGISTER, true),
    QUICK_LOGIN(22, SecurityVerifyCodeType.QUICK_LOGIN, true),
    SET_PASSWORD(23, SecurityVerifyCodeType.SET_PASSWORD, false),
    EDIT_ANTI_PHISHING_CODE(24, SecurityVerifyCodeType.EDIT_ANTI_PHISHING_CODE, false),
    REBIND_EMAIL(25,SecurityVerifyCodeType.REBIND_EMAIL,false),
    REBIND_MOBILE(26,SecurityVerifyCodeType.REBIND_MOBILE,false),
    REBIND_GA(27,SecurityVerifyCodeType.REBIND_GA,false);



    private int value;
    private SecurityVerifyCodeType verifyCodeType;
    private boolean needCaptchaValid;

    VerifyCodeType(int value, SecurityVerifyCodeType verifyCodeType, boolean needCaptchaValid) {
        this.value = value;
        this.verifyCodeType = verifyCodeType;
        this.needCaptchaValid = needCaptchaValid;
    }

    public int value() {
        return this.verifyCodeType.getNumber();
    }

    public SecurityVerifyCodeType verifyCodeType() {
        return this.verifyCodeType;
    }

    public boolean needCaptchaValid() {
        return this.needCaptchaValid;
    }

    public static VerifyCodeType fromValue(int value) {
        for (VerifyCodeType verifyCodeType : VerifyCodeType.values()) {
            if (verifyCodeType.value == value) {
                return verifyCodeType;
            }
        }
        return null;
    }
}
