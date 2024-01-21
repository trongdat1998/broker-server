package io.bhex.broker.server.domain;

/**********************************
 *@项目名称: server-parent
 *@文件名称: io.bhex.broker.server.domain
 *@Date 2018/10/29
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
public enum LoginType {

    MOBILE(1), // 包含使用用户名(手机号)登录
    EMAIL(2), // 包含使用用户名(邮箱)登录
    ORG_API(3), // 这里指的是第三方直接通过用户的uid来登录
    MOBILE_QUICK_LOGIN(4), // 手机号快捷登录
    EMAIL_QUICK_LOGIN(5), // 邮箱快捷登录
    SCAN_QRCODE(6); // 扫码登录

    private int value;

    LoginType(int value) {
        this.value = value;
    }

    public static LoginType fromType(Integer value) {
        for (LoginType loginType : LoginType.values()) {
            if (loginType.value == value) {
                return loginType;
            }
        }
        return null;
    }

    public int value() {
        return this.value;
    }

}
