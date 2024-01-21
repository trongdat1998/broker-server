package io.bhex.broker.server.domain;

/**********************************
 *@项目名称: server-parent
 *@文件名称: io.bhex.broker.server.domain
 *@Date 2018/10/29
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
public enum LoginStatisticType {
    Unknown(0),
    PC(1),
    ANDROID(2),
    IOS(3),;

    private int value;

    LoginStatisticType(int value) {
        this.value = value;
    }

    public static LoginStatisticType fromType(Integer value) {
        for (LoginStatisticType loginType : LoginStatisticType.values()) {
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
