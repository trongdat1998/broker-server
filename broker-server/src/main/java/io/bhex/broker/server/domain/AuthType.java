package io.bhex.broker.server.domain;

/**********************************
 *@项目名称: broker-parent
 *@文件名称: io.bhex.broker.domain
 *@Date 2018/7/1
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
public enum AuthType {
    MOBILE(1),
    EMAIL(2),
    GA(3),;

    private int value;

    AuthType(int value) {
        this.value = value;
    }

    public static AuthType fromValue(int value) {
        for (AuthType authType : AuthType.values()) {
            if (authType.value == value) {
                return authType;
            }
        }
        return null;
    }

    public int value() {
        return value;
    }
}
