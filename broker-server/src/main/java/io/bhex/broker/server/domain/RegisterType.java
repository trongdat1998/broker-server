package io.bhex.broker.server.domain;

/**********************************
 *@项目名称: broker-parent
 *@文件名称: io.bhex.broker.domain
 *@Date 2018/7/1
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
public enum RegisterType {
    MOBILE(1),
    EMAIL(2),;

    private int value;

    RegisterType(int value) {
        this.value = value;
    }

    public static RegisterType fromType(Integer value) {
        for (RegisterType registerType : RegisterType.values()) {
            if (registerType.value == value) {
                return registerType;
            }
        }
        return null;
    }

    public int value() {
        return this.value;
    }

}
