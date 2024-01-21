package io.bhex.broker.server.domain;

/**********************************
 *@项目名称: broker-parent
 *@文件名称: io.bhex.broker.domain
 *@Date 2018/8/9
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
public enum UserType {
    GENERAL_USER(1),
    INSTITUTIONAL_USER(2); // 机构用户

    private int value;

    UserType(int value) {
        this.value = value;
    }

    public int value() {
        return value;
    }
}
