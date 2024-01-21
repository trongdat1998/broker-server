package io.bhex.broker.server.domain;

/**********************************
 *@项目名称: broker-parent
 *@文件名称: io.bhex.broker.domain
 *@Date 2018/7/8
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
public enum UserVerifyStatus {
    NONE(0),
    UNDER_REVIEW(1),
    PASSED(2),
    REFUSED(3);

    private int value;

    UserVerifyStatus(Integer value) {
        this.value = value;
    }

    public int value() {
        return this.value;
    }

    public static UserVerifyStatus fromValue(int value) {
        for (UserVerifyStatus userVerifyStatus : UserVerifyStatus.values()) {
            if (userVerifyStatus.value == value) {
                return userVerifyStatus;
            }
        }
        return null;
    }

}
