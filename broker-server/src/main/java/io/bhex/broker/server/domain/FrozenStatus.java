/**********************************
 *@项目名称: server-parent
 *@文件名称: io.bhex.broker.server.domain
 *@Date 2018/11/30
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
package io.bhex.broker.server.domain;

public enum FrozenStatus {

    UNDER_FROZEN(1),
    UNFREEZE(0);

    private int status;

    FrozenStatus(int status) {
        this.status = status;
    }

    public int status() {
        return status;
    }
}
