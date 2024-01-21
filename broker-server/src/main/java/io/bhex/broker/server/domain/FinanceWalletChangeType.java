/**********************************
 *@项目名称: server-parent
 *@文件名称: io.bhex.broker.server.domain
 *@Date 2019-03-12
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
package io.bhex.broker.server.domain;

public enum FinanceWalletChangeType {

    PURCHASE(0), // 申购
    REDEEM(1), // 赎回
    INTEREST(2);

    private int type;

    FinanceWalletChangeType(int type) {
        this.type = type;
    }

    public int type() {
        return type;
    }

}
