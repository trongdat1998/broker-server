package io.bhex.broker.server.domain;

/**********************************
 *@项目名称: server-parent
 *@文件名称: io.bhex.broker.server.domain
 *@Date 2018/11/30
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
public enum FrozenReason {

    LOGIN_INPUT_ERROR(1), // 登录密码输入错误次数过多
    FIND_LOGIN_PWD(2), // 找回登录密码
    FUND_PWD_ERROR(3), // 资金密码输入错误次数过多
    MODIFY_FUND_PWD(4), // 修改资金密码
    SYSTEM_FROZEN(5), // 系统冻结
    RISK_CHECK(6), // 风控冻结
    MODIFY_LOGIN_PWD(7),
    UNBIND_EMAIL(8),
    UNBIND_MOBILE(9),
    UNBIND_GA(10),
    UNBIND_KYC(11), // 解绑GA
    RED_PACKET_PASSWORD_ERROR(12),
    REBIND_EMAIL(13),
    REBIND_MOBILE(14),
    REBIND_GA(15),
    MARGIN_BLACK_LIST(16);

    private int reason;

    FrozenReason(int reason) {
        this.reason = reason;
    }

    public int reason() {
        return reason;
    }
}
