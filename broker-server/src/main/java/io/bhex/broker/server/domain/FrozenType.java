package io.bhex.broker.server.domain;

import com.google.gson.Gson;

import java.util.Arrays;
import java.util.List;

/**********************************
 *@项目名称: server-parent
 *@文件名称: io.bhex.broker.server.domain
 *@Date 2018/11/30
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
public enum FrozenType {
    FROZEN(0),
    FROZEN_LOGIN(1),
    FROZEN_WITHDRAW(2),//禁止提币
    FROZEN_USE_FUND_PWD(3),
    FROZEN_RECEIVE_RED_PACKET(4),
    FROZEN_SEND_RED_PACKET(5),//禁止发红包
    FROZEN_OTC_TRADE(6),//禁止otc出金
    FROZEN_TRANSFER(7);//禁止跨用户转账

    private int type;

    FrozenType(int type) {
        this.type = type;
    }

    public int type() {
        return type;
    }

    public static List<FrozenType> forbidWithdrawList() {
        return Arrays.asList(FROZEN_WITHDRAW, FROZEN_SEND_RED_PACKET, FROZEN_OTC_TRADE, FROZEN_TRANSFER);
    }

    public static List<Integer> forbidWithdrawValueList() {
        return Arrays.asList(FROZEN_WITHDRAW.type, FROZEN_SEND_RED_PACKET.type, FROZEN_OTC_TRADE.type, FROZEN_TRANSFER.type);
    }
}
