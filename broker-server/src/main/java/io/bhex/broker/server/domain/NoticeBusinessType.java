package io.bhex.broker.server.domain;

/**********************************
 *@项目名称: server-parent
 *@文件名称: io.bhex.broker.server.domain
 *@Date 2018/9/19
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/

/**
 * verification code NoticeBusinessType is not included here.
 */
public enum NoticeBusinessType {
    /*
     * 验证码范围内的业务类型
     */
    REGISTER,
    LOGIN,
    FIND_PASSWORD,
    RESET_PASSWORD,
    BIND_MOBILE,
    BIND_EMAIL,
    BIND_GA,
    ADD_WITHDRAW_ADDRESS,
    DELETE_WITHDRAW_ADDRESS,
    KYC,
    CREATE_API_KEY,
    MODIFY_API_KEY,
    DELETE_API_KEY,
    WITHDRAW,

    /*
     * 通知类的业务类型
     */
    REGISTER_SUCCESS,
    LOGIN_SUCCESS,
    RESET_PASSWORD_SUCCESS,
    FIND_PASSWORD_SUCCESS,
    CREATE_ADDRESS_SUCCESS,
    DELETE_ADDRESS_SUCCESS,
    BIND_EMAIL_SUCCESS,
    BIND_MOBILE_SUCCESS,
    BIND_GA_SUCCESS,
    CREATE_API_KEY_SUCCESS,
    WITHDRAW_SUCCESS_WITH_DETAIL,
    WITHDRAW_VERIFY_REJECTED,
    WITHDRAW_VERIFY_REJECTED_1,
    WITHDRAW_VERIFY_REJECTED_2,
    WITHDRAW_VERIFY_REJECTED_3,
    WITHDRAW_VERIFY_REJECTED_4,
    WITHDRAW_VERIFY_REJECTED_5,
    DEPOSIT_SUCCESS,
    KYC_VERIFY_REJECTED,
    KYC_VERIFY_SUCCESS,
    CREATE_FUND_PWD,
    MODIFY_FUND_PWD,
    ALTER_EMAIL_SUCCESS,//换绑邮箱成功
    ALTER_MOBIL_SUCCESS,//换绑手机成功
    ALTER_GA_SUCCESS,//换绑GA成功

    AIRDROP_NOTICE, //空投成功后的消息通知
    SPOT_BUY_TRADE_SUCCESS,
    SPOT_SELL_TRADE_SUCCESS,
    CONTRACT_BUY_OPEN_TRADE_SUCCESS,
    CONTRACT_BUY_CLOSE_TRADE_SUCCESS,
    CONTRACT_SELL_OPEN_TRADE_SUCCESS,
    CONTRACT_SELL_CLOSE_TRADE_SUCCESS,
    SYMBOL_TASK_FAILED, //币对定时任务失败通知
    ANTI_PHISHING_CODE_SUCCESS, //防钓鱼码设置成功

    // 买单，下单成功（买方消息）
    BUY_CREATE_MSG_TO_BUYER,
    // 买单，下单成功（卖方消息）"
    BUY_CREATE_MSG_TO_SELLER,
    // 买单，下单成功（买方消息）
    BUY_CREATE_MSG_TO_BUYER_TIME,
    // 买单，下单成功（卖方消息）"
    SELL_CREATE_MSG_TO_BUYER_TIME,
    // 支付成功（买方消息）
    PAY_MSG_TO_BUYER,
    // 支付成功（卖方消息）
    PAY_MSG_TO_SELLER,
    // 买方投诉卖方，申诉（买方消息）
    BUY_APPEAL_MSG_TO_BUYER,
    // 买方投诉卖方，申诉（卖方消息）
    BUY_APPEAL_MSG_TO_SELLER,
    // 撤单（买方消息）
    CANCEL_MSG_TO_BUYER,
    // 撤单（卖方消息）
    CANCEL_MSG_TO_SELLER,
    // 放币完结（买方消息）
    FINISH_MSG_TO_BUYER,
    // 放币完结（卖方消息）
    FINISH_MSG_TO_SELLER,

    // 卖单，下单成功（买方消息）
    SELL_CREATE_MSG_TO_BUYER,
    // 卖单，下单成功（卖方消息）
    SELL_CREATE_MSG_TO_SELLER,
    // 卖方投诉买方，申诉（买方消息）
    SELL_APPEAL_MSG_TO_BUYER,
    // 卖方投诉买方，申诉（卖方消息）
    SELL_APPEAL_MSG_TO_SELLER,
    // 超时订单自动取消
    ORDER_AUTO_CANCEL,
    // 确认放币超时订单自动申诉（买方消息）
    ORDER_AUTO_APPEAL_TO_BUYER,
    // 确认放币超时订单自动申诉（卖方消息）
    ORDER_AUTO_APPEAL_TO_SELLER,

    // 期货爆仓通知消息类型

    // 风险过高警告
    LIQUIDATION_ALERT,
    // 强平通知
    LIQUIDATED_NOTIFY,
    // 强制减仓通知（对手仓）
    ADL_NOTIFY,

    // 广告剩余数量较小自动下架
    ITEM_AUTO_OFFLINE_SMALL_QUANTITY,

    //杠杆预警通知
    MARGIN_WARN,
    //杠杆追加通知
    MARGIN_APPEND,
    //杠杆强平通知
    MARGIN_LIQUIDATION_ALERT,
    //杠杆强平结束通知
    MARGIN_LIQUIDATED_NOTIFY,
    //管理员通知
    ADMIN_LIQUIDATION_ALERT,
    //强平结束管理员通知
    ADMIN_LIQUIDATED_NOTIFY,
    //完全强平结束更新初始保证金
    LIQUIDATED_TO_UPDATE_MARGIN,

    ADMIN_KYC_VERIFY_SUC,
    ADMIN_KYC_VERIFY_FAIL,
    REALTIME_UP_NOTIFY,
    REALTIME_DOWN_NOTIFY,

    // BROKER ADMIN
    // 审批知通
    BROKER_ADMIN_FLOW_AUDIT_NOTICE,
    // 审批完成知通
    BROKER_ADMIN_FLOW_AUDIT_FINISH_NOTICE,

    JOIN_HOBBIT_LEADER, //成为队长
    QUIT_HOBBIT_LEADER, //取消队长
    QUITING_HOBBIT_LEADER, //队长退出中，1小时倒计时

    // 合规kyc通过
    COMPLIANCE_KYC_VERIFY_SUC,
    // 合规kyc拒绝
    COMPLIANCE_KYC_VERIFY_FAIL,
    // 合规用户不活跃
    COMPLIANCE_INACTIVE_USER,

}
