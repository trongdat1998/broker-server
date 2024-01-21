package io.bhex.broker.server.domain;

/**
 * 消息类型
 *
 * @author lizhen
 * @date 2018-09-14
 */
public enum OTCMsgCode {

    BUY_CREATE_MSG_TO_BUYER(1010, "买单，下单成功（买方消息）", NoticeBusinessType.BUY_CREATE_MSG_TO_BUYER),

    BUY_CREATE_MSG_TO_SELLER(1011, "买单，下单成功（卖方消息）", NoticeBusinessType.BUY_CREATE_MSG_TO_SELLER),


    BUY_CREATE_MSG_TO_BUYER_TIME(1012, "买单，下单成功（买方消息）", NoticeBusinessType.BUY_CREATE_MSG_TO_BUYER_TIME),

    SELL_CREATE_MSG_TO_BUYER_TIME(1013, "买单，下单成功（卖方消息）", NoticeBusinessType.SELL_CREATE_MSG_TO_BUYER_TIME),

    //没有此模板的消息，暂时不发送了
    //PAY_MSG_TO_BUYER(1020, "支付成功（买方消息）", NoticeBusinessType.PAY_MSG_TO_BUYER),

    PAY_MSG_TO_SELLER(1021, "支付成功（卖方消息）", NoticeBusinessType.PAY_MSG_TO_SELLER),

    BUY_APPEAL_MSG_TO_BUYER(1030, "买方投诉卖方，申诉（买方消息）", NoticeBusinessType.BUY_APPEAL_MSG_TO_BUYER),

    BUY_APPEAL_MSG_TO_SELLER(1031, "买方投诉卖方，申诉（卖方消息）", NoticeBusinessType.BUY_APPEAL_MSG_TO_SELLER),

    CANCEL_MSG_TO_BUYER(1040, "撤单（买方消息）", NoticeBusinessType.CANCEL_MSG_TO_BUYER),

    CANCEL_MSG_TO_SELLER(1041, "撤单（卖方消息）", NoticeBusinessType.CANCEL_MSG_TO_SELLER),

    FINISH_MSG_TO_BUYER(1050, "放币完结（买方消息）", NoticeBusinessType.FINISH_MSG_TO_BUYER),

    FINISH_MSG_TO_SELLER(1051, "放币完结（卖方消息）", NoticeBusinessType.FINISH_MSG_TO_SELLER),


    SELL_CREATE_MSG_TO_BUYER(2010, "卖单，下单成功（买方消息）", NoticeBusinessType.SELL_CREATE_MSG_TO_BUYER),

    SELL_CREATE_MSG_TO_SELLER(2011, "卖单，下单成功（卖方消息）", NoticeBusinessType.SELL_CREATE_MSG_TO_SELLER),

    SELL_APPEAL_MSG_TO_BUYER(2030, "卖方投诉买方，申诉（买方消息）", NoticeBusinessType.SELL_APPEAL_MSG_TO_BUYER),

    SELL_APPEAL_MSG_TO_SELLER(2031, "卖方投诉买方，申诉（卖方消息）", NoticeBusinessType.SELL_APPEAL_MSG_TO_SELLER),

    ORDER_AUTO_CANCEL(3040, "超时订单自动取消", NoticeBusinessType.ORDER_AUTO_CANCEL),

    ORDER_AUTO_APPEAL_TO_BUYER(3030, "确认放币超时订单自动申诉（买方消息）", NoticeBusinessType.ORDER_AUTO_APPEAL_TO_BUYER),

    ORDER_AUTO_APPEAL_TO_SELLER(3031, "确认放币超时订单自动申诉（卖方消息）", NoticeBusinessType.ORDER_AUTO_APPEAL_TO_SELLER),

    ITEM_AUTO_OFFLINE_SMALL_QUANTITY(5001, "广告剩余数量较小自动下架", NoticeBusinessType.ITEM_AUTO_OFFLINE_SMALL_QUANTITY);


    private int code;

    private String name;

    private NoticeBusinessType noticeBusinessType;

    OTCMsgCode(int code, String name, NoticeBusinessType noticeBusinessType) {
        this.code = code;
        this.name = name;
        this.noticeBusinessType = noticeBusinessType;
    }


    public static OTCMsgCode fromCode(int code) {
        for (OTCMsgCode msgCode : OTCMsgCode.values()) {
            if (msgCode.getCode() == code) {
                return msgCode;
            }
        }
        return null;
    }

    public int getCode() {
        return code;
    }

    public String getName() {
        return name;
    }

    public NoticeBusinessType getNoticeBusinessType() {
        return noticeBusinessType;
    }
}
