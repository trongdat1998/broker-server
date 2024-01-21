package io.bhex.broker.server.domain.staking;

import java.math.BigDecimal;

/**
 * Staking constant
 * @author songxd
 * @date
 */
public class StakingConstant {

    /**
     * 默认计算精度
     */
    public static final Integer DEFAULT_SCALE = 18;

    /**
     * 派息计算精度
     */
    public static final Integer INTEREST_SCALE = 8;

    /**
     * 利率精度
     */
    public static final Integer RATE_SCALE = 2;

    /**
     * 计息天数/年 new BigDecimal(java.time.LocalDate.now().lengthOfYear())
     */
    public static final BigDecimal DAY_OF_YEAR = new BigDecimal("365");

    /**
     * Staking Subscribe Lock Key
     */
    public static final String STAKING_PRODUCT_LOCK_PREFIX = "staking_product_lock_prefix_%s_%s";

    /**
     * Staking Subscribe Lock Expire
     */
    public static final Integer STAKING_PRODUCT_LOCK_EXPIRE_SECOND= 30 * 1000;

    /**
     * Staking Subscribe Result Code
     */
    public static final String SUBSCRIBE_STATUS_CODE_PREFIX = "staking_result_%s_%s_%s_%s";

    /**
     * Staking Redeem Result Code
     */
    public static final String REDEEM_STATUS_CODE_PREFIX = "staking_redeem_result_%s_%s_%s_%s";

    /**
     * 申购和赎回处理状态缓存时间
     */
    public static final Integer STAKING_STATUS_CACHE_EXPIRE_MIN = 60;

    /**
     * Staking MQ Topic
     */
    public static final String STAKING_MESSAGE_TOPIC = "staking_product_message_topic";

    /**
     * Staking Subscribe Tag
     */
    public static final String STAKING_SUBSCRIBE_TAG = "subscribe_tag";

    /**
     * Staking order fields
     */
    public static final String CAN_AUTO_RENEW = "can_auto_renew";

    /**
     * batch list size
     */
    public static final Integer BATCH_LIST_SIZE = 200;

    public static final String STAKING_TRANSFER_TAG = "transfer_tag";

    public static final String STAKING_REDEEM_TAG = "redeem_tag";

    public final static String STAKING_CONSUMER_GROUP_NAME = "staking_consumer_group_name_%s";

    /**
     * 赎回Lock Key
     */
    public static final String STAKING_REDEEM_LOCK_KEY = "staking_redeem_lock_key_%s_%s_%s_%s";
    /**
     * 赎回Lock Key
     */
    public static final String STAKING_REDEEM_EVENT_LOCK_KEY = "staking_redeem_event_lock_key_%s_%s_%s_%s";

    /**
     * 订单事件Lock Key
     */
    public static final String STAKING_SUBSCRIBE_EVENT_LOCK_KEY = "staking_subscribe_event_lock_key_%s_%s_%s_%s";

    public static final Integer STAKING_REDEEM_LOCK_EXPIRE_SECOND = 60 * 1000;

    /**
     * 流水号缓存Key
     */
    public static final String STAKING_TRANSFER_CACHE_KEY = "staking_transfer_id_%s_%s_%s_%s";

    /**
     * 申购流水号缓存过期时间
     */
    public static final Integer STAKING_SUBSCRIBE_TRANSFER_EXPIRE_DAY = 1;

    public static final String STAKING_CURRENT_CALC_INTEREST_LOCK = "staking_current_calc_lock_%s";
    public static final Integer STAKING_CURRENT_CALC_INTEREST_LOCK_EXPIRE_MIN = 10 * 60 * 1000;

    public static final String STAKING_SUBSCRIBE_LOCK = "staking_subscribe_key_%s_%s_%s";
    /**
     * 申购锁过期时间
     */
    public static final Integer STAKING_SUBSCRIBE_LOCK_EXPIRE = 10 * 1000;
    /**
     * 定时任务处理遗漏订单锁
     */
    public static final String STAKING_SUBSCRIBE_TASK_LOCK = "staking_subscribe_task_lock_%s";

    /**
     * 定时任务处理申购失败订单锁
     */
    public static final String STAKING_SUBSCRIBE_FAIL_TASK_LOCK = "staking_subscribe_fail_task_lock_%s";

    /**
     * 定时任务处理遗漏订单锁过期时间
     */
    public static final Integer STAKING_SUBSCRIBE_TASK_LOCK_EXPIRE = 10 * 60 * 1000;

    /**
     * 重新计算利息锁
     */
    public static final String STAKING_RE_CALC_INTEREST_LOCK = "staking_re_calc_interest_lock_%s_%s_%s";
    /**
     * 重新计算利息锁过期时间
     */
    public static final Integer STAKING_RE_CALC_INTEREST_LOCK_EXPIRE = 10 * 60 * 1000;

    /**
     * 活期默认保存7条可计息配置
     */
    public static final Integer STAKING_CURRENT_PRODUCT_REBATE_DEFAULT_COUNT = 7;

    /**
     * 派息Lock
     */
    public static final String STAKING_PRODUCT_DIVIDEND_LOCK_KEY = "Staking_Product_Order_Dividend_Lock_Key_%s_%s_%s";


    public static final String STAKING_CURRENT_RETURN_PRINCIPAL_LOCK_KEY = "staking_current_return_principal_lock_key_%s";

    public static final String STAKING_CURRENT_AUTO_DIVIDEND_LOCK_KEY = "staking_current_auto_dividend_lock_key_%s";

}
