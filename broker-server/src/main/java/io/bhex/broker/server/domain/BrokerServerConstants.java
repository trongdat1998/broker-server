/**********************************
 *@项目名称: broker-parent
 *@文件名称: io.bhex.broker.domain
 *@Date 2018/7/9
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
package io.bhex.broker.server.domain;

import com.google.common.collect.Lists;

import java.math.BigDecimal;
import java.util.List;

import io.prometheus.client.Counter;

public class BrokerServerConstants {

    public static final String MASK_EMAIL_REG = "(?<=.).(?=[^@]*?.@)";
    public static final String MASK_MOBILE_REG = "(\\d{3})\\d{4}(\\d{4})";

    public static final List<String> FX_RATE_BASE_TOKENS = Lists.newArrayList("BTC", "USDT", "ETH");

    public static final String LOGIN_WHITE_MOBILE_COMMON_INI_KEY = "login_mobile_white_list";
    public static final String LOGIN_WHITE_EMAIL_COMMON_INI_KEY = "login_email_white_list";
    public static final String LOGIN_WHITE_USER_COMMON_INI_KEY = "login_user_white_list";


    /**
     * Count username login errors
     */
    public static final String LOGIN_ERROR_USERNAME_COUNT = "login_error_username_count_%s_%s";

    public static final String FIND_PWD_24H_ERROR_USERNAME_COUNT = "find_pwd_24_error_username_count_%s_%s";
    /**
     * Count user_id login errors
     */
    public static final String LOGIN_ERROR_USER_ID_COUNT = "login_error_user_id_count_%s_%s";
    /**
     * frozen username
     */
    public static final String FROZEN_LOGIN_USERNAME_KEY = "frozen_username_%s_%s";

    public static final String FROZEN_FIND_PWD_USERNAME_KEY = "frozen_find_pwd_%s_%s";
    /**
     * bind ga status
     */
    public static final Integer BIND_GA_SUCCESS = 1;

    /**
     * bind trade pwd status
     */
    public static final int BIND_TRADE_PWD_SUCCESS = 1;

    /**
     * if user login success but need two step auth, mark request_token and user_id
     */
    public static final String LOGIN_USER_ID = "login_user_id_";
    /**
     * if user login success but need two step auth, mark request_token and login_id
     */
    public static final String LOGIN_RECORD_ID = "login_record_id_";
    /**
     * if user login success but need two step auth, mark request_token and success_token
     */
    public static final String LOGIN_SUCCESS_TOKEN = "login_success_token_";
    /**
     * if some operations require two steps to complete, and first step is success, Keep the
     * effective time of the first step operation success
     */
    public static final Integer PRE_REQUEST_EFFECTIVE_MINUTES = 2 * 60;

    public static final String FIND_PWD_USER_ID = "find_pwd_user_id_";

    public static final int LOGIN_SUCCESS = 1;
    public static final int LOGIN_FAILED = 2;
    public static final int LOGIN_INTERRUPT = 3;
    public static final int SCAN_LOGIN_QRCODE_SUCCESS = 4;
    public static final int QRCODE_UNAUTHORIZED_LOGIN = 5;
    public static final int QRCODE_AUTHORIZED_LOGIN = 6;

    public static final Integer ALLOW_STATUS = 1;
    public static final Integer ONLINE_STATUS = 1;

    public static final int NEED_PREVIEW_CHECK = 1;

    public static final double[] CONTROLLER_TIME_BUCKETS = new double[]{
            5, 10, 20, 50, 75, 100, 200, 500, 1000, 2000, 8000
    };

//    public static final Locale VI_VN = new Locale("vi", "VN");

    /**
     * 身份证件的Id, 必须是1 ！！！
     */
//    public static final ImmutableTable<Integer, String, String> KYC_CARD_TYPE_TABLE =
//            new ImmutableTable.Builder<Integer, String, String>()
//                    .put(1, Locale.CHINA.toString(), "身份证件")
//                    .put(1, Locale.US.toString(), "ID card")
//                    .put(1, Locale.JAPAN.toString(), "IDカード")
//                    .put(1, Locale.KOREA.toString(), "신분증")
//                    .put(1, VI_VN.toString(), "Chứng minh nhân dân")
//
//                    .put(2, Locale.CHINA.toString(), "驾照")
//                    .put(2, Locale.US.toString(), "Driver license")
//                    .put(2, Locale.JAPAN.toString(), "運転免許証")
//                    .put(2, Locale.KOREA.toString(), "운전 면허증")
//                    .put(2, VI_VN.toString(), "Bằng lái xe")
//
//                    .put(3, Locale.CHINA.toString(), "护照")
//                    .put(3, Locale.US.toString(), "Passport")
//                    .put(3, Locale.JAPAN.toString(), "パスポート")
//                    .put(3, Locale.KOREA.toString(), "여권")
//                    .put(3, VI_VN.toString(), "chiếu")
//
//                    .put(5, Locale.CHINA.toString(), "其他")
//                    .put(5, Locale.US.toString(), "Other")
//                    .put(5, Locale.JAPAN.toString(), "その他の")
//                    .put(5, Locale.KOREA.toString(), "기타")
//                    .put(5, VI_VN.toString(), "Khác")
//                    .build();

    public static final String INVITE_RANK_MONTH_KEY = "invite_rank_month";
    public static final String INVITE_RANK_KEY = "invite_rank:";
    public static final String INVITE_LEVEL_KEY = "invite_level_key:";

    public static final String MASK_IP_REG = "^(\\d{1,3})(\\.\\d{1,3})(\\.\\d{1,3})(\\.\\d{1,3})$";

    public static final String CHECK_TRADE_WHITE_KEY = "_checkTradeWhite";
    public static final String CHECK_WITHDRAW_WHITE_KEY = "_checkWithdrawWhite";

    public static final String TRADE_WHITE_CONFIG = "_TRADE_WHITE_CONFIG";
    public static final String WITHDRAW_WHITE_CONFIG = "_WITHDRAW_WHITE_CONFIG";
    public static final String WITHDRAW_BLACK_CONFIG = "_WITHDRAW_BLACK_CONFIG";


    public static final String AUTO_AIRDROP_RUNNING_KEY = "auto_airdrop_running_key:";
    public static final String INVITE_FEEBACK_ACTIVITY_KEY = "invite_feeback_activity_key:";

    public static final String FIRST_WITHDRAW_NEED_KYC_CONFIG = "first_withdraw_need_kyc";
    public static final String WITHDRAW_NEED_KYC_CONFIG = "withdraw_need_kyc";

    public static final String FIRST_WITHDRAW_BROKER_AUDIT_CONFIG = "first_withdraw_broker_audit";
    public static final String WITHDRAW_BROKER_AUDIT_CONFIG = "withdraw_broker_audit";
    public static final String TOTAL_WITHDRAW_BROKER_AUDIT_CONFIG = "total_withdraw_broker_audit";
    public static final String TOTAL_WITHDRAW_BROKER_AUDIT_CONFIG_30D = "total_withdraw_broker_audit_30d";
    public static final String RISK_CONTROL_TOTAL_WITHDRAW_BROKER_AUDIT_CONFIG_24H = "risk_control_total_withdraw_broker_audit_24h";


    /**
     * 24小时累计提币X笔的时候需要检查是否需要KYC
     */
    public static final String WITHDRAW_CHECK_ID_CARD_NO_NUMBER_CONFIG = "withdraw_check_id_card_number";
    public static final int DEFAULT_WITHDRAW_CHECK_ID_CARD_NO_NUMBER = 5;

    /**
     * 24小时累计提币X笔，并且提现金额大于该值的时候需要进行KYC
     */
    public static final String WITHDRAW_CHECK_ID_CARD_NO_CONFIG = "withdraw_check_id_card";

    public static final long MILLISECONDS_WITHIN_24H = 24 * 3600 * 1000; // 24小时
    public static final long WITHDRAW_PRE_APPLY_EFFECTIVE_MILLISECONDS = 1800 * 1000; // 30分钟

    public static final String COMMON_POINT_CARD = "BHEX_COMMON_CARD";

    public static final BigDecimal NEGATIVE_ONE = new BigDecimal(-1);
    public static final int USDT_PRECISION = 2;
    public static final int BTC_PRECISION = 8;
    // 期权精度
    public static final Integer BASE_OPTION_PRECISION = 8;
    public static final Integer OPTION_QUANTITY_PRECISION = 3;
    public static final Integer OPTION_PRICE_PRECISION = 2;
    public static final Integer OPTION_PROFIT_PRECISION = 2;
    public static final Integer OPTION_STRIKE_PRICE_PRECISION = 2;
    public static final Integer OPTION_VOLUME_PRECISION = 3;
    public static final Integer OPTION_SETTLEMENT_PRICE_PRECISION = 2;

    // 期货数量精度
    public static final Integer FUTURES_QUANTITY_PRECISION = 3;

    // 期货杠杆精度
    public static final Integer FUTURES_LEVERAGE_PRECISION = 2;

    // 期货保证金率精度
    public static final Integer FUTURES_MARGIN_RATE_PRECISION = 4;

    // 期货仓位多仓方向i18n
    public static final String FUTURES_I18N_POSITION_LONG_KEY = "FUTURES_POSITION_LONG_TEXT";

    // 期货仓位空仓方向i18n
    public static final String FUTURES_I18N_POSITION_SHORT_KEY = "FUTURES_POSITION_SHORT_TEXT";

    //期权默认分页大小
    public static final int ONE_PAGE_MAX_COUNT = 8;

    // 历史期权 pid page id; s size; oid orgId
    public static final String HISTORY_OPTION_LIST_KEY = "history_option_key:pid_%s_s_%s_oid_%s";

    public static final String ACTIVITY_UPDATE_LOCK_KEY = "activity_update_lock_interest:pid_%s";
    public static final String ACTIVITY_USER_LIMIT_KEY = "activity_user_limit:pid_%s";
    public static final String ACTIVITY_WHITE_LIST = "activity_white_list:pid_%s";

    public static final String CONTRACT_FINANCING_KEY = "contractFinancing";


    //30日队长达标成绩
    public static final BigDecimal HOBBIT_MONTHLY_KPI_HBC = new BigDecimal(8.33);

    //用户行为记录
    public static final String ACTION_TYPE_UPDATE_PASSWORD = "updatePassword";
    public static final String ACTION_TYPE_SET_PASSWORD = "setPassword";
    public static final String ACTION_TYPE_BIND_EMAIL = "bindEmail";
    public static final String ACTION_TYPE_BIND_MOBILE = "bindMobile";
    public static final String ACTION_TYPE_BINDGA = "bindGA";
    public static final String ACTION_TYPE_FIND_PWD = "findPwd";
    public static final String ACTION_TYPE_CREATE_API = "createApiKey";
    public static final String ACTION_TYPE_DELETE_API = "deleteApiKey";
    public static final String ACTION_TYPE_SET_TRADE_PWD = "setTradePassword";
    public static final String ACTION_TYPE_SET_OTC_NICKNAME = "setOtcNickName";
    public static final String ACTION_TYPE_ADD_OTC_PAYMENT = "addOtcPayment";
    public static final String ACTION_TYPE_DELETE_OTC_PAYMENT = "deleteOtcPayment";
    public static final String ACTION_TYPE_CREATE_SUB_ACCOUNT = "createSubAccount";
    public static final String ACTION_TYPE_CREATE_PURCHASE_FINANCE = "purchaseFinance";
    public static final String ACTION_TYPE_ALTER_GA = "alterGa";
    public static final String ACTION_TYPE_ALTER_MOBILE = "alterMobile";
    public static final String ACTION_TYPE_ALTER_EMAIL = "alterEmail";
    public static final String ACTION_TYPE_UNBIND_GA = "userUnbindGA";
    public static final String ACTION_TYPE_UNBIND_MOBILE = "userUnbindMobile";
    public static final String ACTION_TYPE_UNBIND_EMAIL = "userUnbindEmail";

    public static final String SUB_ACCOUNT_AUTHORIZED_ORG_KEY = "subAccountAuthorizedOrg";
    public static final String ORG_CENTRAL_COUNTER_PARTY = "orgCentralCounterParty";
    public static final String ORG_OBTAIN_OPENAPI_SECRETKEY = "orgObtainOpenApiSecretKey";

    public static final String TRANSFER_LIMIT_LOCK = "transfer_limit_lock_%s_%s_%s";
    public static final String TRANSFER_USER_DAY_QUOTA = "transfer_user_day_quota_%s_%s_%s_%s";

    public static final Counter CREATE_ORDER_COUNTER = Counter.build()
            .namespace("order")
            .subsystem("create_order")
            .name("create_order_invoke_count")
            .labelNames("org_id", "order_type", "platform")
            .help("Total number of http request started")
            .register();

    public static final Counter SUCCESS_CREATE_ORDER_COUNTER = Counter.build()
            .namespace("order")
            .subsystem("create_order")
            .name("success_create_order_invoke_count")
            .labelNames("org_id", "order_type", "platform")
            .help("Total number of http request started")
            .register();

    public static final String SPOT_ORDER_TYPE = "spot";
    public static final String OPTION_ORDER_TYPE = "option";
    public static final String FUTURES_ORDER_TYPE = "futures";

    public static final String MARGIN_WITHDRAW_LOCK = "margin_withdraw_lock_%s_%s";
}
