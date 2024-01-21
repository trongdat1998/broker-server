package io.bhex.broker.server.domain;

/**
 * @Description: base config 对应的 group 或 key的常量值
 * @Date: 2019/12/4 下午3:52
 * @Author: liwei
 * @Copyright（C）: 2018 BlueHelix Inc. All rights reserved.
 */
public class BaseConfigConstants {

    public static final String FROZEN_USER_COIN_TRADE_GROUP = "frozen.user.coin.trade";

    public static final String FROZEN_USER_API_COIN_TRADE_GROUP = "frozen.user.api.coin.trade";

    public static final String FROZEN_USER_OPTION_TRADE_GROUP = "frozen.user.option.trade";

    public static final String FROZEN_USER_FUTURE_TRADE_GROUP = "frozen.user.future.trade";

    public static final String FROZEN_USER_OTC_TRADE_GROUP = "frozen.user.otc.trade";

    public static final String FROZEN_USER_BONUS_TRADE_GROUP = "frozen.user.bonus.trade";
    public static final String FROZEN_USER_MARGIN_TRADE_GROUP = "frozen.user.margin.trade";
    //提现必须人工审核
    public static final String FORCE_AUDIT_USER_WITHDRAW_GROUP = "force.audit.user.withdraw";

    //此功能相当于灰名单，如果有用户配置了交易币对必须是配置中的才放行 否则禁止交易，风控使用
    public static final String ALLOW_TRADING_SYMBOLS_GROUP = "allow.trading.symbols";
    //风控禁止用户交易，分开平买卖， 如果交易币对在其中则禁止相应的交易
    public static final String DISABLE_USER_TRADING_SYMBOLS_GROUP = "disable.user.trading.symbols";


    public static final String WHOLE_SITE_CONTROL_SWITCH_GROUP = "site.control.switch"; //全站整体开关配置
    public static final String STOP_REGISTER_KEY = "stop.register";
    public static final String STOP_LOGIN_KEY = "stop.login";
    public static final String STOP_COIN_TRADE_KEY = "stop.coin.trade";
    public static final String STOP_OTC_TRADE_KEY = "stop.otc.trade";
    public static final String STOP_OPTION_TRADE_KEY = "stop.option.trade";
    public static final String STOP_FUTURE_TRADE_KEY = "stop.future.trade";
    public static final String STOP_WITHDRAW_KEY = "stop.withdraw";
    public static final String WHOLE_SITE_FORCE_AUDIT_WITHDRAW_KEY = "force.audit.withdraw"; //全站提现转人工审核
    public static final String STOP_API_COIN_KEY = "stop.api.coin.trade";
    public static final String STOP_API_FUTURE_KEY = "stop.api.future.trade";
    public static final String OPEN_KYC_AGE_CHECK_KEY  = "open.kyc.age.check";

    public static final String APP_DOWNLOAD_URL_GROUP = "app.download.url";
    public static final String TESTFIGHT_URL = "testfight.download.url";
    public static final String APP_STORE_URL = "appstore.download.url";
    public static final String GOOGLE_PLAY_URL = "googleplay.download.url";

    //
    public static final String CUSTOM_CONFIG_GROUP = "custom.config.group";
    public static final String INDEX_NEW_VERSION = "index.new.version"; //首页新版本开关

    public static final String USER_LEVEL_CONFIG_GROUP = "user.level.config";

    public static final String HOBBIT_CONFIG_GROUP = "hobbit.config";

    public static final String APPPUSH_CONFIG_GROUP = "app.push.config";

    public static final String OPEN_API_COIN_SYMBOL_WHITE_GROUP = "open.api.coin.symbol.white"; //open api 交易币对白名单

}
