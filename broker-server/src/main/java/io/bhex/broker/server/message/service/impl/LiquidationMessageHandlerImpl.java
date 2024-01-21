package io.bhex.broker.server.message.service.impl;

import com.google.common.collect.Maps;
import com.google.gson.JsonObject;
import io.bhex.base.common.CommonNotifyMessage;
import io.bhex.base.common.LiquidationMessage;
import io.bhex.base.proto.DecimalUtil;
import io.bhex.base.token.SymbolDetail;
import io.bhex.broker.grpc.common.Header;
import io.bhex.broker.server.domain.NoticeBusinessType;
import io.bhex.broker.server.grpc.server.service.BasicService;
import io.bhex.broker.server.grpc.server.service.NoticeTemplateService;
import io.bhex.broker.server.grpc.server.service.PushDataService;
import io.bhex.broker.server.grpc.server.service.UserService;
import io.bhex.broker.server.grpc.server.service.po.FuturesMessage;
import io.bhex.broker.server.message.service.MessageHandleException;
import io.bhex.broker.server.message.service.MessageHandler;
import io.bhex.broker.server.message.service.NotifyUserStrategy;
import io.bhex.broker.server.model.Account;
import io.bhex.broker.server.model.Internationalization;
import io.bhex.broker.server.model.LoginLog;
import io.bhex.broker.server.model.User;
import io.bhex.broker.server.primary.mapper.AccountMapper;
import io.bhex.broker.server.primary.mapper.InternationalizationMapper;
import io.bhex.broker.server.primary.mapper.LoginLogMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.Map;

import static io.bhex.broker.server.domain.BrokerServerConstants.FUTURES_I18N_POSITION_LONG_KEY;
import static io.bhex.broker.server.domain.BrokerServerConstants.FUTURES_I18N_POSITION_SHORT_KEY;

@Slf4j
@Service("TRADE_FUTURES_LIQUIDATION_MESSAGE")
public class LiquidationMessageHandlerImpl implements MessageHandler {

    @Resource
    private NoticeTemplateService noticeTemplateService;

    @Resource
    private UserService userService;

    @Resource
    private BasicService basicService;

    @Resource
    private AccountMapper accountMapper;

    @Resource
    private LoginLogMapper loginLogMapper;

    @Resource
    private InternationalizationMapper internationalizationMapper;

    @Resource
    private PushDataService pushDataService;

    private static final String DEFAULT_ZH_CN_LONG_TEXT = "多仓";
    private static final String DEFAULT_ZH_CN_SHORT_TEXT = "空仓";
    private static final String DEFAULT_EN_US_LONG_TEXT = "Long Position";
    private static final String DEFAULT_EN_US_SHORT_TEXT = "Short Position";

    private static final String LANG_ZH_CN = "zh_CN";
    private static final String LANG_EN_US = "en_US";

    @Override
    public void handleMessage(CommonNotifyMessage notifyMessage) {
        if (!notifyMessage.hasLiquidationMessage()) {
            throw new MessageHandleException(String.format("notifyMessage id:%s type:%s has no LiquidationMessage.",
                    notifyMessage.getMessageId(), notifyMessage.getType()));
        }

        try {
            handleLiquidationMessage(notifyMessage);
        } catch (Throwable e) {
            throw new MessageHandleException("handleLiquidationMessage error.", e);
        }
    }

    private void handleLiquidationMessage(CommonNotifyMessage notifyMessage) {
        if (!notifyMessage.hasLiquidationMessage()) {
            return;
        }

        LiquidationMessage message = notifyMessage.getLiquidationMessage();
        Account account = accountMapper.getAccountByAccountId(message.getAccountId());
        if (account == null) {
            log.error("sendLiquidationMessage: can not find account by accountId: {}", message.getAccountId());
            return;
        }

        User user = userService.getUser(account.getUserId());
        if (user == null) {
            log.error("sendLiquidationMessage: can not find user by userId: {}", account.getUserId());
            return;
        }

        // 获取语言环境
        String language = getUserLanguage(user);

        // 仓位方向
        String isLongText = getPositionSideText(message, notifyMessage.getOrgId(), language);

        // 币对名称
        String symbolName = getSymbolI18nName(message.getSymbolId(), language);

        // 保证金率处理
        String marginRatePercent = DecimalUtil.toBigDecimal(message.getMarginRate())
                .multiply(new BigDecimal("100"))
                .setScale(2, RoundingMode.UP)
                .stripTrailingZeros()
                .toPlainString() + "%";

        log.info("handleLiquidationMessage push message userId {}", user.getUserId());
        if (user.getIsVirtual() == 0) {
            switch (message.getType()) {
                case LIQUIDATION_ALERT:
                    sendLiquidationAlertMessage(user, language, symbolName, isLongText, marginRatePercent, message.getSymbolId(), notifyMessage.getMessageId());
                    log.info("sendLiquidationAlertMessage ok. userId:{} language: {}", user.getUserId(), language);
                    break;
                case LIQUIDATED_NOTIFY:
                    sendLiquidateionNotifyMessage(user, language, symbolName, isLongText, marginRatePercent, message.getSymbolId(), notifyMessage.getMessageId());
                    log.info("sendLiquidateionNotifyMessage ok. userId:{} language: {}", user.getUserId(), language);
                    break;
                case ADL_NOTIFY:
                    sendADLNotifyMessage(user, language, symbolName, isLongText, message.getSymbolId(), notifyMessage.getMessageId());
                    log.info("sendADLNotifyMessage ok. userId:{} language: {}", user.getUserId(), language);
                    break;
                default:
                    log.error("sendLiquidationMessage error: Unkonwn message type: {}", message.getType());
            }
        } else {
            try {
                log.info("Liquidation message userId {} symbolName {} isLongText {} marginRatePercent {},messageId {} ", user.getUserId(), symbolName, isLongText, marginRatePercent, notifyMessage.getMessageId());
                SymbolDetail symbol = basicService.getSymbolDetailFuturesByOrgIdAndSymbolId(notifyMessage.getOrgId(), message.getSymbolId());
                FuturesMessage futuresMessage = new FuturesMessage();
                futuresMessage.setIsLong(symbol.getIsReverse() != message.getIsLong());//true 多仓 空仓
                futuresMessage.setMessageId(notifyMessage.getMessageId());
                futuresMessage.setMarginRatePercent(marginRatePercent);
                futuresMessage.setSymbolId(symbol.getSymbolId());
                futuresMessage.setAccountId(message.getAccountId());
                futuresMessage.setOrgId(notifyMessage.getOrgId());
                futuresMessage.setLiquidationType(message.getType().name());
                futuresMessage.setUserId(user.getUserId());
                pushDataService.futuresMessagePush(notifyMessage.getOrgId(), futuresMessage);
            } catch (Exception ex) {
                log.error("Create futures message fail ex {}", ex);
            }
        }
    }

    public boolean isTestSymbol(long orgId, String symbolId) {
        SymbolDetail symbolDetail = basicService.getSymbolDetailFuturesByOrgIdAndSymbolId(orgId, symbolId);
        if (symbolDetail == null) {
            return true;
        }
        return symbolDetail.getIsTest();
    }

    /**
     * 发送强平警告
     *
     * @param user       用户实体
     * @param language   多语言
     * @param symbolName 币对名称（要支持多语言）
     * @param isLongText 多空仓标识（要支持多语言）
     * @param marginRate 保证金率
     */
    private void sendLiquidationAlertMessage(User user, String language, String symbolName,
                                             String isLongText, String marginRate, String symbolId, long messageId) {
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("symbolName", symbolName);
        jsonObject.addProperty("isLongText", isLongText);
        jsonObject.addProperty("marginRate", marginRate);

        Map<String, String> pushUrlData = Maps.newHashMap();
        pushUrlData.put("SYMBOL_ID", symbolId);
        NotifyUserStrategy notifyStrategy = getNotifyStrategy(user);
        boolean isTestSymbol = isTestSymbol(user.getOrgId(), symbolId);
        switch (notifyStrategy) {
            case MOBILE:
                if (!isTestSymbol) {
                    noticeTemplateService.sendSmsNotice(toHeader(user, language), user.getUserId(), NoticeBusinessType.LIQUIDATION_ALERT,
                            user.getNationalCode(), user.getMobile(), jsonObject);
                }
                noticeTemplateService.sendBizPushNotice(toHeader(user, language), user.getUserId(), NoticeBusinessType.LIQUIDATION_ALERT,
                        messageId > 0 ? messageId + "" : "", jsonObject, pushUrlData);
                break;
            case EMAIL:
                if (!isTestSymbol) {
                    noticeTemplateService.sendEmailNotice(toHeader(user, language), user.getUserId(), NoticeBusinessType.LIQUIDATION_ALERT,
                            user.getEmail(), jsonObject);
                }
                noticeTemplateService.sendBizPushNotice(toHeader(user, language), user.getUserId(), NoticeBusinessType.LIQUIDATION_ALERT, null, jsonObject, pushUrlData);
                break;
            default:
                log.warn("sendLiquidationAlertMessage: get NotifyStrategy: {} user: {}",
                        notifyStrategy, user);
        }
    }

    /**
     * 发送强平通知
     *
     * @param user               用户实体
     * @param language           多语言
     * @param symbolName         币对名称（要支持多语言）
     * @param isLongText         多空仓标识（要支持多语言）
     * @param maintainMarginRate 维持保证金率
     */
    private void sendLiquidateionNotifyMessage(User user, String language, String symbolName,
                                               String isLongText, String maintainMarginRate, String symbolId, long messageId) {
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("symbolName", symbolName);
        jsonObject.addProperty("isLongText", isLongText);
        jsonObject.addProperty("maintainMarginRate", maintainMarginRate);

        NotifyUserStrategy notifyStrategy = getNotifyStrategy(user);
        Map<String, String> pushUrlData = Maps.newHashMap();
        pushUrlData.put("SYMBOL_ID", symbolId);
        boolean isTestSymbol = isTestSymbol(user.getOrgId(), symbolId);;
        switch (notifyStrategy) {
            case MOBILE:
                if (!isTestSymbol) {
                    noticeTemplateService.sendSmsNotice(toHeader(user, language), user.getUserId(), NoticeBusinessType.LIQUIDATED_NOTIFY,
                            user.getNationalCode(), user.getMobile(), jsonObject);
                }
                noticeTemplateService.sendBizPushNotice(toHeader(user, language), user.getUserId(), NoticeBusinessType.LIQUIDATED_NOTIFY,
                        null, jsonObject, pushUrlData);
                break;
            case EMAIL:
                if (!isTestSymbol) {
                    noticeTemplateService.sendEmailNotice(toHeader(user, language), user.getUserId(), NoticeBusinessType.LIQUIDATED_NOTIFY,
                            user.getEmail(), jsonObject);
                }
                noticeTemplateService.sendBizPushNotice(toHeader(user, language), user.getUserId(), NoticeBusinessType.LIQUIDATED_NOTIFY,
                        messageId > 0 ? messageId + "" : "", jsonObject, pushUrlData);
                break;
            default:
                log.warn("sendLiquidateionNotifyMessage: get NotifyStrategy: {} user: {}",
                        notifyStrategy, user);
        }

    }

    /**
     * 发送强制减仓通知
     *
     * @param user       用户实体
     * @param language   多语言
     * @param symbolName 币对名称（要支持多语言）
     * @param isLongText 多空仓标识（要支持多语言）
     */
    private void sendADLNotifyMessage(User user, String language, String symbolName, String isLongText, String symbolId, long messageId) {
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("symbolName", symbolName);
        jsonObject.addProperty("isLongText", isLongText);

        NotifyUserStrategy notifyStrategy = getNotifyStrategy(user);
        Map<String, String> pushUrlData = Maps.newHashMap();
        pushUrlData.put("SYMBOL_ID", symbolId);
        boolean isTestSymbol = isTestSymbol(user.getOrgId(), symbolId);;
        switch (notifyStrategy) {
            case MOBILE:
                if (!isTestSymbol) {
                    noticeTemplateService.sendSmsNotice(toHeader(user, language), user.getUserId(), NoticeBusinessType.ADL_NOTIFY,
                            user.getNationalCode(), user.getMobile(), jsonObject);
                }
                noticeTemplateService.sendBizPushNotice(toHeader(user, language), user.getUserId(), NoticeBusinessType.ADL_NOTIFY,
                        null, jsonObject, pushUrlData);
                break;
            case EMAIL:
                if (!isTestSymbol) {
                    noticeTemplateService.sendEmailNotice(toHeader(user, language), user.getUserId(), NoticeBusinessType.ADL_NOTIFY,
                            user.getEmail(), jsonObject);
                }
                noticeTemplateService.sendBizPushNotice(toHeader(user, language), user.getUserId(), NoticeBusinessType.ADL_NOTIFY,
                        messageId > 0 ? messageId + "" : "", jsonObject, pushUrlData);
                break;
            default:
                log.warn("sendADLNotifyMessage: get NotifyStrategy: {} user: {}", notifyStrategy, user);
        }
    }

    private Header toHeader(User user, String language) {
        // 注：这里不使用User实体里面的language属性
        return Header.newBuilder().setOrgId(user.getOrgId()).setLanguage(language).build();
    }

    /**
     * 获取通知策略
     */
    private NotifyUserStrategy getNotifyStrategy(User user) {
        if (StringUtils.isNotEmpty(user.getMobile())) {
            return NotifyUserStrategy.MOBILE;
        } else if (StringUtils.isNotEmpty(user.getEmail())) {
            return NotifyUserStrategy.EMAIL;
        } else {
            return NotifyUserStrategy.NONE;
        }
    }

    /**
     * 获取多仓/空仓的多语言文本
     */
    private String getPositionSideText(LiquidationMessage message, Long orgId, String language) {
        SymbolDetail symbol = basicService.getSymbolDetailFuturesByOrgIdAndSymbolId(orgId, message.getSymbolId());
        if (symbol == null) {
            log.error("getPositionSideText: can not find futures symbol by orgId: {} and symbolId: {}",
                    orgId, message.getSymbolId());
            return "Unknown";
        }
        boolean isLong = symbol.getIsReverse() != message.getIsLong();
        return isLong ? getPositionLongI18nText(language) : getPositionShortI18nText(language);
    }

    /**
     * 获取用户当前语言环境
     */
    private String getUserLanguage(User user) {
        String language;

        // 优先获取用户最近一次登录所使用的语言
        List<LoginLog> loginLogs = loginLogMapper.queryLastLoginLogs(user.getUserId(), 1);
        if (!CollectionUtils.isEmpty(loginLogs)) {
            language = loginLogs.get(0).getLanguage();
            if (StringUtils.isNotEmpty(language)) {
                return language;
            }
        }

        // 获取用户的默认语言
        language = user.getLanguage();
        if (StringUtils.isNotEmpty(language)) {
            return language;
        }

        return LANG_ZH_CN;
    }

    private String getPositionLongI18nText(String language) {
        String i18nText = getI18nText(FUTURES_I18N_POSITION_LONG_KEY, language);
        if (StringUtils.isNotEmpty(i18nText)) {
            return i18nText;
        }

        if (LANG_EN_US.equals(language)) {
            return DEFAULT_EN_US_LONG_TEXT;
        }
        return DEFAULT_ZH_CN_LONG_TEXT;
    }

    private String getPositionShortI18nText(String language) {
        String i18nText = getI18nText(FUTURES_I18N_POSITION_SHORT_KEY, language);
        if (StringUtils.isNotEmpty(i18nText)) {
            return i18nText;
        }

        if (LANG_EN_US.equals(language)) {
            return DEFAULT_EN_US_SHORT_TEXT;
        }
        return DEFAULT_ZH_CN_SHORT_TEXT;
    }

    private String getSymbolI18nName(String symbolId, String language) {
        String i18nText = getI18nText(symbolId, language);
        return StringUtils.isNotEmpty(i18nText) ? i18nText : symbolId;
    }

    /**
     * 根据key和env查询对应的国际化文本
     *
     * @param key  需要查找的key
     * @param lang 语言环境
     * @return 国际化文本，如果没有找到则返回null
     */
    private String getI18nText(String key, String lang) {
        /*
         * tb_internationalization表中的env字段是按照ISO 639规定的2字母代码来设置的语种
         * 参数lang有可能是类似于zh_CN的locale文本
         * 需要将参数处理下才能查询到对应的语言国际化配置
         */
        String env;
        if (lang.contains("_")) {
            env = StringUtils.split(lang, "_")[0];
        } else {
            env = lang;
        }

        // TODO: 需要做缓存处理
        Internationalization i18n = internationalizationMapper.queryInternationalizationByKey(key, env);
        if (i18n != null && StringUtils.isNotEmpty(i18n.getInValue())) {
            return i18n.getInValue();
        }
        return null;
    }
}
