package io.bhex.broker.server.push.bo;

import io.bhex.broker.server.model.AdminPushTaskLocale;

/**
 * @author wangsc
 * @description 迟钝的缓存(不存在时可存入null值)
 * @date 2020-08-11 23:57
 */
public final class PushTaskLocaleSlowCache {
    private final AdminPushTaskLocale adminPushTaskLocale;

    public PushTaskLocaleSlowCache(AdminPushTaskLocale adminPushTaskLocale) {
        this.adminPushTaskLocale = adminPushTaskLocale;
    }

    public AdminPushTaskLocale getAdminPushTaskLocale() {
        return adminPushTaskLocale;
    }
}
