package io.bhex.broker.server.util;

import io.bhex.broker.grpc.common.LocaleEnum;

import java.util.Locale;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.util
 * @Author: ming.xu
 * @CreateDate: 14/09/2018 5:43 PM
 * @Copyright（C）: 2018 BHEX Inc. All rights reserved.
 */
public class LocaleUtil {

    public static String getLanguageInfo(LocaleEnum localeEnum) {
        switch (localeEnum) {
            case ENGLISH:
                return Locale.US.getLanguage();
            case CHINESE:
                return Locale.SIMPLIFIED_CHINESE.getLanguage();
            case JAPANESE:
                return Locale.JAPAN.getLanguage();
            case KOREAN:
                return Locale.KOREA.getLanguage();
            case THAI:
                return "thai";
            default:
                return Locale.US.getLanguage();
        }
    }
}
