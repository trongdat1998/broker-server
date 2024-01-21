package io.bhex.broker.server.domain;

/**
 * @author lizhen
 * @date 2018-11-05
 */
public enum  LanguageType {

    ZH_CN("zh_CN"),

    EN_US("en_US");

    private String code;

    LanguageType(String code) {
        this.code = code;
    }

    public String getCode() {
        return code;
    }
}
