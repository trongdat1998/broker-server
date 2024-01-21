/*
 ************************************
 * @项目名称: api-parent
 * @文件名称: TokenType
 * @Date 2019/01/13
 * @Author will.zhao@bhex.io
 * @Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 * 注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 **************************************
 */
package io.bhex.broker.server.domain;

public enum TokenCategory {

    COIN("COIN", "币币（现货）"),
    OPTION("OPTION", "期权");

    private static final String OPTION_TOKEN_TYPE = "OPTION";

    private String id;

    private String name;

    public static TokenCategory getByTokenType(String tokenType) {
        return isOption(tokenType) ? TokenCategory.OPTION : TokenCategory.COIN;
    }

    public static boolean isOption(String tokenType) {
        return tokenType.equalsIgnoreCase(OPTION_TOKEN_TYPE);
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    TokenCategory(String id, String name) {
        this.id = id;
        this.name = name;
    }
}
