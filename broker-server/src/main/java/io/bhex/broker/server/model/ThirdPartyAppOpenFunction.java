package io.bhex.broker.server.model;

import com.google.gson.JsonElement;
import io.bhex.broker.common.util.JsonUtil;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import java.util.Locale;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "tb_third_party_app_open_function")
public class ThirdPartyAppOpenFunction {

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;
    private String function; // eg: get_user_info
    private String showName;  // 这里有多语言问题，保存的值应该是{"zh_CN":"获取用户信息","en_US":"Get user Info"}
    private String showDesc; // 这里有多语言问题，保存的值应该是{"zh_CN":"此功能获取用户在BHEX的信息","en_US":"Get user Info from bhex"}
    private String language;
    private Integer sort;
    private Long created;
    private Long updated;

    public static void main(String[] args) {
        JsonElement jsonElement = JsonUtil.defaultJsonParser().parse("123");
        System.out.println(jsonElement.isJsonPrimitive());
        jsonElement = JsonUtil.defaultJsonParser().parse("hahaha");
        System.out.println(jsonElement.isJsonPrimitive());
        jsonElement = JsonUtil.defaultJsonParser().parse("{\"name\":\"123\"}");
        System.out.println(jsonElement.isJsonPrimitive());
        jsonElement = JsonUtil.defaultJsonParser().parse("{\"zh_CN\":\"123\"}");
        System.out.println(JsonUtil.getString(jsonElement, "." + Locale.CHINA.toString().toLowerCase(), ""));
        jsonElement = JsonUtil.defaultJsonParser().parse("{\"zh-CN\":\"123\"}");
        System.out.println(JsonUtil.getString(jsonElement, "." + Locale.CHINA.toString(), ""));
    }

}
