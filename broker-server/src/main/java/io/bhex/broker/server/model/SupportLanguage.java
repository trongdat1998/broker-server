package io.bhex.broker.server.model;

import io.bhex.broker.core.domain.GsonIgnore;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "tb_support_language")
@Builder(builderClassName = "Builder", toBuilder = true)
public class SupportLanguage {

    @Id
    @GeneratedValue(generator = "JDBC")
    @GsonIgnore
    private Long id;
    @GsonIgnore
    private String languageLocalName; // 默认都用英文。
    private String showName; // 按需求给出页面要展示的文字
    private String language; // 语言文字。zh-cn、en-us等
    private String icon; // icon url
    private String jsLoadUrl; // js语言包加载url
    private String currency;
    @GsonIgnore
    private Long created;

}
