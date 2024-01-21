package io.bhex.broker.server.model;

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
@Builder
@Table(name = "tb_app_index_module_config")
public class AppIndexModuleConfig {

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;

    private Long orgId;

    private Integer moduleType;

    private String language;

    @Deprecated
    private String moduleName;

    private String moduleIcon;

    private Integer jumpType;

    private String jumpUrl;

    private Integer needLogin;

    private Integer loginShow; //0-未登录展示  1-登录展示  2-不区分登录状态都展示

    private Integer customOrder;

    private Integer status;

    private Long created;

    private String darkDefaultIcon;

    private String darkSelectedIcon;

    private String lightDefaultIcon;

    private String lightSelectedIcon;

}
