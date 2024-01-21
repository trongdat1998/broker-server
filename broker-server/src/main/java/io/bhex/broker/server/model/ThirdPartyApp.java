package io.bhex.broker.server.model;

import io.bhex.broker.core.domain.GsonIgnore;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Transient;
import java.util.List;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "tb_third_party_app")
public class ThirdPartyApp {

    public transient static final int ENABLE_STATUS = 1;
    public transient static final int DISABLE_STATUS = 0;
    public transient static final int CANCELLED_AUTHORIZATION_STATUS = -1;
    public transient static final int WEB_APP = 1;
    public transient static final int MOBILE_APP = 2;

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;
    private Long orgId;
    private Integer appType; // 应用类型，网站应用  或者 移动应用
    private String appId; // 这个不一定有值，如果是移动应用的时候才会有这个appId
    private String appName; // 应用名称
    private String clientId;
    private String clientSecret;
    @GsonIgnore
    private String snow; // key for clientSecret
    private Long bindUserId; // 绑定一个用户Id
    private Integer transferPermission; // 是否开启转账功能
    private Integer status; // 状态：是否开放
    private String callback; //
    private String randomCode;
    private String functions; // 开放功能
    private Long created;
    private Long updated;
    @Transient
    private transient List<ThirdPartyAppOpenFunction> openFunctionList;

}
