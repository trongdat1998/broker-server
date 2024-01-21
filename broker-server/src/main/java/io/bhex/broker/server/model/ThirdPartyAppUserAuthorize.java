package io.bhex.broker.server.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "tb_third_party_app_user_authorize")
public class ThirdPartyAppUserAuthorize {

    public transient static final int STATUS_NORMAL = 1;

    public transient static final int CODE_UNUSED_STATUS = 0;
    public transient static final int CODE_USED_STATUS = 1;

    public transient static final int ACCESS_TOKEN_NOT_ACQUIRED = 0;
    public transient static final int ACCESS_TOKEN_ACQUIRED = 1;
    public transient static final int ACCESS_TOKEN_DISABLE_BY_USER = -1;
    public transient static final int ACCESS_TOKEN_DISABLE_BY_CHANGE_PWD = -2;
    public transient static final int ACCESS_TOKEN_DISABLE_BY_USER_STATUS_CHANGE = -3;
    public transient static final int ACCESS_TOKEN_DISABLE_BY_APP_STATUS_CHANGE = -4;

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;
    private Long orgId;
    private Long userId;
    private String openId;
    private Long thirdPartyAppId; // ThirdPartyApp.id, not ThirdPartyApp.appId
    private Long authorizeId;
    private String requestId; // 冗余
    private String authorizeFunctions; // 授权功能列表
    private Integer status; // 状态，1 正常 好像没什么用
    private String oauthCode; // oauthCode
    private Long codeExpired; // code失效时间
    private Integer codeStatus; // 0 未使用  1 已使用
    private String accessToken; // 用户授权给该APP的accessToken
    private Long tokenExpired; // token失效时间
    private Integer tokenStatus; // 0 已生成，暂未获取 1 已获取，使用中 -1 用户取消授权 -2 用户修改密码不可用 -3 用户状态变更不可用 -4 APP被取消授权使用
    private String platform;
    private String userAgent;
    private String language;
    private String appBaseHeader;
    private Long created;
    private Long updated;

}
