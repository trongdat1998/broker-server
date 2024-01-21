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
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "tb_oauth_authorize_record")
public class OAuthAuthorizeRecord {

    public transient static final int UNUSED_STATUS = 0;
    public transient static final int USED_STATUS = 1;

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;
    private Long orgId;
    private Long thirdPartyAppId;
    private String requestId; // 唯一标示
    private String clientId; // 用户请求过来的clientId,
    private String redirectUrl;
    private String state;
    private String scope;
    private Integer status; // 0 requestId未使用 1 requestId已使用
    private String platform;
    private String userAgent;
    private String language;
    private String appBaseHeader;
    private Long created;
    private Long updated;

}
