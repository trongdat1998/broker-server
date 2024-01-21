package io.bhex.broker.server.model;

import lombok.Data;

import javax.persistence.Id;
import javax.persistence.Table;

@Data
@Table(name = "tb_user_action_log")
public class UserActionLog {

    @Id
    private Long id;
    private Long orgId;
    private Long userId;
    private String actionType;
    private String action;
    private String remoteIp;
    private Integer status; //
    private Integer byAdmin; // 0 or 1
    private String adminUser; // byAdmin = 0, 0; byAdmin = 1, adminUser;
    private String platform;
    private String userAgent;
    private String language;
    private String appBaseHeader;
    private Integer resultCode;
    private Long created;
    private Long updated;

}
