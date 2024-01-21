package io.bhex.broker.server.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@AllArgsConstructor
@Data
public class SimpleUserInfo {

    private Long userId;
    private String nationalCode;
    private String mobile;
    private String email;
    private Integer registerType;
    private Integer userType;
    private Integer verifyStatus;
    private Long inviteUserId;
    private Long secondLevelInviteUserId;
    private String source;
    private Long created;
    private Integer userStatus;

}
