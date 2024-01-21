package io.bhex.broker.server.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@AllArgsConstructor
@Data
public class UserInviteInfo {

    private Long inviteId;
    private Long userId;
    private String nationalCode;
    private String mobile;
    private String email;
    private Integer registerType;

    private String source;
    private Integer inviteType;
    private Long created;

    private Integer verifyStatus;
    private String kycName;

    private Integer inviteIndirectVaildCount = 0;
    private String firstName;
    private String secondName;
    private String dataSecret;
    private Integer dataEncrypt;
    private Integer nationality;


    /**
     * 被邀请用户是队长
     */
    private Integer inviteHobbitLeader;

}
