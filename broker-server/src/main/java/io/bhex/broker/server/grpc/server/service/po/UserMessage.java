package io.bhex.broker.server.grpc.server.service.po;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class UserMessage {
    private Long id;

    private Long orgId;

    private Long userId;

    private String nationalCode;

    private Long inviteUserId;

    private Long registerTime;

    private Long created;

    private Long updated;

    private String signature;

    /**
     * 新增字段
     **/

    private String mobile;

    private String email;

    private Integer registerType;

    private String ip;

    private Integer userStatus;

    private String inputInviteCode;

    private Long secondLevelInviteUserId;

    private String inviteCode;

    private Integer bindGa;

    private Integer bindTradePwd;

    private Integer authType;

    private Integer userType;

    private String channel;

    private String source;

    private String platform;

    private String language;

    private String appBaseHeader;
}
