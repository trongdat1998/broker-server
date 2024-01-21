package io.bhex.broker.server.grpc.server.service.po;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 *
 * @description
 * @author JinYuYuan
 * @Date 2020-04-23 15:55
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class UserLoginMessagePO {
    private Long id;

    private Long orgId;

    private Long userId;

    private Long loginDate;

    private String tokenFor;

    private String loginType;

    private String ip;
}
