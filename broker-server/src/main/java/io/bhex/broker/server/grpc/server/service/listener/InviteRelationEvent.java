package io.bhex.broker.server.grpc.server.service.listener;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class InviteRelationEvent {

    private Long userId;

    private Long orgId;

    private Long accountId;

    private String name;

    private Long inviteId;
}
