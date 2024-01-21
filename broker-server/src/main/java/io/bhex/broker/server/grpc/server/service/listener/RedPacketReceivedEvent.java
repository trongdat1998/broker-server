package io.bhex.broker.server.grpc.server.service.listener;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RedPacketReceivedEvent {

    private Long receiveId;

}
