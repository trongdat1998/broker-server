package io.bhex.broker.server.grpc.server.service.listener;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class SendFCodeEvent {

    private String email;

    private String nationalCode;

    private String phoneNo;

}
