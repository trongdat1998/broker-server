package io.bhex.broker.server.domain.kyc.tencent;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class NonceTicket {
    private Long userId;
    private Long orgId;
    private String nonce;
    private String ticket;
}
