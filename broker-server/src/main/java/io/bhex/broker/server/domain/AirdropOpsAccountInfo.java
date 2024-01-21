package io.bhex.broker.server.domain;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class AirdropOpsAccountInfo {
    private Long opsAccountId;
    private Long opsAccountOrgId;
    private String opsTokenId;
    private String opsAirdropAmount;
}
