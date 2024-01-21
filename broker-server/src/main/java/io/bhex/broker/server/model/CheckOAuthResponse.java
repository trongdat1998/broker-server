package io.bhex.broker.server.model;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class CheckOAuthResponse {

    private Long orgId;
    private Long userId;

}
