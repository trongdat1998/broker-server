package io.bhex.broker.server.grpc.server.service.po;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class ActivityLockInterestUserInfo {

    private Long userId;

    private BigDecimal backAmount;

    private BigDecimal userAmount;

    private BigDecimal luckyAmount;
}
