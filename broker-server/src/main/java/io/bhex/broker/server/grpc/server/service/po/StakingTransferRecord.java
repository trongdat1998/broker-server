package io.bhex.broker.server.grpc.server.service.po;

import io.bhex.base.account.AccountType;
import io.bhex.base.account.BusinessSubject;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * Transfer DTO
 * @author songxd
 * @date 2020-08-01
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class StakingTransferRecord {
    private Long orgId;
    private Long userId;
    private Long transferId;
    private String tokenId;
    private BigDecimal amount;

    private Long sourceOrgId;
    private Long sourceAccountId;
    private AccountType sourceAccountType;
    private BusinessSubject sourceBusinessSubject;

    private Long targetOrgId;
    private Long targetAccountId;
    private AccountType targetAccountType;
    private BusinessSubject targetBusinessSubject;
}
