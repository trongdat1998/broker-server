package io.bhex.broker.server.model;


import java.math.BigDecimal;

import javax.persistence.Id;
import javax.persistence.Table;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
@Table(name = "tb_contract_temporary_info")
public class ContractTemporaryInfo {

    @Id
    private Long id;

    private Long orgId;

    private String mobile;

    private String tokenId;

    private BigDecimal amount;

    private Long lockOrderId;

    private Long unlockOrderId;

    private Integer status;
}
