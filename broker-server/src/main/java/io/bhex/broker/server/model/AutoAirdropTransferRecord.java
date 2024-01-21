package io.bhex.broker.server.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import java.math.BigDecimal;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "tb_auto_airdrop_transfer_record")
public class AutoAirdropTransferRecord {

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;

    private Long clientId;

    private Long brokerId;

    private Long accountId;

    private Integer airdropType;

    private String token;

    private BigDecimal amount;

    private Integer status;

    private Long createdAt;

    private Long updatedAt;

}
