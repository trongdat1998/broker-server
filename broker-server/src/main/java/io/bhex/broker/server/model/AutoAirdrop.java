package io.bhex.broker.server.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Id;
import javax.persistence.Table;
import java.math.BigDecimal;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.model
 * @Author: ming.xu
 * @CreateDate: 30/11/2018 3:11 PM
 * @Copyright（C）: 2018 BHEX Inc. All rights reserved.
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "tb_auto_airdrop")
public class AutoAirdrop {

    @Id
    private Long id;
    private Long brokerId;
    private String tokenId;
    private Integer airdropType;
    private Integer accountType;
    private Integer status;
    private BigDecimal airdropTokenNum;
    private Long updatedAt;
    private Long createdAt;
}
