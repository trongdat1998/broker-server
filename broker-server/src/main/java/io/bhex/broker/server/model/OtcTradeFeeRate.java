package io.bhex.broker.server.model;


import lombok.*;

import javax.persistence.Id;
import javax.persistence.Table;
import java.math.BigDecimal;
import java.util.Date;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Table(name = "tb_otc_trade_fee_rate")
public class OtcTradeFeeRate {
    /**
     * id
     */
    @Id
    private Integer id;

    /**
     * 券商ID
     */
    private Long orgId;
    /**
     * token币种
     */
    private String tokenId;

    /**
     * maker fee rate
     */
    private BigDecimal makerFeeRate;
    /**
     * trade fee rate
     */
    private BigDecimal takerFeeRate;

    private Date createdAt;

    private Date updateAt;

    private Integer deleted;
}
