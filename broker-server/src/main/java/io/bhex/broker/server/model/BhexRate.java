package io.bhex.broker.server.model;


import java.math.BigDecimal;
import java.util.Date;

import javax.persistence.Table;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Table(name = "clear_snp_bhex_rate")
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class BhexRate {

    private Date dt;

    private String tokenId;

    private BigDecimal cny;

    private BigDecimal usd;

    private BigDecimal btc;

    private BigDecimal eth;

    private BigDecimal usdt;

    private Date updatedAt;

    private BigDecimal multiplier;

}
