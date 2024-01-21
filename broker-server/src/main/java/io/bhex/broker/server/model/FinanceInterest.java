package io.bhex.broker.server.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Id;
import javax.persistence.Table;
import java.math.BigDecimal;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "tb_finance_interest")
public class FinanceInterest {

    @Id
    private Long id;

    private Long orgId;

    private Long productId;

    private String token;

    private String statisticsTime;

    private BigDecimal rate;

    private Integer status;

    private Long createdAt;

    private Long updatedAt;


}
