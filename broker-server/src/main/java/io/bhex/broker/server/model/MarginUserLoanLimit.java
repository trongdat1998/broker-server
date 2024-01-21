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
@Table(name = "tb_margin_user_loan_limit")
public class MarginUserLoanLimit {

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;

    private Long orgId;

    private Long userId;

    private Long accountId;

    private String tokenId;
    private BigDecimal limitAmount;
    /**
     * 1=有效 2=无效
     */
    private Integer status;

    private Long created;
    private Long updated;
}
