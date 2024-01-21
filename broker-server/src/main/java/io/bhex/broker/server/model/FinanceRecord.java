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
@Table(name = "tb_finance_record")
public class FinanceRecord {

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;

    private Long orgId;

    private Long userId;

    private Long accountId;

    private Long transferId;

    private Long productId;

    private Integer type;

    private String token;

    private BigDecimal amount;

    private Integer status;

    private Integer redeemType;

    private String statisticsTime;

    private String platform;

    private String userAgent;

    private String language;

    private String appBaseHeader;

    private Long createdAt;

    private Long updatedAt;
}
