package io.bhex.broker.server.model;

import java.math.BigDecimal;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author JinYuYuan
 * @description
 * @date 2020-05-21 14:47
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "tb_finance_transfer_limit")
public class TransferLimit {
    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;
    private Long orgId;
    private String tokenId;
    private BigDecimal dayQuota; //每日限额
    private BigDecimal maxAmount; //单次最大数量
    private Long created;
    private Long updated;
}
