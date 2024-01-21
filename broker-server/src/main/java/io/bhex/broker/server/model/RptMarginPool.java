package io.bhex.broker.server.model;

import lombok.Data;

import javax.persistence.Id;
import javax.persistence.Table;
import java.math.BigDecimal;

/**
 * @author JinYuYuan
 * @description
 * @date 2020-12-22 15:35
 */
@Data
@Table(name = "tb_rpt_margin_pool")
public class RptMarginPool {
    @Id
    private Long id;

    public Long orgId;

   public String tokenId;

   public BigDecimal available;

   public BigDecimal unpaidAmount;

   public BigDecimal interestUnpaid;

   public BigDecimal interestPaid;

   public Long created;
   public Long updated;

}
