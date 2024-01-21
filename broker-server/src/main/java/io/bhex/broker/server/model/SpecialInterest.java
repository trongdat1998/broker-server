package io.bhex.broker.server.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import java.math.BigDecimal;

/**
 * @author JinYuYuan
 * @description
 * @date 2021-02-25 18:47
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "tb_margin_special_interest")
public class SpecialInterest {
    @Id
    @GeneratedValue(generator = "JDBC")
    Long id;
    Long orgId;
    String tokenId;
    Long accountId;
    Long userId;
    BigDecimal interest;
    BigDecimal showInterest;
    Integer effectiveFlag;
    Long created;
    Long updated;

}
