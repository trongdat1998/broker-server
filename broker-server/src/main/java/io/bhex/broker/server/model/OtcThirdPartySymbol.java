package io.bhex.broker.server.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import java.io.Serializable;
import java.math.BigDecimal;

/**
 * tb_otc_third_party_symbol
 *
 * @author cookie.yuan
 */

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "tb_otc_third_party_symbol")
public class OtcThirdPartySymbol implements Serializable {

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;
    private Long otcSymbolId;
    private Long orgId;
    private Long thirdPartyId;
    private String tokenId;
    private String currencyId;
    private Long paymentId;
    private String paymentType;
    private String paymentName;
    private BigDecimal fee;
    private BigDecimal minAmount;
    private BigDecimal maxAmount;
    private Integer side;
    private Integer status;
    private Long created;
    private Long updated;
}
