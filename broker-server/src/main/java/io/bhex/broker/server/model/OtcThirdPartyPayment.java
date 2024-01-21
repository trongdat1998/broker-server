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
 * tb_third_party_payment
 *
 * @author cookie.yuan
 */

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "tb_otc_third_party_payment")
public class OtcThirdPartyPayment implements Serializable {

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;
    private Long thirdPartyId;
    private Long paymentId;
    private String paymentType;
    private String paymentName;
    private String tokenId;
    private String currencyId;
    private BigDecimal fee;
    private BigDecimal minAmount;
    private BigDecimal maxAmount;
    private Integer status;
    private Long created;
    private Long updated;
}
