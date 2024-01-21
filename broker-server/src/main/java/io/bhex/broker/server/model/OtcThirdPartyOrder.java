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
 * tb_otc_third_party_order
 *
 * @author cookie.yuan
 */

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "tb_otc_third_party_order")
public class OtcThirdPartyOrder implements Serializable {

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;
    private Long orderId;
    private String clientOrderId;
    private String thirdPartyOrderId;
    private Long otcSymbolId;
    private String tokenId;
    private String currencyId;
    private String walletAddress;
    private String walletAddressExt;
    private Integer side;
    private BigDecimal tokenAmount;
    private BigDecimal currencyAmount;
    private BigDecimal price;
    private BigDecimal feeAmount;
    private Long orgId;
    private Long userId;
    private Long accountId;
    private Long otcOrgId;
    private Long otcUserId;
    private Long otcAccountId;
    private String otcUrl;
    private Integer status;
    private String errorMessage;
    private Long created;
    private Long updated;
}
