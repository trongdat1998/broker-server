package io.bhex.broker.server.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import java.io.Serializable;

/**
 * tb_otc_third_party_chain
 *
 * @author cookie.yuan
 */

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "tb_otc_third_party_chain")
public class OtcThirdPartyChain implements Serializable {

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;
    private Long orgId;
    private String tokenId;
    private String chainType;
    private Integer status;
    private Long created;
    private Long updated;
}
