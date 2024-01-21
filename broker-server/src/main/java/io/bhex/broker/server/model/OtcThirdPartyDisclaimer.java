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
 * tb_third_party
 *
 * @author cookie.yuan
 */

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "tb_otc_third_party_disclaimer")
public class OtcThirdPartyDisclaimer implements Serializable {

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;
    private Long orgId;
    private Long thirdPartyId;
    private String language;
    private String disclaimer;
    private Long created;
    private Long updated;
}
