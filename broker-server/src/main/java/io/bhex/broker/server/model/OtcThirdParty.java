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
@Table(name = "tb_otc_third_party")
public class OtcThirdParty implements Serializable {

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;
    private Long thirdPartyId;
    private String thirdPartyName;
    private String endPoint;
    private String apiKey;
    private String secret;
    private Long bindOrgId;
    private Long bindUserId;
    private Long bindAccountId;
    private Integer controlType;
    private Long orgId;
    private String successUrl;
    private String failureUrl;
    private String cancelUrl;
    private String iconUrl;
    private Integer side;
    private Integer status;
    private Long created;
    private Long updated;
}
