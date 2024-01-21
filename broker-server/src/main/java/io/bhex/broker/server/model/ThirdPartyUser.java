
package io.bhex.broker.server.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import java.util.Date;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "tb_third_party_user")
public class ThirdPartyUser {
    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;
    private Long orgId;
    private Long masterUserId; //主用户Id
    private Long userId;//券商用户Id
    private String thirdUserId;//三方用户Id
    private Date created;
    private Date updated;
}