package io.bhex.broker.server.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Id;
import javax.persistence.Table;
import java.math.BigDecimal;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "tb_third_party_app_transfer")
public class ThirdPartyAppTransfer {

    @Id
    private Long id;
    private Long orgId;
    private Long thirdPartyAppId;
    private String clientOrderId;
    private Long transferId;
    private Long sourceUserId;
    private Long sourceAccountId;
    private Long targetUserId;
    private Long targetAccountId;
    private String tokenId;
    private BigDecimal amount;
    private Integer fromSourceLock;
    private Integer toTargetLock;
    private Integer businessSubject;
    private Integer status;
    private String response;
    private Long created;
    private Long updated;

}
