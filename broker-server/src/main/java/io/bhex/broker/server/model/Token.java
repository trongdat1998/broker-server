package io.bhex.broker.server.model;

import lombok.Data;

import javax.persistence.Id;
import javax.persistence.Table;
import java.math.BigDecimal;

/**
 * @ProjectName: broker-server
 * @Package: io.bhex.broker.server.model
 * @Author: ming.xu
 * @CreateDate: 20/08/2018 9:13 PM
 * @Copyright（C）: 2018 BHEX Inc. All rights reserved.
 */
@Data
@Table(name = "tb_token")
public class Token {

    public static final Integer FORBID_INT = 0;
    public static final Integer ALLOW_INT = 1;

    @Id
    private Long id;
    private Long orgId;
    private Long exchangeId;
    private String tokenId;
    private Long tokenIndex;
    private String tokenName;
    private String tokenFullName;
    private String tokenIcon;
    private BigDecimal maxWithdrawQuota = BigDecimal.ZERO;
    private BigDecimal minWithdrawQuantity = BigDecimal.ZERO;
    private BigDecimal maxWithdrawQuantity = BigDecimal.ZERO;
    private BigDecimal needKycQuantity = BigDecimal.ZERO;
    private String feeTokenId;
    private String feeTokenName;
    private BigDecimal fee;
    private Integer allowDeposit;
    private Integer allowWithdraw;
    private Integer isHighRiskToken;
    private Integer status;
    private Integer customOrder;
    private Long created;
    private Long updated;
    private Integer category; //1主类别，2创新类别, 3期权, 4期货
    private String extraTag;
    private String extraConfig;

}
