package io.bhex.broker.server.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import java.math.BigDecimal;
import java.util.Date;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "tb_kyc_level_config")
public class KycLevelConfig {

    public static final Long DEFAULT_ORG_ID = 0L;
    public static final Long DEFAULT_COUNTRY_ID = 0L;

    public static final Integer TRUE = 1;
    public static final Integer FALSE = 1;

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;

    private Long orgId;

    private Integer kycLevel;

    private Long countryId;

    private Integer allowOtc;

    private Integer faceCompare;

    private BigDecimal otcDailyLimit;

    private String otcLimitCurrency;

    private BigDecimal withdrawDailyLimit;

    private String withdrawLimitToken;

    private String memo;

    private Long created;

    private Long updated;
}
