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
@Table(name = "tb_kyc_level")
public class KycLevel {

    public static final int STATUS_ENABLE = 1;
    public static final int STATUS_DISABLE = 2;

    @Id
    private Integer levelId;

    private String levelName;

    private String precondition;

    private String memo;

    private String displayLevel;

    private Integer status;

    private Long created;

    private Long updated;
}
