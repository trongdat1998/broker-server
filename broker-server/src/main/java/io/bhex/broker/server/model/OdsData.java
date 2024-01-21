package io.bhex.broker.server.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Id;
import javax.persistence.Table;

@Data
@Builder(builderClassName = "Builder", toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "tb_ods_data")
public class OdsData {

    @Id
    private Long id;

    private String dataKey;

    private Long orgId;

    private Long startTime;

    private Long endTime;

    private String dateStr;

    private String symbol;

    private String token;

    private String dataValue;

    private Long created;

}
