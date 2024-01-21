package io.bhex.broker.server.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Id;
import javax.persistence.Table;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "ods_symbol")
public class StatisticsSymbol {

    @Id
    private Long id;
    private String symbolId;
    private String symbolName;
    private String baseTokenId;
    private String quoteTokenId;
    private Integer securityType;

}
