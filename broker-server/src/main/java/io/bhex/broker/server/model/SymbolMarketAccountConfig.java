package io.bhex.broker.server.model;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Id;
import javax.persistence.Table;
import java.util.Date;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
@Table(name = "tb_symbol_market_account_config")
public class SymbolMarketAccountConfig {

    @Id
    private Long id;

    private Long orgId;

    private Long accountId;

    private String symbolId;

    private Integer status;

    private Date created;

    private Date updated;

}
