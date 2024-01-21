package io.bhex.broker.server.model;

import lombok.Data;

import javax.persistence.Table;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.model
 * @Author: ming.xu
 * @CreateDate: 27/11/2018 4:09 PM
 * @Copyright（C）: 2018 BHEX Inc. All rights reserved.
 */
@Data
@Table(name = "tb_quote_token_init_data")
public class QuoteTokenInitData {

    private Long id;
    private String tokenId;
    private String tokenName;
    private String tokenIcon;
    private Integer customOrder;
}
