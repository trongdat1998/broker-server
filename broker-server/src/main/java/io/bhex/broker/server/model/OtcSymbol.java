package io.bhex.broker.server.model;

import java.util.Date;

import javax.persistence.Id;
import javax.persistence.Table;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author lizhen
 * @date 2018-11-04
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Table(name = "tb_otc_symbol")
public class OtcSymbol {

    public static final int AVAILABLE = 1;

    /**
     * id
     */
    @Id
    private Long id;
    /**
     * 交易所ID
     */
    private Long exchangeId;
    /**
     * 券商ID
     */
    private Long orgId;
    /**
     * token币种
     */
    private String tokenId;
    /**
     * 法币币种
     */
    private String currencyId;
    /**
     * 状态  1：可用   -1：不可用
     */
    private Integer status;
    /**
     * 创建时间
     */
    private Date createDate;
    /**
     * 修改时间
     */
    private Date updateDate;
}
