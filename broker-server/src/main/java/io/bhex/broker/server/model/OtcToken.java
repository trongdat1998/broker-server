package io.bhex.broker.server.model;

import java.math.BigDecimal;
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
@Table(name = "tb_otc_token")
public class OtcToken {

    public static final int AVAILABLE = 1;

    /**
     * id
     */
    @Id
    private Integer id;
    /**
     * 券商ID
     */
    private Long orgId;
    /**
     * token币种
     */
    private String tokenId;
    /**
     * 最小计价单位
     */
    private BigDecimal minQuote;
    /**
     * 最大交易单位
      */
    private BigDecimal maxQuote;
    /**
     * 精度
     */
    private Integer scale;
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

    /**
     * 浮动范围最大值
     */
    private BigDecimal upRange;

    /**
     * 浮动范围最小值
     */
    private BigDecimal downRange;

    /**
     * 排序值
     */
    private Integer sequence;
}
