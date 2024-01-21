package io.bhex.broker.server.model;

import lombok.Data;

import javax.persistence.Table;
import java.math.BigDecimal;

/**
 * @author JinYuYuan
 * @description
 * @date 2021-02-24 16:12
 */
@Data
@Table(name = "tb_rpt_margin_trade")
public class RptMarginTrade {
    private Long id;
    private Long orgId;
    private Long createTime;
    //交易人数
    private Long tradePeopleNum = 0L;
    //买入人数
    private Long buyPeopleNum = 0L;
    //卖出人数
    private Long sellPeopleNum = 0L;
    //交易总笔数
    private Long tradeNum = 0L;
    //买入笔数
    private Long buyTradeNum = 0L;
    //卖出笔数
    private Long sellTradeNum = 0L;
    //手续费折合USDT
    private BigDecimal fee = BigDecimal.ZERO;
    //交易额折合USDT
    private BigDecimal amount = BigDecimal.ZERO;
}
