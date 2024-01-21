package io.bhex.broker.server.model;

import lombok.Data;

import javax.persistence.Table;
import java.math.BigDecimal;

/**
 * @author JinYuYuan
 * @description
 * @date 2021-02-24 16:19
 */
@Data
@Table(name = "tb_rpt_margin_trade_detail")
public class RptMarginTradeDetail {
    private Long relationId;
    private Long orgId;
    private String symbolId;
    //交易人数
    private Long tradePeopleNum;
    //买入人数
    private Long buyPeopleNum;
    //卖出人数
    private Long sellPeopleNum;
    //交易总笔数
    private Long tradeNum;
    //买入笔数
    private Long buyTradeNum;
    //卖出笔数
    private Long sellTradeNum;
    //基础币费用
    private BigDecimal baseFee;
    //计价币费用
    private BigDecimal quoteFee;
    //手续费折合USDT
    private BigDecimal fee;
    //交易额折合USDT
    private BigDecimal amount;
    private Long created;
}
