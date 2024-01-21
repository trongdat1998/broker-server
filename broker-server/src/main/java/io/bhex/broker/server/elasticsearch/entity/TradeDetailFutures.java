package io.bhex.broker.server.elasticsearch.entity;

import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;

import java.math.BigDecimal;

@Document(indexName = "trade_detail_futures_*")
@Data
public class TradeDetailFutures {

    @Id
    @Field("trade_detail_id")
    private Long tradeDetailId;

    @Field("account_id")
    private Long accountId;

    @Field("margin_changed")
    private BigDecimal marginChanged;

    private BigDecimal pnl;

    private BigDecimal residual;

    @Field("index_price")
    private BigDecimal indexPrice;

    private Long version;

    @Field("extra_info")
    private Integer extraInfo;

    @Field("bankruptcy_price")
    private BigDecimal bankruptcyPrice;

}
