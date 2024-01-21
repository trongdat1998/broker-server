package io.bhex.broker.server.elasticsearch.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.DateFormat;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

import java.math.BigDecimal;
import java.util.Date;

@Document(indexName = "trade_detail_*")
@Data
@Builder(builderClassName = "Builder", toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
public class TradeDetail {

    @Id
    @Field("trade_detail_id")
    private Long tradeDetailId;

    @Field("broker_id")
    private Long orgId;

    @Field("broker_user_id")
    private Long userId;

    @Field("account_id")
    private Long accountId;

    @Field("exchange_id")
    private Long exchangeId;

    @Field("symbol_id")
    private String symbolId;

    @Field("base_token_id")
    private String baseTokenId;

    @Field("quote_token_id")
    private String quoteTokenId;

    @Field("contract_multiplier")
    private BigDecimal contractMultiplier;

    @Field("extra_info")
    private Integer extraInfo;

    @Field("order_id")
    private Long orderId;

    @Field("side")
    private Integer orderSide;

    @Field("order_type")
    private Integer orderType;

    private BigDecimal price;

    private BigDecimal quantity;

    private BigDecimal amount;

    @Field("is_close")
    private Integer isClose;

    @Field("is_maker")
    private Integer isMaker;

    @Field("maker_bonus")
    private BigDecimal makerBonus;

    @Field("match_broker_id")
    private Long matchOrgId;

    @Field("match_broker_user_id")
    private Long matchUserId;

    @Field("match_account_id")
    private Long matchAccountId;

    @Field("match_order_id")
    private Long matchOrderId;

    @Field("fee_rate")
    private BigDecimal feeRate;

    @Field("fee_token_id")
    private String feeToken;

    @Field("token_fee")
    private BigDecimal fee;

    @Field(value = "match_time", type = FieldType.Date, format = DateFormat.date_optional_time, pattern = "yyyy-MM-dd HH:mm:ss")
    // FIXME:原来使用旧版本的data-elasticsearch的时候有自动转化，但是换了新版本的这个不可以了，无法自动转换，只能换成string，所以在使用中需要注意用到的地方
//    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss", timezone = "GMT+8")
    private Date matchTime;

    @Field(value = "created_at", type = FieldType.Date, format = DateFormat.date_optional_time, pattern = "yyyy-MM-dd HH:mm:ss")
    // FIXME:原来使用旧版本的data-elasticsearch的时候有自动转化，但是换了新版本的这个不可以了，无法自动转换，只能换成string，所以在使用中需要注意用到的地方
    private Date createdAt;

    @Field(value = "updated_at", type = FieldType.Date, format = DateFormat.date_optional_time, pattern = "yyyy-MM-dd HH:mm:ss")
    // FIXME:原来使用旧版本的data-elasticsearch的时候有自动转化，但是换了新版本的这个不可以了，无法自动转换，只能换成string，所以在使用中需要注意用到的地方
    private Date updatedAt;

    private TradeDetailFutures tradeDetailFutures;

    private Boolean isFuturesTrade;

}
