package io.bhex.broker.server.elasticsearch.entity;

import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.DateFormat;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

import java.math.BigDecimal;
import java.util.Date;

@Document(indexName = "balance_flow_*")
@Data
public class BalanceFlow {

    @Id
    @Field("balance_flow_id")
    private Long balanceFlowId;

    @Field("org_id")
    private Long orgId;

    @Field("broker_user_id")
    private Long userId;

    @Field("account_id")
    private Long accountId;

    @Field("token_id")
    private String tokenId;

    private BigDecimal changed;

    private BigDecimal total;

    @Field("balance_id")
    private Long balanceId;

    @Field("business_subject")
    private Integer businessSubject;

    @Field("second_business_subject")
    private Integer secondBusinessSubject;

    @Field(value = "created_at", type = FieldType.Date, format = DateFormat.date_optional_time, pattern = "yyyy-MM-dd HH:mm:ss")
    // elasticsearch中的数据类型是string，格式是yyyy-MM-dd'T'HH:mm:ssSSSZ
    // FIXME:原来使用旧版本的data-elasticsearch的时候有自动转化，但是换了新版本的这个不可以了，无法自动转换，只能换成string，所以在使用中需要注意用到的地方
    private Date createdAt;

}
