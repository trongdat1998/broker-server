package io.bhex.broker.server.model;

import lombok.Data;

import javax.persistence.Id;
import javax.persistence.Table;
import java.math.BigDecimal;
import java.sql.Timestamp;

@Data
@Table(name = "tb_token_option")
public class TokenOption {

    @Id
    private Long id;  // 自增id

    private String tokenId;//期权id

    private BigDecimal strikePrice;//行权价

    private Timestamp issueDate;//生效/上线日期

    private Timestamp settlementDate;//到期/交割时间

    private Integer isCall;//call or put

    private BigDecimal maxPayOff;//最大赔付

    private Timestamp createdAt;//创建时间

    private Timestamp updatedAt;//修改时间

    private String coinToken; //计价货币

    private String indexToken; //标的指数

    private BigDecimal positionLimit;

}
