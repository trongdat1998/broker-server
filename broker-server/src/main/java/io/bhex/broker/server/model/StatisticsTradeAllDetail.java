package io.bhex.broker.server.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import java.math.BigDecimal;
import java.util.Date;

@NoArgsConstructor
@AllArgsConstructor
@Data
@Builder
@Table(name = "ods_trade_detail")
public class StatisticsTradeAllDetail {

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long tradeDetailId;
    private Long accountId;
    private Long ticketId;
    private Long brokerId;
    private Long brokerUserId;
    private Long orderId;
    private Integer orderType;
    private Long matchOrderId;
    private String symbolId;
    private Long exchangeId;
    private Long matchExchangeId;
    private Integer side;
    private BigDecimal quantity;
    private BigDecimal amount;
    private BigDecimal price;
    private BigDecimal tokenFee;
    private BigDecimal feeRate;
    private BigDecimal sysTokenFee;
    private BigDecimal sysTokenId;
    private BigDecimal sysTokenPrice;
    private Integer isUseSysToken;
    private Integer isActuallyUsedSysToken;
    private Integer status;
    private Integer isMaker;
    private Date matchTime;
    private Date createdAt;
    private Date  updatedAt;
}
