/**********************************
 *@项目名称: server-parent
 *@文件名称: io.bhex.broker.server.model
 *@Date 2018/9/12
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
package io.bhex.broker.server.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Transient;
import java.math.BigDecimal;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "tb_withdraw_order")
public class WithdrawOrder {

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;
    private Long orgId;
    private Long userId;
    private Long accountId;
    private Long addressId;
    private String address;
    private String addressExt;
    private Integer isInnerAddress;
    private Long orderId;
    private String tokenId;
    private String chainType;
    private String clientOrderId;
    private String feeTokenId;
    private BigDecimal platformFee;
    private BigDecimal brokerFee;
    private String minerFeeTokenId;
    private BigDecimal minerFee;
    private Integer isAutoConvert;
    private BigDecimal convertRate;
    private BigDecimal convertQuantity;
    private BigDecimal quantity;
    private BigDecimal arrivalQuantity;
    private Integer status; // broker_audit_status
    private Integer orderStatus; // bh_status
    private Integer needBrokerAudit;
    private Integer needCheckCardNo;
    private String cardNo;
    private String platform;
    private String ip;
    private String userAgent;
    private String language;
    private String appBaseHeader;
    private String currentFxRate;
    private BigDecimal btcValue;
    private transient Boolean hasCheckIdCardNo;
    private Long created;
    private Long updated;
    private String remarks;
    @Transient
    private Integer failedReason;

}
