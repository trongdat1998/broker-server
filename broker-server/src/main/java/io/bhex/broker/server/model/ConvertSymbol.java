/**********************************
 *@项目名称: broker-parent
 *@文件名称: io.bhex.broker.domain.entity
 *@Date 2018/6/10
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
import java.math.BigDecimal;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "tb_convert_symbol")
public class ConvertSymbol {

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;
    private Long brokerId;
    private Long brokerAccountId;
    private String symbolId;
    private String purchaseTokenId;
    private String purchaseTokenName;
    private String offeringsTokenId;
    private String offeringsTokenName;
    private Integer purchasePrecision;
    private Integer offeringsPrecision;
    private Integer priceType;
    private BigDecimal priceValue;
    private BigDecimal minQuantity;
    private BigDecimal maxQuantity;
    private BigDecimal accountDailyLimit;
    private BigDecimal accountTotalLimit;
    private BigDecimal symbolDailyLimit;
    private Integer status;
    private Boolean verifyKyc;
    private Boolean verifyMobile;
    private Integer verifyVipLevel;
    private Long created;
    private Long updated;

}
