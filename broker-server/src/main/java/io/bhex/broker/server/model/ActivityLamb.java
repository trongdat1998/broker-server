package io.bhex.broker.server.model;

import lombok.Data;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import java.math.BigDecimal;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.domain
 * @Author: ming.xu
 * @CreateDate: 2019/5/28 9:09 PM
 * @Copyright（C）: 2019 BHEX Inc. All rights reserved.
 */
@Data
@Table(name = "tb_activity_lamb")
public class ActivityLamb {

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;
    private Long brokerId;
    private String projectName;
    private String title;
    private String descript;
    private String projectNameZh;
    private String titleZh;
    private String descriptZh;
    private Integer lockedPeriod;
    private BigDecimal platformLimit;
    private BigDecimal userLimit;
    private BigDecimal minPurchaseLimit;
    private String purchaseTokenId;
    private Long startTime;
    private Long endTime;
    private Long createdTime;
    private Long updatedTime;
    private Integer projectType;
    private Integer status;
    private BigDecimal purchaseableQuantity; //平台剩余可购买数量
    private String fixedInterestRate; //固定利率 用于展示 String
    private String floatingRate; //浮动利率 用户展示 String
    private BigDecimal soldAmount; //用于展示的销售总额
    private BigDecimal realSoldAmount; //真实的销售总额

}
