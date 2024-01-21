package io.bhex.broker.server.model;

import io.bhex.base.constants.ProtoConstants;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Objects;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.model
 * @Author: ming.xu
 * @CreateDate: 2019/6/4 2:49 PM
 * @Copyright（C）: 2019 BHEX Inc. All rights reserved.
 */
@Slf4j
@Data
@Table(name = "tb_activity_lock_interest")
public class ActivityLockInterest {

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;
    private Long brokerId;
    @Deprecated
    private String projectName;
    private String projectCode;
    @Deprecated
    private String title;
    @Deprecated
    private String descript;
    @Deprecated
    private Integer lockedPeriod;
    private BigDecimal platformLimit;
    private BigDecimal userLimit;
    private BigDecimal minPurchaseLimit;
    private String purchaseTokenId;
    private String purchaseTokenName;
    private Long startTime;
    private Long endTime;
    private Long createdTime;
    private Long updatedTime;
    private Integer projectType;
    //状态 0 删除 1 开启 2 关闭 3 提前结束
    private Integer status;
    private BigDecimal purchaseableQuantity; //平台剩余可购买数量
    private BigDecimal soldAmount; //用于展示的销售总额,申购币种
    private BigDecimal realSoldAmount; //真实的销售总额,申购币种
    private Integer isPurchaseLimit; //是否有购买限额 0 没有购买限额 1 有购买限额
    private String receiveTokenId;
    private String receiveTokenName;
    private BigDecimal exchangeRate;
    private BigDecimal totalCirculation;   // 总支付货币发行量
    private BigDecimal totalReceiveCirculation; // 总收到货币发行量
    private BigDecimal valuationTokenQuantity; // 展示比例 计价货币数量
    private BigDecimal receiveTokenQuantity; // 展示比例 获得货币数量
    private Integer haveGift; // 展示比例 获得货币数量
    private String baseProcessPercent;//初始进度百分比
    private Integer isShow;

    private String domain; //官网
    private String whitePaper;//白皮书
    private String browser;//区块链浏览器
    private Integer version;

    //活动类型 1锁仓派息  2IEO 3 抢购 4 自由模式

    public String platformLimitStr() {
        if (Objects.isNull(platformLimit)) {
            return "";
        }

        return platformLimit.stripTrailingZeros().toPlainString();
    }

    public String userLimitStr() {
        if (Objects.isNull(userLimit)) {
            return "";
        }

        return userLimit.stripTrailingZeros().toPlainString();
    }

    public String minPurchaseLimitStr() {
        if (Objects.isNull(minPurchaseLimit)) {
            return "";
        }

        return minPurchaseLimit.stripTrailingZeros().toPlainString();
    }

    public String purchaseableQuantityStr() {
        if (Objects.isNull(purchaseableQuantity)) {
            return "";
        }

        return purchaseableQuantity.stripTrailingZeros().toPlainString();
    }

    public String totalReceiveCirculationStr() {
        if (Objects.isNull(totalReceiveCirculation)) {
            return "";
        }

        return totalReceiveCirculation.stripTrailingZeros().toPlainString();
    }

    public String valuationTokenQuantityStr() {
        if (Objects.isNull(valuationTokenQuantity)) {
            return "";
        }

        return valuationTokenQuantity.stripTrailingZeros().toPlainString();
    }

    public String receiveTokenQuantityStr() {
        if (Objects.isNull(receiveTokenQuantity)) {
            return "";
        }

        return receiveTokenQuantity.stripTrailingZeros().toPlainString();
    }

    public String realSoldAmountStr() {
        if (Objects.isNull(realSoldAmount)) {
            return "";
        }

        return realSoldAmount.stripTrailingZeros().toPlainString();
    }

    public BigDecimal getTotalCirculationSafe() {
        if (Objects.isNull(totalCirculation)) {
            return BigDecimal.ZERO;
        }

        return totalCirculation;
    }

    public String totalCirculationStr() {
        if (Objects.isNull(totalCirculation)) {
            return "";
        }

        return totalCirculation.stripTrailingZeros().toPlainString();
    }

    public String soldAmountStr() {
        if (Objects.isNull(soldAmount)) {
            return "";
        }

        return soldAmount.stripTrailingZeros().toPlainString();
    }

    //计算申购系数
    public BigDecimal getPurchaseCoefficient() {
        BigDecimal coefficient = BigDecimal.ONE;
        if (this.getTotalCirculation().compareTo(this.getSoldAmount()) >= 0) {
            return coefficient;
        } else {
            return this.getTotalCirculation().divide(this.getSoldAmount(), ProtoConstants.PRECISION, RoundingMode.DOWN);
        }
    }

    //计算真实申购系数
    public BigDecimal getRealPurchaseCoefficient() {
        BigDecimal coefficient = BigDecimal.ONE;
        if (this.getTotalCirculation().compareTo(this.getRealSoldAmount()) >= 0) {
            return coefficient;
        } else {
            return this.getTotalCirculation().divide(this.getRealSoldAmount(), ProtoConstants.PRECISION, RoundingMode.DOWN);
        }
    }

    //计算兑换比例
    public BigDecimal getExchangeProportion() {
        return this.getReceiveTokenQuantity().divide(this.getValuationTokenQuantity(), 18, RoundingMode.DOWN);
    }
}
