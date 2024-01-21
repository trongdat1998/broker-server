package io.bhex.broker.server.domain;

import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;

/**
 * @Description:
 * @Date: 2021/1/4 下午2:26
 * @Author: liwei
 * @Copyright（C）: 2018 BlueHelix Inc. All rights reserved.
 */
@Data
@Builder
public class RcSubAccountConfig {
    private Boolean allowTrade;
    private BigDecimal maxTransferAmount; // 禁止这个子账户单次转账大于1000万usdt
    private BigDecimal minHoldingAmount; // 这个账户usdt资产小于等于1.7亿usdt,以后禁止转账提现
    private String unit;
}