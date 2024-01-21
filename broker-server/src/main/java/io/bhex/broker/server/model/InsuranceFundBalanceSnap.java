package io.bhex.broker.server.model;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Table;
import java.math.BigDecimal;
import java.util.Date;

/**
 * @Description: 合约保证金资产快照
 * @Date: 2020/7/18 下午12:53
 * @Author: liwei
 * @Copyright（C）: 2018 BlueHelix Inc. All rights reserved.
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class InsuranceFundBalanceSnap {
    private Date date;
    private Long accountId;
    private String tokenId;
    private BigDecimal total;
    private BigDecimal available;
}
