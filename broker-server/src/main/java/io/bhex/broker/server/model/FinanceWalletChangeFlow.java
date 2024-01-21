/**********************************
 *@项目名称: server-parent
 *@文件名称: io.bhex.broker.server.domain
 *@Date 2019-03-07
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
@Builder(builderClassName = "Builder", toBuilder = true)
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "tb_finance_wallet_change_flow")
public class FinanceWalletChangeFlow {

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;
    private Long walletId;
    private Long orgId;
    private Long userId;
    private Long accountId;
    private Long productId;
    private String token;
    private Integer changeType;
    private BigDecimal amount;
    private Long referenceId;
    private BigDecimal originalBalance;
    private BigDecimal changedBalance;
    private BigDecimal originalPurchase;
    private BigDecimal changedPurchase;
    private String originalWallet;
    private String changedWallet;
    private Long created;

}
