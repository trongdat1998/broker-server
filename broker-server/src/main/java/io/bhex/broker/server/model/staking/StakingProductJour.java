package io.bhex.broker.server.model.staking;

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
@Table(name = "tb_staking_product_jour")
public class StakingProductJour {

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;
   
    private Long orgId;
   
    private Long userId;
   
    private Long accountId;
   
    private Long productId;
    /**
     * 产品类型
     */
    private Integer productType;
    /**
     * 0=申购 1=派息 2=赎回
     */
    private Integer type;
    /**
     *
     **/
    private BigDecimal amount;
 
    private String tokenId;
 
    private Long transferId;

    private Long createdAt;

    private Long updatedAt;
}
