package io.bhex.broker.server.model.staking;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "tb_staking_product_permission")
public class StakingProductPermission {

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;
    /**
     *
     **/
    private Long orgId;
    /**
     * 允许定期产品开立  0=不允许 1=允许
     */
    private Integer allowFixed;
    /**
     * 允许定期锁仓产品开立 0=不允许 1=允许
     */
    private Integer allowFixedLock;
    /**
     * 允许活期产品开立 0=不允许 1允许
     */
    private Integer allowFlexible;
    private Long created_at;
    private Long updated_at;
}
