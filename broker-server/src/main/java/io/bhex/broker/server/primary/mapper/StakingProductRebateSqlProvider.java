package io.bhex.broker.server.primary.mapper;

import com.google.common.base.Strings;
import io.bhex.broker.server.model.Account;
import io.bhex.broker.server.model.staking.StakingProductRebate;
import org.apache.ibatis.jdbc.SQL;

/**
 * StakingProductRebateSqlProvider
 * @author songxd
 * @date 2020-08-19
 */
public class StakingProductRebateSqlProvider {

    private static final String TABLE_NAME = "tb_staking_product_rebate";

    public String updateStatusAndAmountOrRate(StakingProductRebate stakingProductRebate) {
        return new SQL() {
            {
                UPDATE(TABLE_NAME);
                SET("status = #{status}");
                SET("updated_at = #{updatedAt}");
                if (stakingProductRebate.getRebateRate() != null) {
                    SET("rebate_rate = #{rebateRate}");
                }
                if (stakingProductRebate.getRebateAmount() != null) {
                    SET("rebate_amount = #{rebateAmount}");
                }
                WHERE("id = #{id} and org_id = #{orgId}");
            }
        }.toString();
    }
}
