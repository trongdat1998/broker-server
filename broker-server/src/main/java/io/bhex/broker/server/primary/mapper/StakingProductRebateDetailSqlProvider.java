package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.grpc.server.service.staking.StakingTransferEvent;
import io.bhex.broker.server.model.staking.StakingAssetSnapshot;
import org.apache.commons.lang3.StringUtils;
import org.apache.ibatis.jdbc.SQL;

import java.util.List;
import java.util.stream.Collectors;

/**
 * StakingProductRebateDetailSqlProvider
 * @author songxd
 * @date 2020-08-19
 */
public class StakingProductRebateDetailSqlProvider {

    private static final String TABLE_NAME = "tb_staking_product_rebate_detail";

    public String selectRebateTotal(StakingTransferEvent transferEvent) {
        return new SQL() {
            {
                SELECT("rebate_type rebateType,token_id tokenId,sum(rebate_amount) amount").FROM(TABLE_NAME);
                WHERE("org_id = #{orgId} and product_id = #{productId} and product_rebate_id = #{rebateId} and status = 0 ");
                if(transferEvent.getRebateType() == 0 || transferEvent.getRebateType() == 1){
                    WHERE("rebate_type = #{rebateType}");
                }
                GROUP_BY("rebate_type,token_id");
            }
        }.toString();
    }

    /**
     * 没有使用 bak
     *
     * @param stakingAssetSnapshotList
     * @return
     */
    public String batchUpdateAssetSnapshot(List<StakingAssetSnapshot> stakingAssetSnapshotList) {
        return new SQL() {
            {
                UPDATE("tb_staking_asset_snapshot");
                SET("net_asset = net_asset + " + getNetAssetCaseWhen(stakingAssetSnapshotList));
                SET("pay_interest = " + getInterestCaseWhen(stakingAssetSnapshotList));
                SET("updated_at = #{updateAt}");
                SET("version_lock = #{updateAt}");
                WHERE("org_id = #{orgId} and user_id = #{userId} and product_id = #{productId} and product_rebate_id = #{rebateId} and id in (" +
                        StringUtils.join(stakingAssetSnapshotList.stream().map(StakingAssetSnapshot::getId).collect(Collectors.toList()),",") + ") and version_lock is null ");
            }
        }.toString();
    }

    /**
     * bak
     *
     * @param stakingAssetSnapshotList
     * @return
     */
    private String getNetAssetCaseWhen(List<StakingAssetSnapshot> stakingAssetSnapshotList){
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(" case id ");
        stakingAssetSnapshotList.forEach(assetSnapshot -> { stringBuilder.append(" when ").append(assetSnapshot.getId()).append(" then ").append(assetSnapshot.getNetAsset()); });
        stringBuilder.append(" else 0 end ");
        return stringBuilder.toString();
    }

    /**
     * bak
     *
     * @param stakingAssetSnapshotList
     * @return
     */
    private String getInterestCaseWhen(List<StakingAssetSnapshot> stakingAssetSnapshotList){
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(" case id ");
        stakingAssetSnapshotList.forEach(assetSnapshot -> { stringBuilder.append(" when ").append(assetSnapshot.getId()).append(" then ").append(assetSnapshot.getPayInterest()); });
        stringBuilder.append(" else 0 end ");
        return stringBuilder.toString();
    }
}
