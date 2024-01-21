package io.bhex.broker.server.model;

import lombok.Data;

import javax.persistence.Table;
import java.math.BigDecimal;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.admin.model
 * @Author: ming.xu
 * @CreateDate: 10/11/2018 6:53 PM
 * @Copyright（C）: 2018 BHEX Inc. All rights reserved.
 */
@Data
@Table(name = "tb_asset_snapshot_record")
public class AssetSnapshotRecord {

    private Long id;
    private Long brokerId;
    private Long snapshotTime;
    private Long accountId;
    private String tokenId;
    private BigDecimal assetAmount;
    private Long createdAt;
}
