package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.AssetSnapshotRecord;
import org.springframework.stereotype.Component;
import tk.mybatis.mapper.common.Mapper;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.admin.mapper
 * @Author: ming.xu
 * @CreateDate: 10/11/2018 7:21 PM
 * @Copyright（C）: 2018 BHEX Inc. All rights reserved.
 */
@Component
@org.apache.ibatis.annotations.Mapper
public interface AssetSnapshotRecordMapper extends Mapper<AssetSnapshotRecord> {

    String TABLE_NAME = " tb_asset_snapshot_record ";

    String COLUMNS = "id, broker_id, snapshot_time, account_id, token_id, token_amount, status, created_at";
}
