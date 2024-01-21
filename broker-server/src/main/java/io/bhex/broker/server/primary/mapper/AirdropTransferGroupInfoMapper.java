package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.AirdropTransferGroupInfo;
import org.springframework.stereotype.Component;
import tk.mybatis.mapper.common.Mapper;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.admin.mapper
 * @Author: ming.xu
 * @CreateDate: 13/11/2018 10:45 AM
 * @Copyright（C）: 2018 BHEX Inc. All rights reserved.
 */
@org.apache.ibatis.annotations.Mapper
@Component
public interface AirdropTransferGroupInfoMapper extends Mapper<AirdropTransferGroupInfo> {

    String TABLE_NAME = " tb_transfer_group_info ";

    String COLUMNS = "id, broker_id, airdrop_id, status, transfer_count, token_id, transfer_asset_amount, created_at";
}
