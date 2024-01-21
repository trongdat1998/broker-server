package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.BanSellConfig;
import org.apache.ibatis.annotations.*;
import org.springframework.stereotype.Component;
import tk.mybatis.mapper.common.Mapper;

import java.util.List;

/**
 * @ProjectName: broker-server
 * @Package: io.bhex.broker.server.mapper
 * @Author: yuehao  <hao.yue@bhex.com>
 * @CreateDate: 2018/11/15 下午2:00
 * @Copyright（C）: 2018 BHEX Inc. All rights reserved.
 */
@org.apache.ibatis.annotations.Mapper
@Component
public interface BanSellConfigMapper extends Mapper<BanSellConfig> {

    String TABLE_NAME = " tb_ban_sell_config ";
    String COLUMNS = "id, org_id, account_id, user_name, created , updated";

    @InsertProvider(type = BanSellConfigProvider.class, method = "insert")
    @Options(useGeneratedKeys = true, keyColumn = "id", keyProperty = "id")
    int insert(BanSellConfig banSellConfig);

    @Select("SELECT " + COLUMNS + " FROM tb_ban_sell_config WHERE org_id = #{orgId}")
    List<BanSellConfig> queryAllByOrgId(@Param("orgId") Long orgId);

    @DeleteProvider(type = BanSellConfigProvider.class, method = "deleteByOrgId")
    int deleteByOrgId(Long orgId);
}
