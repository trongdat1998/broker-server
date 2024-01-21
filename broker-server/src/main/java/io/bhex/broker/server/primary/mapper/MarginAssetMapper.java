package io.bhex.broker.server.primary.mapper;

import io.bhex.base.margin.Margin;
import io.bhex.broker.server.model.MarginAssetRecord;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

@Mapper
public interface MarginAssetMapper extends tk.mybatis.mapper.common.Mapper<MarginAssetRecord> {

    @Select("select * from tb_margin_asset where org_id = #{orgId} and account_id = #{accountId}")
    MarginAssetRecord getMarginAssetByAccount(@Param("orgId") Long orgId, @Param("accountId")Long accountId);

    @Select("select * from tb_margin_asset where org_id = #{orgId} and account_id = #{accountId} for update")
    MarginAssetRecord lockMarginAssetByAccount(@Param("orgId") Long orgId, @Param("accountId")Long accountId);

    @Select("select * from tb_margin_asset where org_id = #{orgId}")
    List<MarginAssetRecord> queryMarginAssetByOrgId(@Param("orgId") Long orgId);


}
