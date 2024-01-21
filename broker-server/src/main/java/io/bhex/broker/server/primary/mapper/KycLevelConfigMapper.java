package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.KycLevelConfig;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@Mapper
public interface KycLevelConfigMapper extends tk.mybatis.mapper.common.Mapper<KycLevelConfig> {

    @Select("SELECT * FROM tb_kyc_level_config WHERE org_id=#{orgId} AND country_id=#{countryId} AND kyc_level=#{kycLevel}")
    KycLevelConfig getKycLevelConfig(@Param("orgId") Long orgId, @Param("countryId") Long countryId, @Param("kycLevel") Integer kycLevel);

    @Select("SELECT * FROM tb_kyc_level_config WHERE org_id=#{orgId} AND country_id=#{countryId}")
    List<KycLevelConfig> getBrokerCountryConfigs(@Param("orgId") Long orgId, @Param("countryId") Long countryId);
}