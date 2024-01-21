package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.staking.StakingProductJour;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.SelectProvider;

import java.util.List;

@Mapper
public interface StakingProductJourMapper extends tk.mybatis.mapper.common.Mapper<StakingProductJour> {

    @SelectProvider(type = StakingProductJourProvider.class, method = "getProductJourList")
    List<StakingProductJour> getProductJourList(@Param("orgId") long orgId,
                                                @Param("userId") long userId,
                                                @Param("productId") long productId,
                                                @Param("productType") int productType,
                                                @Param("type") int type,
                                                @Param("startId") long startId,
                                                @Param("limit") int limit,
                                                @Param("jourId") long jourId);


    /**
     * 为兼容原币多多接口功能，特殊新增此接口，其他业务不能使用
     */
    @Select("select * from tb_staking_product_jour where org_id = #{orgId} and (id = #{jourId} or transfer_id = #{orderId}) and product_type = 1;")
    List<StakingProductJour> getSimpleFinanceRecord(@Param("orgId") long orgId, @Param("jourId") long jourId, @Param("orderId") long orderId);
}