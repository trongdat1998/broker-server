package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.DiscountFeeConfig;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.math.BigDecimal;


@Mapper
public interface DiscountFeeConfigMapper extends tk.mybatis.mapper.common.Mapper<DiscountFeeConfig> {


    @Select("select *from tb_discount_fee_config where org_id = #{orgId} and exchange_id = #{exchangeId} and symbol_id = #{symbolId} and coin_taker_buy_fee_discount = #{coinTakerBuyFeeDiscount} and coin_maker_buy_fee_discount = #{coinMakerBuyFeeDiscount}")
    DiscountFeeConfig selectCoinDiscountFeeConfig(@Param("orgId") Long orgId,
                                                  @Param("exchangeId") Long exchangeId,
                                                  @Param("symbolId") String symbolId,
                                                  @Param("coinTakerBuyFeeDiscount") BigDecimal coinTakerBuyFeeDiscount,
                                                  @Param("coinMakerBuyFeeDiscount") BigDecimal coinMakerBuyFeeDiscount);

    @Select("select *from tb_discount_fee_config where org_id = #{orgId} and exchange_id = #{exchangeId} and symbol_id = #{symbolId} and option_taker_buy_fee_discount = #{optionTakerBuyFeeDiscount} and option_maker_buy_fee_discount = #{optionMakerBuyFeeDiscount}")
    DiscountFeeConfig selectOptionDiscountFeeConfig(@Param("orgId") Long orgId,
                                                    @Param("exchangeId") Long exchangeId,
                                                    @Param("symbolId") String symbolId,
                                                    @Param("optionTakerBuyFeeDiscount") BigDecimal optionTakerBuyFeeDiscount,
                                                    @Param("optionMakerBuyFeeDiscount") BigDecimal optionMakerBuyFeeDiscount);


    @Select("select *from tb_discount_fee_config where org_id = #{orgId} and exchange_id = #{exchangeId} and symbol_id = #{symbolId} and contract_taker_buy_fee_discount = #{contractTakerBuyFeeDiscount} and contract_maker_buy_fee_discount = #{contractMakerBuyFeeDiscount}")
    DiscountFeeConfig selectContractDiscountFeeConfig(@Param("orgId") Long orgId,
                                                      @Param("exchangeId") Long exchangeId,
                                                      @Param("symbolId") String symbolId,
                                                      @Param("contractTakerBuyFeeDiscount") BigDecimal contractTakerBuyFeeDiscount,
                                                      @Param("contractMakerBuyFeeDiscount") BigDecimal contractMakerBuyFeeDiscount);
}
