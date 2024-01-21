package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.FinanceWallet;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import java.math.BigDecimal;
import java.util.List;

@Mapper
public interface FinanceWalletMapper extends tk.mybatis.mapper.common.Mapper<FinanceWallet> {

    String TABLE_NAME = "tb_finance_wallet";

    @Select(" select * from " + TABLE_NAME + " where org_id = #{orgId} and user_id = #{userId} ")
    List<FinanceWallet> getFinanceWalletByOrgIdAndUserId(@Param("orgId") Long orgId, @Param("userId") Long userId);

    @Select(" select * from " + TABLE_NAME + " where org_id = #{orgId} and user_id = #{userId} and product_id = #{productId} ")
    FinanceWallet getFinanceWallet(@Param("orgId") Long orgId, @Param("userId") Long userId, @Param("productId") Long productId);

    @Select(" select * from " + TABLE_NAME + " where org_id = #{orgId} and user_id = #{userId} and product_id = #{productId} for update ")
    FinanceWallet getFinanceWalletLock(@Param("orgId") Long orgId, @Param("userId") Long userId, @Param("productId") Long productId);

    @Select(" select * from " + TABLE_NAME + " where product_id=#{productId} limit #{startIndex}, #{pageSize} ")
    List<FinanceWallet> getFinanceWalletByPage(@Param("productId") Long productId,
                                               @Param("startIndex") Integer startIndex,
                                               @Param("pageSize") Integer pageSize);

    @Select("select count(1) from " + TABLE_NAME + " for update")
    int lockWalletTable();

    @Update(" update " + TABLE_NAME + " set purchase = purchase + #{purchase}, updated_at = #{updateTime} where id = #{id} ")
    int updateWalletAfterPurchase(@Param("id") Long id,
                                  @Param("purchase") BigDecimal purchaseChangeValue,
                                  @Param("updateTime") Long updateTime);

    @Update(" update " + TABLE_NAME + " set balance = balance + #{balance}, purchase = purchase + #{purchase}, redeem = redeem + #{redeem}, updated_at = #{updateTime} where id=#{id} ")
    int updateWalletAfterRedeem(@Param("id") Long id,
                                @Param("balance") BigDecimal balanceChangeValue,
                                @Param("purchase") BigDecimal purchaseChangeValue,
                                @Param("redeem") BigDecimal redeem,
                                @Param("updateTime") Long updateTime);

    @Update(" update " + TABLE_NAME + " set balance = balance + #{balance}, purchase = purchase + #{purchase}, updated_at = #{updateTime} where id = #{id} ")
    int updateWalletWhenDailyTaskAutoExecute(@Param("id") Long id,
                                             @Param("balance") BigDecimal balanceChangeValue,
                                             @Param("purchase") BigDecimal purchaseChangeValue,
                                             @Param("updateTime") Long updateTime);

    @Update(" update " + TABLE_NAME + " set balance = #{balance}, purchase = #{purchase}, updated_at = #{updateTime} where id = #{id} ")
    int updateWalletAfterDailyTaskManualExecute(@Param("id") Long id,
                                                @Param("balance") BigDecimal balance,
                                                @Param("purchase") BigDecimal purchase,
                                                @Param("updateTime") Long updateTime);

    @Update(" update " + TABLE_NAME + " set redeem = redeem + #{redeem}, updated_at = #{updateTime} where id = #{id} ")
    int updateWalletAfterRedeemSuccess(@Param("id") Long id,
                                       @Param("redeem") BigDecimal redeem,
                                       @Param("updateTime") Long updateTime);

    @Update(" update " + TABLE_NAME + " set last_profit = #{lastProfit}, total_profit = total_profit + #{lastProfit}, updated_at = #{updateTime} where id = #{id} ")
    int updateLastProfit(@Param("id") Long id,
                         @Param("lastProfit") BigDecimal lastProfit,
                         @Param("updateTime") Long updateTime);

    @Select(" select * from " + TABLE_NAME + " where org_id = #{orgId} and product_id = #{productId} order by id limit #{startIndex}, #{pageSize} ")
    List<FinanceWallet> getFinanceWalletByOrgIdAndProductId(@Param("orgId") Long orgId,
                                                            @Param("productId") Long productId,
                                                            @Param("startIndex") Integer startIndex,
                                                            @Param("pageSize") Integer pageSize);
}
