package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.Account;
import org.apache.ibatis.annotations.*;

import java.util.List;

/**********************************
 *@项目名称: broker-parent
 *@文件名称: io.bhex.broker.mapper
 *@Date 2018/6/10
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/

@Mapper
public interface AccountMapper extends tk.mybatis.mapper.common.Mapper<Account> {

    @SelectProvider(type = AccountSqlProvider.class, method = "getMainAccount")
    Account getMainAccount(@Param("orgId") Long orgId, @Param("userId") Long userId);

    @SelectProvider(type = AccountSqlProvider.class, method = "getAccountByType")
    Account getAccountByType(@Param("orgId") Long orgId, @Param("userId") Long userId, @Param("accountType") Integer accountType, @Param("accountIndex") Integer accountIndex);

    @Select("SELECT * FROM tb_account where org_id=#{orgId} AND user_id=#{userId} AND account_type > 0 order by account_type, account_index")
    List<Account> queryByUserId(@Param("orgId") Long orgId, @Param("userId") Long userId);

    @Select("SELECT * FROM tb_account where org_id=#{orgId} AND user_id=#{userId} AND account_type=#{accountType} order by account_type, account_index")
    List<Account> queryByUserIdAndAccountType(@Param("orgId") Long orgId, @Param("userId") Long userId, @Param("accountType") Integer accountType);

    @Select("SELECT MAX(account_index) FROM tb_account where org_id=#{orgId} AND user_id=#{userId} and account_type=#{accountType}")
    int getMaxIndex(@Param("orgId") Long orgId, @Param("userId") Long userId, @Param("accountType") Integer accountType);

    @SelectProvider(type = AccountSqlProvider.class, method = "getByAccountId")
    Account getAccountByAccountId(@Param("accountId") Long accountId);

    @InsertProvider(type = AccountSqlProvider.class, method = "insert")
    @Options(useGeneratedKeys = true, keyColumn = "id", keyProperty = "id")
    int insertRecord(Account account);

    @UpdateProvider(type = AccountSqlProvider.class, method = "update")
    int update(Account account);

    @Select("SELECT id, org_id, user_id, account_id, account_name, account_type, account_status, created "
            + "FROM tb_account WHERE account_id > 90000")
    List<Account> queryAll();

    @Deprecated
    @SelectProvider(type = AccountSqlProvider.class, method = "getByAccountIdLock")
    Account getByAccountIdLock(@Param("accountId") Long accountId);

    @SelectProvider(type = AccountSqlProvider.class, method = "queryFuturesAccountList")
    List<Account> queryFuturesAccountList(@Param("userId") List<Long> userId);

    @Select("SELECT * FROM tb_account where org_id=#{orgId} AND user_id=#{userId} AND account_index = #{accountIndex}")
    List<Account> queryByUserIdAndIndex(@Param("orgId") Long orgId, @Param("userId") Long userId, @Param("accountIndex") Integer accountIndex);

    @Update("update tb_account set is_forbid = 1,forbid_end_time = #{limitEndTime},forbid_start_time = #{limitStartTime} where account_id=#{accountId}")
    int updateForbidEndTimeByAccountId(@Param("limitStartTime") Long limitStartTime, @Param("limitEndTime") Long limitEndTime, @Param("accountId") Long accountId);

    @Update("update tb_account set is_forbid = 0,forbid_end_time = 0,forbid_start_time = 0 where account_id=#{accountId}")
    int closeForbidByAccountId(@Param("accountId") Long accountId);

    @Select("SELECT * FROM tb_account where org_id=#{orgId} AND user_id=#{userId} AND account_type=#{accountType} and account_index = 0")
    Account queryAccountByUserIdAndAccountType(@Param("orgId") Long orgId, @Param("userId") Long userId, @Param("accountType") Integer accountType);

    @Select("SELECT * FROM tb_account where org_id=#{orgId} AND account_id = #{accountId}")
    Account getAccountByOrgIdAndAccountId(@Param("orgId") Long orgId, @Param("accountId") Long accountId );

    @Select("SELECT id, org_id, user_id, account_id, account_name, account_type, account_status, created "
            + "FROM tb_account WHERE org_id = #{orgId} AND account_type = #{accountType} AND account_id > 90000")
    List<Account> queryAccountByType(@Param("orgId") Long orgId,@Param("accountType") Integer accountType);
}
