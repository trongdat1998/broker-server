package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.AccountFinance;
import org.apache.ibatis.annotations.Select;

import java.util.List;

public interface AccountFinanceMapper extends tk.mybatis.mapper.common.Mapper<AccountFinance> {

    @Select("SELECT * FROM tb_account_finance WHERE org_id=#{orgId}")
    AccountFinance getByOrgId(Long orgId);

    @Select("SELECT * FROM tb_account_finance WHERE account_id=#{accountId}")
    AccountFinance getByAccountId(Long accountId);

    @Select("SELECT * FROM tb_account_finance WHERE status=1")
    List<AccountFinance> getAccountFinanceList();
}
