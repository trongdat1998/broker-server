package io.bhex.broker.server.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import java.util.Date;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "tb_account_finance")
public class AccountFinance {

    // 自动创建的账户
    public static final int CREATE_TYPE_AUTO = 0;
    // 手动创建的账户
    public static final int CREATE_TYPE_MANUAL = 1;

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;
    private Long orgId;
    private Long realOrgId;
    private Long userId;
    private Long accountId;
    private Integer accountType;
    private Integer accountIndex;
    private Integer createType;
    private Integer status;
    private String memo;
    private Date created;
    private Date updated;
}
