package io.bhex.broker.server.model;

import java.math.BigDecimal;

import javax.persistence.Column;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.validation.constraints.NotNull;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @ProjectName: broker-server
 * @Package: io.bhex.broker.server.model
 * @Author: yuehao  <hao.yue@bhex.com>
 * @CreateDate: 2018/11/16 下午4:30
 * @Copyright（C）: 2018 BHEX Inc. All rights reserved.
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "tb_activity_transfer_account_log")
public class ActivityTransferAccountLog {

    @Column(name = "id")
    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;

    @Column(name = "transfer_id")
    @NotNull
    private Long transferId;

    @Column(name = "source_account_id")
    @NotNull
    private Long sourceAccountId;

    @Column(name = "target_account_id")
    @NotNull
    private Long targetAccountId;

    @Column(name = "amount")
    @NotNull
    private BigDecimal amount;

    @Column(name = "token")
    private String token;

    @Column(name = "create_time")
    private Long createTime;

    @Column(name = "user_id")
    private Long userId;
}
