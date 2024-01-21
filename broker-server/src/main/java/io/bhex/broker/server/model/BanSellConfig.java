package io.bhex.broker.server.model;

import javax.persistence.Id;
import javax.persistence.Table;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @ProjectName: broker-server
 * @Package: io.bhex.broker.server.model
 * @Author: yuehao
 * @CreateDate: 15/11/2018 12:28 PM
 * @Copyright（C）: 2018 BHEX Inc. All rights reserved.
 */
@Data
@Builder
@Table(name = "tb_ban_sell_config")
@AllArgsConstructor
@NoArgsConstructor
public class BanSellConfig {

    @Id
    private Long id;
    private Long orgId;
    private Long accountId;
    private String userName;
    private Long created;
    private Long updated;
}
