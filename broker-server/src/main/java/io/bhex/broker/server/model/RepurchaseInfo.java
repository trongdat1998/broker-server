package io.bhex.broker.server.model;

import java.util.Date;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @ProjectName:
 * @Package:
 * @Author: yuehao  <hao.yue@bhex.com>
 * @CreateDate: 2021/1/26 下午2:53
 * @Copyright（C）: 2018 BHEX Inc. All rights reserved.
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "tb_repurchase_info")
public class RepurchaseInfo {

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;
    private String dt;
    private Long orgId;
    private String tokenId;
    private String resultTokenId;
    private Date startTime;
    private Date endTime;
    private Integer status;
    private Date created;
}
