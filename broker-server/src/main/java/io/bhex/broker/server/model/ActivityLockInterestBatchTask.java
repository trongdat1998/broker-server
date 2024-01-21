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
 * @CreateDate: 2020/9/7 下午5:26
 * @Copyright（C）: 2018 BHEX Inc. All rights reserved.
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "tb_activity_lock_interest_batch_task")
public class ActivityLockInterestBatchTask {

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;

    private Long orgId;

    private Long projectId;

    private String projectCode;

    private String url;

    private Integer status;//0未处理 1处理成功 2失败

    private Date created;

    private Date updated;
}
