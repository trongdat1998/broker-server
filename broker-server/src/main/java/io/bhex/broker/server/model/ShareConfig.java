package io.bhex.broker.server.model;

import lombok.*;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.model
 * @Author: ming.xu
 * @CreateDate: 2019/6/27 5:48 PM
 * @Copyright（C）: 2019 BHEX Inc. All rights reserved.
 */
@Data
@Table(name = "tb_share_config")
public class ShareConfig {

    public final static Integer OFF_STATUS = 0;
    public final static Integer ON_STATUS = 1;

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;
    private Long brokerId;
    private String logoUrl;
    private String watermarkImageUrl;
    private Integer status;
    private Integer type;
    private Long adminUserId;
    private Long createdTime;
    private Long updatedTime;
}
