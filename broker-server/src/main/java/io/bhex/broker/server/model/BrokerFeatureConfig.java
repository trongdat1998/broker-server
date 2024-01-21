package io.bhex.broker.server.model;

import lombok.Builder;
import lombok.Data;

import javax.persistence.Table;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.model
 * @Author: ming.xu
 * @CreateDate: 28/09/2018 11:44 AM
 * @Copyright（C）: 2018 BHEX Inc. All rights reserved.
 */
//券商自定义配置信息，需要支持多语言的部分，每个券商有多套并且有多种语音。
@Data
@Builder
@Table(name = "tb_broker_feature_config")
public class BrokerFeatureConfig {

    private Long id;
    private Long brokerId;
    private String imageUrl;
    private String title;
    private String description;
    private String locale;
    private Integer rank;
    private Integer status;
    private Long createdAt;
}
