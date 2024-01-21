package io.bhex.broker.server.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "tb_data_push_config")
public class DataPushConfig {

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;

    private Long orgId;

    private String callBackUrl;

    private String key;

    private String value;

    private Integer status;

    private Integer encrypt;

    private Integer futures; //是否开启期货推送 0不开启 1开启

    private String futuresCallBackUrl; //期货推送url

    private Integer retryCount; //重试次数配置

    private Integer otcStatus; // otc是否callback 0不推送 1推送
}


