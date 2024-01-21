package io.bhex.broker.server.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Id;
import javax.persistence.Table;

/**
 * @Description: 券商首页自定义配置
 * @Date: 2020/2/11 上午7:26
 * @Author: liwei
 * @Copyright（C）: 2018 BlueHelix Inc. All rights reserved.
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "tb_broker_index_customer_config")
public class BrokerIndexCustomerConfig {

    public static final int WEB_CONFIG = 0;
    public static final int APP_CONFIG = 1;

    @Id
    private Long id;

    private Long brokerId;

    private String moduleName;

    private Integer openStatus; //0-关闭 1-打开

    private Integer status; //BrokerConfigConstants

    private String locale;

    private String content;

    private Long created;

    private Long updated;

    private String tabName;

    private Integer type;

    private String platform;

    private String useModule;

    private Integer configType; //0-web 1-app



}
