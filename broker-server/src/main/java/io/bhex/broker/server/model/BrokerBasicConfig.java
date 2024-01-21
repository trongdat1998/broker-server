package io.bhex.broker.server.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Table;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.model
 * @Author: ming.xu
 * @CreateDate: 28/09/2018 5:10 PM
 * @Copyright（C）: 2018 BHEX Inc. All rights reserved.
 */
//券商自定义配置信息，无关多语言的部分，每个券商只有一套。
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "tb_broker_basic_config")
public class BrokerBasicConfig {

    private Long id;
    private Long brokerId;
    private String sslCrtFile;
    private String sslKeyFile;
    private String logo;
    private String domainHost;
    private String copyright;
    private String zendesk;
    private String facebook;
    private String twitter;
    private String telegram;
    private String reddit;
    private String wechat;
    private String weibo;
    private Integer status;
    private Long createdAt;
    private String favicon;
    private String extraContact;
    private String logoUrl;
}
