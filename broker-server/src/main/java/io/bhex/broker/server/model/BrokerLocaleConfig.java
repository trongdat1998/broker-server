package io.bhex.broker.server.model;

import lombok.Builder;
import lombok.Data;

import javax.persistence.Table;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.model
 * @Author: ming.xu
 * @CreateDate: 28/09/2018 11:40 AM
 * @Copyright（C）: 2018 BHEX Inc. All rights reserved.
 */
//券商自定义配置信息，需要支持多语言的部分，每个券商只有一套但是有多种语音。
@Data
@Builder
@Table(name = "tb_broker_locale_config")
public class BrokerLocaleConfig {

    public static final Integer ENABLE = 1;

    private Long id;
    private Long brokerId;

    private String featureTitle;
    private String locale;
    private String browserTitle;

    private String userAgreement;
    private String userOptionAgreement;
    private String userFuturesAgreement;
    private String privacyAgreement;
    private String legalDescription;
    private String helpCenter;

    // 录制视频认证协议文本
    private String videoVerifyAgreement;

    private Integer enable;
    private Integer status;
    private Long createdAt;

    private String headConfig;

    private String footConfig;

    private String seoDescription;

    private String seoKeywords;

}
