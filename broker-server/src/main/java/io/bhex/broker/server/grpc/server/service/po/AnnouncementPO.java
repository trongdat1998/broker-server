package io.bhex.broker.server.grpc.server.service.po;

import lombok.Data;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.grpc.server.service.po
 * @Author: ming.xu
 * @CreateDate: 27/08/2018 2:38 PM
 * @Copyright（C）: 2018 BHEX Inc. All rights reserved.
 */
@Data
public class AnnouncementPO {

    private Long id;
    private Long brokerId;
    private Long adminUserId;
    private Integer rank;
    private Integer status;
    private Long createdAt;
    private String title;
    private String content;
    private String locale;
    private Integer type;
    private String pageUrl;
    private Long beginAt;
    private Integer isDefault;
    private Integer platform;
    private Integer channel;

}
