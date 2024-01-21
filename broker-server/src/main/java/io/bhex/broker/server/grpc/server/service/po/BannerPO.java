package io.bhex.broker.server.grpc.server.service.po;

import lombok.Data;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.grpc.server.service.po
 * @Author: ming.xu
 * @CreateDate: 28/08/2018 3:24 PM
 * @Copyright（C）: 2018 BHEX Inc. All rights reserved.
 */
@Data
public class BannerPO {

    private Long id;
    private Long brokerId;
    private Long adminUserId;
    private Integer rank;
    private Integer type;
    private String pageUrl;
    private Long beginAt;
    private Long endAt;
    private String remark;
    private Boolean isWeb;
    private Boolean isAndroid;
    private Boolean isIos;
    private Integer platform;
    private Integer bannerPosition;
    private String title;
    private String content;
    private String imageUrl;
    private String h5ImageUrl;
    private String locale;
    private Boolean isDefault;
    private Long createdAt;
}
