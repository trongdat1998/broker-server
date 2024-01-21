package io.bhex.broker.server.grpc.server.service.po;

import lombok.Data;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.grpc.server.service.po
 * @Author: ming.xu
 * @CreateDate: 2019/6/28 4:47 PM
 * @Copyright（C）: 2019 BHEX Inc. All rights reserved.
 */
@Data
public class ShareConfigPO {
    private Long brokerId;
    private String logoUrl;
    private String watermarkImageUrl;
    private String title;
    private String description;
    private String downloadUrl;
    private String language;
}
