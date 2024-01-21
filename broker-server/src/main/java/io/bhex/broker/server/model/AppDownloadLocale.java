package io.bhex.broker.server.model;

import lombok.Data;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * @Description:
 * @Date: 2019/8/11 下午6:29
 * @Author: liwei
 * @Copyright（C）: 2018 BlueHelix Inc. All rights reserved.
 */
@Data
@Table(name = "tb_app_download_locale")
public class AppDownloadLocale {

    public static final Integer OFF_STATUS = 0;
    public static final Integer ON_STATUS = 1;

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;
    private Long brokerId;

    private Integer downloadType;
    private String androidGuideImageUrl;
    private String iosGuideImageUrl;
    private String downloadWebUrl;
    private String language;
    private Integer status;
    private Long adminUserId;
    private Long createdTime;
    private Long updatedTime;
}
