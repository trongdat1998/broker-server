package io.bhex.broker.server.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Id;
import javax.persistence.Table;
import java.sql.Timestamp;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.model
 * @Author: ming.xu
 * @CreateDate: 28/08/2018 2:58 PM
 * @Copyright（C）: 2018 BHEX Inc. All rights reserved.
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "tb_banner_detail")
public class BannerDetail {

    @Id
    private Long id;
    private Long adminUserId;
    private Long brokerId;
    private Long bannerId;
    private String title;
    private String content;
    private String imageUrl;
    private String h5ImageUrl;
    private String remark;
    private Integer type;
    private String pageUrl;
    private String locale;
    private Integer isDefault;
    private Integer status;
    private Timestamp createdAt;
}
