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
 * @CreateDate: 27/08/2018 11:35 AM
 * @Copyright（C）: 2018 BHEX Inc. All rights reserved.
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "tb_announcement_detail")
public class AnnouncementDetail {

    @Id
    private Long id;
    private Long  announcementId;
    private Long adminUserId;
    private String title;
    private String content;
    private String locale;
    private Integer type;
    private String pageUrl;
    private Integer isDefault;
    private Integer status;
    private Timestamp createdAt;
}
