package io.bhex.broker.server.model;

import lombok.Data;

import javax.persistence.Table;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.model
 * @Author: ming.xu
 * @CreateDate: 27/11/2018 4:07 PM
 * @Copyright（C）: 2018 BHEX Inc. All rights reserved.
 */
@Data
@Table(name = "tb_notice_template_init_data")
public class NoticeTemplateInitData {

    private Long id;
    private Integer noticeType;
    private String businessType;
    private String language;
    private Long templateId;
    private String templateContent;
    private Integer sendType;
    private Long created;
    private Long updated;
}
