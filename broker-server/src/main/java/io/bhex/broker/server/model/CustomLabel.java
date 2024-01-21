package io.bhex.broker.server.model;

import io.bhex.broker.server.primary.mapper.CustomLabelMapper;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.model
 * @Author: ming.xu
 * @CreateDate: 2019/12/11 4:25 PM
 * @Copyright（C）: 2019 BHEX Inc. All rights reserved.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table(name = CustomLabelMapper.TABLE_NAME)
public class CustomLabel {

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;
    private Long orgId;
    private Long labelId;
    private String language;
    private String labelValue;
    private String colorCode;
    private Integer userCount;
    private String userIdsStr;
    private Integer status;
    private Integer isDel;
    private Integer type;
    private Long createdAt;
    private Long updatedAt;
}
