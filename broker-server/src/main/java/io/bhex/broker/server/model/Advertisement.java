package io.bhex.broker.server.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "tb_advertisement")
public class Advertisement {

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;

    private Long orgId;

    private Long adminUserId;

    private Integer status;

    private Integer targetType;

    private String picUrl;

    private String targetUrl;

    private String targetSign;

    private String targetObject;

    private String locale;

    private Long createdAt;

    private Long updatedAt;
}
