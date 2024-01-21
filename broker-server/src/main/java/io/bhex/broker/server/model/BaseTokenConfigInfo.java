package io.bhex.broker.server.model;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Transient;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "tb_base_token_config")
@Builder(builderClassName = "Builder", toBuilder = true)
public class BaseTokenConfigInfo {

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;

    private Long orgId;

    private String token;

    private String confGroup;

    private String confKey;

    private String confValue;

    private String extraValue;

    private Integer status;

    //private Integer openStatus;

    private String language;

    private String adminUserName;

    private Long created;

    private Long updated;

    private String newConfValue;

    private String newExtraValue;

    private Long newStartTime;

    private Long newEndTime;

    @Transient
    @lombok.Builder.Default
    private Boolean isOpen = true;

}
