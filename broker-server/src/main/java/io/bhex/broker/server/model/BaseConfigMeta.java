package io.bhex.broker.server.model;


import lombok.Data;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;

@Data
@Table(name = "tb_base_config_meta")
public class BaseConfigMeta {

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;

    private String confGroup;

    private String confKey;

    private String description;

    private String target;

    private String authIds;

    private String dataType;

    private String dataLimit;

    private String displayArea;

    private String displaySubArea;

    private Long created;
}
