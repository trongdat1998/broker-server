package io.bhex.broker.server.model;

import io.bhex.broker.core.domain.GsonIgnore;
import java.sql.Timestamp;
import javax.persistence.Id;
import javax.persistence.Table;
import lombok.Data;

/**
 * @author wangsc
 * @description org配置
 * @date 2020-06-01 16:15
 */
@Data
@Table(name = "tb_broker_org_config")
public class BrokerOrgConfig {
    @Id
    private Long id;

    private String apiKey;

    @GsonIgnore
    private String secretKey;

    private Timestamp created;

    private String tag;

    private Long orgId;
}