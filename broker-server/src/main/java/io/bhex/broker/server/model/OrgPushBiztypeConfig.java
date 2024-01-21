package io.bhex.broker.server.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * author: wangshouchao
 * Date: 2020/07/25 06:27:16
 */
@Data
@Builder(builderClassName = "Builder", toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "tb_org_push_biztype_config")
public class OrgPushBiztypeConfig {

    /**
     * auto increase
     */
    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;
    /**
     * broker id
     */
    private Long orgId;
    /**
     * 业务类型
     */
    private String biztype;
    /**
     * 管理用户
     */
    private String adminUser;
    /**
     * created
     */
    private Long created;
    /**
     * updated
     */
    private Long updated;
}