package io.bhex.broker.server.model;


import lombok.Builder;
import lombok.Data;

import javax.persistence.Table;

@Data
@Builder
@Table(name = "tb_broker_dict_config")
public class BrokerDictConfig {

    public static final int STATUS_ENABLE = 1;
    public static final int STATUS_PREVIEW = 2;
    public static final int STATUS_DISABLE = 0;

    // 客服URL地址
    public static final String KEY_CUSTOMER_SERVICE = "CUSTOMER_SERVICE";
    // 期权客服地址
    public static final String KEY_OPTION_CUSTOMER_SERVICE = "OPTION_CUSTOMER_SERVICE";
    // 期货客服地址
    public static final String KEY_FUTURES_CUSTOMER_SERVICE = "FUTURES_CUSTOMER_SERVICE";

    /**
     * 期货空投运营账户信息配置
     * 对应值的信息使用json，格式如下:
     * {
     *     "opsAccountId": 220229299463102720,  // 运营账户ID
     *     "opsAccountOrgId": 6001,             // 运营账户机构ID
     *     "opsTokenId": "BUSDT",               // 空投币对
     *     "opsAirdropAmount": "10000"          // 空投数量
     * }
     */
    public static final String KEY_FUTURES_AIRDROP_OPS_ACCOUNT_INFO = "FUTURES_AIRDROP_OPS_ACCOUNT_INFO";

    /**
     * 答题空投系统账户配置
     * 格式为:
     * accoutId|orgId
     *
     * 如果配置的系统账户ID不为空，则会使用系统账户代替运营账户给用户空投体验币
     * 目前线上环境分配的系统账户的值为：27145|21
     *
     * 这个配置用来兼容之前的老配置，如果有新的券商要配置需要配置系统运营账户，直接在FUTURES_AIRDROP_OPS_ACCOUNT_INFO里配置就可以，
     * 不需要单独加这个FUTURES_AIRDROP_OPS_SYSTEM_ACCOUNT配置信息
     */
    public static final String KEY_FUTURES_AIRDROP_OPS_SYSTEM_ACCOUNT = "FUTURES_AIRDROP_OPS_SYSTEM_ACCOUNT";

    public static final String KEY_FUTURES_OPEN_WITH_ANSWER = "FUTURES_OPEN_WITH_ANSWER";

    public static final String KEY_OPTION_OPEN_WITH_ANSWER = "OPTION_OPEN_WITH_ANSWER";

    private Long id;
    private Long orgId;
    private String configKey;
    private String configValue;
    private String configType;
    private Integer status;
    private Long createdAt;
}
