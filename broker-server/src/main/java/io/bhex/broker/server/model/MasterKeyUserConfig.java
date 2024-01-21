package io.bhex.broker.server.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import java.math.BigDecimal;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "tb_master_key_user_config")
public class MasterKeyUserConfig {

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;
    private Long orgId;
    private Long userId;
    private String callback;
    private Integer creditAccount; // 是否信用账户
    private BigDecimal creditAmount; // 总的售馨额度
    private Integer createAccount; // 默认1
    private Integer transferIn; // 默认1
    private Integer transferOut; // 默认1
    private Long created;
    private Long updated;

}
