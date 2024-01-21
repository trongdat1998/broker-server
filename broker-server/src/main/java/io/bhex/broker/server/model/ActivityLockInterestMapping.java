package io.bhex.broker.server.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import java.math.BigDecimal;
import java.util.Date;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "tb_activity_lock_interest_mapping")
public class ActivityLockInterestMapping {

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;

    private Long brokerId;

    private Long projectId;

    private Long userId;

    private Long accountId;

    private String tokenId;

    private String sourceTokenId;

    private BigDecimal amount;

    private BigDecimal useAmount;

    private BigDecimal luckyAmount;

    private BigDecimal backAmount;

    private Long sourceAccountId;

    private BigDecimal modulus;

    private Integer transferStatus; ////0未处理 1已处理 2处理失败

    private Integer unlockStatus; //0未处理 1已处理 2处理失败

    private Integer isLock;

    private Date transferTime;

    private Date unlockTime;

    private Date createTime;

    private Long orderId;

    private Long unlockClientOrderId;

    private Long userClientOrderId;

    private Long ieoAccountClientOrderId;
}
