package io.bhex.broker.server.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Column;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "tb_payment_order")
public class PaymentOrder {

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;
    private Long orgId;
    private String clientOrderId;
    private Long orderId; // 平台生成
    private Long payerUserId;
    private Long payerAccountId;
    private Long payeeUserId;
    private Long payeeAccountId;
    private Integer isMapping;
    private String payInfo; // proto json
    private String productInfo;
    private Long expiredTime;
    private Integer orderStatus;
    @Column(name = "`desc`")
    private String desc;
    private String extendInfo;
    private Long created;
    private Long updated;

}
