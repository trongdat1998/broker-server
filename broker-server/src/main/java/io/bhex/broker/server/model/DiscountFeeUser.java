package io.bhex.broker.server.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Id;
import javax.persistence.Table;
import java.util.Date;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "tb_discount_fee_user")
public class DiscountFeeUser {


    @Id
    private Long id;

    private Long orgId;

    private Long exchangeId;

    private String symbolId;

    private Long userId;

    private Long baseGroupId;

    private Long temporaryGroupId;

    private Integer status;

    private Date created;

    private Date updated;
}
