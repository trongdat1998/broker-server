package io.bhex.broker.server.model;


import java.math.BigDecimal;
import java.util.Date;

import javax.persistence.Table;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Table(name = "clear_user_buy_back")
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class UserBuyBack {

    private Long id;

    private Long dt;

    private Long userId;

    private String tokenId;

    private BigDecimal quantity;

    private BigDecimal avgPrice;

    private BigDecimal buyPrice;
}
