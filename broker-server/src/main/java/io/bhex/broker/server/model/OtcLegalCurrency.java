package io.bhex.broker.server.model;


import lombok.*;

import javax.persistence.Id;
import javax.persistence.Table;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Table(name = "tb_otc_legal_currency")
public class OtcLegalCurrency {
    @Id
    private Long id;

    private Long orgId;

    private Long userId;

    private String code;
}
