package io.bhex.broker.server.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Id;
import javax.persistence.Table;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "tb_card_type_locale")
public class CardTypeLocale {
    @Id
    private Long id;
    private Integer cardTypeId;
    private String locale;
    private String name;
}
