package io.bhex.broker.server.model;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Id;
import javax.persistence.Table;


@Builder(builderClassName = "Builder",toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
@Data
@Table(name = "tb_trade_competition_ext")
public class TradeCompetitionExt {

    @Id
    private Long id;

    private Long orgId;

    private Long competitionId;

    private String competitionCode;

    private String language;

    private String name;

    private String description;

    private String bannerUrl;

    private String mobileBannerUrl;
}
