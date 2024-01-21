package io.bhex.broker.server.model;


import javax.persistence.Id;
import javax.persistence.Table;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@Builder(builderClassName = "Builder", toBuilder = true)
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode(of = {"id"})
@Data
@Table(name = "tb_trade_competition_participant")
public class TradeCompetitionParticipant {

    @Id
    private Long id;

    private Long orgId;

    private String competitionCode;

    private Long competitionId;

    private Long userId;

    private String nickname;

    private String wechat;
}
