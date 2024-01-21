package io.bhex.broker.server.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "tb_activity_trade_statistics")
public class ActivityTradeStatistics {

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;

    private Long activityId;

    private Long cardId;

    private Long userId;

    private Long lastTradeId;

    private Long createdAt;

    private Long updatedAt;


}
