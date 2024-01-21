package io.bhex.broker.server.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import java.sql.Timestamp;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "tb_activity_draw_item")
public class ActivityDrawItem {

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;

    private Long brokerId;

    private String name;

    private String token;

    private Double amount;

    private Integer totalCount;

    private Integer doneCount;

    private Integer limitCount;

    private Integer valueSort;

    private Double userProbability;

    private Double shareProbability;

    private Timestamp createdAt;

    private Timestamp updatedAt;

}
