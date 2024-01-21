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
@Table(name = "tb_activity_draw_chance")
public class ActivityDrawChance {

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;

    private Long accountId;

    private String shareTicket;

    private Integer count;

    private Integer usedCount;

    private Integer status;

    private Timestamp createdAt;

    private Timestamp updatedAt;

}
