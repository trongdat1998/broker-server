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
@Table(name = "tb_activity_draw_record")
public class ActivityDrawRecord {

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;

    private Long targetAccountId;

    private Long itemId;

    private String itemName;

    private Integer type;

    private String openId;

    private String userName;

    private String headerUrl;

    private Integer status;

    private Timestamp createdAt;

    private Timestamp updatedAt;
}
