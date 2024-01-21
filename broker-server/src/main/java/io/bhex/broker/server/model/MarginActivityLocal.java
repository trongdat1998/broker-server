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
@Table(name = "tb_margin_activity_local")
public class MarginActivityLocal {
    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;
    private Long activityId;
    private Long orgId;
    private String title;
    private String titleColor;
    private String language;
    private String activityUrl;
    private Integer status;
    private Long created;
    private Long updated;

}
