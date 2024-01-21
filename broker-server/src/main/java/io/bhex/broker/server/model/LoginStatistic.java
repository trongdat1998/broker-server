package io.bhex.broker.server.model;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import java.util.Date;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "tb_login_statistic")
public class LoginStatistic {

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;

    private String statisticDate;

    private Long userId;

    private Long orgId;

    private Integer type;

    private Date updated;

    private Date created;
}
