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
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "tb_invite_statistics_record")
public class InviteStatisticsRecord {

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;

    private Long orgId;

    private Integer inviteType;

    private Long statisticsTime;

    private String token;

    private Double amount;

    private Double transferAmount;

    private Integer status;

    private Date createdAt;

    private Date updatedAt;

}
