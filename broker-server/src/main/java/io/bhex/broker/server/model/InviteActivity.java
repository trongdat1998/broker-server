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
@Table(name = "tb_invite_activity")
public class InviteActivity {

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;

    private Long orgId;

    private Integer type;

    private String grantTokenId;

    private Double amount;

    private Long period;

    private Integer autoTransfer;

    private Integer status;

    private Date createdAt;

    private Date updatedAt;

    private Integer coinStatus;

    private Integer futuresStatus;
}
