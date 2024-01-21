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
@Table(name = "tb_invite_register_flow")
public class InviteRegisterFlow {

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;

    private Long userId;

    private Long accountId;

    private Long orgId;

    private String tokenId;

    private Double amount;

    private Date createdAt;

    private Date updatedAt;




}
