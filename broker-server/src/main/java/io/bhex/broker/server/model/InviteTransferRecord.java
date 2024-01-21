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
@Table(name = "tb_invite_transfer_record")
public class InviteTransferRecord {

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;

    private Long clientId;

    private Long accountId;

    private Integer actType;

    private Long statisticsTime;

    private Timestamp createdAt;

    private Timestamp updatedAt;



}
