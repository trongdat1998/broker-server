package io.bhex.broker.server.model;


import lombok.*;

import javax.persistence.Column;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import java.sql.Timestamp;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Table(name = "tb_import_user_log")
public class ImportUserLog {

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;

    @Column(name = "org_id")
    private Long orgId;

    @Column(name = "bhop_uid")
    private Long bhopUid;

    @Column(name = "orig_uid")
    private Long origUid;

    @Column(name = "user_identity")
    private String identity;

    @Column(name = "comment")
    private String comment;

    @Column(name = "create_at")
    private Timestamp createdAt;
}
