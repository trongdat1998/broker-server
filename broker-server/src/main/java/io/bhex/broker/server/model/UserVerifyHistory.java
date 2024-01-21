package io.bhex.broker.server.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import java.sql.Timestamp;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.model
 * @Author: ming.xu
 * @CreateDate: 24/08/2018 2:54 PM
 * @Copyright（C）: 2018 BHEX Inc. All rights reserved.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "tb_user_verify_history")
public class UserVerifyHistory {

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;
    private Long userVerifyId;
    private Integer status;
    private Long verifyReasonId;
    private String remark;
    private Long adminUserId;
    private Timestamp infoUploadAt;
    private Timestamp createdAt;
    private Timestamp updatedAt;
}
