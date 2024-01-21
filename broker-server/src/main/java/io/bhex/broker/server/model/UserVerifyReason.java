package io.bhex.broker.server.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Id;
import javax.persistence.Table;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.model
 * @Author: ming.xu
 * @CreateDate: 24/08/2018 11:23 AM
 * @Copyright（C）: 2018 BHEX Inc. All rights reserved.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "tb_user_verify_reason")
public class UserVerifyReason {

    @Id
    private Long id;
    private Integer status;
    private Long created;
    private Long updated;
}
