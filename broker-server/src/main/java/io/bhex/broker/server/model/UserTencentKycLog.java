package io.bhex.broker.server.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Id;
import javax.persistence.Table;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "tb_user_tencent_kyc_log")
public class UserTencentKycLog {

    public static final String RESP_CODE_UNKNOWN = "999999";

    @Id
    private Long id;

    private Long orgId;

    private Long userId;

    private Long kycApplyId;

    private String action;

    private String reqParam;

    private String respCode;

    private String respMsg;

    private Long created;

    private Long updated;
}
