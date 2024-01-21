package io.bhex.broker.server.grpc.server.service.po;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class UserKycMessage {

    private Long id;

    private Long orgId;

    private Long userId;

    private String firstName;

    private String secondName;

    private Integer gender;

    private Integer verifyStatus;

    private Long created;

    private Long updated;

    private String signature;

    /** 新字段 **/
    private String countryCode;

    private Integer cardType;

    private String cardNo;
}
