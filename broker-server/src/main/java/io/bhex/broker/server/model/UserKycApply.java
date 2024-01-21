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
@Table(name = "tb_user_kyc_apply")
public class UserKycApply {
    @Id
    private Long id;
    private Long userId;
    private Long orgId;
    private String countryCode;
    private Integer kycLevel;
    private String firstName;
    private String secondName;
    private Integer gender;
    private Integer cardType;
    private String cardNo;
    private String cardFrontUrl;
    private String cardBackUrl;
    private String cardHandUrl;
    private String facePhotoUrl;
    private String faceVideoUrl;
    private String videoUrl;
    private String dataSecret;
    private Integer verifyStatus;
    private String verifyMessage;
    private Long verifyReasonId;
    private Long created;
    private Long updated;
}