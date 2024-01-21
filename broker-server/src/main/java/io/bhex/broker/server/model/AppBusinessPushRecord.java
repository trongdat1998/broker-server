package io.bhex.broker.server.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Table;

@Data
@Builder(builderClassName = "Builder", toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "tb_app_business_push_record")
public class AppBusinessPushRecord {
    private Long id;
    private Long orgId;
    private Long userId;
    private String pushChannel;
    private String pushToken;
    private String reqOrderId;
    private String businessType;
    private String deliveryStatus;
    private Long deliveryTime;
    private Long clickTime;
    private Long created;
    private Long updated;
}
