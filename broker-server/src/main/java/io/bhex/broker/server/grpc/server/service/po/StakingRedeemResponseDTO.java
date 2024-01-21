package io.bhex.broker.server.grpc.server.service.po;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 理财：赎回返回结果 Server-Service-DTO
 * @author songxd
 * @date 2020-07-29
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class StakingRedeemResponseDTO {

    private Integer code;

    private Long transferId;

}
