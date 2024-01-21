package io.bhex.broker.server.grpc.server.service.po;

import io.bhex.broker.common.exception.BrokerErrorCode;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 理财：申购返回结果 Server Service
 * @author songxd
 * @date 2020-07-30
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class StakingSubscribeResponseDTO {

    private Long transferId;

    private Integer code;
}
