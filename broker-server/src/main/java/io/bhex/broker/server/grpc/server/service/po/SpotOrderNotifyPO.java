package io.bhex.broker.server.grpc.server.service.po;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author wangsc
 * @description
 * @date 2020-12-14 16:44
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class SpotOrderNotifyPO {
    private Boolean planOrder;
    private Long orgId;
    private Long orderId;
    private String clientOrderId;
    private Long accountId;
    private Integer accountType;
    private String symbolId;
    private String symbolName;
    private String price;
    private String origQty;
    private Integer orderType;
    private Integer orderSide;
    private String triggerPrice;
    private String quotePrice;
    private Integer notifyType;
}
