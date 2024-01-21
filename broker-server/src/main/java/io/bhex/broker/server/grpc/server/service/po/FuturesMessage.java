package io.bhex.broker.server.grpc.server.service.po;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class FuturesMessage {

    private Long messageId; //消息ID

    private Long orgId; //券商ID

    private Long userId; //券商用户ID

    private String thirdUserId; //三方用户ID

    private String liquidationType; // LIQUIDATION_ALERT(强平警告) LIQUIDATED_NOTIFY(强平通知 已强平) ADL_NOTIFY (减仓通知)

    private Boolean isLong; ///true 多仓 false 空仓

    private String symbolId; //symbolId

    private String marginRatePercent; //保证金率 例 15%

    private Long accountId; //账户ID
}
