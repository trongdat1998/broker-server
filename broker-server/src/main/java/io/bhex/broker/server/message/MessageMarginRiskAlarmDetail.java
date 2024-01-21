package io.bhex.broker.server.message;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author JinYuYuan
 * @description
 * @date 2020-06-30 10:41
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MessageMarginRiskAlarmDetail {
    public Long accountId;
    //安全度
    public String safety;
    /**
     * MARGIN_WARN 预警通知
     * MARGIN_APPEND 追加通知
     * MARGIN_LIQUIDATION_ALERT 强求警告（开始强平）
     * MARGIN_LIQUIDATED_NOTIFY(强平通知 已强平)
     */
    public String loanRiskNotifyType;

    public Long timespace;
}
