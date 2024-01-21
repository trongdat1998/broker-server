package io.bhex.broker.server.grpc.server.service.po;


import java.math.BigDecimal;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class UserRepurchaseBaseInfo {

    private Long time;

    private Long userId;//用户Id

    private Integer effectiveDay;//有效天数

    private BigDecimal excessPerformance;//超额业绩

    private BigDecimal excessPerformanceTotal;//所有队长超额总业绩

    private BigDecimal excessRate;//超额业绩占比

    private BigDecimal performance;//总业绩

    private Long startTime;//绩效计算开始时间

    private Long endTime;//绩效计算结束时间

    private BigDecimal avgIncome;//平均部分收益 30%部分

    private BigDecimal excessIncome;//超额业绩部分收益 20%部分

    private BigDecimal invitationIncome;//邀请总收益

    private BigDecimal invitationExpenses;//邀请总支出


    public static UserRepurchaseBaseInfo getUserRepurchaseBaseInfo() {
        return UserRepurchaseBaseInfo
                .builder()
                .time(0L)
                .userId(0L)
                .effectiveDay(0)
                .excessPerformance(BigDecimal.ZERO)
                .excessPerformanceTotal(BigDecimal.ZERO)
                .performance(BigDecimal.ZERO)
                .startTime(0L)
                .endTime(0L)
                .avgIncome(BigDecimal.ZERO)
                .excessIncome(BigDecimal.ZERO)
                .invitationIncome(BigDecimal.ZERO)
                .invitationExpenses(BigDecimal.ZERO)
                .build();
    }
}