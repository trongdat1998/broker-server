package io.bhex.broker.server.grpc.server.service.po;

import lombok.Data;

import java.math.BigDecimal;

@Data
public class HobbitUserRepurchaseTotal {

    private BigDecimal inviteIncome;//'队长邀请收益'

    private BigDecimal total;//'总收益'

}
