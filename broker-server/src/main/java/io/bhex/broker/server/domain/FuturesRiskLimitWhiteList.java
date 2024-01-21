package io.bhex.broker.server.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * 风险限额白名单，在tb_common_ini中配置，key为：futures_risklimit_whitelist（临时方案在broker中做处理）
 *
 * 判断规则：如果配置了白名单记录，则对于配置里每一个riskLimitId，只有用户在userIdList内的，才会返回给用户
 */
@Data
@Builder(builderClassName = "Builder")
@NoArgsConstructor
@AllArgsConstructor
public class FuturesRiskLimitWhiteList {
    private Long riskLimitId;
    private List<Long> userIdList;
}
