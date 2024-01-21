package io.bhex.broker.server.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @ProjectName:
 * @Package:
 * @Author: yuehao  <hao.yue@bhex.com>
 * @CreateDate: 2021/2/24 上午10:25
 * @Copyright（C）: 2018 BHEX Inc. All rights reserved.
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class UserRiskControlConfig {
//            "SPOT_TRADE_OPEN":"disable", //现货开仓
//            "SPOT_TRADE_CLOSE":"disable", //现货平仓
//            "MARGIN_TRADE_OPEN":"disable", //杠杆开仓
//            "MARGIN_TRADE_CLOSE":"disable", //杠杆平仓
//            "OPTION_TRADE_OPEN":"disable", //期权开仓
//            "OPTION_TRADE_CLOSE":"disable", //期权平仓
//            "CONTRACT_TRADE_OPEN":"disable"  //合约开仓
//            "CONTRACT_TRADE_CLOSE":"disable"  //合约平仓
//            "FINANCE_TRADE":"disable", //禁止金融产品交易，如币多多
//            "OTC_TRADE_BUY":"disable", //OTC买入
//            "OTC_TRADE_SELL":"disable", //OTC卖出
//            "ASSET_OUTFLOW":"disable"  //资产流出，包括提币、红包、OTC等所有出金
//            "ASSET_TRANSFER_OUT":"disable"  //资产流出，不包括OTC和提币的其他

    private Long orgId;

    private Long userId;

    /** 0不限制 1限制 **/

    private Integer otcTradeBuy;

    private Integer otcTradeSell;

    private Integer otcItemBuy;

    private Integer otcItemSell;

    private Integer assetTransferOut;
}
