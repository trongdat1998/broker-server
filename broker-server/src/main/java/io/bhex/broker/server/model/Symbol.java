package io.bhex.broker.server.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Id;
import javax.persistence.Table;

/**
 * @ProjectName: broker-server
 * @Package: io.bhex.broker.server.model
 * @Author: ming.xu
 * @CreateDate: 20/08/2018 9:14 PM
 * @Copyright（C）: 2018 BHEX Inc. All rights reserved.
 */
@Data
@Builder
@Table(name = "tb_symbol")
@AllArgsConstructor
@NoArgsConstructor
public class Symbol {

    public static final Integer OPTION_CATEGORY = 3;
    public static final Integer FUTURES_CATEGORY = 4;

    @Id
    private Long id;
    private Long orgId;
    private Long exchangeId;
    private String symbolId;
    private String symbolName;
    private String baseTokenId;
    private String baseTokenName;
    private String quoteTokenId;
    private String quoteTokenName;
    private Integer allowTrade; // 币对管理 禁买+禁卖 取代允许交易字段
    private Integer status; // 是否启用
    private Integer banSellStatus; // 0 关闭打新 1 开启打新
    private String whiteAccountIdList; // 白名单 逗号分隔
    private Integer customOrder;
    private Integer indexShow;
    private Integer indexShowOrder;
    private Integer needPreviewCheck;
    private Long openTime;
    private Long created;
    private Long updated;
    private Integer category; //1主类别，2创新类别, 3期权, 4期货
    private Integer indexRecommendOrder;
//    private Integer isAggregate;
    private Integer showStatus; //前端显示状态 1-显示 0-隐藏
    private Integer banBuyStatus; // 0 关闭禁买 1 开启禁买
    private Integer allowMargin;// 0 未开通杠杆  1 开通杠杆
    private Long filterTime;//用于配置过滤历史k线
    private Integer filterTopStatus; //topN涨幅榜展示状态 1 过滤 0不过滤
    private Long labelId;
    private Integer hideFromOpenapi;
    private Integer forbidOpenapiTrade;
    private Integer allowPlan; //是否允许计划委托 0不允许 1允许（目前现货使用该配置，日后合约也可以考虑使用该配置）

    private String extraTag;
    private String extraConfig;

}
