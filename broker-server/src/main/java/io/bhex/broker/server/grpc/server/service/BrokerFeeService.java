package io.bhex.broker.server.grpc.server.service;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.JsonObject;

import com.alibaba.fastjson.JSON;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.ibatis.session.RowBounds;
import org.joda.time.DateTime;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

import io.bhex.base.idgen.api.ISequenceGenerator;
import io.bhex.base.token.MakerBonusConfig;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.common.util.JsonUtil;
import io.bhex.broker.grpc.common.BasicRet;
import io.bhex.broker.grpc.fee.AddDiscountFeeConfigRequest;
import io.bhex.broker.grpc.fee.AddSymbolFeeConfigRequest;
import io.bhex.broker.grpc.fee.AddSymbolFeeConfigResponse;
import io.bhex.broker.grpc.fee.BindUserDiscountConfigRequest;
import io.bhex.broker.grpc.fee.BindUserDiscountConfigResponse;
import io.bhex.broker.grpc.fee.DeleteAccountTradeFeeConfigRequest;
import io.bhex.broker.grpc.fee.QueryAccountTradeFeeConfigRequest;
import io.bhex.broker.grpc.fee.QueryAccountTradeFeeConfigResponse;
import io.bhex.broker.grpc.fee.QueryAllSymbolMarketAccountResponse;
import io.bhex.broker.grpc.fee.QueryDiscountFeeConfigResponse;
import io.bhex.broker.grpc.fee.QueryOneDiscountFeeConfigResponse;
import io.bhex.broker.grpc.fee.QuerySymbolFeeConfigRequest;
import io.bhex.broker.grpc.fee.QuerySymbolFeeConfigResponse;
import io.bhex.broker.grpc.fee.QueryUserDiscountConfigRequest;
import io.bhex.broker.grpc.fee.QueryUserDiscountConfigResponse;
import io.bhex.broker.grpc.fee.SaveSymbolMarketAccountRequest;
import io.bhex.broker.grpc.fee.UnbindUserDiscountConfigRequest;
import io.bhex.broker.grpc.fee.UnbindUserDiscountConfigResponse;
import io.bhex.broker.grpc.order.OrderSide;
import io.bhex.broker.grpc.security.ChangeUserApiLevelRequest;
import io.bhex.broker.grpc.security.ChangeUserApiLevelResponse;
import io.bhex.broker.grpc.user.level.QueryMyLevelConfigResponse;
import io.bhex.broker.server.domain.AccountType;
import io.bhex.broker.server.domain.TradeFeeConfig;
import io.bhex.broker.server.domain.VipLevelDiscount;
import io.bhex.broker.server.grpc.client.service.GrpcSecurityService;
import io.bhex.broker.server.grpc.client.service.GrpcSymbolService;
import io.bhex.broker.server.model.Account;
import io.bhex.broker.server.model.AccountTradeFeeConfig;
import io.bhex.broker.server.model.Broker;
import io.bhex.broker.server.model.DiscountFeeConfig;
import io.bhex.broker.server.model.DiscountFeeUser;
import io.bhex.broker.server.model.Symbol;
import io.bhex.broker.server.model.SymbolFeeConfig;
import io.bhex.broker.server.primary.mapper.AccountMapper;
import io.bhex.broker.server.primary.mapper.AccountTradeFeeConfigMapper;
import io.bhex.broker.server.primary.mapper.BrokerMapper;
import io.bhex.broker.server.primary.mapper.DiscountFeeConfigMapper;
import io.bhex.broker.server.primary.mapper.DiscountFeeUserMapper;
import io.bhex.broker.server.primary.mapper.SymbolFeeConfigMapper;
import io.bhex.broker.server.primary.mapper.SymbolMapper;
import io.bhex.broker.server.primary.mapper.WithdrawAddressMapper;
import lombok.extern.slf4j.Slf4j;
import tk.mybatis.mapper.entity.Example;

@Service
@Slf4j
public class BrokerFeeService {

    private static final String SYMBOL_FEE_CONFIG_KEY = "symbolFeeConfig:%s_%s_%s";
    private static ImmutableMap<String, SymbolFeeConfig> SYMBOL_FEE_CONFIG = ImmutableMap.of();

    private static final String ACCOUNT_TRADE_FEE_CONFIG_KEY = "accountTradeFeeConfig:%s_%s_%s_%s";
    private static ImmutableMap<String, AccountTradeFeeConfig> ACCOUNT_TRADE_FEE_CONFIG = ImmutableMap.of();

    private static final String USER_DISCOUNT_FEE_KEY = "userDiscountFee:%s_%s_%s_%s";
    private static ImmutableMap<String, DiscountFeeUser> USER_DISCOUNT_FEE_CONFIG = ImmutableMap.of();

    private static final int DEFAULT_RECORDS_PER_PAGE = 500;

    @Resource
    private SymbolFeeConfigMapper symbolFeeConfigMapper;

    @Resource
    private AccountTradeFeeConfigMapper accountTradeFeeConfigMapper;

    @Resource
    private DiscountFeeUserMapper discountFeeUserMapper;

    @Resource
    private DiscountFeeConfigMapper discountFeeConfigMapper;

    @Resource
    private BrokerMapper brokerMapper;

    @Resource
    private SymbolMapper symbolMapper;

    @Resource
    private ISequenceGenerator<Long> sequenceGenerator;

    @Resource
    private AccountMapper accountMapper;
    @Resource
    private UserLevelService userLevelService;

    @Resource
    private GrpcSecurityService grpcSecurityService;

    @Resource
    private GrpcSymbolService grpcSymbolService;

    @Resource
    private WithdrawAddressMapper withdrawAddressMapper;

    @PostConstruct
    @Scheduled(cron = "0 0/2 * * * ?")
    public void init() {
        List<Broker> brokerList = brokerMapper.queryAvailableBroker();

        // init SymbolFeeConfig
        try {
            Map<String, SymbolFeeConfig> symbolFeeConfigMap = Maps.newHashMap();
            for (Broker broker : brokerList) {
                List<SymbolFeeConfig> symbolFeeConfigs = symbolFeeConfigMapper.select(SymbolFeeConfig.builder().orgId(broker.getOrgId()).status(1).build());
                symbolFeeConfigMap.putAll(symbolFeeConfigs.stream()
                        .collect(Collectors.toMap(config -> String.format(SYMBOL_FEE_CONFIG_KEY, config.getOrgId(), config.getExchangeId(), config.getSymbolId().trim()), config -> config, (p, q) -> q)));
            }
            SYMBOL_FEE_CONFIG = ImmutableMap.copyOf(symbolFeeConfigMap);
        } catch (Exception e) {
            log.warn("load SymbolFeeConfig cache data error", e);
        }

        // init AccountTradeFeeConfig
        try {
            Map<String, AccountTradeFeeConfig> accountTradeFeeConfigMap = Maps.newHashMap();
            for (Broker broker : brokerList) {
                int recordsCount = accountTradeFeeConfigMapper.selectCount(AccountTradeFeeConfig.builder().orgId(broker.getOrgId()).status(1).build());
                List<AccountTradeFeeConfig> accountTradeFeeConfigs = Lists.newArrayList();
                if (recordsCount <= DEFAULT_RECORDS_PER_PAGE) {
                    accountTradeFeeConfigs = accountTradeFeeConfigMapper.select(AccountTradeFeeConfig.builder().orgId(broker.getOrgId()).status(1).build());
                } else {
                    long lastId = 0;
                    for (;;) {
                        List<AccountTradeFeeConfig> tradeFeeConfigs = accountTradeFeeConfigMapper.listAvailableConfigs(broker.getOrgId(), lastId, DEFAULT_RECORDS_PER_PAGE);
                        if (CollectionUtils.isEmpty(tradeFeeConfigs)) {
                            break;
                        }
                        accountTradeFeeConfigs.addAll(tradeFeeConfigs);
                        lastId = tradeFeeConfigs.get(tradeFeeConfigs.size() - 1).getId();
                        if (tradeFeeConfigs.size() < DEFAULT_RECORDS_PER_PAGE) {
                            break;
                        }
                    }
                }
//                log.info("orgId:{} load {} accountTradeFeeConfigs data, total records:{}", broker.getOrgId(), accountTradeFeeConfigs.size(), recordsCount);
                for (AccountTradeFeeConfig config : accountTradeFeeConfigs) {
                    String key = String.format(ACCOUNT_TRADE_FEE_CONFIG_KEY, config.getOrgId(), config.getExchangeId(), config.getSymbolId().trim(), config.getAccountId());
//                    log.info("AccountTradeFeeConfigCacheData: {} -> {}", key, JsonUtil.defaultGson().toJson(config));
                    accountTradeFeeConfigMap.put(key, config);
                }
//                accountTradeFeeConfigMap.putAll(accountTradeFeeConfigs.stream()
//                        .collect(Collectors.toMap(config -> String.format(ACCOUNT_TRADE_FEE_CONFIG_KEY, config.getOrgId(), config.getExchangeId(), config.getSymbolId(), config.getAccountId()),
//                                config -> config, (p, q) -> q)));
            }
            ACCOUNT_TRADE_FEE_CONFIG = ImmutableMap.copyOf(accountTradeFeeConfigMap);
            log.info("load account trade fee config!{}", ACCOUNT_TRADE_FEE_CONFIG.size());

            //6004账户API提币配置(临时需求使用)
            withdrawAddressMapper.updateSuccessNum();
        } catch (Exception e) {
            log.warn("load AccountTradeFeeConfig cache data error", e);
        }

        // init DiscountFeeConfig
        try {
            Map<String, DiscountFeeUser> discountFeeUserMap = Maps.newHashMap();
            for (Broker broker : brokerList) {
                List<DiscountFeeUser> discountFeeUsers = Lists.newArrayList();
                int recordsCount = discountFeeUserMapper.selectCount(DiscountFeeUser.builder().orgId(broker.getOrgId()).status(1).build());
                if (recordsCount <= DEFAULT_RECORDS_PER_PAGE) {
                    discountFeeUsers = discountFeeUserMapper.select(DiscountFeeUser.builder().orgId(broker.getOrgId()).status(1).build());
                } else {
                    int page = recordsCount % DEFAULT_RECORDS_PER_PAGE == 0 ? recordsCount / DEFAULT_RECORDS_PER_PAGE : recordsCount / DEFAULT_RECORDS_PER_PAGE + 1;
                    for (int pageSize = 0; pageSize < page; pageSize++) {
                        discountFeeUsers.addAll(discountFeeUserMapper.selectByRowBounds(DiscountFeeUser.builder().orgId(broker.getOrgId()).status(1).build(),
                                new RowBounds(pageSize * DEFAULT_RECORDS_PER_PAGE, DEFAULT_RECORDS_PER_PAGE)));
                    }
                }
//                log.info("orgId:{} load {} accountTradeFeeConfigs data, total records:{}", broker.getOrgId(), discountFeeUsers.size(), recordsCount);
                for (DiscountFeeUser config : discountFeeUsers) {
                    String key = String.format(USER_DISCOUNT_FEE_KEY, config.getOrgId(), config.getUserId(), config.getExchangeId(), config.getSymbolId());
//                    log.info("DiscountCacheData: {} -> {}", key, JsonUtil.defaultGson().toJson(config));
                    discountFeeUserMap.put(key, config);
                }
//                discountFeeUserMap.putAll(discountFeeUsers.stream()
//                        .collect(Collectors.toMap(config -> String.format(USER_DISCOUNT_FEE_KEY, config.getOrgId(), config.getUserId()), config -> config, (p, q) -> q)));
            }
            USER_DISCOUNT_FEE_CONFIG = ImmutableMap.copyOf(discountFeeUserMap);
        } catch (Exception e) {
            log.warn("load DiscountFeeUser cache data error", e);
        }
    }

    public AccountTradeFeeConfig getCachedTradeFeeConfig(Long orgId, Long exchangeId, String symbolId, Long accountId) {
        String key = String.format(ACCOUNT_TRADE_FEE_CONFIG_KEY, orgId, exchangeId, symbolId, accountId);
        log.info("internal getAccountTradeFee:{} -> {}", key, JsonUtil.defaultGson().toJson(ACCOUNT_TRADE_FEE_CONFIG.get(key)));
        return ACCOUNT_TRADE_FEE_CONFIG.get(key);
    }

    public TradeFeeConfig getOrderTradeFeeConfig(Long orgId, Long exchangeId, String symbolId, Boolean isTransferOrder,
                                                 Long userId, Long accountId, AccountType accountType, OrderSide side,
                                                 TradeFeeConfig defaultTradeFeeConfig, TradeFeeConfig defaultTransferOrderTradeFeeConfig,
                                                 BigDecimal makerBonusRate, BigDecimal minInterestFeeRate, BigDecimal minTakerFeeRate) {
        String logPrefix = String.format("%s_%s_%s_%s_%s_%s_%s_%s_%s", orgId, exchangeId, symbolId, isTransferOrder, userId, accountId, side,
                makerBonusRate.toPlainString(), minInterestFeeRate.toPlainString());
        // 获取币对的费率配置
        SymbolFeeConfig symbolFeeConfig = SYMBOL_FEE_CONFIG.get(String.format(SYMBOL_FEE_CONFIG_KEY, orgId, exchangeId, symbolId));
        if (symbolFeeConfig == null) {
            log.warn("defaultSymbolFeeConfig: prefix:{}, {}", logPrefix,
                    JsonUtil.defaultGson().toJson(isTransferOrder ? defaultTransferOrderTradeFeeConfig : defaultTradeFeeConfig));
            return isTransferOrder ? defaultTransferOrderTradeFeeConfig : defaultTradeFeeConfig;
        }

        // 获取accountId的特殊费率配置，可能是做市账号，也可能是负手续费配置，也可能是某个账号大于0 的特殊费率配置
        TradeFeeConfig tradeFeeConfig = queryAccountTradeFeeConfig(orgId, exchangeId, symbolId, side, accountId);
//        if (tradeFeeConfig != null) {
//            log.info("accountTradeFeeConfig: {} - {}", logPrefix, JsonUtil.defaultGson().toJson(tradeFeeConfig));
//        }
        if (tradeFeeConfig == null) {
            // 折扣费率配置，这里使用的是userId
            tradeFeeConfig = queryDiscountTradeFeeConfig(orgId, exchangeId, symbolId, side, userId, accountType);
//            log.info("accountDiscountFeeConfig: {} - {}", logPrefix, JsonUtil.defaultGson().toJson(tradeFeeConfig));
        }

        if (tradeFeeConfig.getMakerFeeRate().compareTo(BigDecimal.ZERO) < 0) {
            if (makerBonusRate.compareTo(BigDecimal.ZERO) == 0) {
                log.error("handle negativeSymbolFeeError, makerFeeRate < 0 and makerBonusRate == 0, prefix: {}, tradeFeeConfig: {}",
                        logPrefix, JsonUtil.defaultGson().toJson(tradeFeeConfig));
            }
            if (tradeFeeConfig.getMakerFeeRate().abs().compareTo(makerBonusRate) > 0) {
                log.info("handle negativeMakerFeeRate, prefix:{}, tradeFeeConfig:{}, ", logPrefix, JsonUtil.defaultGson().toJson(tradeFeeConfig));
                tradeFeeConfig = tradeFeeConfig.toBuilder().makerFeeRate(makerBonusRate.negate()).build();
            }
        }

//        BigDecimal baseRate = makerBonusRate.add(minInterestFeeRate); // taker 最低费率限制
//        if (tradeFeeConfig.getTakerFeeRate().compareTo(baseRate) < 0) {
//            tradeFeeConfig = tradeFeeConfig.toBuilder().takerFeeRate(baseRate).build();
//        }

        if (tradeFeeConfig.getTakerFeeRate().compareTo(minTakerFeeRate) < 0) {
            tradeFeeConfig = tradeFeeConfig.toBuilder().takerFeeRate(minTakerFeeRate).build();
        }

        if (isTransferOrder && tradeFeeConfig.getMakerFeeRate().compareTo(BigDecimal.ZERO) >= 0) {
            log.info("transferOrderTradeFeeConfig: prefix: {}, current:{}, default:{}", logPrefix,
                    JsonUtil.defaultGson().toJson(tradeFeeConfig), JsonUtil.defaultGson().toJson(defaultTransferOrderTradeFeeConfig));
            if (side == OrderSide.BUY && StringUtils.isNotEmpty(symbolId) && symbolId.contains("CHZ") && !symbolId.equalsIgnoreCase("CHZUSDT")) {
                return tradeFeeConfig;
            }
            if (tradeFeeConfig.getMakerFeeRate().compareTo(defaultTransferOrderTradeFeeConfig.getMakerFeeRate()) < 0) {
                tradeFeeConfig = tradeFeeConfig.toBuilder().makerFeeRate(defaultTransferOrderTradeFeeConfig.getMakerFeeRate()).build();
            }
            if (tradeFeeConfig.getTakerFeeRate().compareTo(defaultTransferOrderTradeFeeConfig.getTakerFeeRate()) < 0) {
                tradeFeeConfig = tradeFeeConfig.toBuilder().takerFeeRate(defaultTransferOrderTradeFeeConfig.getTakerFeeRate()).build();
            }
        }
        return tradeFeeConfig;
    }

    public TradeFeeConfig queryAccountTradeFeeConfig(Long orgId, Long exchangeId, String symbolId, OrderSide orderSide, Long accountId) {
        String key = String.format(ACCOUNT_TRADE_FEE_CONFIG_KEY, orgId, exchangeId, symbolId.trim(), accountId);
        AccountTradeFeeConfig marketConfigDetail = ACCOUNT_TRADE_FEE_CONFIG.get(key);

        if (marketConfigDetail == null) {
            return null;
        }
        if (orderSide == OrderSide.BUY) {
            return TradeFeeConfig.builder()
                    .makerFeeRate(marketConfigDetail.getMakerBuyFeeRate())
                    .takerFeeRate(marketConfigDetail.getTakerBuyFeeRate())
                    .build();
        } else {
            return TradeFeeConfig.builder()
                    .makerFeeRate(marketConfigDetail.getMakerSellFeeRate())
                    .takerFeeRate(marketConfigDetail.getTakerSellFeeRate())
                    .build();
        }
    }

    private VipLevelDiscount getMyVipLevelDiscountConfig(Long orgId, Long userId, AccountType accountType, OrderSide orderSide) {
        QueryMyLevelConfigResponse myVipLevelConfig = userLevelService.queryMyLevelConfig(orgId, userId, false, false);
        BigDecimal buyMakerDiscount = BigDecimal.ONE;
        BigDecimal buyTakerDiscount = BigDecimal.ONE;
        BigDecimal sellMakerDiscount = BigDecimal.ONE;
        BigDecimal sellTakerDiscount = BigDecimal.ONE;
        if (myVipLevelConfig != null) {
            if (accountType == AccountType.MAIN) {
                buyMakerDiscount = new BigDecimal(myVipLevelConfig.getSpotBuyMakerDiscount());
                buyTakerDiscount = new BigDecimal(myVipLevelConfig.getSpotBuyTakerDiscount());
                sellMakerDiscount = new BigDecimal(myVipLevelConfig.getSpotSellMakerDiscount());
                sellTakerDiscount = new BigDecimal(myVipLevelConfig.getSpotSellTakerDiscount());
            } else if (accountType == AccountType.FUTURES) {
                buyMakerDiscount = new BigDecimal(myVipLevelConfig.getContractBuyMakerDiscount());
                buyTakerDiscount = new BigDecimal(myVipLevelConfig.getContractBuyTakerDiscount());
                sellMakerDiscount = new BigDecimal(myVipLevelConfig.getContractSellMakerDiscount());
                sellTakerDiscount = new BigDecimal(myVipLevelConfig.getContractSellTakerDiscount());
            } else if (accountType == AccountType.OPTION) {
                buyMakerDiscount = new BigDecimal(myVipLevelConfig.getOptionBuyMakerDiscount());
                buyTakerDiscount = new BigDecimal(myVipLevelConfig.getOptionBuyTakerDiscount());
                sellMakerDiscount = new BigDecimal(myVipLevelConfig.getOptionSellMakerDiscount());
                sellTakerDiscount = new BigDecimal(myVipLevelConfig.getOptionSellTakerDiscount());
            }
        }
        return VipLevelDiscount.builder()
                .vipMakerDiscount(orderSide == OrderSide.BUY ? buyMakerDiscount : sellMakerDiscount)
                .vipTakerDiscount(orderSide == OrderSide.BUY ? buyTakerDiscount : sellTakerDiscount)
                .build();
    }

    /**
     * 比较折扣费率配置和用户vip等级折扣，取小值
     *
     * @param originFeeRate      原始币对费率
     * @param userConfigDiscount 用户折扣配置
     * @param vipDiscount        vip用户等级折扣
     * @return 折扣后的费率
     */
    private BigDecimal getDiscountFeeRate(BigDecimal originFeeRate, BigDecimal userConfigDiscount, BigDecimal vipDiscount) {
        if (originFeeRate.compareTo(BigDecimal.ZERO) <= 0) { //负费率不打折扣
            return originFeeRate;
        }
        BigDecimal realDiscount = userConfigDiscount.compareTo(vipDiscount) < 0 ? userConfigDiscount : vipDiscount; //两折扣取小
        return originFeeRate.multiply(realDiscount).setScale(8, RoundingMode.UP);
    }

    public TradeFeeConfig queryDiscountTradeFeeConfig(Long orgId, Long exchangeId, String symbolId, OrderSide orderSide, Long userId, AccountType accountType) {
        try {

            // config with symbol
            SymbolFeeConfig symbolFeeConfig = querySymbolFeeConfig(orgId, exchangeId, symbolId);
            VipLevelDiscount vipLevelDiscount = getMyVipLevelDiscountConfig(orgId, userId, accountType, orderSide);

            // 用户折扣
            Map<String, DiscountFeeConfig> discountFeeConfigMap = queryUserLevelConfig(orgId, userId, exchangeId, symbolId);
            DiscountFeeConfig baseConfig = discountFeeConfigMap.get("base");
            DiscountFeeConfig temporaryConfig = discountFeeConfigMap.get("temporary");
            TradeFeeConfig.Builder tradeFeeConfigBuilder = TradeFeeConfig.builder();
            if (baseConfig == null && temporaryConfig == null) {
                if (orderSide == OrderSide.BUY) {
                    tradeFeeConfigBuilder
                            .makerFeeRate(getDiscountFeeRate(symbolFeeConfig.getMakerBuyFee(), BigDecimal.ONE, vipLevelDiscount.getVipMakerDiscount()))
                            .takerFeeRate(getDiscountFeeRate(symbolFeeConfig.getTakerBuyFee(), BigDecimal.ONE, vipLevelDiscount.getVipTakerDiscount()));
                } else {
                    tradeFeeConfigBuilder
                            .makerFeeRate(getDiscountFeeRate(symbolFeeConfig.getMakerSellFee(), BigDecimal.ONE, vipLevelDiscount.getVipMakerDiscount()))
                            .takerFeeRate(getDiscountFeeRate(symbolFeeConfig.getTakerSellFee(), BigDecimal.ONE, vipLevelDiscount.getVipTakerDiscount()));
                }
                return tradeFeeConfigBuilder.build();
            }

            Map<String, BigDecimal> discountConfigMap;
            if (temporaryConfig != null) {
                discountConfigMap = queryDiscountConfig(temporaryConfig, accountType, orderSide);
            } else {
                discountConfigMap = queryDiscountConfig(baseConfig, accountType, orderSide);
            }

            if (orderSide == OrderSide.BUY) {
                BigDecimal takerBuy = discountConfigMap.get("takerBuy");
                BigDecimal makerBuy = discountConfigMap.get("makerBuy");
                if (takerBuy != null && takerBuy.compareTo(BigDecimal.ZERO) > 0 && symbolFeeConfig.getMakerBuyFee().compareTo(BigDecimal.ZERO) > 0) {
                    BigDecimal takerRate = getDiscountFeeRate(symbolFeeConfig.getTakerBuyFee(), takerBuy, vipLevelDiscount.getVipTakerDiscount());
                    tradeFeeConfigBuilder.takerFeeRate(takerRate);
                } else {
                    tradeFeeConfigBuilder.takerFeeRate(symbolFeeConfig.getTakerBuyFee());
                }

                //支持合约币对maker buy费率折扣能为0
                if ((makerBuy != null && (makerBuy.compareTo(BigDecimal.ZERO) > 0 || (makerBuy.compareTo(BigDecimal.ZERO) == 0 && symbolFeeConfig.getCategory() == 4))) && symbolFeeConfig.getMakerBuyFee().compareTo(BigDecimal.ZERO) > 0) {
                    BigDecimal makerRate = getDiscountFeeRate(symbolFeeConfig.getMakerBuyFee(), makerBuy, vipLevelDiscount.getVipMakerDiscount());
                    tradeFeeConfigBuilder.makerFeeRate(makerRate);
                } else {
                    tradeFeeConfigBuilder.makerFeeRate(symbolFeeConfig.getMakerBuyFee());
                }
                return tradeFeeConfigBuilder.build();
            } else {
                BigDecimal takerSell = discountConfigMap.get("takerSell");
                BigDecimal makerSell = discountConfigMap.get("makerSell");
                if (takerSell != null && takerSell.compareTo(BigDecimal.ZERO) > 0 && symbolFeeConfig.getMakerBuyFee().compareTo(BigDecimal.ZERO) > 0) {
                    BigDecimal takerRate = getDiscountFeeRate(symbolFeeConfig.getTakerSellFee(), takerSell, vipLevelDiscount.getVipTakerDiscount());
                    tradeFeeConfigBuilder.takerFeeRate(takerRate);
                } else {
                    tradeFeeConfigBuilder.takerFeeRate(symbolFeeConfig.getTakerSellFee());
                }

                //支持合约币对maker sell费率折扣能为0
                if ((makerSell != null && (makerSell.compareTo(BigDecimal.ZERO) > 0 || (makerSell.compareTo(BigDecimal.ZERO) == 0 && symbolFeeConfig.getCategory() == 4))) && symbolFeeConfig.getMakerSellFee().compareTo(BigDecimal.ZERO) > 0) {
                    BigDecimal makerRate = getDiscountFeeRate(symbolFeeConfig.getMakerSellFee(), makerSell, vipLevelDiscount.getVipMakerDiscount());
                    tradeFeeConfigBuilder.makerFeeRate(makerRate);
                } else {
                    tradeFeeConfigBuilder.makerFeeRate(symbolFeeConfig.getMakerSellFee());
                }
                return tradeFeeConfigBuilder.build();
            }
        } catch (Exception ex) {
            log.error("Query order fee config error {}", ex);
        }
        return null;
    }

    public Map<String, BigDecimal> queryDiscountConfig(DiscountFeeConfig discountFeeConfig, AccountType accountType, OrderSide orderSide) {
        Map<String, BigDecimal> decimalMap = new HashMap<>();
        if (orderSide == OrderSide.BUY) {
            if (accountType == AccountType.MAIN) {
                decimalMap.put("takerBuy", discountFeeConfig.getCoinTakerBuyFeeDiscount());
                decimalMap.put("makerBuy", discountFeeConfig.getCoinMakerBuyFeeDiscount());
            } else if (accountType == AccountType.OPTION) {
                decimalMap.put("takerBuy", discountFeeConfig.getOptionTakerBuyFeeDiscount());
                decimalMap.put("makerBuy", discountFeeConfig.getOptionMakerBuyFeeDiscount());
            } else if (accountType == AccountType.FUTURES) {
                decimalMap.put("takerBuy", discountFeeConfig.getContractTakerBuyFeeDiscount());
                decimalMap.put("makerBuy", discountFeeConfig.getContractMakerBuyFeeDiscount());
            }
        } else {
            if (accountType == AccountType.MAIN) {
                decimalMap.put("takerSell", discountFeeConfig.getCoinTakerSellFeeDiscount());
                decimalMap.put("makerSell", discountFeeConfig.getCoinMakerSellFeeDiscount());

            } else if (accountType == AccountType.OPTION) {
                decimalMap.put("takerSell", discountFeeConfig.getOptionTakerSellFeeDiscount());
                decimalMap.put("makerSell", discountFeeConfig.getOptionMakerSellFeeDiscount());

            } else if (accountType == AccountType.FUTURES) {
                decimalMap.put("takerSell", discountFeeConfig.getContractTakerSellFeeDiscount());
                decimalMap.put("makerSell", discountFeeConfig.getContractMakerSellFeeDiscount());
            }
        }
        return decimalMap;
    }

    /**
     * 获取交易币对的费率设置
     *
     * @param orgId
     * @param exchangeId
     * @param symbolId
     * @return
     */
    public SymbolFeeConfig querySymbolFeeConfig(Long orgId, Long exchangeId, String symbolId) {
        String key = String.format(SYMBOL_FEE_CONFIG_KEY, orgId, exchangeId, symbolId);
        return SYMBOL_FEE_CONFIG.get(key);
    }

    public SymbolFeeConfig querySymbolFeeConfigNoCache(Long orgId, Long exchangeId, String symbolId) {
        Example example = Example.builder(SymbolFeeConfig.class).build();
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("orgId", orgId);
        criteria.andEqualTo("exchangeId", exchangeId);
        criteria.andEqualTo("symbolId", symbolId);
        criteria.andEqualTo("status", 1);
        return this.symbolFeeConfigMapper.selectOneByExample(example);
    }


    // 用户等级
    public Map<String, DiscountFeeConfig> queryUserLevelConfig(Long orgId, Long userId, Long exchangeId, String symbolId) {
        Map<String, DiscountFeeConfig> userGroup = new HashMap<>();
        //获取具体币对的折扣缓存
        String symbolIdKey = String.format(USER_DISCOUNT_FEE_KEY, orgId, userId, exchangeId, symbolId);
        DiscountFeeUser symbolIdDiscountFeeUser = USER_DISCOUNT_FEE_CONFIG.get(symbolIdKey);

        //获取全局的折扣缓存
        String key = String.format(USER_DISCOUNT_FEE_KEY, orgId, userId, 0L, "");
        DiscountFeeUser discountFeeUser = USER_DISCOUNT_FEE_CONFIG.get(key);

        if (symbolIdDiscountFeeUser == null || symbolIdDiscountFeeUser.getStatus().equals(0)) {
            //具体exchangeId和symbolId没有则再找0和""全局的
            if (discountFeeUser == null || discountFeeUser.getStatus().equals(0)) {
                return Maps.newHashMap();
            }

            DiscountFeeConfig baseDiscountFeeConfig = discountFeeConfigMapper.selectByPrimaryKey(discountFeeUser.getBaseGroupId());
            if (baseDiscountFeeConfig != null && baseDiscountFeeConfig.getStatus().equals(1)) {
                userGroup.put("base", baseDiscountFeeConfig);
            }

            DiscountFeeConfig temporaryDiscountFeeConfig = discountFeeConfigMapper.selectByPrimaryKey(discountFeeUser.getTemporaryGroupId());
            if (temporaryDiscountFeeConfig != null && temporaryDiscountFeeConfig.getStatus().equals(1)) {
                userGroup.put("temporary", temporaryDiscountFeeConfig);
            }

        } else {
            //有找到具体的exchangeId和symbolId的缓存配置，获取temporary的配置，目前只找一次
            DiscountFeeConfig temporarySymbolIdDiscountFeeConfig = discountFeeConfigMapper.selectByPrimaryKey(symbolIdDiscountFeeUser.getTemporaryGroupId());
            if (temporarySymbolIdDiscountFeeConfig != null && temporarySymbolIdDiscountFeeConfig.getStatus().equals(1)) {
                userGroup.put("temporary", temporarySymbolIdDiscountFeeConfig);
            }

            DiscountFeeConfig symbolIdBaseDiscountFeeConfig = discountFeeConfigMapper.selectByPrimaryKey(symbolIdDiscountFeeUser.getBaseGroupId());
            if (symbolIdBaseDiscountFeeConfig != null && symbolIdBaseDiscountFeeConfig.getStatus().equals(1)) {
                //config配置为有效则加入返回中
                userGroup.put("base", symbolIdBaseDiscountFeeConfig);
            } else {
                //config配置无效则再看全局的
                DiscountFeeConfig baseDiscountFeeConfig = discountFeeConfigMapper.selectByPrimaryKey(discountFeeUser.getBaseGroupId());
                if (baseDiscountFeeConfig != null && baseDiscountFeeConfig.getStatus().equals(1)) {
                    //config配置为有效则加入返回中
                    userGroup.put("base", baseDiscountFeeConfig);
                }
            }
        }

        return userGroup;
    }

    /**
     * 创建用户与折扣等级关系 支持基础等级 临时等级
     */
    @Transactional(isolation = Isolation.READ_COMMITTED, propagation = Propagation.REQUIRED, rollbackFor = Throwable.class)
    public void saveDiscountUser(Long orgId, Long userId, Long discountId, Integer isBase) {
//        if (StringUtils.isEmpty(userId)) {
//            return;
//        }

//        List<String> idList = Splitter.on(",").omitEmptyStrings().trimResults().splitToList(userId);

        Example example = Example.builder(DiscountFeeConfig.class).build();
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("orgId", orgId);
        criteria.andEqualTo("id", discountId);
        criteria.andEqualTo("status", 1);

        DiscountFeeConfig discountFeeConfig = this.discountFeeConfigMapper.selectOneByExample(example);
        if (discountFeeConfig == null) {
            throw new BrokerException(BrokerErrorCode.SYSTEM_ERROR);
        }

//        idList.forEach(id -> {
        DiscountFeeUser discountFeeUser = queryDiscountFeeUserByUserId(orgId, userId, discountFeeConfig.getExchangeId(), discountFeeConfig.getSymbolId());
        if (discountFeeUser == null) {
            DiscountFeeUser levelUser = DiscountFeeUser.
                    builder()
                    .baseGroupId(isBase.equals(1) ? discountFeeConfig.getId() : 0)
                    .temporaryGroupId(isBase.equals(2) ? discountFeeConfig.getId() : 0)
                    .orgId(orgId)
                    .userId(userId)
                    .exchangeId(discountFeeConfig.getExchangeId())
                    .symbolId(discountFeeConfig.getSymbolId())
                    .status(1)
                    .created(new Date())
                    .updated(new Date())
                    .build();
            this.discountFeeUserMapper.insert(levelUser);
        } else {
            if (isBase.equals(1)) {
                discountFeeUser.setBaseGroupId(discountFeeConfig.getId());
            } else if (isBase.equals(2)) {
                discountFeeUser.setTemporaryGroupId(discountFeeConfig.getId());
            } else {
                throw new BrokerException(BrokerErrorCode.SYSTEM_ERROR);
            }
            log.info(JSON.toJSONString(discountFeeUser));
            int row = this.discountFeeUserMapper.updateByPrimaryKeySelective(discountFeeUser);
            if (row != 1) {
                throw new BrokerException(BrokerErrorCode.SYSTEM_ERROR);
            }
        }
//        });
    }

    private DiscountFeeUser queryDiscountFeeUserByUserId(Long orgId, Long userId, Long exchangeId, String symbolId) {
        Example userExample = Example.builder(DiscountFeeUser.class).build();
        Example.Criteria userCriteria = userExample.createCriteria();
        userCriteria.andEqualTo("orgId", orgId);
        userCriteria.andEqualTo("userId", userId);
        userCriteria.andEqualTo("exchangeId", exchangeId);
        userCriteria.andEqualTo("symbolId", symbolId);
        userCriteria.andEqualTo("status", 1);
        return this.discountFeeUserMapper.selectOneByExample(userExample);
    }

    public QueryUserDiscountConfigResponse queryFeeLevelUserByUserId(QueryUserDiscountConfigRequest request) {
        DiscountFeeUser discountFeeUser = queryDiscountFeeUserByUserId(request.getOrgId(), request.getUserId(), request.getExchangeId(), request.getSymbolId());

        if (discountFeeUser == null) {
            return QueryUserDiscountConfigResponse.getDefaultInstance();
        }

        String baseGroupName = "";
        String temporaryGroupName = "";
        if (discountFeeUser.getBaseGroupId() != null && discountFeeUser.getBaseGroupId() > 0) {
            DiscountFeeConfig discountFeeConfig = this.discountFeeConfigMapper.selectByPrimaryKey(discountFeeUser.getBaseGroupId());
            if (discountFeeConfig != null) {
                baseGroupName = discountFeeConfig.getName();
            }
        }

        if (discountFeeUser.getTemporaryGroupId() != null && discountFeeUser.getTemporaryGroupId() > 0) {
            DiscountFeeConfig discountFeeConfig = this.discountFeeConfigMapper.selectByPrimaryKey(discountFeeUser.getTemporaryGroupId());
            if (discountFeeConfig != null) {
                temporaryGroupName = discountFeeConfig.getName();
            }
        }

        return QueryUserDiscountConfigResponse.newBuilder()
                .setUserId(discountFeeUser.getUserId())
                .setBaseGroupId(discountFeeUser.getBaseGroupId())
                .setTemporaryGroupId(discountFeeUser.getTemporaryGroupId())
                .setBaseGroupName(baseGroupName)
                .setTemporaryGroupName(temporaryGroupName)
                .setStatus(discountFeeUser.getStatus())
                .build();
    }

    /**
     * 解除用户手续费折扣绑定
     */
    public void cancelUserLevel(Long orgId, Long userId, Integer isBase) {
        log.info("orgId {} userId {} isBase {}", orgId, userId, isBase);
        Example userExample = Example.builder(DiscountFeeUser.class).build();
        Example.Criteria userCriteria = userExample.createCriteria();
        userCriteria.andEqualTo("orgId", orgId);
        userCriteria.andEqualTo("userId", userId);

        DiscountFeeUser discountFeeUser;
        if (isBase.equals(1)) {
            discountFeeUser = DiscountFeeUser.builder().baseGroupId(0L).build();
        } else if (isBase.equals(2)) {
            discountFeeUser = DiscountFeeUser.builder().temporaryGroupId(0L).build();
        } else {
            throw new BrokerException(BrokerErrorCode.SYSTEM_ERROR);
        }
        this.discountFeeUserMapper.updateByExampleSelective(discountFeeUser, userExample);
    }

    public String saveDiscountFeeConfig(AddDiscountFeeConfigRequest request) {
        //0 全部 1币币，2创新（目前没有用到），3期权（目前没有用到），4期货
        int type = 0;
        if (request.getExchangeId() != 0 || StringUtils.isNotBlank(request.getSymbolId())) {
            Symbol symbol = symbolMapper.getBySymbolId(request.getExchangeId(), request.getSymbolId(), request.getOrgId());
            if (symbol == null) {
                throw new BrokerException(BrokerErrorCode.SYSTEM_ERROR);
            }

            type = symbol.getCategory();
        }

        long id = sequenceGenerator.getLong();
        DiscountFeeConfig discountFeeConfig = DiscountFeeConfig
                .builder()
                .orgId(request.getOrgId())
                .exchangeId(request.getExchangeId())
                .symbolId(request.getSymbolId())
                .type(type)
                .id(request.getId() > 0 ? request.getId() : id)
                .name(request.getName())
                .mark(request.getMark())
                .status(request.getStatus())
                .coinMakerBuyFeeDiscount(StringUtils.isNotEmpty(request.getCoinMakerBuyFeeDiscount()) ? new BigDecimal(request.getCoinMakerBuyFeeDiscount()) : BigDecimal.ZERO)
                .coinMakerSellFeeDiscount(StringUtils.isNotEmpty(request.getCoinMakerSellFeeDiscount()) ? new BigDecimal(request.getCoinMakerSellFeeDiscount()) : BigDecimal.ZERO)
                .coinTakerBuyFeeDiscount(StringUtils.isNotEmpty(request.getCoinTakerBuyFeeDiscount()) ? new BigDecimal(request.getCoinTakerBuyFeeDiscount()) : BigDecimal.ZERO)
                .coinTakerSellFeeDiscount(StringUtils.isNotEmpty(request.getCoinTakerSellFeeDiscount()) ? new BigDecimal(request.getCoinTakerSellFeeDiscount()) : BigDecimal.ZERO)
                .optionMakerBuyFeeDiscount(StringUtils.isNotEmpty(request.getOptionMakerBuyFeeDiscount()) ? new BigDecimal(request.getOptionMakerBuyFeeDiscount()) : BigDecimal.ZERO)
                .optionMakerSellFeeDiscount(StringUtils.isNotEmpty(request.getOptionMakerSellFeeDiscount()) ? new BigDecimal(request.getOptionMakerSellFeeDiscount()) : BigDecimal.ZERO)
                .optionTakerBuyFeeDiscount(StringUtils.isNotEmpty(request.getOptionTakerBuyFeeDiscount()) ? new BigDecimal(request.getOptionTakerBuyFeeDiscount()) : BigDecimal.ZERO)
                .optionTakerSellFeeDiscount(StringUtils.isNotEmpty(request.getOptionTakerSellFeeDiscount()) ? new BigDecimal(request.getOptionTakerSellFeeDiscount()) : BigDecimal.ZERO)
                .contractMakerBuyFeeDiscount(StringUtils.isNotEmpty(request.getContractMakerBuyFeeDiscount()) ? new BigDecimal(request.getContractMakerBuyFeeDiscount()) : BigDecimal.ZERO)
                .contractMakerSellFeeDiscount(StringUtils.isNotEmpty(request.getContractMakerSellFeeDiscount()) ? new BigDecimal(request.getContractMakerSellFeeDiscount()) : BigDecimal.ZERO)
                .contractTakerBuyFeeDiscount(StringUtils.isNotEmpty(request.getContractTakerBuyFeeDiscount()) ? new BigDecimal(request.getContractTakerBuyFeeDiscount()) : BigDecimal.ZERO)
                .contractTakerSellFeeDiscount(StringUtils.isNotEmpty(request.getContractTakerSellFeeDiscount()) ? new BigDecimal(request.getContractTakerSellFeeDiscount()) : BigDecimal.ZERO)
                .build();

        if (request.getId() > 0) {
            discountFeeConfig.setUpdated(new Date());
            this.discountFeeConfigMapper.updateByPrimaryKeySelective(discountFeeConfig);
        } else {
            discountFeeConfig.setCreated(new Date());
            discountFeeConfig.setUpdated(new Date());
            this.discountFeeConfigMapper.insertSelective(discountFeeConfig);
        }

        List<String> existList = new ArrayList<>();
        if (StringUtils.isNotEmpty(request.getUserList()) && discountFeeConfig.getId() > 0) {
            List<String> userList = Splitter.on(",").omitEmptyStrings().trimResults().splitToList(request.getUserList());
            this.discountFeeUserMapper.updateStatusByBaseGroupId(request.getOrgId(), discountFeeConfig.getId());
            userList.forEach(user -> {
                Example userExample = Example.builder(DiscountFeeUser.class).build();
                Example.Criteria userCriteria = userExample.createCriteria();
                userCriteria.andEqualTo("orgId", request.getOrgId());
                userCriteria.andEqualTo("userId", user);
                userCriteria.andEqualTo("exchangeId", discountFeeConfig.getExchangeId());
                userCriteria.andEqualTo("symbolId", discountFeeConfig.getSymbolId());
                userCriteria.andEqualTo("status", 1);
                DiscountFeeUser discountFeeUser = this.discountFeeUserMapper.selectOneByExample(userExample);
                if (discountFeeUser == null) {
                    DiscountFeeUser newDiscountFeeUser = DiscountFeeUser.builder()
                            .orgId(request.getOrgId())
                            .baseGroupId(discountFeeConfig.getId())
                            .userId(Long.parseLong(user))
                            .exchangeId(discountFeeConfig.getExchangeId())
                            .symbolId(discountFeeConfig.getSymbolId())
                            .status(1)
                            .temporaryGroupId(0l)
                            .created(new Date())
                            .updated(new Date())
                            .build();
                    this.discountFeeUserMapper.insertSelective(newDiscountFeeUser);
                } else {
                    existList.add(String.valueOf(discountFeeUser.getUserId()));
                }
            });
        } else {
            if (discountFeeConfig.getId() != null && discountFeeConfig.getId() > 0) {
                this.discountFeeUserMapper.updateStatusByBaseGroupId(request.getOrgId(), discountFeeConfig.getId());
            }
        }
        if (CollectionUtils.isEmpty(existList)) {
            return "";
        } else {
            return Joiner.on(",").join(existList);
        }
    }

    /**
     * 获取折扣配置列表
     */
    public QueryDiscountFeeConfigResponse queryDiscountFeeConfigList(Long orgId) {
        Example example = Example.builder(DiscountFeeConfig.class).orderByDesc("id").build();
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("orgId", orgId);
        List<DiscountFeeConfig> symbolFeeConfigList = discountFeeConfigMapper.selectByExample(example);

        if (CollectionUtils.isEmpty(symbolFeeConfigList)) {
            return QueryDiscountFeeConfigResponse.getDefaultInstance();
        }

        List<QueryDiscountFeeConfigResponse.DiscountConfig> feeConfigList = new ArrayList<>();
        symbolFeeConfigList.forEach(fee -> {
            Example userConfig = Example.builder(DiscountFeeUser.class).build();
            Example.Criteria userCriteria = userConfig.createCriteria();
            userCriteria.andEqualTo("orgId", orgId);
            userCriteria.andEqualTo("status", 1);
            userCriteria.andEqualTo("baseGroupId", fee.getId());
            List<DiscountFeeUser> discountFeeUser
                    = this.discountFeeUserMapper.selectByExample(userConfig);
            String userListStr = "";
            if (CollectionUtils.isNotEmpty(discountFeeUser)) {
                List<String> userList
                        = discountFeeUser.stream().map(s -> String.valueOf(s.getUserId())).collect(Collectors.toList());
                userListStr = Joiner.on(",").join(userList);
            }
            feeConfigList.add(QueryDiscountFeeConfigResponse.DiscountConfig.newBuilder()
                    .setId(fee.getId())
                    .setExchangeId(fee.getExchangeId())
                    .setSymbolId(fee.getSymbolId())
                    .setType(fee.getType())
                    .setStatus(fee.getStatus())
                    .setName(fee.getName())
                    .setMark(fee.getMark())
                    .setCoinMakerBuyFeeDiscount(fee.getCoinMakerBuyFeeDiscount().stripTrailingZeros().toPlainString())
                    .setCoinMakerSellFeeDiscount(fee.getCoinMakerSellFeeDiscount().stripTrailingZeros().toPlainString())
                    .setCoinTakerBuyFeeDiscount(fee.getCoinTakerBuyFeeDiscount().stripTrailingZeros().toPlainString())
                    .setCoinTakerSellFeeDiscount(fee.getCoinTakerSellFeeDiscount().stripTrailingZeros().toPlainString())
//                    .setOptionMakerBuyFeeDiscount(fee.getOptionMakerBuyFeeDiscount().stripTrailingZeros().toPlainString())
//                    .setOptionMakerSellFeeDiscount(fee.getOptionMakerSellFeeDiscount().stripTrailingZeros().toPlainString())
//                    .setOptionTakerBuyFeeDiscount(fee.getOptionTakerBuyFeeDiscount().stripTrailingZeros().toPlainString())
//                    .setOptionTakerSellFeeDiscount(fee.getOptionTakerSellFeeDiscount().stripTrailingZeros().toPlainString())
                    .setContractMakerBuyFeeDiscount(fee.getContractMakerBuyFeeDiscount().stripTrailingZeros().toPlainString())
                    .setContractMakerSellFeeDiscount(fee.getContractMakerSellFeeDiscount().stripTrailingZeros().toPlainString())
                    .setContractTakerBuyFeeDiscount(fee.getContractTakerBuyFeeDiscount().stripTrailingZeros().toPlainString())
                    .setContractTakerSellFeeDiscount(fee.getContractTakerSellFeeDiscount().stripTrailingZeros().toPlainString())
                    .setCreated(new DateTime(fee.getCreated()).toString("yyyy-MM-dd HH:mm:ss"))
                    .setUpdated(new DateTime(fee.getUpdated()).toString("yyyy-MM-dd HH:mm:ss"))
                    .setUserList(userListStr)
                    .build());
        });
        return QueryDiscountFeeConfigResponse.newBuilder().addAllDiscountConfig(feeConfigList).build();
    }

    public QueryOneDiscountFeeConfigResponse queryOneDiscountFeeConfigList(Long id) {
        DiscountFeeConfig fee = discountFeeConfigMapper.selectByPrimaryKey(id);

        if (fee == null) {
            return QueryOneDiscountFeeConfigResponse.getDefaultInstance();
        }
        log.info("query one {}", JSON.toJSONString(fee));

        return QueryOneDiscountFeeConfigResponse.newBuilder()
                .setId(fee.getId())
                .setOrgId(fee.getOrgId())
                .setExchangeId(fee.getExchangeId())
                .setSymbolId(fee.getSymbolId())
                .setType(fee.getType())
                .setName(fee.getName())
                .setMark(fee.getMark())
                .setCoinMakerBuyFeeDiscount(fee.getCoinMakerBuyFeeDiscount().stripTrailingZeros().toPlainString())
                .setCoinMakerSellFeeDiscount(fee.getCoinMakerSellFeeDiscount().stripTrailingZeros().toPlainString())
                .setCoinTakerBuyFeeDiscount(fee.getCoinTakerBuyFeeDiscount().stripTrailingZeros().toPlainString())
                .setCoinTakerSellFeeDiscount(fee.getCoinTakerSellFeeDiscount().stripTrailingZeros().toPlainString())
                .setOptionMakerBuyFeeDiscount(fee.getOptionMakerBuyFeeDiscount().stripTrailingZeros().toPlainString())
                .setOptionMakerSellFeeDiscount(fee.getOptionMakerSellFeeDiscount().stripTrailingZeros().toPlainString())
                .setOptionTakerBuyFeeDiscount(fee.getOptionTakerBuyFeeDiscount().stripTrailingZeros().toPlainString())
                .setOptionTakerSellFeeDiscount(fee.getOptionTakerSellFeeDiscount().stripTrailingZeros().toPlainString())
                .setContractMakerBuyFeeDiscount(fee.getContractMakerBuyFeeDiscount().stripTrailingZeros().toPlainString())
                .setContractMakerSellFeeDiscount(fee.getContractMakerSellFeeDiscount().stripTrailingZeros().toPlainString())
                .setContractTakerBuyFeeDiscount(fee.getContractTakerBuyFeeDiscount().stripTrailingZeros().toPlainString())
                .setContractTakerSellFeeDiscount(fee.getContractTakerSellFeeDiscount().stripTrailingZeros().toPlainString())
                .setCreated(new DateTime(fee.getCreated()).toString("yyyy-MM-dd HH:mm:ss"))
                .setUpdated(new DateTime(fee.getUpdated()).toString("yyyy-MM-dd HH:mm:ss"))
                .build();
    }

    @Transactional(isolation = Isolation.READ_COMMITTED, propagation = Propagation.REQUIRED, rollbackFor = Throwable.class)
    public AddSymbolFeeConfigResponse saveSymbolFeeConfig(AddSymbolFeeConfigRequest request) {
        //6004手续费特殊处理支持负手续费配置
        if (request.getOrgId() == 6004l) {
            return symbolNegativeFeeConfig(request);
        }
        Example example = new Example(Symbol.class);
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("orgId", request.getOrgId());
        criteria.andEqualTo("exchangeId", request.getExchangeId());
        criteria.andEqualTo("symbolId", request.getSymbolId());
        Symbol symbol = this.symbolMapper.selectOneByExample(example);
        if (symbol == null) {
            return AddSymbolFeeConfigResponse.newBuilder().setBasicRet(BasicRet.newBuilder().setCode(1).setMsg("save.fee.symbol.is.null").build()).build();
        }
        if (symbol.getCategory().equals(1)) {
            if (new BigDecimal(request.getTakerSellFee()).compareTo(new BigDecimal("0.0001")) < 0) {
                return AddSymbolFeeConfigResponse.newBuilder().setBasicRet(BasicRet.newBuilder().setCode(2).setMsg("save.fee.symbol.low.fee").build()).build();
            }
            if (new BigDecimal(request.getTakerBuyFee()).compareTo(new BigDecimal("0.0001")) < 0) {
                return AddSymbolFeeConfigResponse.newBuilder().setBasicRet(BasicRet.newBuilder().setCode(2).setMsg("save.fee.symbol.low.fee").build()).build();
            }
            if (new BigDecimal(request.getMakerSellFee()).compareTo(new BigDecimal("0.0001")) < 0) {
                return AddSymbolFeeConfigResponse.newBuilder().setBasicRet(BasicRet.newBuilder().setCode(2).setMsg("save.fee.symbol.low.fee").build()).build();
            }
            if (new BigDecimal(request.getMakerBuyFee()).compareTo(new BigDecimal("0.0001")) < 0) {
                return AddSymbolFeeConfigResponse.newBuilder().setBasicRet(BasicRet.newBuilder().setCode(2).setMsg("save.fee.symbol.low.fee").build()).build();
            }
        } else if (symbol.getCategory().equals(4)) {
            if (new BigDecimal(request.getMakerBuyFee()).compareTo(BigDecimal.ZERO) < 0) {
                return AddSymbolFeeConfigResponse.newBuilder().setBasicRet(BasicRet.newBuilder().setCode(1).setMsg("save.fee.symbol.low.fee").build()).build();
            }
            if (new BigDecimal(request.getMakerSellFee()).compareTo(BigDecimal.ZERO) < 0) {
                return AddSymbolFeeConfigResponse.newBuilder().setBasicRet(BasicRet.newBuilder().setCode(1).setMsg("save.fee.symbol.low.fee").build()).build();
            }
            if (new BigDecimal(request.getTakerBuyFee()).compareTo(new BigDecimal("0.0001")) < 0) {
                return AddSymbolFeeConfigResponse.newBuilder().setBasicRet(BasicRet.newBuilder().setCode(1).setMsg("save.fee.symbol.low.fee").build()).build();
            }
            if (new BigDecimal(request.getTakerSellFee()).compareTo(new BigDecimal("0.0001")) < 0) {
                return AddSymbolFeeConfigResponse.newBuilder().setBasicRet(BasicRet.newBuilder().setCode(1).setMsg("save.fee.symbol.low.fee").build()).build();
            }
        }

        SymbolFeeConfig symbolFeeConfig = SymbolFeeConfig
                .builder()
                .id(request.getId() > 0 ? request.getId() : 0)
                .orgId(request.getOrgId())
                .symbolId(request.getSymbolId())
                .makerBuyFee(StringUtils.isNotEmpty(request.getMakerBuyFee()) ? new BigDecimal(request.getMakerBuyFee()) : BigDecimal.ZERO)
                .makerSellFee(StringUtils.isNotEmpty(request.getMakerSellFee()) ? new BigDecimal(request.getMakerSellFee()) : BigDecimal.ZERO)
                .takerBuyFee(StringUtils.isNotEmpty(request.getTakerBuyFee()) ? new BigDecimal(request.getTakerBuyFee()) : BigDecimal.ZERO)
                .takerSellFee(StringUtils.isNotEmpty(request.getTakerSellFee()) ? new BigDecimal(request.getTakerSellFee()) : BigDecimal.ZERO)
                .status(1)
                .build();
        if (request.getId() > 0) {
            symbolFeeConfig.setUpdated(new Date());
            this.symbolFeeConfigMapper.updateByPrimaryKeySelective(symbolFeeConfig);
        } else {
            symbolFeeConfig.setCreated(new Date());
            symbolFeeConfig.setUpdated(new Date());
            this.symbolFeeConfigMapper.insertSelective(symbolFeeConfig);
        }
        return AddSymbolFeeConfigResponse.getDefaultInstance();
    }

    //仅限6004券商使用
    @Transactional(isolation = Isolation.READ_COMMITTED, propagation = Propagation.REQUIRED, rollbackFor = Throwable.class)
    public AddSymbolFeeConfigResponse symbolNegativeFeeConfig(AddSymbolFeeConfigRequest request) {
        Example example = new Example(Symbol.class);
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("orgId", request.getOrgId());
        criteria.andEqualTo("exchangeId", request.getExchangeId());
        criteria.andEqualTo("symbolId", request.getSymbolId());
        Symbol symbol = this.symbolMapper.selectOneByExample(example);
        if (symbol == null) {
            return AddSymbolFeeConfigResponse.newBuilder().setBasicRet(BasicRet.newBuilder().setCode(1).setMsg("save.fee.symbol.is.null").build()).build();
        }
        log.info("symbolNegativeFeeConfig orgId {} makerBuyFee {} makerSellFee {} takerBuyFee {} takerSellFee {}",
                request.getOrgId(), request.getMakerBuyFee(), request.getMakerSellFee(), request.getTakerBuyFee(), request.getTakerSellFee());
        BigDecimal makerBuyFee = new BigDecimal(request.getMakerBuyFee());
        BigDecimal makerSellFee = new BigDecimal(request.getMakerSellFee());
        BigDecimal makerBonusRate = BigDecimal.ZERO;
        if (makerBuyFee.compareTo(BigDecimal.ZERO) < 0 || makerSellFee.compareTo(BigDecimal.ZERO) < 0) {
            makerBonusRate = makerBuyFee.abs().compareTo(makerSellFee.abs()) > 0 ? makerBuyFee.abs() : makerSellFee.abs();
            if (makerBonusRate.compareTo(BigDecimal.ZERO) > 0) {
                //更新平台负maker奖励
                grpcSymbolService.setMakerBonusConfig(MakerBonusConfig
                        .newBuilder()
                        .setBrokerId(request.getOrgId())
                        .setSymbolId(request.getSymbolId())
                        .setMakerBonusRate(makerBonusRate.stripTrailingZeros().toPlainString())
                        .setMinInterestFeeRate(BigDecimal.ZERO.toPlainString())
                        .setMinTakerFeeRate(BigDecimal.ZERO.toPlainString()).build(), request.getOrgId());
            }
        }

        if (new BigDecimal(request.getTakerSellFee()).compareTo(BigDecimal.ZERO) < 0) {
            return AddSymbolFeeConfigResponse.newBuilder().setBasicRet(BasicRet.newBuilder().setCode(2).setMsg("save.fee.symbol.low.fee").build()).build();
        }
        if (new BigDecimal(request.getTakerBuyFee()).compareTo(BigDecimal.ZERO) < 0) {
            return AddSymbolFeeConfigResponse.newBuilder().setBasicRet(BasicRet.newBuilder().setCode(2).setMsg("save.fee.symbol.low.fee").build()).build();
        }
        SymbolFeeConfig symbolFeeConfig = SymbolFeeConfig
                .builder()
                .id(request.getId() > 0 ? request.getId() : 0)
                .orgId(request.getOrgId())
                .symbolId(request.getSymbolId())
                .makerBuyFee(StringUtils.isNotEmpty(request.getMakerBuyFee()) ? new BigDecimal(request.getMakerBuyFee()) : BigDecimal.ZERO)
                .makerSellFee(StringUtils.isNotEmpty(request.getMakerSellFee()) ? new BigDecimal(request.getMakerSellFee()) : BigDecimal.ZERO)
                .takerBuyFee(StringUtils.isNotEmpty(request.getTakerBuyFee()) ? new BigDecimal(request.getTakerBuyFee()) : BigDecimal.ZERO)
                .takerSellFee(StringUtils.isNotEmpty(request.getTakerSellFee()) ? new BigDecimal(request.getTakerSellFee()) : BigDecimal.ZERO)
                .status(1)
                .build();
        if (request.getId() > 0) {
            symbolFeeConfig.setUpdated(new Date());
            this.symbolFeeConfigMapper.updateByPrimaryKeySelective(symbolFeeConfig);
        } else {
            symbolFeeConfig.setCreated(new Date());
            symbolFeeConfig.setUpdated(new Date());
            this.symbolFeeConfigMapper.insertSelective(symbolFeeConfig);
        }
        return AddSymbolFeeConfigResponse.getDefaultInstance();
    }

    public QuerySymbolFeeConfigResponse querySymbolFeeConfigList(QuerySymbolFeeConfigRequest request) {
        Example example = Example.builder(SymbolFeeConfig.class).build();
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("orgId", request.getOrgId());
        criteria.andEqualTo("status", 1);
        criteria.andEqualTo("exchangeId", request.getExchangeId());

        if (request.getCategory() > 0) {
            criteria.andEqualTo("category", request.getCategory());
        }

        if (StringUtils.isNotEmpty(request.getSymbolId())) {
            criteria.andEqualTo("symbolId", request.getSymbolId());
        }

        List<SymbolFeeConfig> symbolFeeConfigList = this.symbolFeeConfigMapper.selectByExample(example);
        if (CollectionUtils.isEmpty(symbolFeeConfigList)) {
            return QuerySymbolFeeConfigResponse.getDefaultInstance();
        }

        List<QuerySymbolFeeConfigResponse.SymbolFeeConfig> symbolFeeConfig = new ArrayList<>();
        symbolFeeConfigList.forEach(fee -> {
            symbolFeeConfig.add(QuerySymbolFeeConfigResponse.SymbolFeeConfig
                    .newBuilder()
                    .setId(fee.getId())
                    .setSymbolId(fee.getSymbolId())
                    .setOrgId(fee.getOrgId())
                    .setMakerBuyFee(fee.getMakerBuyFee().stripTrailingZeros().toPlainString())
                    .setMakerSellFee(fee.getMakerSellFee().stripTrailingZeros().toPlainString())
                    .setTakerBuyFee(fee.getTakerBuyFee().stripTrailingZeros().toPlainString())
                    .setTakerSellFee(fee.getTakerSellFee().stripTrailingZeros().toPlainString())
                    .setStatus(fee.getStatus())
                    .setBaseTokenId(fee.getBaseTokenId())
                    .setQuoteTokenId(fee.getQuoteTokenId())
                    .setCreated(new DateTime(fee.getCreated()).toString("yyyy-MM-dd HH:mm:ss"))
                    .setUpdated(new DateTime(fee.getUpdated()).toString("yyyy-MM-dd HH:mm:ss"))
                    .setCategory(fee.getCategory())
                    .build());
        });
        return QuerySymbolFeeConfigResponse.newBuilder().addAllSymbolFee(symbolFeeConfig).build();
    }

    @Transactional(isolation = Isolation.READ_COMMITTED, propagation = Propagation.REQUIRED, rollbackFor = Throwable.class)
    public void saveSymbolMarketAccountConfig(List<SaveSymbolMarketAccountRequest.MarketAccount> marketAccountList) {
        if (CollectionUtils.isEmpty(marketAccountList)) {
            return;
        }
        List<Long> accountList
                = marketAccountList.stream().map(SaveSymbolMarketAccountRequest.MarketAccount::getAccountId).distinct().collect(Collectors.toList());
        Example example = new Example(Account.class);
        Example.Criteria criteria = example.createCriteria();
        criteria.andIn("accountId", accountList);
        List<Account> accounts = this.accountMapper.selectByExample(example);
        try {
            accounts.forEach(account -> {
                ChangeUserApiLevelResponse response = this.grpcSecurityService.changeUserApiLevel(ChangeUserApiLevelRequest
                        .newBuilder()
                        .setOrgId(account.getOrgId())
                        .setUserId(account.getUserId())
                        .setLevel(3)
                        .build());
                if (response.getRet() != 0 && response.getRet() > 1) {
                    log.info("changeUserApiLevel fail orgId {} userId {}", account.getOrgId(), account.getUserId());
                }
            });
        } catch (Exception ex) {
            log.info("changeUserApiLevel fail error {}", ex);
        }

        marketAccountList.forEach(account -> {
            Account accountInfo = this.accountMapper.getAccountByOrgIdAndAccountId(account.getOrgId(), account.getAccountId());
            if (accountInfo == null) {
                throw new BrokerException(BrokerErrorCode.ACCOUNT_NOT_EXIST);
            }
            Long exchangeId = symbolMapper.getExchangeIdBySymbolId(account.getOrgId(), account.getSymbolId());
            if (exchangeId == null) {
                log.error("cannot find symbol info in tb_symbol when add market account: orgId:{} userId:{} accountId:{} symbolId:{}",
                        account.getOrgId(), accountInfo.getUserId(), account.getAccountId(), account.getSymbolId());
                throw new BrokerException(BrokerErrorCode.SYMBOL_PROHIBIT_ORDER);
            }
            int count = accountTradeFeeConfigMapper.selectCount(AccountTradeFeeConfig.builder()
                    .orgId(account.getOrgId())
                    .accountId(account.getAccountId())
                    .symbolId(account.getSymbolId())
                    .build());
            if (count > 0) {
                return;
            }
            AccountTradeFeeConfig tradeFeeConfig = AccountTradeFeeConfig.builder()
                    .orgId(account.getOrgId())
                    .exchangeId(exchangeId)
                    .symbolId(account.getSymbolId())
                    .userId(accountInfo.getUserId())
                    .accountId(accountInfo.getAccountId())
                    .makerBuyFeeRate(StringUtils.isEmpty(account.getMakerBuyFee()) ? BigDecimal.ZERO : new BigDecimal(account.getMakerBuyFee()))
                    .takerBuyFeeRate(StringUtils.isEmpty(account.getTakerBuyFee()) ? BigDecimal.ZERO : new BigDecimal(account.getTakerBuyFee()))
                    .makerSellFeeRate(StringUtils.isEmpty(account.getMakerSellFee()) ? BigDecimal.ZERO : new BigDecimal(account.getMakerSellFee()))
                    .takerSellFeeRate(StringUtils.isEmpty(account.getTakerSellFee()) ? BigDecimal.ZERO : new BigDecimal(account.getTakerSellFee()))
                    .status(1)
                    .created(new Date())
                    .updated(new Date())
                    .build();
            log.info("saveSymbolMarketAccountConfig info {}", new Gson().toJson(tradeFeeConfig));
            accountTradeFeeConfigMapper.insertSelective(tradeFeeConfig);
        });
    }

    public QueryAccountTradeFeeConfigResponse queryAccountTradeFeeConfig(QueryAccountTradeFeeConfigRequest request) {
        Long orgId = request.getOrgId();
        String symbolId = request.getSymbolId();
        long accountId = request.getAccountId();
        long fromId = request.getFromId();
        int limit = request.getLimit();
        Example example = Example.builder(AccountTradeFeeConfig.class).orderByDesc("id").build();
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("orgId", orgId);
        if (!Strings.isNullOrEmpty(symbolId)) {
            criteria.andEqualTo("symbolId", symbolId);
        }
        if (accountId > 0) {
            criteria.andEqualTo("accountId", accountId);
        }
        if (fromId > 0) {
            criteria.andLessThan("id", fromId);
        }
        List<AccountTradeFeeConfig> configList = accountTradeFeeConfigMapper.selectByExampleAndRowBounds(example, new RowBounds(0, limit));
        List<QueryAccountTradeFeeConfigResponse.AccountTradeFeeConfig> accountTradeFeeConfigList = configList.stream()
                .map(config -> QueryAccountTradeFeeConfigResponse.AccountTradeFeeConfig.newBuilder()
                        .setId(config.getId())
                        .setOrgId(config.getOrgId())
                        .setExchangeId(config.getExchangeId())
                        .setSymbolId(config.getSymbolId())
                        .setUserId(config.getUserId())
                        .setAccountId(config.getAccountId())
                        .setMakerBuyFeeRate(config.getMakerBuyFeeRate().stripTrailingZeros().toPlainString())
                        .setMakerSellFeeRate(config.getMakerSellFeeRate().stripTrailingZeros().toPlainString())
                        .setTakerBuyFeeRate(config.getTakerBuyFeeRate().stripTrailingZeros().toPlainString())
                        .setTakerSellFeeRate(config.getTakerSellFeeRate().stripTrailingZeros().toPlainString())
                        .build()).collect(Collectors.toList());
        return QueryAccountTradeFeeConfigResponse.newBuilder().addAllConfigs(accountTradeFeeConfigList).build();
    }

    public void deleteAccountTradeFeeConfig(DeleteAccountTradeFeeConfigRequest request) {
        Long orgId = request.getOrgId();
        Long id = request.getId();
        this.accountTradeFeeConfigMapper.deleteRecord(orgId, id);
    }

    public QueryAllSymbolMarketAccountResponse queryAllSymbolMarketAccount(Long orgId, String symbolId) {
        List<QueryAllSymbolMarketAccountResponse.SymbolMarketAccount> marketAccountList = new ArrayList<>();

        Example example = Example.builder(AccountTradeFeeConfig.class).build();
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("orgId", orgId);
        if (StringUtils.isNotEmpty(symbolId)) {
            criteria.andEqualTo("symbolId", symbolId);
        }
        criteria.andEqualTo("status", 1);
        List<AccountTradeFeeConfig> accountTradeFeeConfigList = accountTradeFeeConfigMapper.selectByExample(example);
        marketAccountList = accountTradeFeeConfigList.stream()
                .map(config -> QueryAllSymbolMarketAccountResponse.SymbolMarketAccount.newBuilder()
                        .setId(config.getId())
                        .setOrgId(config.getOrgId())
                        .setSymbolId(config.getSymbolId())
                        .setAccountId(config.getAccountId())
                        .setStatus(config.getStatus())
                        .setCategory(0)
                        .build()).collect(Collectors.toList());

        return QueryAllSymbolMarketAccountResponse.newBuilder().addAllMarketAccount(marketAccountList).build();
    }

    public void deleteSymbolMarketAccount(Long id) {
        this.accountTradeFeeConfigMapper.deleteByPrimaryKey(id);
    }

    public JsonObject resetSymbolNegativeMakerFeeRate(Long orgId, String symbolId, String makerBuyFeeRate, String makerSellFeeRate, String takerBuyFeeRate, String takerSellFeeRate, boolean refreshAccountTradeFeeConfig) {
        Symbol symbol = symbolMapper.getOrgSymbol(orgId, symbolId);
        JsonObject dataObj = new JsonObject();
        SymbolFeeConfig symbolFeeConfig = symbolFeeConfigMapper.selectOne(SymbolFeeConfig.builder().orgId(orgId).symbolId(symbolId).build());
        if (symbolFeeConfig == null) {
            symbolFeeConfig = SymbolFeeConfig.builder()
                    .orgId(orgId)
                    .exchangeId(symbol.getExchangeId())
                    .symbolId(symbolId)
                    .makerBuyFee(new BigDecimal(makerBuyFeeRate))
                    .makerSellFee(new BigDecimal(makerSellFeeRate))
                    .takerBuyFee(new BigDecimal(takerBuyFeeRate))
                    .takerSellFee(new BigDecimal(takerSellFeeRate))
                    .status(1)
                    .category(symbol.getCategory())
                    .baseTokenId(symbol.getBaseTokenId())
                    .baseTokenName(symbol.getBaseTokenName())
                    .quoteTokenId(symbol.getQuoteTokenId())
                    .quoteTokenName(symbol.getQuoteTokenName())
                    .created(new Date())
                    .updated(new Date())
                    .build();
            symbolFeeConfigMapper.insertSelective(symbolFeeConfig);
        } else {
            dataObj.addProperty("oldValue", JsonUtil.defaultGson().toJson(symbolFeeConfig));
            symbolFeeConfig = symbolFeeConfig.toBuilder()
                    .makerBuyFee(new BigDecimal(makerBuyFeeRate))
                    .makerSellFee(new BigDecimal(makerSellFeeRate))
                    .takerBuyFee(new BigDecimal(takerBuyFeeRate))
                    .takerSellFee(new BigDecimal(takerSellFeeRate))
                    .updated(new Date())
                    .build();
            symbolFeeConfigMapper.updateByPrimaryKey(symbolFeeConfig);
        }
        dataObj.addProperty("newValue", JsonUtil.defaultGson().toJson(symbolFeeConfig));
        if (refreshAccountTradeFeeConfig) {
            List<AccountTradeFeeConfig> accountTradeFeeConfigs = accountTradeFeeConfigMapper.select(AccountTradeFeeConfig.builder().orgId(orgId).symbolId(symbolId).build());
            dataObj.addProperty("refreshRecordsCount", accountTradeFeeConfigs.size());
            accountTradeFeeConfigs.forEach(config -> {
                config = config.toBuilder()
                        .makerBuyFeeRate(new BigDecimal(makerBuyFeeRate))
                        .makerSellFeeRate(new BigDecimal(makerSellFeeRate))
                        .takerBuyFeeRate(new BigDecimal(takerBuyFeeRate))
                        .takerSellFeeRate(new BigDecimal(takerSellFeeRate))
                        .updated(new Date())
                        .build();
                accountTradeFeeConfigMapper.updateByPrimaryKey(config);
            });
        }
        return dataObj;
    }

    /**
     * @param request
     * @return
     * @title: 绑定用户和手续费的关系, 绑定返回1
     * @add: created by yanyj 2020-08-05
     */
    @Transactional(isolation = Isolation.READ_COMMITTED, propagation = Propagation.REQUIRED, rollbackFor = Throwable.class)
    public BindUserDiscountConfigResponse bindUserDiscountConfig(BindUserDiscountConfigRequest request) {
        Long orgId = request.getOrgId();
        Long discountId = request.getDiscountId();
        DiscountFeeConfig config = getDiscountConfig(orgId, discountId, true);
        Long userId = request.getUserId();

        Example userExample = Example.builder(DiscountFeeUser.class).build();
        Example.Criteria userCriteria = userExample.createCriteria();
        userCriteria.andEqualTo("orgId", request.getOrgId());
        userCriteria.andEqualTo("userId", userId);
        userCriteria.andEqualTo("exchangeId", config.getExchangeId());
        userCriteria.andEqualTo("symbolId", config.getSymbolId());
        userCriteria.andEqualTo("status", 1);
        DiscountFeeUser discountFeeUser = this.discountFeeUserMapper.selectOneByExample(userExample);
        if (discountFeeUser != null) {
            BindUserDiscountConfigResponse response =
                    BindUserDiscountConfigResponse.newBuilder().setStatus(1).setOrgId(orgId).setUserId(userId).setDiscountId(discountId).build();
            return response;
        }

        DiscountFeeUser newDiscountFeeUser = DiscountFeeUser.builder()
                .orgId(request.getOrgId())
                .baseGroupId(config.getId())
                .userId(userId)
                .exchangeId(config.getExchangeId())
                .symbolId(config.getSymbolId())
                .status(1)
                .temporaryGroupId(0l)
                .created(new Date())
                .updated(new Date())
                .build();
        this.discountFeeUserMapper.insertSelective(newDiscountFeeUser);

        BindUserDiscountConfigResponse response =
                BindUserDiscountConfigResponse.newBuilder().setStatus(1).setOrgId(orgId).setUserId(userId).setDiscountId(discountId).build();
        return response;
    }

    /**
     * @param request
     * @return
     * @title: 解绑用户和手续费的关系, 如果解绑，则返回status为0
     * @add: created by yanyj 2020-08-05
     */
    @Transactional(isolation = Isolation.READ_COMMITTED, propagation = Propagation.REQUIRED, rollbackFor = Throwable.class)
    public UnbindUserDiscountConfigResponse unbindUserDiscountConfig(UnbindUserDiscountConfigRequest request) {
        Long orgId = request.getOrgId();
        Long discountId = request.getDiscountId();
        DiscountFeeConfig config = getDiscountConfig(orgId, discountId, true);
        Long userId = request.getUserId();

        Example userExample = Example.builder(DiscountFeeUser.class).build();
        Example.Criteria userCriteria = userExample.createCriteria();
        userCriteria.andEqualTo("orgId", request.getOrgId());
        userCriteria.andEqualTo("userId", userId);
        userCriteria.andEqualTo("exchangeId", config.getExchangeId());
        userCriteria.andEqualTo("symbolId", config.getSymbolId());
        userCriteria.andEqualTo("status", 1);
        DiscountFeeUser discountFeeUser = this.discountFeeUserMapper.selectOneByExample(userExample);
        if (discountFeeUser == null) {
            UnbindUserDiscountConfigResponse response =
                    UnbindUserDiscountConfigResponse.newBuilder().setStatus(0).setOrgId(orgId).setUserId(userId).setDiscountId(discountId).build();
            return response;
        }

        discountFeeUserMapper.updateStatusByBaseGroupIdAndUserId(orgId, discountId, userId);
        UnbindUserDiscountConfigResponse response =
                UnbindUserDiscountConfigResponse.newBuilder().setStatus(0).setOrgId(orgId).setUserId(userId).setDiscountId(discountId).build();
        return response;
    }

    /**
     * @param orgId
     * @param discountId
     * @param thrownNullException: 如果为空，则抛出异常
     * @return
     * @title根据orgid和discountid获取折扣配置
     * @add：created by yanyj at 2020-08-05
     */
    private DiscountFeeConfig getDiscountConfig(Long orgId, Long discountId, boolean thrownNullException) {
        Example example = Example.builder(DiscountFeeConfig.class).build();
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("orgId", orgId);
        criteria.andEqualTo("id", discountId);
        criteria.andEqualTo("status", 1);

        DiscountFeeConfig discountFeeConfig = this.discountFeeConfigMapper.selectOneByExample(example);
        if (thrownNullException && discountFeeConfig == null) {
            throw new BrokerException(BrokerErrorCode.SYSTEM_ERROR);
        }
        return discountFeeConfig;
    }

    public void batchBrokerAccountFee(long orgId, long exchangeId, long userId, String account, String symbols, String takerBuyFee, String takerSellFee, String makerBuyFee, String makerSellFee) {
        if (StringUtils.isEmpty(account) || StringUtils.isEmpty(symbols)) {
            return;
        }
        List<Long> accountId = Splitter.on(",").omitEmptyStrings().splitToList(account).stream().map(id -> Long.parseLong(id)).collect(Collectors.toList());
        List<String> symbolId = Splitter.on(",").omitEmptyStrings().splitToList(symbols);
        for (String symbol : symbolId) {
            BigDecimal makerBonusRate = BigDecimal.ZERO;
            if (new BigDecimal(makerBuyFee).compareTo(BigDecimal.ZERO) < 0 || new BigDecimal(makerSellFee).compareTo(BigDecimal.ZERO) < 0) {
                makerBonusRate = new BigDecimal(makerBuyFee).abs().compareTo(new BigDecimal(makerSellFee).abs()) > 0 ? new BigDecimal(makerBuyFee).abs() : new BigDecimal(makerSellFee).abs();
                if (makerBonusRate.compareTo(BigDecimal.ZERO) > 0) {
                    //更新平台负maker奖励
                    grpcSymbolService.setMakerBonusConfig(MakerBonusConfig
                            .newBuilder()
                            .setBrokerId(orgId)
                            .setSymbolId(symbol)
                            .setMakerBonusRate(makerBonusRate.stripTrailingZeros().toPlainString())
                            .setMinInterestFeeRate(BigDecimal.ZERO.toPlainString())
                            .setMinTakerFeeRate(BigDecimal.ZERO.toPlainString()).build(), orgId);
                }
            }
            for (Long aid : accountId) {
                AccountTradeFeeConfig accountTradeFeeConfig = accountTradeFeeConfigMapper.query(orgId, userId, aid, symbol);
                if (accountTradeFeeConfig == null) {
                    AccountTradeFeeConfig tradeFeeConfig = AccountTradeFeeConfig.builder()
                            .orgId(orgId)
                            .exchangeId(exchangeId)
                            .symbolId(symbol)
                            .userId(userId)
                            .accountId(aid)
                            .makerBuyFeeRate(StringUtils.isEmpty(makerBuyFee) ? BigDecimal.ZERO : new BigDecimal(makerBuyFee))
                            .makerSellFeeRate(StringUtils.isEmpty(makerSellFee) ? BigDecimal.ZERO : new BigDecimal(makerSellFee))
                            .takerBuyFeeRate(StringUtils.isEmpty(takerBuyFee) ? BigDecimal.ZERO : new BigDecimal(takerBuyFee))
                            .takerSellFeeRate(StringUtils.isEmpty(takerSellFee) ? BigDecimal.ZERO : new BigDecimal(takerSellFee))
                            .status(1)
                            .created(new Date())
                            .updated(new Date())
                            .build();
                    log.info("saveSymbolMarketAccountConfig info {}", new Gson().toJson(tradeFeeConfig));
                    accountTradeFeeConfigMapper.insertSelective(tradeFeeConfig);
                } else {
                    AccountTradeFeeConfig tradeFeeConfig = AccountTradeFeeConfig.builder()
                            .id(accountTradeFeeConfig.getId())
                            .orgId(orgId)
                            .exchangeId(exchangeId)
                            .symbolId(symbol)
                            .userId(userId)
                            .accountId(aid)
                            .makerBuyFeeRate(StringUtils.isEmpty(makerBuyFee) ? BigDecimal.ZERO : new BigDecimal(makerBuyFee))
                            .makerSellFeeRate(StringUtils.isEmpty(makerSellFee) ? BigDecimal.ZERO : new BigDecimal(makerSellFee))
                            .takerBuyFeeRate(StringUtils.isEmpty(takerBuyFee) ? BigDecimal.ZERO : new BigDecimal(takerBuyFee))
                            .takerSellFeeRate(StringUtils.isEmpty(takerSellFee) ? BigDecimal.ZERO : new BigDecimal(takerSellFee))
                            .status(1)
                            .created(new Date())
                            .updated(new Date())
                            .build();
                    accountTradeFeeConfigMapper.updateByPrimaryKey(tradeFeeConfig);
                }
            }
        }
    }
}
