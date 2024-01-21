package io.bhex.broker.server.grpc.server.service;

import com.google.api.client.util.Lists;
import com.google.common.base.Strings;
import io.bhex.broker.grpc.admin.EditLetfInfoRequest;
import io.bhex.broker.grpc.admin.GetLetfInfoRequest;
import io.bhex.broker.grpc.admin.LetfInfos;
import io.bhex.broker.grpc.basic.Symbol;
import io.bhex.broker.grpc.common.AdminSimplyReply;
import io.bhex.broker.server.model.AccountTradeFeeConfig;
import io.bhex.broker.server.model.OrderTokenHoldLimit;
import io.bhex.broker.server.primary.mapper.AccountTradeFeeConfigMapper;
import io.bhex.broker.server.primary.mapper.OrderTokenHoldLimitMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import tk.mybatis.mapper.entity.Example;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@Service
public class LetfService {
    @Resource
    private OrderTokenHoldLimitMapper orderTokenHoldLimitMapper;
    @Resource
    private AccountTradeFeeConfigMapper accountTradeFeeConfigMapper;
    @Resource
    private BasicService basicService;

    public LetfInfos getLetfInfos(GetLetfInfoRequest request) {
        List<String> symbols = request.getSymbolIdList();
        Map<String, OrderTokenHoldLimit> limitMap = getHoldLimit(request.getOrgId(), symbols);
        Map<String, List<AccountTradeFeeConfig>> marketListMap = getMarketAccount(request.getOrgId(), symbols);
        LetfInfos.Builder resultBuilder = LetfInfos.newBuilder();
        for (String symbolId : symbols) {

            OrderTokenHoldLimit limit = limitMap.get(symbolId);
            LetfInfos.LetfInfo.Builder builder = LetfInfos.LetfInfo.newBuilder();
            builder.setSymbolId(symbolId);
            if (limit != null) {
                builder.setHoldQuantity(limit.getHoldQuantity().stripTrailingZeros().toPlainString());
                builder.setWhiteListUserId(Strings.nullToEmpty(limit.getWhiteListUserId()));
            }
            List<AccountTradeFeeConfig> marketAccounts = marketListMap.get(symbolId);
            if (!CollectionUtils.isEmpty(marketAccounts)) {
                List<Long> accounts = marketAccounts.stream().map(m -> m.getAccountId()).collect(Collectors.toList());
                builder.addAllMarketAccounts(accounts);
            }
            Symbol symbol = basicService.getOrgSymbol(request.getOrgId(), symbolId);
            if (symbol != null) {
                builder.setTokenId(symbol.getBaseTokenId());
            }

            resultBuilder.addLetfInfo(builder.build());
        }
        return resultBuilder.build();
    }

    private Map<String, OrderTokenHoldLimit> getHoldLimit(long orgId, List<String> symbols) {
        Example example = new Example(OrderTokenHoldLimit.class);
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("orgId", orgId);
        criteria.andIn("symbolId", symbols);
        List<OrderTokenHoldLimit> limits = orderTokenHoldLimitMapper.selectByExample(example);
        Map<String, OrderTokenHoldLimit> limitMap = Lists.newArrayList(limits).stream().collect(Collectors.toMap(l -> l.getSymbolId(), l -> l));
        return limitMap;
    }

    private Map<String, List<AccountTradeFeeConfig>> getMarketAccount(long orgId, List<String> symbols) {
        Example example = Example.builder(AccountTradeFeeConfig.class).build();
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("orgId", orgId);
        criteria.andIn("symbolId", symbols);
        criteria.andEqualTo("status", 1);
        List<AccountTradeFeeConfig> accountTradeFeeConfigList = accountTradeFeeConfigMapper.selectByExample(example);
        Map<String, List<AccountTradeFeeConfig>> marketListMap = Lists.newArrayList(accountTradeFeeConfigList).stream()
                .collect(Collectors.groupingBy(AccountTradeFeeConfig::getSymbolId));
        return marketListMap;
    }

    public AdminSimplyReply editLetfInfo(EditLetfInfoRequest request) {
        Example example = new Example(OrderTokenHoldLimit.class);
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("orgId", request.getOrgId());
        criteria.andEqualTo("symbolId", request.getSymbolId());
        OrderTokenHoldLimit holdLimit = orderTokenHoldLimitMapper.selectOneByExample(example);
        if (holdLimit != null) {
            holdLimit.setHoldQuantity(new BigDecimal(request.getHoldQuantity()));
            holdLimit.setWhiteListUserId(Strings.nullToEmpty(request.getWhiteListUserId()));
            holdLimit.setUpdated(System.currentTimeMillis());
            orderTokenHoldLimitMapper.updateByPrimaryKey(holdLimit);
        } else {
            holdLimit = OrderTokenHoldLimit.builder()
                    .orgId(request.getOrgId())
                    .symbolId(request.getSymbolId())
                    .whiteListUserId(Strings.nullToEmpty(request.getWhiteListUserId()))
                    .holdQuantity(new BigDecimal(request.getHoldQuantity()))
                    .whiteListUserId(Strings.nullToEmpty(request.getWhiteListUserId()))
                    .created(System.currentTimeMillis())
                    .updated(System.currentTimeMillis())
                    .build();
            orderTokenHoldLimitMapper.insertSelective(holdLimit);
        }
        return AdminSimplyReply.getDefaultInstance();
    }
}
