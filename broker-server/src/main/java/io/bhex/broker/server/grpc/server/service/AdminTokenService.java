/**********************************
 *@项目名称: broker-server-parent
 *@文件名称: io.bhex.broker.server.grpc.server.service
 *@Date 2018/8/19
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
package io.bhex.broker.server.grpc.server.service;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import io.bhex.base.proto.BaseRequest;
import io.bhex.base.proto.DecimalUtil;
import io.bhex.base.token.*;
import io.bhex.broker.common.util.ExtraConfigUtil;
import io.bhex.broker.common.util.ExtraTagUtil;
import io.bhex.broker.grpc.admin.QueryTokenReply;
import io.bhex.broker.grpc.admin.SimpleToken;
import io.bhex.broker.grpc.admin.TokenDetail;
import io.bhex.broker.grpc.common.AdminSimplyReply;
import io.bhex.broker.server.grpc.client.service.GrpcTokenService;
import io.bhex.broker.server.model.Symbol;
import io.bhex.broker.server.primary.mapper.SymbolMapper;
import io.bhex.broker.server.primary.mapper.TokenMapper;
import io.bhex.broker.server.model.Token;
import io.bhex.broker.server.util.BaseReqUtil;
import io.bhex.broker.server.util.BeanCopyUtils;
import io.bhex.broker.server.util.PageUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Service
public class AdminTokenService {

    @Resource
    private TokenMapper tokenMapper;

    @Resource
    private BasicService basicService;
    @Resource
    private SymbolMapper symbolMapper;

    @Resource
    private GrpcTokenService grpcTokenService;

    public Integer countToken(Long brokerId, String tokenId, String tokenName, Integer category) {
        return tokenMapper.countByBrokerId(brokerId, tokenId, tokenName, category);
    }

    public QueryTokenReply listTokenByOrgId(Integer category, Long orgId) {
        List<Token> tokens = tokenMapper.queryAllByOrgId(Lists.newArrayList(category), orgId);
        List<TokenDetail> result = new ArrayList<>();
        for (Token t : tokens) {
            TokenDetail.Builder builder = TokenDetail.newBuilder();
            BeanUtils.copyProperties(t, builder);

            builder.setAllowDeposit(t.getAllowDeposit() == 1);
            builder.setAllowWithdraw(t.getAllowWithdraw() == 1);
            builder.putAllExtraTag(ExtraTagUtil.newInstance(t.getExtraTag()).map());
            builder.putAllExtraConfig(ExtraConfigUtil.newInstance(t.getExtraConfig()).map());
            result.add(builder.build());
        }
        QueryTokenReply reply = QueryTokenReply.newBuilder()
                .addAllTokenDetails(result)
                .build();
        return reply;
    }

    public Token getToken(Long brokerId, String tokenId) {
        return tokenMapper.getToken(brokerId, tokenId);
    }

    public QueryTokenReply queryToken(Integer current, Integer pageSize, String tokenId, String tokenName, Long brokerId, Integer category) {
        Integer total = countToken(brokerId, tokenId, tokenName, category);
        PageUtil.Page page = PageUtil.pageCount(current, pageSize, total);
        List<Token> tokens = tokenMapper.queryToken(page.getStart(), page.getOffset(), brokerId, tokenId, tokenName, category);
        List<TokenDetail> result = new ArrayList<>();
        for (Token t : tokens) {
            TokenDetail.Builder builder = TokenDetail.newBuilder();
            BeanCopyUtils.copyPropertiesIgnoreNull(t, builder);
            builder.setTokenIcon(Strings.nullToEmpty(t.getTokenIcon()));

            builder.setAllowDeposit(t.getAllowDeposit() == 1);
            builder.setAllowWithdraw(t.getAllowWithdraw() == 1);

            builder.setIsHighRiskToken(t.getIsHighRiskToken() == 1);
            builder.setExchangeId(Objects.nonNull(t.getExchangeId()) ? t.getExchangeId() : 0L);
            builder.setWithdrawFee(t.getFee().stripTrailingZeros().toPlainString());
            builder.putAllExtraTag(ExtraTagUtil.newInstance(t.getExtraTag()).map());
            builder.putAllExtraConfig(ExtraConfigUtil.newInstance(t.getExtraConfig()).map());
            result.add(builder.build());
        }
        QueryTokenReply reply = QueryTokenReply.newBuilder()
                .addAllTokenDetails(result)
                .setTotal(total)
                .setCurrent(current)
                .setPageSize(pageSize)
                .build();
        return reply;
    }

    public List<SimpleToken> getSimpleTokens(long orgId, int category) {
        List<Token> tokens = tokenMapper.querySimpleTokens(orgId, category);
        if (CollectionUtils.isEmpty(tokens)) {
            return Lists.newArrayList();
        }
        List<SimpleToken> result = tokens.stream().map(t -> {
            SimpleToken.Builder builder = SimpleToken.newBuilder();
            BeanCopyUtils.copyPropertiesIgnoreNull(t, builder);
            return builder.build();
        }).collect(Collectors.toList());
        return result;
    }

    public Boolean allowDeposit(String tokenId, Boolean allowDeposit, Long brokerId) {
        if (allowDeposit) {
            io.bhex.base.token.TokenDetail tokenDetail = grpcTokenService.getToken(GetTokenRequest.newBuilder()
                    .setBaseRequest(BaseReqUtil.getBaseRequest(brokerId))
                    .setTokenId(tokenId).build());
            if (tokenDetail.getIsAggregate()) {
                List<String> allowDepositWithdrawList = basicService.aggregateTokenAllowDepositList(brokerId);
                if (!allowDepositWithdrawList.contains(tokenId)) {
                    return false;
                }
            }
        }
        bhAllowDeposit(tokenId, allowDeposit, brokerId);
        return tokenMapper.allowDeposit(tokenId, allowDeposit ? 1 : 0, brokerId) > 0 ? true : false;
    }

    public Boolean bhAllowDeposit(String tokenId, Boolean allowDeposit, Long brokerId) {
        ConfigTokenSwitchReply reply = grpcTokenService.configTokenSwitch(ConfigTokenSwitchRequest.newBuilder()
                .setBrokerId(brokerId)
                .setTokenId(tokenId)
                .setIsOpen(allowDeposit)
                .setType(TokenSwitchType.DEPOSIT)
                .build());

        return true;
    }

    public Boolean allowWithdraw(String tokenId, Boolean allowWithdraw, Long brokerId) {
        if (allowWithdraw) {
            io.bhex.base.token.TokenDetail tokenDetail = grpcTokenService.getToken(GetTokenRequest.newBuilder()
                    .setBaseRequest(BaseReqUtil.getBaseRequest(brokerId))
                    .setTokenId(tokenId).build());
            if (tokenDetail.getIsAggregate()) {
                List<String> allowDepositWithdrawList = basicService.aggregateTokenAllowDepositList(brokerId);
                if (!allowDepositWithdrawList.contains(tokenId)) {
                    return false;
                }
            }
        }
        bhAllowWithdraw(tokenId, allowWithdraw, brokerId);
        return tokenMapper.allowWithdraw(tokenId, allowWithdraw ? 1 : 0, brokerId) > 0;
    }

    public Boolean bhAllowWithdraw(String tokenId, Boolean allowWithdraw, Long brokerId) {
        ConfigTokenSwitchReply reply = grpcTokenService.configTokenSwitch(ConfigTokenSwitchRequest.newBuilder()
                .setBrokerId(brokerId)
                .setTokenId(tokenId)
                .setIsOpen(allowWithdraw)
                .setType(TokenSwitchType.WITHDRAW)
                .build());
        return true;
    }

    //币种无论打开还是关闭 充提都是关闭的
    public Boolean publish(String tokenId, Boolean isPublish, Long brokerId) {
        allowDeposit(tokenId, false, brokerId);
        allowWithdraw(tokenId, false, brokerId);
        return tokenMapper.publish(tokenId, isPublish ? 1 : 0, brokerId) > 0;
    }

    public boolean setTokenIsHighRiskToken(long orgId, String tokenId, boolean isHighRiskToken) {
        return tokenMapper.setIsHighRiskToken(orgId, tokenId, isHighRiskToken ? 1 : 0) == 1;
    }

    public boolean setTokenWithdrawFee(long orgId, String tokenId, String withdrawFee) {
        return tokenMapper.setFee(orgId, tokenId, new BigDecimal(withdrawFee)) == 1;
    }

    public Boolean addToken(Token token) {
        return tokenMapper.insertSelective(token) > 0 ? true : false;
    }

    public Boolean updateTokenIcon(String tokenId, String tokenIcon) {
        return tokenMapper.updateTokenIcon(tokenId, tokenIcon) > 0 ? true : false;
    }

    @Transactional
    public AdminSimplyReply editTokenName(long orgId, String tokenId, String newTokenName, boolean changePlatformTokenName) {
        Token token = getToken(orgId, tokenId);
        if (token == null || newTokenName.equals(token.getTokenName())) {
            return AdminSimplyReply.newBuilder().setResult(true).build();
        }
        if (tokenMapper.getTokenByTokenName(orgId, newTokenName) != null) {
            return AdminSimplyReply.newBuilder().setResult(false).setMessage("token name existed").build();
        }

        if (changePlatformTokenName) {
            ChangeTokenNameRequest request = ChangeTokenNameRequest.newBuilder()
                    .setBaseRequest(BaseRequest.newBuilder().setOrganizationId(orgId).build())
                    .setTokenId(tokenId)
                    .setToTokenName(newTokenName)
                    .build();
            ChangeTokenNameReply changeTokenNameReply = grpcTokenService.changeTokenName(request);
            if (changeTokenNameReply.getResult() != 0) {
                return AdminSimplyReply.newBuilder().setResult(false).setMessage(changeTokenNameReply.getMessage()).build();
            }
        }

        token.setTokenName(newTokenName);
        token.setUpdated(System.currentTimeMillis());
        tokenMapper.updateByPrimaryKeySelective(token);
        symbolMapper.updateBaseTokenName(orgId, tokenId, newTokenName);
        symbolMapper.updateQuoteTokenName(orgId, tokenId, newTokenName);

        return AdminSimplyReply.newBuilder().setResult(true).build();
    }

    public AdminSimplyReply editTokenFullName(long orgId, String tokenId, String newTokenFullName) {
        Token token = getToken(orgId, tokenId);
        if (token == null || newTokenFullName.equals(token.getTokenFullName())) {
            return AdminSimplyReply.newBuilder().setResult(true).build();
        }
        token.setTokenFullName(newTokenFullName);
        token.setUpdated(System.currentTimeMillis());
        tokenMapper.updateByPrimaryKeySelective(token);

        return AdminSimplyReply.newBuilder().setResult(true).build();
    }

    public AdminSimplyReply editTokenExtraTags(long orgId, String tokenId, Map<String, Integer> tagMap) {
        Token token = getToken(orgId, tokenId);
        if (token == null) {
            return AdminSimplyReply.newBuilder().setResult(true).build();
        }
        String newTagJson = ExtraTagUtil.newInstance(token.getExtraTag()).putAll(tagMap).jsonStr();
        token.setExtraTag(newTagJson);
        token.setUpdated(System.currentTimeMillis());
        tokenMapper.updateByPrimaryKeySelective(token);
        return AdminSimplyReply.newBuilder().setResult(true).build();
    }

    public AdminSimplyReply editTokenExtraConfigs(long orgId, String tokenId, Map<String, String> configMap) {
        Token token = getToken(orgId, tokenId);
        if (token == null) {
            return AdminSimplyReply.newBuilder().setResult(true).build();
        }
        String newTagJson = ExtraConfigUtil.newInstance(token.getExtraConfig()).putAll(configMap).jsonStr();
        token.setExtraConfig(newTagJson);
        token.setUpdated(System.currentTimeMillis());
        tokenMapper.updateByPrimaryKeySelective(token);
        return AdminSimplyReply.newBuilder().setResult(true).build();
    }


    public AdminSimplyReply setWithdrawMinQuantity(long orgId, String tokenId, String withdrawMinQuantity) {
        Token token = getToken(orgId, tokenId);
        if (token == null) {
            return AdminSimplyReply.newBuilder().setResult(true).build();
        }
        GetTokenRequest request = GetTokenRequest.newBuilder()
                .setBaseRequest(BaseRequest.newBuilder().setOrganizationId(orgId).build())
                .setTokenId(tokenId)
                .build();
        BigDecimal minQuantity = new BigDecimal(withdrawMinQuantity);
        io.bhex.base.token.TokenDetail tokenDetail = grpcTokenService.getToken(request);
        if (DecimalUtil.toBigDecimal(tokenDetail.getWithdrawMinQuantity()).compareTo(minQuantity) < 0) {
            return AdminSimplyReply.newBuilder().setResult(false).setMessage("value.less.than.platform").build();
        }
        token.setMinWithdrawQuantity(minQuantity);
        token.setUpdated(System.currentTimeMillis());
        tokenMapper.updateByPrimaryKeySelective(token);
        return AdminSimplyReply.newBuilder().setResult(true).build();
    }

    public AdminSimplyReply deleteToken(long orgId, String tokenId) {
        Token token = getToken(orgId, tokenId);
        if (token == null) {
            return AdminSimplyReply.newBuilder().setResult(true).build();
        }
        List<Symbol> symbols = symbolMapper.queryOrgSymbols(orgId, Arrays.asList(TokenCategory.MAIN_CATEGORY_VALUE));
        if (CollectionUtils.isEmpty(symbols)) {
            symbols = Lists.newArrayList();
        }
        for (Symbol symbol : symbols) {
            if (symbol.getBaseTokenId().equals(tokenId) || symbol.getQuoteTokenId().equals(tokenId)) {
                if (symbol.getStatus() == 1) {
                    return AdminSimplyReply.newBuilder().setResult(false).setMessage(symbol.getSymbolId() + " online").build();
                }
            }
        }
        token.setStatus(0);
        token.setUpdated(System.currentTimeMillis());
        tokenMapper.updateByPrimaryKeySelective(token);
        return AdminSimplyReply.newBuilder().setResult(true).build();
    }
}
