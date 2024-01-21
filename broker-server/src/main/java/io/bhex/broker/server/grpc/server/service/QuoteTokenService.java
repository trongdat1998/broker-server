package io.bhex.broker.server.grpc.server.service;

import io.bhex.broker.server.model.Token;
import io.bhex.broker.server.primary.mapper.QuoteTokenMapper;
import io.bhex.broker.server.primary.mapper.TokenMapper;
import io.bhex.broker.server.model.QuoteToken;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;
import tk.mybatis.mapper.entity.Example;

import javax.annotation.Resource;
import java.util.List;

/**
 * @Description:
 * @Date: 2019/3/14 下午5:17
 * @Author: liwei
 * @Copyright（C）: 2018 BlueHelix Inc. All rights reserved.
 */
@Slf4j
@Service
public class QuoteTokenService {

    @Resource
    private QuoteTokenMapper quoteTokenMapper;
    @Resource
    private TokenMapper tokenMapper;

    public List<QuoteToken> handleQuoteToken(long orgId, String token, String tokenIcon, List<String> orders) {
        Example example = Example.builder(QuoteToken.class).build();
        example.createCriteria()
                .andEqualTo("orgId", orgId)
                .andEqualTo("tokenId", token);
        List<QuoteToken> quoteTokens = quoteTokenMapper.selectByExample(example);
        if (CollectionUtils.isEmpty(quoteTokens)) {
            QuoteToken quoteToken = QuoteToken.builder()
                    .orgId(orgId).tokenId(token).tokenName(token).tokenIcon(tokenIcon)
                    .customOrder(0).status(1).created(System.currentTimeMillis())
                    .updated(System.currentTimeMillis()).category(1).build();
            quoteTokenMapper.insertSelective(quoteToken);
        }

        if (!CollectionUtils.isEmpty(orders)) {
            for (int i = 0; i < orders.size(); i++) {
                quoteTokenMapper.updateCustomOrder(orgId, orders.get(i),  i + 1);
            }
        }

        Example example2 = Example.builder(QuoteToken.class).build();
        example2.createCriteria()
                .andEqualTo("orgId", orgId);
        return quoteTokenMapper.selectByExample(example2);
    }

    @Transactional
    public boolean editQuoteTokens(long orgId, List<String> tokens) {
        int deletedNum = quoteTokenMapper.deleteQuoteTokens(orgId);
        int size = tokens.size();
        for (int i = 0; i < size; i++) {
            String tokenId = tokens.get(i);
            Token token = tokenMapper.getToken(orgId, tokenId);
            QuoteToken quoteToken = QuoteToken.builder()
                    .orgId(orgId)
                    .tokenId(tokenId).tokenName(token.getTokenName()).tokenIcon(token.getTokenIcon())
                    .customOrder(i + 1)
                    .status(1)
                    .created(System.currentTimeMillis())
                    .updated(System.currentTimeMillis())
                    .category(1)
                    .build();
            quoteTokenMapper.insertSelective(quoteToken);
        }
        return true;
    }

}
