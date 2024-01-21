package io.bhex.broker.server.grpc.server.service;

import com.google.gson.Gson;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.ibatis.session.RowBounds;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;

import javax.annotation.Resource;

import io.bhex.base.account.CreateNewOptionReply;
import io.bhex.base.account.CreateNewOptionReq;
import io.bhex.base.proto.DecimalUtil;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.grpc.admin.CreateOptionReply;
import io.bhex.broker.grpc.admin.CreateOptionRequest;
import io.bhex.broker.grpc.admin.QueryOptionListResponse;
import io.bhex.broker.grpc.order.GetOptionNameResponse;
import io.bhex.broker.server.grpc.client.service.GrpcOptionOrderService;
import io.bhex.broker.server.model.Internationalization;
import io.bhex.broker.server.model.OptionInfo;
import io.bhex.broker.server.model.Symbol;
import io.bhex.broker.server.model.Token;
import io.bhex.broker.server.model.TokenOption;
import io.bhex.broker.server.primary.mapper.InternationalizationMapper;
import io.bhex.broker.server.primary.mapper.OptionInfoMapper;
import io.bhex.broker.server.primary.mapper.SymbolMapper;
import io.bhex.broker.server.primary.mapper.TokenMapper;
import io.bhex.broker.server.primary.mapper.TokenOptionMapper;
import lombok.extern.slf4j.Slf4j;
import tk.mybatis.mapper.entity.Example;

/**
 * @Author: yuehao  <hao.yue@bhex.com>
 * @CreateDate: 2019-03-08 10:09
 * @Copyright（C）: 2018 BHEX Inc. All rights reserved.
 */

@Service
@Slf4j
public class OptionService {

    @Resource
    private TokenMapper tokenMapper;

    @Autowired
    private TokenOptionMapper tokenOptionMapper;

    @Autowired
    private SymbolMapper symbolMapper;

    @Resource
    private InternationalizationMapper internationalizationMapper;

    @Autowired
    private GrpcOptionOrderService grpcOptionOrderService;

    @Resource
    private OptionInfoMapper optionInfoMapper;

    private static final String SUCCESS = "Creating Options Successfully";

    private static final String FAIL = "Failure to create options";

    // create tb_token tb_token_option tb_symbol tb_quote_token

    @Transactional(isolation = Isolation.READ_COMMITTED, propagation = Propagation.REQUIRED, rollbackFor = Throwable.class)
    public CreateOptionReply createOrModifyNewOption(CreateOptionRequest request) {
        try {
            OptionInfo optionInfo = createOptionInfo(request);
            Example example = new Example(OptionInfo.class);
            Example.Criteria criteria = example.createCriteria();
            criteria.andEqualTo("orgId", optionInfo.getOrgId());
            criteria.andEqualTo("tokenId", optionInfo.getTokenId());
            OptionInfo selectOne = this.optionInfoMapper.selectOneByExample(example);
            CreateNewOptionReply createNewOptionReply = this.grpcOptionOrderService.createNewOption(structureReq(optionInfo));
            if (createNewOptionReply.getOk()) {
                createInternationalization(optionInfo);
                createTokenOption(optionInfo);
                createToken(optionInfo);
                createSymbol(optionInfo);
            }
            log.info("createOrModifyNewOption optionInfo {}  selectOne {}", new Gson().toJson(optionInfo), new Gson().toJson(selectOne));
            if (selectOne == null) {
                this.optionInfoMapper.insert(optionInfo);
            } else {
                this.optionInfoMapper.updateByPrimaryKey(optionInfo);
            }
        } catch (Exception ex) {
            log.info("createOrModifyNewOption error {}", ex);
            throw new BrokerException(BrokerErrorCode.SYSTEM_ERROR);
        }
        return CreateOptionReply.getDefaultInstance();
    }

    public QueryOptionListResponse queryOptionList(Long orgId, Long fromId, Long limit) {
        Example example = Example.builder(OptionInfo.class).orderByDesc("id").build();
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("orgId", orgId);
        if (Objects.nonNull(fromId) && fromId > 0) {
            criteria.andLessThan("id", fromId);
        }
        if (Objects.isNull(limit) || limit > 100) {
            limit = 100L;
        }

        List<OptionInfo> optionInfos
                = optionInfoMapper.selectByExampleAndRowBounds(example, new RowBounds(0, limit.intValue()));
        if (CollectionUtils.isEmpty(optionInfos)) {
            return QueryOptionListResponse.getDefaultInstance();
        }

        List<QueryOptionListResponse.OptionInfo> optionInfoList = new ArrayList<>();
        optionInfos.forEach(info -> {
            optionInfoList.add(newOptionInfo(info));
        });
        if (CollectionUtils.isEmpty(optionInfoList)) {
            return QueryOptionListResponse.getDefaultInstance();
        }
        return QueryOptionListResponse.newBuilder().addAllInfo(optionInfoList).build();
    }

    @Transactional(isolation = Isolation.READ_COMMITTED, propagation = Propagation.REQUIRED, rollbackFor = Throwable.class)
    public String createOption(OptionInfo optionInfo) {
        CreateNewOptionReply createNewOptionReply = this.grpcOptionOrderService.createNewOption(structureReq(optionInfo));
        if (createNewOptionReply.getOk()) {
            createInternationalization(optionInfo);
            createTokenOption(optionInfo);
            createToken(optionInfo);
            createSymbol(optionInfo);
        }
        return SUCCESS;
    }

    @Transactional(rollbackFor = Exception.class)
    public String createBhopOption(OptionInfo optionInfo) {
        createInternationalization(optionInfo);
        createTokenOption(optionInfo);
        createToken(optionInfo);
        createSymbol(optionInfo);
        return SUCCESS;
    }

    /**
     * 构造req
     *
     * @param optionInfo optionInfo
     * @return req
     */
    private CreateNewOptionReq structureReq(OptionInfo optionInfo) {
        log.info("structureReq info {}", new Gson().toJson(optionInfo));
        return CreateNewOptionReq
                .newBuilder()
                .setTokenId(optionInfo.getTokenId())
                .setTokenName(optionInfo.getTokenName())
                .setStrikePrice(DecimalUtil.fromBigDecimal(optionInfo.getStrikePrice()))
                .setIssueDate(optionInfo.getIssueDate())
                .setSettlementDate(optionInfo.getSettlementDate())
                .setIsCall(optionInfo.getIsCall())
                .setMaxPayOff(DecimalUtil.fromBigDecimal(optionInfo.getMaxPayOff()))
                .setPositionLimit(DecimalUtil.fromBigDecimal(new BigDecimal(optionInfo.getPositionLimit())))
                .setCoinToken("USDT")
                .setIndexToken(optionInfo.getIndexToken())
                .setMinTradeAmount(DecimalUtil.fromBigDecimal(optionInfo.getMinTradeAmount()))
                .setMinPricePrecision(DecimalUtil.fromBigDecimal(optionInfo.getMinPricePrecision()))
                .setMinTradeQuantity(DecimalUtil.fromBigDecimal(optionInfo.getMinTradeQuantity()))
                .setDigitMergeList(optionInfo.getDigitMergeList())
                .setBasePrecision(DecimalUtil.fromBigDecimal(optionInfo.getBasePrecision()))
                .setQuotePrecision(DecimalUtil.fromBigDecimal(optionInfo.getQuotePrecision()))
                .setCategory(optionInfo.getCategory())
                .setType(optionInfo.getType())
                .setMinPrecision(optionInfo.getMinPrecision().intValue())
                .setBrokerId(optionInfo.getOrgId())
                .setExchangeId(optionInfo.getExchangeId())
                .setTakerFeeRate(DecimalUtil.fromBigDecimal(optionInfo.getTakerFeeRate()))
                .setMakerFeeRate(DecimalUtil.fromBigDecimal(optionInfo.getMakerFeeRate()))
                .setUnderlyingId(optionInfo.getUnderlyingId())
                .build();
    }

    /**
     * 国际化期权
     *
     * @param optionInfo optionInfo
     */
    private void createInternationalization(OptionInfo optionInfo) {
        Internationalization internationalization = new Internationalization();
        internationalization.setInKey(optionInfo.getTokenId());
        internationalization.setInValue(optionInfo.getTokenId());
        internationalization.setEnv("en");
        internationalization.setInStatus(0);
        internationalization.setType(0);
        internationalization.setCreateAt(new Timestamp(new Date().getTime()));

        Internationalization internationalizationByKeyEN = internationalizationMapper
                .queryInternationalizationByKey(optionInfo.getTokenId(), "en");
        if (internationalizationByKeyEN != null) {
            internationalization.setId(internationalizationByKeyEN.getId());
            internationalizationMapper.updateByPrimaryKeySelective(internationalization);
        } else {
            internationalizationMapper.insertSelective(internationalization);
        }

        Internationalization internationalizationByKeyZH = internationalizationMapper
                .queryInternationalizationByKey(optionInfo.getTokenId(), "zh");

        Internationalization internationalizationZH = new Internationalization();
        internationalizationZH.setInKey(optionInfo.getTokenId());
        internationalizationZH.setInValue(optionInfo.getTokenName());
        internationalizationZH.setInStatus(0);
        internationalizationZH.setType(0);
        internationalizationZH.setCreateAt(new Timestamp(new Date().getTime()));
        internationalizationZH.setEnv("zh");
        if (internationalizationByKeyZH != null) {
            internationalization.setId(internationalizationByKeyZH.getId());
            internationalizationMapper.updateByPrimaryKeySelective(internationalizationZH);
        } else {
            internationalizationMapper.insertSelective(internationalizationZH);
        }
    }

    /**
     * 初始化tokenOption表
     *
     * @param optionInfo optionInfo
     */
    private void createTokenOption(OptionInfo optionInfo) {
        TokenOption tokenOption = new TokenOption();
        tokenOption.setTokenId(optionInfo.getTokenId());
        tokenOption.setIsCall(optionInfo.getIsCall());
        tokenOption.setCoinToken(optionInfo.getCoinToken());
        tokenOption.setMaxPayOff(optionInfo.getMaxPayOff());
        tokenOption.setPositionLimit(new BigDecimal(optionInfo.getPositionLimit()));
        tokenOption.setStrikePrice(optionInfo.getStrikePrice());
        tokenOption.setIndexToken(optionInfo.getIndexToken());
        tokenOption.setIssueDate(new Timestamp(optionInfo.getIssueDate()));
        tokenOption.setCreatedAt(new Timestamp(new Date().getTime()));
        tokenOption.setSettlementDate(new Timestamp(optionInfo.getSettlementDate()));
        tokenOption.setUpdatedAt(new Timestamp(new Date().getTime()));

        Long id = tokenOptionMapper.selectIdFromTokenOptionByTokenId(optionInfo.getTokenId());
        if (id != null && id > 0) {
            tokenOption.setId(id);
            tokenOptionMapper.updateByPrimaryKeySelective(tokenOption);
        } else {
            tokenOptionMapper.insertSelective(tokenOption);
        }
    }

    /**
     * 初始化token表
     *
     * @param optionInfo optionInfo
     */
    private void createToken(OptionInfo optionInfo) {
        Token token = new Token();
        token.setOrgId(optionInfo.getOrgId());
        token.setExchangeId(optionInfo.getExchangeId());
        token.setTokenId(optionInfo.getTokenId());
        token.setTokenName(optionInfo.getTokenId());
        token.setTokenFullName(optionInfo.getTokenId());
        token.setCategory(optionInfo.getCategory());
        token.setTokenIcon("");
        token.setMaxWithdrawQuota(BigDecimal.ZERO);
        token.setMinWithdrawQuantity(BigDecimal.ZERO);
        token.setMaxWithdrawQuantity(BigDecimal.ZERO);
        token.setNeedKycQuantity(BigDecimal.ZERO);
        token.setFeeTokenId(optionInfo.getTokenId());
        token.setFeeTokenName(optionInfo.getTokenId());
        token.setFee(BigDecimal.ZERO);
        token.setAllowDeposit(1);
        token.setAllowWithdraw(1);
        token.setStatus(1);
        token.setCreated(new Date().getTime());
        token.setUpdated(new Date().getTime());

        Long id = tokenMapper.getIdByTokenId(optionInfo.getTokenId(), optionInfo.getOrgId());
        if (id != null && id > 0) {
            token.setId(id);
            tokenMapper.updateByPrimaryKeySelective(token);
        } else {
            tokenMapper.insertSelective(token);
        }
    }

    /**
     * 初始化symbol
     *
     * @param optionInfo optionInfo
     */
    private void createSymbol(OptionInfo optionInfo) {
        Symbol symbol = new Symbol();
        symbol.setOrgId(optionInfo.getOrgId());
        symbol.setExchangeId(optionInfo.getExchangeId());
        symbol.setSymbolId(optionInfo.getTokenId());
        symbol.setSymbolName(optionInfo.getTokenId());
        symbol.setBaseTokenId(optionInfo.getTokenId());
        symbol.setBaseTokenName(optionInfo.getTokenId());
        symbol.setQuoteTokenId(optionInfo.getCoinToken());
        symbol.setQuoteTokenName(optionInfo.getCoinToken());
        symbol.setCategory(optionInfo.getCategory());
        symbol.setAllowTrade(1);
        symbol.setStatus(1);
        symbol.setCustomOrder(0);
        symbol.setIndexShow(0);
        symbol.setIndexShowOrder(0);
        symbol.setNeedPreviewCheck(0);
        symbol.setOpenTime(0L);
        symbol.setCreated(new Date().getTime());
        symbol.setUpdated(new Date().getTime());
        symbol.setBanSellStatus(0);

        Long id = symbolMapper.getIdBySymbolId(optionInfo.getTokenId(), optionInfo.getOrgId());
        if (id != null && id > 0) {
            symbol.setId(id);
            symbolMapper.updateByPrimaryKeySelective(symbol);
        } else {
            symbolMapper.insertSelective(symbol);
        }
    }

    public GetOptionNameResponse getOptionName(String inKey, String env) {
        Internationalization internationalization
                = internationalizationMapper.queryInternationalizationByKey(inKey, env);

        if (internationalization == null) {
            if ("en".equals(env)) {
                return GetOptionNameResponse.newBuilder().setEnv(env).setInKey(inKey).setInValue(inKey).build();
            } else {
                Internationalization i18nEN = internationalizationMapper.queryInternationalizationByKey(inKey, "en");
                if (i18nEN != null) {
                    return GetOptionNameResponse.newBuilder().setEnv(env).setInKey(inKey).setInValue(i18nEN.getInValue()).build();
                } else {
                    return GetOptionNameResponse.newBuilder().setEnv(env).setInKey(inKey).setInValue(inKey).build();
                }
            }
        }
        return GetOptionNameResponse
                .newBuilder()
                .setInValue(internationalization.getInValue()).setEnv(env).setInKey(inKey).build();
    }

    private OptionInfo createOptionInfo(CreateOptionRequest request) {
        return OptionInfo
                .builder()
                .id(request.getId())
                .tokenId(request.getTokenId())
                .tokenName(request.getTokenName())
                .strikePrice(new BigDecimal(request.getStrikePrice()))
                .issueDate(request.getIssueDate())
                .settlementDate(request.getSettlementDate())
                .isCall(request.getIsCall())
                .maxPayOff(new BigDecimal(request.getMaxPayOff()))
                .positionLimit(Integer.parseInt(request.getPositionLimit()))
                .coinToken("USDT")
                .indexToken(request.getIndexToken())
                .minTradeAmount(new BigDecimal(request.getMinTradeAmount()))
                .minPricePrecision(new BigDecimal(request.getMinPricePrecision()))
                .minTradeQuantity(new BigDecimal(request.getMinTradeQuantity()))
                .digitMergeList(request.getDigitMergeList())
                .basePrecision(new BigDecimal(request.getBasePrecision()))
                .quotePrecision(new BigDecimal(request.getQuotePrecision()))
                .category(3)
                .type(8)
                .status(0)
                .minPrecision(new BigDecimal(8))
                .orgId(request.getBrokerId())
                .exchangeId(request.getExchangeId())
                .takerFeeRate(new BigDecimal(request.getTakerFeeRate()))
                .makerFeeRate(new BigDecimal(request.getMakerFeeRate()))
                .underlyingId(request.getUnderlyingId())
                .created(new Date())
                .updated(new Date())
                .build();
    }


    private QueryOptionListResponse.OptionInfo newOptionInfo(OptionInfo optionInfo) {
        log.info("structureReq info {}", new Gson().toJson(optionInfo));
        return QueryOptionListResponse.OptionInfo
                .newBuilder()
                .setId(optionInfo != null && optionInfo.getId() > 0 ? optionInfo.getId() : 0)
                .setTokenId(optionInfo.getTokenId())
                .setTokenName(optionInfo.getTokenName())
                .setStrikePrice(optionInfo.getStrikePrice().setScale(8, RoundingMode.DOWN).toPlainString())
                .setIssueDate(optionInfo.getIssueDate())
                .setSettlementDate(optionInfo.getSettlementDate())
                .setIsCall(optionInfo.getIsCall())
                .setMaxPayOff(optionInfo.getMaxPayOff().setScale(8, RoundingMode.DOWN).toPlainString())
                .setPositionLimit(new BigDecimal(optionInfo.getPositionLimit()).setScale(8, RoundingMode.DOWN).toPlainString())
                .setCoinToken(optionInfo.getCoinToken())
                .setIndexToken(optionInfo.getIndexToken())
                .setMinTradeAmount(optionInfo.getMinTradeAmount().setScale(8, RoundingMode.DOWN).toPlainString())
                .setMinPricePrecision(optionInfo.getMinPricePrecision().setScale(8, RoundingMode.DOWN).toPlainString())
                .setMinTradeQuantity(optionInfo.getMinTradeQuantity().setScale(8, RoundingMode.DOWN).toPlainString())
                .setDigitMergeList(optionInfo.getDigitMergeList())
                .setBasePrecision(optionInfo.getBasePrecision().setScale(8, RoundingMode.DOWN).toPlainString())
                .setQuotePrecision(optionInfo.getQuotePrecision().setScale(8, RoundingMode.DOWN).toPlainString())
                .setCategory(optionInfo.getCategory())
                .setType(optionInfo.getType())
                .setMinPrecision(optionInfo.getMinPrecision().intValue())
                .setBrokerId(optionInfo.getOrgId())
                .setExchangeId(optionInfo.getExchangeId())
                .setTakerFeeRate(optionInfo.getTakerFeeRate().setScale(8, RoundingMode.DOWN).toPlainString())
                .setMakerFeeRate(optionInfo.getMakerFeeRate().setScale(8, RoundingMode.DOWN).toPlainString())
                .setUnderlyingId(optionInfo.getUnderlyingId())
                .build();
    }
}
