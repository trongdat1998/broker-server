package io.bhex.broker.server.grpc.server.service;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.alibaba.fastjson.JSON;
import com.github.pagehelper.PageHelper;
import com.github.pagehelper.PageInfo;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.joda.time.DateTime;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Resource;

import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.grpc.activity.contract.competition.AddParticipantRequest;
import io.bhex.broker.grpc.activity.contract.competition.CompetitionInfo;
import io.bhex.broker.grpc.activity.contract.competition.ContractCompetitionAbbr;
import io.bhex.broker.grpc.activity.contract.competition.ListContractCompetitionResponse;
import io.bhex.broker.grpc.activity.contract.competition.ListParticipantResponse;
import io.bhex.broker.grpc.activity.contract.competition.Participant;
import io.bhex.broker.grpc.activity.contract.competition.RankType;
import io.bhex.broker.grpc.activity.contract.competition.RankingListResponse;
import io.bhex.broker.grpc.order.GetFuturesI18nNameRequest;
import io.bhex.broker.grpc.order.GetFuturesI18nNameResponse;
import io.bhex.broker.server.model.Broker;
import io.bhex.broker.server.model.Symbol;
import io.bhex.broker.server.model.TradeCompetition;
import io.bhex.broker.server.model.TradeCompetitionExt;
import io.bhex.broker.server.model.TradeCompetitionLimit;
import io.bhex.broker.server.model.TradeCompetitionParticipant;
import io.bhex.broker.server.model.TradeCompetitionResult;
import io.bhex.broker.server.model.User;
import io.bhex.broker.server.primary.mapper.SymbolMapper;
import io.bhex.broker.server.primary.mapper.TradeCompetitionExtMapper;
import io.bhex.broker.server.primary.mapper.TradeCompetitionLimitMapper;
import io.bhex.broker.server.primary.mapper.TradeCompetitionMapper;
import io.bhex.broker.server.primary.mapper.TradeCompetitionParticipantMapper;
import io.bhex.broker.server.primary.mapper.TradeCompetitionResultDailyMapper;
import io.bhex.broker.server.primary.mapper.UserMapper;
import io.bhex.broker.server.util.CommonUtil;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import tk.mybatis.mapper.entity.Example;

@Slf4j
@Service
public class AdminContractCompetitionService {

    @Resource
    private TradeCompetitionMapper tradeCompetitionMapper;

    @Resource
    private TradeCompetitionLimitMapper tradeCompetitionLimitMapper;

    @Resource
    private TradeCompetitionExtMapper tradeCompetitionExtMapper;

    @Resource
    private TradeCompetitionParticipantMapper tradeCompetitionParticipantMapper;

    @Resource
    private SymbolMapper symbolMapper;

    @Resource
    private UserMapper userMapper;

    @Resource
    private TradeCompetitionService tradeCompetitionService;

    @Resource
    private FuturesOrderService futuresOrderService;

    @Resource
    private TradeCompetitionResultDailyMapper tradeCompetitionResultDailyMapper;

    @Resource
    private ShortUrlService shortUrlService;

    @Resource
    private BrokerService brokerService;

    private final static String PC_PATH = "/topics/trade_activity/";
    private final static String H5_PATH = "/m/topics/trade_activity/";
    private final static String WWW_PROTOCOL = "https://www";

    private final static String SHORT_PATH = "/j/";

    public ListContractCompetitionResponse.Page listWithPage(long orgId, String language, int pageNo, int pageSize) {

        PageHelper.startPage(pageNo, pageSize);
        Example exp = new Example(TradeCompetition.class);
        exp.orderBy("createTime").desc();
        exp.createCriteria()
                .andEqualTo("orgId", orgId)
                .andIn("status", Lists.newArrayList(0, 1, 2));
        List<TradeCompetition> list = tradeCompetitionMapper.selectByExample(exp);
        PageInfo page = new PageInfo(list);

        List<ContractCompetitionAbbr> tmp_list = list.stream().map(i -> {

            GetFuturesI18nNameRequest req = GetFuturesI18nNameRequest.newBuilder()
                    .setEnv(language)
                    .setInKey(i.getSymbolId())
                    .build();

            GetFuturesI18nNameResponse resp = futuresOrderService.getFuturesI18nName(req);

            return ContractCompetitionAbbr.newBuilder()
                    .setCode(i.getCompetitionCode())
                    .setContractId(i.getSymbolId())
                    .setContractName(resp.getInValue())
                    .setId(i.getId())
                    .setTokenId(getContractToken(i.getOrgId(), i.getSymbolId()))
                    .setStatus(ContractCompetitionAbbr.CompetitionStatus.forNumber(i.getStatus()))
                    //.setDuration(formatDate(i.getStartTime())+"-"+formatDate(i.getEndTime()))
                    .setDuration(i.getStartTime().getTime() + "-" + i.getEndTime().getTime())
                    .build();
        }).collect(Collectors.toList());


        return ListContractCompetitionResponse.Page.newBuilder()
                .addAllList(tmp_list)
                .setTotal((int) page.getTotal())
                .build();
    }


    public String getContractToken(Long orgId, String symbolId) {
        //获取到期货的计价tokenId
        Symbol symbol = this.symbolMapper.getOrgSymbol(orgId, symbolId);
        if (Objects.isNull(symbol)) {
            return "";
        }

        return symbol.getQuoteTokenId();
    }

    public String formatDate(Date date) {
        ZonedDateTime zdt = date.toInstant().atZone(ZoneId.of("Asia/Shanghai"));
        return zdt.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm"));

    }

    @Transactional(rollbackFor = Exception.class)
    public boolean save(long orgId, CompetitionInfo competition, String domain) {
        long activityId = competition.getActivityId();
        String contractId = competition.getContractId();
        List<Integer> rankTypes = competition.getRankTypesValueList();

        Date begin = stringToDate(competition.getBegin());
        Date end = stringToDate(competition.getEnd());

        TradeCompetition exist = null;
        if (activityId > 0L) {
            exist = findTradeCompetition(orgId, activityId);
            if (Objects.isNull(exist)) {
                throw new BrokerException(BrokerErrorCode.DB_RECORD_NOT_EXISTS);
            }
        }

        //保存合约交易大赛信息
        TradeCompetition tc = null;
        String code = getCode(orgId, contractId);

        //检查是否存在相同的合约大赛
        Example exp = new Example(TradeCompetition.class);
        Example.Criteria criteria = exp.createCriteria()
                .andEqualTo("orgId", orgId)
                .andEqualTo("symbolId", competition.getContractId())
                .andIn("status", Lists.newArrayList(0, 1));
        if (activityId > 0) {
            criteria.andNotEqualTo("id", activityId);
        }

        List<TradeCompetition> list = tradeCompetitionMapper.selectByExample(exp);
        log.info("saveCompetition,param={}", CommonUtil.formatMessage(competition));
        //相同编码活动，活动时间重叠则不能设置
        if (CollectionUtils.isNotEmpty(list)) {
            for (TradeCompetition tmp_tc : list) {

                log.info("saveCompetition exist competition={}", JSON.toJSONString(tmp_tc));
                Date beginExist = tmp_tc.getStartTime();
                Date endExist = tmp_tc.getEndTime();
                if (begin.before(beginExist) && end.after(beginExist)) {
                    throw new BrokerException(BrokerErrorCode.DB_RECORD_NOT_UNIQUE);
                }

                if (begin.before(endExist) && end.after(endExist)) {
                    throw new BrokerException(BrokerErrorCode.DB_RECORD_NOT_UNIQUE);
                }

                if (begin.before(beginExist) && end.after(endExist)) {
                    throw new BrokerException(BrokerErrorCode.DB_RECORD_NOT_UNIQUE);
                }

                if (begin.after(beginExist) && end.before(endExist)) {
                    throw new BrokerException(BrokerErrorCode.DB_RECORD_NOT_UNIQUE);
                }
            }
        }

        TradeCompetition.Builder tc_builder = TradeCompetition.builder()
                .orgId(orgId)
                .effectiveQuantity(competition.getRankingNumber())
                .symbolId(contractId)
                .startTime(begin)
                .endTime(end)
                .rankTypes(Joiner.on(",").join(rankTypes))
                .receiveTokenAmount(new BigDecimal("100000000"))
                .receiveTokenId("BHT")
                .receiveTokenName("BHT")
                .updateTime(new Date())
                .isReverse(competition.getIsReverse());
        boolean success = true;
        boolean modifiableBaseInfo = false;
        if (exist != null && exist.getStatus() < TradeCompetition.CompetitionStatus.PROCESSING.getValue()) {
            modifiableBaseInfo = true;
        }

        if (activityId > 0L) {
            //基本信息只能在开始前修改
            tc_builder.id(activityId);
            tc = tc_builder.build();
            if (modifiableBaseInfo) {
                code = exist.getCompetitionCode();

                tc_builder.id(activityId);
                success = tradeCompetitionMapper.updateByPrimaryKeySelective(tc) == 1;
            }
        } else {
            tc_builder.competitionCode(code)
                    .type(1)
                    .createTime(new Date())
                    .status(0);
            tc = tc_builder.build();
            success = tradeCompetitionMapper.insertSelective(tc) == 1;
        }

        if (!success) {
            log.error("Save competiton fail,orgId={},contractId={}", orgId, competition.getContractId());
            throw new BrokerException(BrokerErrorCode.DB_ERROR);
        }

        activityId = tc.getId();

        TradeCompetitionLimit limitExist = findTradeCompetitionLimit(orgId, activityId);
        long limitId = 0;
        if (Objects.nonNull(limitExist)) {
            limitId = limitExist.getId();
        }

        //保存参赛资格信息
        CompetitionInfo.Qualify qualify = competition.getQualify();

        Map<String, String> positionMap = Maps.newHashMap();
        positionMap.put(TradeCompetitionService.FUTURES_POSITION_QUANTITY, qualify.getPosition());

        TradeCompetitionLimit.Builder limit_builder = TradeCompetitionLimit.builder()
                .orgId(orgId)
                .limitStr(JSON.toJSONString(positionMap))
                .limitStatus(1);

        if (limitId == 0) {

            limit_builder
                    .enterIdStr("")
                    .enterStatus(0)
                    .enterWhiteStr("")
                    .enterWhiteStatus(0)
                    .competitionCode(code)
                    .competitionId(activityId);
            ;

            success = tradeCompetitionLimitMapper.insertSelective(limit_builder.build()) == 1;
        } else {
            if (modifiableBaseInfo) {
                limit_builder.id(limitId);
                success = tradeCompetitionLimitMapper.updateByPrimaryKeySelective(limit_builder.build()) == 1;
            }
        }

        if (!success) {
            log.error("Save competiton limit fail,orgId={},contractId={}", orgId, competition.getContractId());
            throw new BrokerException(BrokerErrorCode.DB_ERROR);
        }

        //保存多语言配置信息
        final long competitionId = activityId;
        final String tmpCode = code;
        List<CompetitionInfo.Extend> extend_list = competition.getExtendsList();
        Map<String, TradeCompetitionExt> extendMap = extend_list.stream().map(i -> {
            return TradeCompetitionExt.builder()
                    .bannerUrl(i.getPcBanner())
                    .mobileBannerUrl(i.getAppBanner())
                    .description(i.getDescription())
                    .orgId(orgId)
                    .competitionCode(tmpCode)
                    .competitionId(competitionId)
                    .language(i.getLanguage())
                    .name("")
                    .build();
        }).collect(Collectors.toMap(i -> i.getLanguage(), i -> i));

        List<TradeCompetitionExt> existList = tradeCompetitionExtMapper.queryTradeCompetitionExtByCode(orgId, code);
        Map<String, TradeCompetitionExt> existMap = existList.stream()
                .collect(Collectors.toMap(i -> i.getLanguage(), i -> i));

        Set<String> addLanguage = Sets.difference(extendMap.keySet(), existMap.keySet());
        Set<String> updateLanguage = Sets.intersection(extendMap.keySet(), existMap.keySet());
        Set<String> deleteLanguage = Sets.difference(existMap.keySet(), extendMap.keySet());

        if (CollectionUtils.isNotEmpty(deleteLanguage)) {
            Set<Long> deleteIds = existList.stream().filter(i -> deleteLanguage.contains(i.getLanguage()))
                    .map(i -> i.getId()).collect(Collectors.toSet());

            String ids = Joiner.on(",").join(deleteIds);
            tradeCompetitionExtMapper.deleteByIds(ids);
        }

        if (CollectionUtils.isNotEmpty(updateLanguage)) {
            List<TradeCompetitionExt> updateList = existList.stream().filter(i -> updateLanguage.contains(i.getLanguage()))
                    .collect(Collectors.toList());
            updateList.forEach(item -> {
                TradeCompetitionExt ext = extendMap.get(item.getLanguage());
                if (Objects.isNull(ext)) {
                    return;
                }

                item.setBannerUrl(ext.getBannerUrl());
                item.setDescription(ext.getDescription());
                item.setMobileBannerUrl(ext.getMobileBannerUrl());
                tradeCompetitionExtMapper.updateByPrimaryKeySelective(item);
            });
        }

        if (CollectionUtils.isNotEmpty(addLanguage)) {
            List<TradeCompetitionExt> addList = extendMap.values().stream().filter(i -> addLanguage.contains(i.getLanguage()))
                    .collect(Collectors.toList());

            success = tradeCompetitionExtMapper.insertList(addList) > 0;

            if (!success) {
                log.error("Save competiton ext fail,orgId={},contractId={}", orgId, competition.getContractId());
                throw new BrokerException(BrokerErrorCode.DB_ERROR);
            }
        }


        domain = getDomain(orgId);
        this.saveShortUrl(domain + PC_PATH + code);
        this.saveShortUrl(domain + H5_PATH + code);

        return true;
    }

    //创建短地址
    private void saveShortUrl(String url) {

        if (StringUtils.isBlank(url)) {
            return;
        }

        log.info("create short url,{}", url);

        String exist = shortUrlService.findShortUrl(url);
        //已经存在
        if (StringUtils.isNoneBlank(exist)) {
            return;
        }

        shortUrlService.createShortUrl(url);
    }

    public TradeCompetition findTradeCompetition(long orgId, long activityId) {
        TradeCompetition exist;
        TradeCompetition sample = new TradeCompetition();
        sample.setId(activityId);
        sample.setOrgId(orgId);

        exist = tradeCompetitionMapper.selectOne(sample);
        return exist;
    }

    private Date stringToDate(String str) {

        return new Date(Long.parseLong(str));
/*        LocalDateTime ldt=LocalDateTime.parse(str,DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        ZoneId zoneId = ZoneId.systemDefault();
        ZonedDateTime zdt = ldt.atZone(zoneId);
        return  Date.from(zdt.toInstant());*/

    }

    public String getCode(long orgId, String contractId) {
        return Joiner.on("-").join(contractId, orgId, new DateTime().toString("yyyyMMddHHmmss"));
    }


    public CompetitionInfo getDetail(long orgId, String ext) {
        Long id = Long.parseLong(ext);
        TradeCompetition competition = tradeCompetitionMapper.selectByPrimaryKey(id);
        if (Objects.isNull(competition)) {
            throw new BrokerException(BrokerErrorCode.DB_RECORD_NOT_EXISTS);
        }

        TradeCompetitionLimit limit = findTradeCompetitionLimit(orgId, id);


        List<TradeCompetitionExt> ext_list = listTradeCompetitionExt(orgId, id);

        List<CompetitionInfo.Extend> extList = ext_list.stream()
                .map(i -> {
                            return CompetitionInfo.Extend.newBuilder()
                                    .setAppBanner(i.getMobileBannerUrl())
                                    .setPcBanner(i.getBannerUrl())
                                    .setDescription(i.getDescription())
                                    .setLanguage(i.getLanguage())
                                    .build();
                        }
                ).collect(Collectors.toList());


        String position = JSON.parseObject(limit.getLimitStr()).getString(TradeCompetitionService.FUTURES_POSITION_QUANTITY);
        CompetitionInfo.Qualify qualify = CompetitionInfo.Qualify.newBuilder()
                .setPosition(position)
                .build();

        List<RankType> rankTypes = Splitter.on(",").omitEmptyStrings().omitEmptyStrings().splitToList(competition.getRankTypes())
                .stream().map(i -> RankType.forNumber(Integer.parseInt(i))).collect(Collectors.toList());

        return CompetitionInfo.newBuilder()
                .setActivityId(id)
                .setBegin(competition.getStartTime().getTime() + "")
                .setEnd(competition.getEndTime().getTime() + "")
                .setContractId(competition.getSymbolId())
                .addAllExtends(extList)
                .setQualify(qualify)
                .setRankingNumber(competition.getEffectiveQuantity())
                .addAllRankTypes(rankTypes)
                .setStatus(CompetitionInfo.CompetitionStatus.forNumber(competition.getStatus()))
                .setIsReverse(competition.getIsReverse())
                .build();
    }

    private List<TradeCompetitionExt> listTradeCompetitionExt(long orgId, Long id) {
        Example extExp = new Example(TradeCompetitionExt.class);
        extExp.createCriteria()
                .andEqualTo("orgId", orgId)
                .andEqualTo("competitionId", id);
        return tradeCompetitionExtMapper.selectByExample(extExp);
    }

    private TradeCompetitionLimit findTradeCompetitionLimit(long orgId, Long id) {
        Example limitExp = new Example(TradeCompetitionLimit.class);
        limitExp.createCriteria()
                .andEqualTo("orgId", orgId)
                .andEqualTo("competitionId", id);

        return tradeCompetitionLimitMapper.selectOneByExample(limitExp);
    }

    public ListParticipantResponse.Page listParticipant(long orgId, int pageNo, int pageSize, long activityId) {

        if (pageSize == 0) {
            pageSize = 1000;
        }

        PageHelper.startPage(pageNo, pageSize);
        List<TradeCompetitionParticipant> list = listParticipants(orgId, activityId);

        if (CollectionUtils.isEmpty(list)) {
            return ListParticipantResponse.Page.newBuilder()
                    .setTotal(0)
                    .build();
        }
        log.info("listParticipant,result size={}", list.size());

        TradeCompetitionLimit limit = findTradeCompetitionLimit(orgId, activityId);
        Collection<Long> whiteList = strToCollection(limit.getEnterWhiteStr());

        List<Participant> participants = list.stream().map(i -> {
                    boolean wl = whiteList.contains(i.getUserId());
                    return Participant.newBuilder()
                            .setNickname(i.getNickname())
                            .setUid(i.getUserId())
                            .setIsWhiteList(wl)
                            .setWechat(i.getWechat())
                            .build();
                }
        ).collect(Collectors.toList());
        PageInfo page = new PageInfo(list);

        log.info("listParticipant,participants size={}", participants.size());

        return ListParticipantResponse.Page.newBuilder()
                .addAllList(participants)
                .setTotal((int) page.getTotal())
                .build();

    }

    private List<TradeCompetitionParticipant> listParticipants(long orgId, long activityId) {
        Example exp = new Example(TradeCompetitionParticipant.class);
        exp.createCriteria()
                .andEqualTo("orgId", orgId)
                .andEqualTo("competitionId", activityId);

        return tradeCompetitionParticipantMapper.selectByExample(exp);
    }


    @Transactional(rollbackFor = Exception.class)
    public boolean addParticipant(long orgId, long activityId, List<Participant> participantsList, AddParticipantRequest.MODE mode) {

        TradeCompetitionLimit limit = findTradeCompetitionLimit(orgId, activityId);
        if (Objects.isNull(limit)) {
            throw new BrokerException(BrokerErrorCode.DB_RECORD_NOT_EXISTS);
        }

        TradeCompetition competition = findTradeCompetition(orgId, activityId);
        if (Objects.isNull(competition)) {
            throw new BrokerException(BrokerErrorCode.DB_RECORD_NOT_EXISTS);
        }

        //完成或禁用状态不执行
        if (TradeCompetition.CompetitionStatus.INVALID.getValue() == competition.getStatus() ||
                TradeCompetition.CompetitionStatus.FINISH.getValue() == competition.getStatus()) {
            return false;
        }

        Map<Long, Participant> pariticipantMap = participantsList.stream().collect(Collectors.toMap(i -> i.getUid(), i -> i, (n, o) -> o));

        //检查是否合法的用户id
        Example userExp = new Example(User.class);
        userExp.selectProperties("userId");
        userExp.createCriteria()
                .andEqualTo("orgId", orgId)
                .andIn("userId", pariticipantMap.keySet())
                .andEqualTo("userStatus", 1)
        ;

        List<User> exist_users = userMapper.selectByExample(userExp);
        if (CollectionUtils.isEmpty(exist_users)) {
            throw new BrokerException(BrokerErrorCode.PARAM_INVALID);
        }

        //区分白名单和普通用户
        Map<Boolean, List<Participant>> group = pariticipantMap.values().stream().collect(Collectors.partitioningBy(i -> i.getIsWhiteList()));
        List<Participant> user_list = group.get(Boolean.FALSE);
        List<Participant> white_list = group.get(Boolean.TRUE);

        Set<Long> user_list_ids = user_list.stream().map(i -> i.getUid()).collect(Collectors.toSet());
        Set<Long> white_list_ids = white_list.stream().map(i -> i.getUid()).collect(Collectors.toSet());

        boolean success = false;
        //新建模式，清除所有信息，重新创建
        if (AddParticipantRequest.MODE.CREATE == mode) {
            //开始后不允许清除
            if (TradeCompetition.CompetitionStatus.PROCESSING.getValue() == competition.getStatus()) {
                return false;
            }
            TradeCompetitionLimit update_limit = TradeCompetitionLimit.builder()
                    .id(limit.getId())
                    .enterIdStr(Joiner.on(",").join(user_list_ids))
                    .enterWhiteStr(Joiner.on(",").join(white_list_ids))
                    .build();
            success = tradeCompetitionLimitMapper.updateByPrimaryKeySelective(update_limit) == 1;
            if (!success) {
                throw new BrokerException(BrokerErrorCode.DB_ERROR);
            }
            Example deleteExp = new Example(TradeCompetitionParticipant.class);
            deleteExp.createCriteria()
                    .andEqualTo("orgId", orgId)
                    .andEqualTo("competitionId", activityId);
            tradeCompetitionParticipantMapper.deleteByExample(deleteExp);
            List<TradeCompetitionParticipant> new_list = buildNewParticipants(pariticipantMap.values(), orgId, activityId, competition.getCompetitionCode());
            int rows = tradeCompetitionParticipantMapper.insertList(new_list);
            return rows > 0;
        }

        //添加参与人
        String participantIdStr = limit.getEnterIdStr();
        String whiteListIdStr = limit.getEnterWhiteStr();

        Set<Long> existUids = strToCollection(participantIdStr);
        Set<Long> existWhiteListUids = strToCollection(whiteListIdStr);

        //从已存在的用户id与本次白名单比较，如果存在相同uid则移动到已存在的白名单中
        moveSameUid(existUids, white_list_ids, existWhiteListUids);
        moveSameUid(existWhiteListUids, user_list_ids, existUids);

        Set<Long> appendUids = Sets.difference(user_list_ids, existUids);
        Set<Long> appendWhiteListUids = Sets.difference(white_list_ids, existWhiteListUids);
        if (CollectionUtils.isNotEmpty(existUids)
                || CollectionUtils.isNotEmpty(appendUids)) {
            existUids.addAll(appendUids);
            String uidStr = Joiner.on(",").join(existUids);
            limit.setEnterIdStr(uidStr);
        }

        if (CollectionUtils.isNotEmpty(appendWhiteListUids) ||
                CollectionUtils.isNotEmpty(existWhiteListUids)) {
            existWhiteListUids.addAll(appendWhiteListUids);
            String uidStr = Joiner.on(",").join(existWhiteListUids);
            limit.setEnterWhiteStr(uidStr);
        }

        if (CollectionUtils.isEmpty(existUids) &&
                CollectionUtils.isEmpty(existWhiteListUids)) {
            return true;
        }

        Set<Long> allUids = Sets.newHashSet(existUids);
        allUids.addAll(existWhiteListUids);
        //检查已经存在的参与人
        Example participantExp = new Example(TradeCompetitionParticipant.class);
        participantExp.selectProperties("id", "userId", "nickname");
        participantExp.createCriteria()
                .andEqualTo("orgId", orgId)
                .andEqualTo("competitionId", activityId)
                .andIn("userId", allUids);

        List<TradeCompetitionParticipant> existPaticipants = tradeCompetitionParticipantMapper.selectByExample(participantExp);
        Set<Long> existPaticipantUids = existPaticipants.stream().map(i -> i.getUserId()).collect(Collectors.toSet());
        //过滤掉已经存在的参与人
        List<Participant> newParticipants = pariticipantMap.values().stream().filter(i -> !existPaticipantUids.contains(i.getUid())).collect(Collectors.toList());
        List<Participant> updateParticipants = pariticipantMap.values().stream().filter(i -> existPaticipantUids.contains(i.getUid())).collect(Collectors.toList());

        List<TradeCompetitionParticipant> insertList = buildNewParticipants(newParticipants, orgId, activityId, competition.getCompetitionCode());
        List<TradeCompetitionParticipant> updateList = buildUpdateParticipants(updateParticipants, existPaticipants);

        success = tradeCompetitionLimitMapper.updateByPrimaryKeySelective(limit) == 1;
        if (!success) {
            throw new BrokerException(BrokerErrorCode.DB_ERROR);
        }

        if (CollectionUtils.isNotEmpty(insertList)) {
            tradeCompetitionParticipantMapper.insertList(insertList);
        } else if (CollectionUtils.isNotEmpty(updateList)) {
            updateList.forEach(i -> tradeCompetitionParticipantMapper.updateByPrimaryKeySelective(i));
        }
        return true;
    }

    private List<TradeCompetitionParticipant> buildUpdateParticipants(List<Participant> updateParticipants, List<TradeCompetitionParticipant> existPaticipants) {

        if (CollectionUtils.isEmpty(updateParticipants) || CollectionUtils.isEmpty(existPaticipants)) {
            return Lists.newArrayList();
        }

        Map<Long, TradeCompetitionParticipant> map = existPaticipants.stream().collect(Collectors.toMap(i -> i.getUserId(), i -> i));

        return updateParticipants.stream().filter(i -> map.containsKey(i.getUid()))
                .map(i -> {
                    TradeCompetitionParticipant tp = map.get(i.getUid());
                    if (tp.getNickname().equals(i.getNickname())) {
                        return null;
                    }
                    tp.setNickname(i.getNickname());
                    return tp;
                }).filter(Objects::nonNull).collect(Collectors.toList());
    }

    private void moveSameUid(Set<Long> fromIds, Set<Long> baseIds, Set<Long> toIds) {
        Iterator<Long> iterator = fromIds.iterator();
        while (iterator.hasNext()) {
            Long id = iterator.next();
            if (baseIds.contains(id)) {
                toIds.add(id);
                iterator.remove();
            }
        }
    }

    private List<TradeCompetitionParticipant> buildNewParticipants(Collection<Participant> input, long orgId, long activityId, String code) {
        return input.stream().map(i -> TradeCompetitionParticipant.builder()
                .orgId(orgId)
                .competitionId(activityId)
                .competitionCode(code)
                .nickname(i.getNickname())
                .userId(i.getUid())
                .wechat(StringUtils.isNotEmpty(i.getWechat()) ? i.getWechat() : "")
                .build()
        ).collect(Collectors.toList());
    }


    private Set<Long> strToCollection(String str) {

        if (StringUtils.isBlank(str)) {
            return Sets.newHashSet();
        }

        return Splitter.on(",").trimResults().omitEmptyStrings().splitToList(str)
                .stream().map(i -> Long.parseLong(i)).collect(Collectors.toSet());
    }

    public RankingListResult rankingList(long orgId, long activityId,
                                         String day, RankType rankType) {

        TradeCompetition competition = this.findTradeCompetition(orgId, activityId);

        log.info("rankingList param,orgId={},activityId={},day={},rankType={}",
                orgId, activityId, day, rankType);

        if (Objects.isNull(rankType) || rankType == RankType.UNkNOW) {

            log.error("Invalid rankType,rankType={}", rankType);
            throw new BrokerException(BrokerErrorCode.PARAM_INVALID);
        }

        if (Objects.isNull(competition)) {
            log.error("orgId={},competitionId={},Competition is null", orgId, activityId);
            throw new BrokerException(BrokerErrorCode.PARAM_INVALID);
        }

        if (Objects.isNull(rankType)) {
            rankType = Splitter.on(",").splitToList(competition.getRankTypes())
                    .stream().map(i -> RankType.forNumber(Integer.parseInt(i))).findFirst().orElse(null);
        }

        List<String> days = Lists.newArrayList();
        //String day=Strings.EMPTY;
        if (RankType.RATE_RETURN_DAILY == rankType || RankType.AMOUNT_RETURN_DAILY == rankType) {
            days = tradeCompetitionResultDailyMapper.listDay(orgId, activityId);
            if (StringUtils.isBlank(day) && days.size() > 0) {
                day = days.get(0);
            }

            if (StringUtils.isBlank(day)) {
                return RankingListResult.builder()
                        .days(Lists.newArrayList())
                        .rankings(Lists.newArrayList())
                        .rankType(rankType)
                        .currentDay(day)
                        .build();
            }
        }

        log.info("rankingList param,orgId={},activityId={},day={},rankType={}",
                orgId, activityId, day, rankType);

        List<TradeCompetitionResult> rankList = Lists.newArrayList();

        if (RankType.RATE_RETURN == rankType) {
            rankList = tradeCompetitionService.queryRateListByCode(orgId, competition.getCompetitionCode(), competition.getEffectiveQuantity());
        }
        if (RankType.AMOUNT_RETURN == rankType) {
            rankList = tradeCompetitionService.listIncomeAmountByCode(orgId, competition.getCompetitionCode(), competition.getEffectiveQuantity());
        }

        if (RankType.RATE_RETURN_DAILY == rankType) {
            rankList = tradeCompetitionService.listRateDailyListByCode(orgId, competition.getCompetitionCode(), competition.getEffectiveQuantity(), day);
        }

        if (RankType.AMOUNT_RETURN_DAILY == rankType) {
            rankList = tradeCompetitionService.listIncomeDailyAmountByCode(orgId, competition.getCompetitionCode(), competition.getEffectiveQuantity(), day);
        }

        Set<Long> userIds = rankList.stream().map(i -> i.getUserId()).collect(Collectors.toSet());

        Map<Long, String> nicknameMap = Maps.newHashMap();

        if (org.apache.commons.collections4.CollectionUtils.isNotEmpty(userIds)) {

            //查询昵称
            Example tcpExp = new Example(TradeCompetitionParticipant.class);
            tcpExp.createCriteria()
                    .andEqualTo("orgId", orgId)
                    .andEqualTo("competitionId", activityId)
                    .andIn("userId", userIds);

            List<TradeCompetitionParticipant> participants = tradeCompetitionParticipantMapper.selectByExample(tcpExp);
            nicknameMap.putAll(participants.stream().collect(Collectors.toMap(i -> i.getUserId(), i -> i.getNickname())));
        }

        List<RankingListResponse.Ranking> list = rankList.stream().map(i -> {
                    String nickname = nicknameMap.getOrDefault(i.getUserId(), "");
                    return RankingListResponse.Ranking.newBuilder()
                            .setAmountReturn(i.getIncomeAmountSafe())
                            .setRateReturn(i.getRateSafe())
                            .setNickname(nickname)
                            .setUid(i.getUserId())
                            .build()
                            ;
                }
        ).collect(Collectors.toList());

        return RankingListResult.builder()
                .days(days)
                .rankings(list)
                .rankType(rankType)
                .currentDay(day)
                .build();
    }

    public Pair<String, String> getShortUrl(long orgId, long activityId, String domain) {

        TradeCompetition competition = this.findTradeCompetition(orgId, activityId);
        if (Objects.isNull(competition)) {
            throw new BrokerException(BrokerErrorCode.PARAM_INVALID);
        }

        domain = getDomain(orgId);
        String code = competition.getCompetitionCode();
        String shortCodePC = shortUrlService.findShortUrl(domain + PC_PATH + code);
        String shortCodeH5 = shortUrlService.findShortUrl(domain + H5_PATH + code);

        String shortUrlPC = domain + SHORT_PATH + shortCodePC;
        String shortUrlH5 = domain + SHORT_PATH + shortCodeH5;

        return Pair.of(shortUrlPC, shortUrlH5);
    }

    private String getDomain(long orgId) {

        if (orgId < 1) {
            throw new IllegalArgumentException("invalid orgId");
        }

        Broker broker = brokerService.getBrokerByOrgId(orgId);
        if (Objects.isNull(broker)) {
            throw new IllegalArgumentException("Broker not exists,orgId" + orgId);
        }

        List<String> domains = Splitter.on(",").omitEmptyStrings().trimResults().splitToList(broker.getApiDomain());
        if (CollectionUtils.isEmpty(domains)) {
            log.warn("getShortUrl api domin is empty,orgId={}", orgId);
            throw new IllegalArgumentException("miss short url,orgId=" + orgId);
        }

        String tmp = domains.get(0);
        if (tmp.startsWith(".")) {
            return WWW_PROTOCOL + domains.get(0);
        } else {
            return WWW_PROTOCOL + "." + domains.get(0);
        }
    }


    @Builder(builderClassName = "Builder")
    @Data
    public static class RankingListResult {
        private RankType rankType;
        private List<String> days;
        private List<RankingListResponse.Ranking> rankings;
        private String currentDay;

        public String getCurrentDaySafe() {
            if (StringUtils.isBlank(currentDay)) {
                return "";
            }

            return this.currentDay;
        }
    }
}
