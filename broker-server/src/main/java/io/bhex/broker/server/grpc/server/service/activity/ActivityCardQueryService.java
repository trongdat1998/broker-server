package io.bhex.broker.server.grpc.server.service.activity;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.grpc.activity.*;
import io.bhex.broker.server.model.ActivityCard;
import io.bhex.broker.server.model.ActivityUserCard;
import io.bhex.broker.server.model.ActivityWelfareCardLog;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
@Service
public class ActivityCardQueryService {

    @Resource
    private ActivityRepositoryService activityRepositoryService;

    private List<Byte> validUserCardStatus = Lists.newArrayList(
            ActivityConfig.UserCardStatus.VALID,
            ActivityConfig.UserCardStatus.USED,
            ActivityConfig.UserCardStatus.EXPIRE
    );

    public GetUserCardAmountResponse getUserCardAmount(CommonCardRequest request) {

        CardType ct = request.getCardType();
        long userId = request.getHeader().getUserId();
        String symbol = request.getSymbol();

        if (userId == 0L) {
            throw new BrokerException(BrokerErrorCode.PARAM_INVALID);
        }
        int cardType = 0;

        if (ct == CardType.DOYEN) {
            cardType = ActivityConfig.CardType.DOYEN_CARD;
        }

        if (ct == CardType.NEW_HAND) {
            cardType = ActivityConfig.CardType.NEW_HAND_CARD;
        }

        if (ct == CardType.WELFARE) {
            cardType = ActivityConfig.CardType.DOYEN_CARD;
        }

        List<ActivityUserCard> list = activityRepositoryService.listValidUserCard(userId, cardType);
        //Timestamp now=Timestamp.from(new Date().toInstant());
        List<ActivityUserCard> validList = list.stream()
                //.filter(i->i.getExpireTime().after(now))
                .filter(i -> i.getSymbol().equalsIgnoreCase(symbol))
                .collect(Collectors.toList());

        String value = validList.stream().map(i -> i.getParValue())
                .reduce(new BigDecimal("0"), (a, b) -> a.add(b)).toString();

        UserCardAmount uca = UserCardAmount.newBuilder()
                .setSymol(symbol)
                .setUserCardAmount(value)
                .setUserId(userId).build();

        return GetUserCardAmountResponse.newBuilder().setAmount(uca).build();
    }

    public ListUserCardResponse listUserCard(CommonListRequest request) {

        long userId = request.getHeader().getUserId();
        int page = request.getPage();
        int size = request.getSize();
        String statusStr = request.getExt();

        List<Byte> statusList = Lists.newArrayList();
        if (Strings.isNullOrEmpty(statusStr)) {
            statusList = validUserCardStatus;
        } else {
            byte status = Byte.valueOf(statusStr);
            if (validUserCardStatus.contains(status)) {
                statusList.add(status);
            } else {
                statusList = validUserCardStatus;
            }
        }

        int total = activityRepositoryService.countUserCard(Lists.newArrayList(userId), statusList);
        if (total == 0) {
            return ListUserCardResponse.newBuilder()
                    .addAllCards(Lists.newArrayList())
                    .setPage(page)
                    .setSize(size)
                    .setTotal(total)
                    .build();
        }

        List<ActivityUserCard> list = activityRepositoryService.listUserCardByPage(userId, page, size, statusList);
        Set<Long> cardIds = list.stream().map(i -> i.getCardId()).collect(Collectors.toSet());
        Map<Long, ActivityCard> cardMap = buildActivityCardMap(cardIds);

        List<UserCard> userCards = list.stream()
                .map(i -> {
                    ActivityCard card = cardMap.get(i.getCardId());
                    return UserCard.newBuilder()
                            .setUserId(userId)
                            .setCardName(card.getCardName())
                            .setCardType(CardType.forNumber(i.getCardType().intValue()))
                            .setExpireTime(i.getExpireTime().getTime())
                            .setParValue(i.getParValue().toString())
                            .setStartTime(i.getGetTime().getTime())
                            .setStatus(CardStatus.forNumber(i.getCardStatus().intValue()))
                            .setSymbol(i.getSymbol())
                            .setGetTime(i.getGetTime().getTime())
                            .setUsedTime(i.getUsedTimeOut().getTime())
                            .setDesc(card.getDesc())
                            .build();
                }).collect(Collectors.toList());

        return ListUserCardResponse.newBuilder()
                .addAllCards(Lists.newArrayList())
                .setPage(page)
                .setSize(size)
                .setTotal(total)
                .addAllCards(userCards)
                .build();

    }

    public ListWelfareCardLogResponse listWelfareCardLog(WelfardCardRequest request) {

        long guildId = request.getGuildId();
        int page = request.getPage();
        int size = request.getSize();

        if (guildId < 1L) {
            log.error("invalid param,guildId={}", guildId);
            throw new BrokerException(BrokerErrorCode.PARAM_INVALID);
        }

        int totalRow = activityRepositoryService.countWelfareCardLog(guildId);
        if (totalRow == 0) {
            return ListWelfareCardLogResponse.newBuilder()
                    .addAllCards(Lists.newArrayList())
                    .setPage(page)
                    .setSize(size)
                    .setTotal(totalRow)
                    .setTotalCardValue("0")
                    .build();
        }

        List<ActivityWelfareCardLog> logs = activityRepositoryService.queryWelfareCardLog(guildId, page, size);
        if (CollectionUtils.isEmpty(logs)) {
            log.error("welfarelogs hasn't any records");
            throw new BrokerException(BrokerErrorCode.DB_ERROR);
        }

        Set<Long> cardIds = logs.stream().map(i -> i.getCardId()).collect(Collectors.toSet());
        Map<Long, ActivityCard> cardMap = buildActivityCardMap(cardIds);

        List<WelfardCard> welfardCards = logs.stream().map(i -> {
            ActivityCard card = cardMap.get(i.getCardId());
            return WelfardCard.newBuilder()
                    .setAmount(card.getParValue().toString())
                    .setGroupId(guildId)
                    .setId(i.getId())
                    .setReleaseTime(i.getCreateTime().getTime())
                    .build();
        }).collect(Collectors.toList());

        BigDecimal totalReward = welfardCards.stream().map(i -> new BigDecimal(i.getAmount()))
                .reduce(new BigDecimal("0"), (a, b) -> a.add(b));

        return ListWelfareCardLogResponse.newBuilder()
                .addAllCards(welfardCards)
                .setPage(page)
                .setSize(size)
                .setTotal(totalRow)
                .setTotalCardValue(totalReward.toString())
                .build();

    }

    private Map<Long, ActivityCard> buildActivityCardMap(Set<Long> cardIds) {
        List<ActivityCard> cards = activityRepositoryService.listCardByIds(Lists.newArrayList(cardIds));
        return cards.stream().collect(Collectors.toMap(i -> i.getId(), i -> i));
    }

    public CountUserCardResponse countUserCard(HeaderReqest request) {
        long userId = request.getHeader().getUserId();
        if (userId < 1L) {
            throw new BrokerException(BrokerErrorCode.INCORRECT_PARAMETER);
        }

        List<ActivityUserCard> list = activityRepositoryService.countUserCard(userId);
        Map<Byte, Long> map = list.stream().collect(Collectors.groupingBy(i -> i.getCardStatus(), Collectors.counting()));
        if (Objects.isNull(map) || map.isEmpty()) {
            return CountUserCardResponse.newBuilder().addAllCountUserCard(Lists.newArrayList()).build();
        }

        List<CountUserCard> cucList = map.keySet().stream().map(k -> {
            Long value = map.get(k);
            return CountUserCard.newBuilder().setCount(value.intValue()).setStatus(k.intValue()).build();
        }).collect(Collectors.toList());

        return CountUserCardResponse.newBuilder().addAllCountUserCard(cucList).build();

    }
}
