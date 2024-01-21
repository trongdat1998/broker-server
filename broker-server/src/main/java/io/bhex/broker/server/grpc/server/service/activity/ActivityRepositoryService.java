package io.bhex.broker.server.grpc.server.service.activity;

import org.apache.commons.lang3.StringUtils;
import org.apache.ibatis.session.RowBounds;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Resource;

import io.bhex.broker.server.primary.mapper.ActivityCardMapper;
import io.bhex.broker.server.primary.mapper.ActivityDepositLogMapper;
import io.bhex.broker.server.primary.mapper.ActivityDoyenCardLogMapper;
import io.bhex.broker.server.primary.mapper.ActivityJoinerLogMapper;
import io.bhex.broker.server.primary.mapper.ActivityMapper;
import io.bhex.broker.server.primary.mapper.ActivitySendCardLogDailyMapper;
import io.bhex.broker.server.primary.mapper.ActivityTransferAccountLogMapper;
import io.bhex.broker.server.primary.mapper.ActivityUserCardMapper;
import io.bhex.broker.server.primary.mapper.ActivityWelfareCardLogMapper;
import io.bhex.broker.server.primary.mapper.ActivityWelfareCardStaticMapper;
import io.bhex.broker.server.primary.mapper.UserMapper;
import io.bhex.broker.server.model.Activity;
import io.bhex.broker.server.model.ActivityCard;
import io.bhex.broker.server.model.ActivityDepositLog;
import io.bhex.broker.server.model.ActivityDoyenCardLog;
import io.bhex.broker.server.model.ActivityJoinerLog;
import io.bhex.broker.server.model.ActivitySendCardLogDaily;
import io.bhex.broker.server.model.ActivityTransferAccountLog;
import io.bhex.broker.server.model.ActivityUserCard;
import io.bhex.broker.server.model.ActivityWelfareCardLog;
import io.bhex.broker.server.model.ActivityWelfareCardStatic;
import lombok.extern.slf4j.Slf4j;
import tk.mybatis.mapper.entity.Example;

@Slf4j
@Service("activityRepositoryService")
public class ActivityRepositoryService {

    @Resource
    private ActivityCardMapper activityCardMapper;

    @Resource
    private ActivityMapper activityMapper;

    @Resource
    private ActivityDoyenCardLogMapper activityDoyenCardLogMapper;

    @Resource
    private ActivityUserCardMapper activityUserCardMapper;

    @Resource
    private ActivityJoinerLogMapper activityJoinerLogMapper;

    @Resource
    private ActivityWelfareCardLogMapper activityWelfareCardLogMapper;

    @Resource
    private ActivityWelfareCardStaticMapper activityWelfareCardStaticMapper;

    @Resource
    private ActivityDepositLogMapper activityDepositLogMapper;

    @Resource
    private ActivityTransferAccountLogMapper activityTransferAccountLogMapper;

    @Resource
    private ActivitySendCardLogDailyMapper activitySendCardLogDailyMapper;

    @Resource
    private UserMapper userMapper;

    public ActivityCard findCard(int cardType) {

        Example exp = new Example(ActivityCard.class);
        exp.createCriteria()
                .andEqualTo("cardType", (short) cardType)
                .andEqualTo("cardStatus", (byte) 1);

        List<ActivityCard> list = activityCardMapper.selectByExample(exp);
        if (CollectionUtils.isEmpty(list)) {
            return null;
        }

        if (list.size() != 1) {
            log.error("More than one record,cardType={}", cardType);
            throw new IllegalStateException("More than one record");
        }

        return list.get(0);
    }

    public Activity findActivity(Long activityId) {

        return activityMapper.selectByPrimaryKey(activityId);
    }

    public List<ActivityDoyenCardLog> listDoyenCard(long doyenId, List<Long> followerIds, ActivityCard card) {

        Example exp = new Example(ActivityDoyenCardLog.class);
        exp.createCriteria()
                .andEqualTo("activityId", card.getActivityId())
                .andEqualTo("cardId", card.getId())
                .andEqualTo("ownerId", doyenId)
                .andIn("friendId", followerIds);

        return activityDoyenCardLogMapper.selectByExample(exp);
    }

    public List<ActivityUserCard> listUserCard(List<Long> userIds, Byte cardType) {

        Example exp = new Example(ActivityUserCard.class);
        exp.createCriteria()
                .andIn("userId", userIds)
                .andEqualTo("cardType", cardType);
        return activityUserCardMapper.selectByExample(exp);
    }

    public boolean saveUserCard(ActivityUserCard userCard) {
        //获取orgId 存表内
        Long orgId = this.userMapper.getOrgIdByUserId(userCard.getUserId());
        userCard.setOrgId(orgId);
        int rows = activityUserCardMapper.insertSelective(userCard);
        return rows > 0;
    }

    public boolean saveDoyenCardLog(ActivityDoyenCardLog cardLog) {
        ActivityDoyenCardLog activityDoyenCardLog = new ActivityDoyenCardLog();
        activityDoyenCardLog.setActivityId(cardLog.getActivityId());
        activityDoyenCardLog.setCardId(cardLog.getCardId());
        activityDoyenCardLog.setFriendId(cardLog.getFriendId());
        activityDoyenCardLog.setOwnerId(cardLog.getOwnerId());
        List<ActivityDoyenCardLog> activityDoyenCardLogs
                = activityDoyenCardLogMapper.select(activityDoyenCardLog);
        int rows = 0;
        try {
            if (activityDoyenCardLogs == null || activityDoyenCardLogs.size() == 0) {
                rows = activityDoyenCardLogMapper.insert(cardLog);
            }
        } catch (Exception ex) {
            if (StringUtils.isNotEmpty(ex.getMessage())
                    && ex.getMessage().contains("MySQLIntegrityConstraintViolationException")) {
                log.info("doyen card is exist");
            } else {
                throw ex;
            }
        }
        return rows > 0;
    }

    public boolean saveActivityJoinerLog(ActivityJoinerLog jlog) {

        int rows = activityJoinerLogMapper.insert(jlog);
        return rows > 0;
    }

    public ActivityJoinerLog findJoinerLog(Long userId, Long followerId) {

        Example exp = new Example(ActivityJoinerLog.class);
        exp.createCriteria().andEqualTo("userId", userId)
                .andEqualTo("followerId", followerId);

        List<ActivityJoinerLog> list = activityJoinerLogMapper.selectByExample(exp);
        if (CollectionUtils.isEmpty(list)) {
            return null;
        }

        if (list.size() == 1) {
            return list.get(0);
        }

        log.error("More than one joiner log,userId={},followId={}", userId, followerId);
        throw new IllegalStateException("More than one joiner log");

    }

    public List<ActivityJoinerLog> listJoinerLog() {

        return activityJoinerLogMapper.selectAll();
    }

    public List<ActivityJoinerLog> listJoinerLog(Set<Long> invitorIds) {

        Example exp = new Example(ActivityJoinerLog.class);
        exp.createCriteria().andIn("userId", invitorIds);

        return activityJoinerLogMapper.selectByExample(exp);

    }

    public List<ActivityWelfareCardLog> getWelfareLog(Long activityId, Long cardId, Long groupId) {

        Example exp = new Example(ActivityWelfareCardLog.class);
        exp.createCriteria()
                .andEqualTo("groupId", groupId)
                .andEqualTo("activityId", activityId)
                .andEqualTo("cardId", cardId);

        return activityWelfareCardLogMapper.selectByExample(exp);
    }


    public boolean saveWelfareCardLog(ActivityWelfareCardLog cardLog) {

        if (Objects.isNull(cardLog)) {
            throw new NullPointerException("saveWelfareCardLog null param");
        }

        int row = activityWelfareCardLogMapper.insert(cardLog);
        return row == 1;
    }

    public boolean saveWelfareCardStatic(ActivityWelfareCardStatic statis) {

        int row = activityWelfareCardStaticMapper.insert(statis);
        return row == 1;
    }

    public List<ActivityUserCard> listUserCard(Example example) {

        return activityUserCardMapper.selectByExample(example);
    }

    public void updateUserCardBatch(List<ActivityUserCard> list) {
        list.forEach(card -> {
            activityUserCardMapper.updateByPrimaryKeySelective(card);
        });
    }

    public List<ActivityUserCard> listValidUserCard(long userId, int cardType) {

        Timestamp now = Timestamp.from(new Date().toInstant());

        Example exp = new Example(ActivityUserCard.class);
        exp.createCriteria().andEqualTo("userId", userId)
                .andEqualTo("cardType", cardType)
                .andEqualTo("cardStatus", ActivityConfig.UserCardStatus.VALID)
                .andGreaterThan("expireTime", now);

        return activityUserCardMapper.selectByExample(exp);
    }

    public ActivityWelfareCardStatic getWelfareStatic(Long activityId, Long cardId, Long groupId) {

        Example exp = new Example(ActivityWelfareCardStatic.class);
        exp.createCriteria()
                .andEqualTo("groupId", groupId)
                .andEqualTo("activityId", activityId)
                .andEqualTo("cardId", cardId);

        List<ActivityWelfareCardStatic> list = activityWelfareCardStaticMapper.selectByExample(exp);
        if (CollectionUtils.isEmpty(list)) {
            return null;
        }

        if (list.size() == 1) {
            return list.get(0);
        }

        log.error("More than one welfare log,groupId={},activityId={},cardId={}", groupId, activityId, cardId);
        throw new IllegalStateException("More than one welfare log");
    }

    public boolean updateWelfareCardStatic(ActivityWelfareCardStatic statis) {
        int row = activityWelfareCardStaticMapper.updateByPrimaryKey(statis);
        return row == 1;
    }

    public int countUserCard(List<Long> userIds, List<Byte> status) {

        Example exp = new Example(ActivityUserCard.class);
        exp.createCriteria()
                .andIn("userId", userIds)
                .andIn("cardStatus", status);
        return activityUserCardMapper.selectCountByExample(exp);
    }


    public List<ActivityWelfareCardLog> queryWelfareCardLog(long guildId, int page, int size) {

        int currentPage = page < 1 ? 0 : page - 1;
        int offset = currentPage * size;

        return activityWelfareCardLogMapper.selectByPage(guildId, offset, size);
    }

    public int countWelfareCardLog(long guildId) {

        Example exp = new Example(ActivityWelfareCardLog.class);
        exp.createCriteria().andEqualTo("groupId", guildId);

        return activityWelfareCardLogMapper.selectCountByExample(exp);
    }

    public List<ActivityCard> listCardByIds(List<Long> cardIds) {

        Example exp = new Example(ActivityCard.class);
        exp.createCriteria().andIn("id", cardIds);
        return activityCardMapper.selectByExample(exp);
    }

    public List<ActivityUserCard> listUserCardByPage(long userId, int page, int size, List<Byte> statusList) {

        int currentPage = page < 1 ? 0 : page - 1;
        int offset = currentPage * size;

        if (statusList.size() == 1) {
            return activityUserCardMapper.selectByPge(userId, offset, size, statusList.get(0));
        }

        return activityUserCardMapper.selectAllByPge(userId, offset, size);
    }

    public boolean saveDepositoryLog(ActivityDepositLog depositLog) {

        int row = activityDepositLogMapper.insert(depositLog);
        return row == 1;
    }


    public List<ActivityDepositLog> listDeposit(List<Long> depositUserIds, short status, BigDecimal greaterThan) {

        Example exp = new Example(ActivityDepositLog.class);
        exp.createCriteria()
                .andIn("userId", depositUserIds)
                .andGreaterThanOrEqualTo("btcValue", greaterThan)
                .andEqualTo("status", status);

        return activityDepositLogMapper.selectByExample(exp);
    }

    public List<ActivityDepositLog> listDeposit(List<Long> depositUserIds, short status) {

        Example exp = new Example(ActivityDepositLog.class);
        exp.createCriteria()
                .andIn("userId", depositUserIds)
                .andEqualTo("status", status);

        return activityDepositLogMapper.selectByExample(exp);
    }


    public List<ActivityDepositLog> listValidDepositLogExclusiveEOS(List<Long> depositorIds) {

        Example exp = new Example(ActivityDepositLog.class);
        exp.createCriteria()
                .andIn("userId", depositorIds)
                .andEqualTo("status", ActivityDepositLog.STATUS.VALID)
                .andNotEqualTo("tokenName", "EOS");

        List<ActivityDepositLog> depositorLogs = activityDepositLogMapper.selectByExample(exp);
        return depositorLogs;
    }


    public boolean updateUserCardSelective(ActivityUserCard record) {

        int row = activityUserCardMapper.updateByPrimaryKeySelective(record);
        return row == 1;
    }

    public boolean updateUserCardByExampleSelective(ActivityUserCard record, Example exp) {

        int row = activityUserCardMapper.updateByExampleSelective(record, exp);
        return row == 1;
    }

    public boolean updateDepositLogSelective(ActivityDepositLog record) {

        int row = activityDepositLogMapper.updateByPrimaryKeySelective(record);
        return row == 1;
    }

    public List<ActivityUserCard> countUserCard(long userId) {

        Example exp = new Example(ActivityUserCard.class);
        exp.selectProperties("cardStatus", "userId");
        exp.createCriteria().andEqualTo("userId", userId);
        return activityUserCardMapper.selectByExample(exp);
    }

    public boolean updateActivityBalance(Activity activity, BigDecimal balance) {
        if (balance.compareTo(BigDecimal.ZERO) != 1) {
            throw new IllegalStateException("Not sufficient balance...");
        }

        Timestamp now = Timestamp.from(new Date().toInstant());

        Activity tmp = Activity.builder().balance(balance).updateTime(now).build();

        Example exp = new Example(Activity.class);
        exp.createCriteria()
                .andEqualTo("id", activity.getId())
                .andEqualTo("updateTime", activity.getUpdateTime());

        int row = activityMapper.updateByExampleSelective(tmp, exp);
        return row == 1;
    }

    public List<ActivityUserCard> selectUserCard(int page, int size, int status, int cardType) {
        int currentPage = page < 1 ? 0 : page - 1;
        int offset = currentPage * size;
        return activityUserCardMapper.selectUserCard(status, cardType, offset, size);
    }

    public void saveActivityTransferAccountLog(List<ActivityTransferAccountLog> logList) {
        logList.stream().forEach(log -> {
            activityTransferAccountLogMapper.insertSelective(log);
        });
    }

    public List<ActivityUserCard> getActivityUserCard(Example example) {
        return activityUserCardMapper.selectByExample(example);
    }

    public ActivityUserCard findUserCardById(Long userCardId) {
        return activityUserCardMapper.selectByPrimaryKey(userCardId);
    }

    public List<ActivityUserCard> getActivityUserCardByPage(Example example, RowBounds rowBounds) {
        return activityUserCardMapper.selectByExampleAndRowBounds(example, rowBounds);
    }

/*    public List<ActivityUserCard> getActivityUserCardByPage(Byte cardType,Byte cardStatus, int loop,int offset) {
        return activityUserCardMapper.selectByPage(cardType,cardStatus,loop*offset,offset);
    }*/

    public void removeJoinerLog(long invitorId, long userId, long groupId) {

        Example exp = new Example(ActivityJoinerLog.class);
        exp.createCriteria().andEqualTo("userId", invitorId)
                .andEqualTo("groupId", groupId)
                .andEqualTo("followerId", userId);

        activityJoinerLogMapper.deleteByExample(exp);
    }

    public List<Long> distinctJoinerLogUserId(int followerNumber) {

        return activityJoinerLogMapper.groupByUserId(followerNumber);
    }

    public List<Long> listFollowerIdsByUserId(Long userId) {

        return activityJoinerLogMapper.listFollowerIdByUserId(userId);
    }

    public List<Long> distinctJoinerLogGroupId(int followerNumber) {

        return activityJoinerLogMapper.distinctGroupId(followerNumber);
    }

    public List<Long> listFollowerIdsByGroupId(Long groupId) {
        return activityJoinerLogMapper.listFollowerIdByGroupId(groupId);
    }

    public List<ActivitySendCardLogDaily> listSendCardDailyLog(Long userId, List<String> identities, Date date) {

        Example exp = new Example(ActivitySendCardLogDaily.class);
        exp.createCriteria()
                .andEqualTo("userId", userId)
                .andIn("identity", identities)
                .andEqualTo("sendDay", date.getTime());

        return activitySendCardLogDailyMapper.selectByExample(exp);
    }

    public boolean saveSendCardLogDaily(ActivitySendCardLogDaily ld) {

        return activitySendCardLogDailyMapper.insertSelective(ld) == 1;
    }

    public boolean updateSendCardLogDaily(Integer version, ActivitySendCardLogDaily logDaily) {

        Example exp = new Example(ActivitySendCardLogDaily.class);
        exp.createCriteria()
                .andEqualTo("id", logDaily.getId())
                .andEqualTo("version", version);

        return activitySendCardLogDailyMapper.updateByExampleSelective(logDaily, exp) == 1;
    }

    public boolean existDepositLog(Long dataId, Long userId) {

        Example exp = new Example(ActivityDepositLog.class);
        exp.createCriteria().andEqualTo("dataId", dataId)
                .andEqualTo("userId", userId);

        return activityDepositLogMapper.selectCountByExample(exp) > 0L;


    }

    public int countUserCard(byte status, byte cardType) {

        Example example = new Example(ActivityUserCard.class);
        example.createCriteria()
                .andEqualTo("cardStatus", status)
                .andEqualTo("cardType", cardType);

        return activityUserCardMapper.selectCountByExample(example);


    }

    public List<Long> selectUserCardIds(int page, int size, int status, int cardType) {

        int currentPage = page < 1 ? 0 : page - 1;
        int offset = currentPage * size;
        return activityUserCardMapper.selectUserCardIds(status, cardType, offset, size);
    }

    public List<ActivityUserCard> selectUserCardByIds(List<Long> ids) {

        Example exp=new Example(ActivityUserCard.class);
        exp.createCriteria().andIn("id",ids);

        return activityUserCardMapper.selectByExample(exp);
    }

    public List<ActivityUserCard> selectUserCardByPage(long id, int size, byte status, byte cardType) {

        return activityUserCardMapper.selectUserCardByPage(status, cardType, id, size);
    }
}
