package io.bhex.broker.server.grpc.server.service;

import com.google.gson.Gson;
import io.bhex.base.account.BalanceDetail;
import io.bhex.base.account.BalanceDetailList;
import io.bhex.base.account.GetBalanceDetailRequest;
import io.bhex.base.proto.DecimalUtil;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.common.util.JsonUtil;
import io.bhex.broker.grpc.common.Header;
import io.bhex.broker.server.domain.EnumBusinessLimit;
import io.bhex.broker.server.domain.HoldPositionVerifyRule;
import io.bhex.broker.server.grpc.client.service.GrpcBalanceService;
import io.bhex.broker.server.grpc.server.service.statistics.StatisticsBalanceService;
import io.bhex.broker.server.model.User;
import io.bhex.broker.server.model.UserLevel;
import io.bhex.broker.server.model.UserVerify;
import io.bhex.broker.server.model.staking.StakingProductSubscribeLimit;
import io.bhex.broker.server.primary.mapper.StakingProductSubscribeLimitMapper;
import io.bhex.broker.server.primary.mapper.UserLevelMapper;
import io.bhex.broker.server.primary.mapper.UserVerifyMapper;
import io.bhex.broker.server.util.BaseReqUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import tk.mybatis.mapper.util.StringUtil;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.DateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.*;

/**
 * 业务符合条件校验
 * 包括：kyc、绑定手机号、代币持仓（持仓数量）、平均持仓（平均数量、持仓时间）、用户等级
 *
 * @author songxd
 * @link ActivityLockInterestService verifyPurchaseLimit
 * @date 2020-09-17
 */
@Slf4j
@Service
public class BusinessLimitService {

    private final AccountService accountService;
    private final UserService userService;
    private final GrpcBalanceService grpcBalanceService;
    private final StatisticsBalanceService statisticsBalanceService;

    private final UserVerifyMapper userVerifyMapper;
    private final UserLevelMapper userLevelMapper;

    @Resource
    private StakingProductSubscribeLimitMapper stakingProductSubscribeLimitMapper;


    @Autowired
    public BusinessLimitService(UserVerifyMapper userVerifyMapper, UserLevelMapper userLevelMapper
            , UserService userService
            , AccountService accountService, GrpcBalanceService grpcBalanceService, StatisticsBalanceService statisticsBalanceService) {
        this.userVerifyMapper = userVerifyMapper;
        this.userLevelMapper = userLevelMapper;
        this.userService = userService;
        this.accountService = accountService;
        this.grpcBalanceService = grpcBalanceService;
        this.statisticsBalanceService = statisticsBalanceService;
    }

    /**
     * 校验
     *
     * @param orgId         机构ID
     * @param userId        用户ID
     * @param pkId          主键ID
     * @param businessLimit 业务类型
     */
    public void verifyLimit(Long orgId, Long userId, Long pkId, EnumBusinessLimit businessLimit) throws Exception, BrokerException {
        StakingProductSubscribeLimit stakingProductSubscribeLimit = stakingProductSubscribeLimitMapper.selectOne(StakingProductSubscribeLimit.builder()
                .orgId(orgId)
                .productId(pkId)
                .businessType(businessLimit.code())
                .build());
        if (Objects.isNull(stakingProductSubscribeLimit)) {
            return;
        }
        // user kyc
        if (Objects.nonNull(stakingProductSubscribeLimit.getVerifyKyc()) && stakingProductSubscribeLimit.getVerifyKyc().equals(1)) {
            userKycVerify(orgId, userId);
        }
        // user bind mobile
        if (Objects.nonNull(stakingProductSubscribeLimit.getVerifyBindPhone()) && stakingProductSubscribeLimit.getVerifyBindPhone().equals(1)) {
            userBindMobileVerify(orgId, userId);
        }
        // user vip level
        if (StringUtils.isNotEmpty(stakingProductSubscribeLimit.getLevelLimit())) {
            userLevelVerify(orgId, userId, stakingProductSubscribeLimit.getLevelLimit());
        }
        // hold position
        if (Objects.nonNull(stakingProductSubscribeLimit.getVerifyBalance()) && stakingProductSubscribeLimit.getVerifyBalance().equals(1)
                && Objects.nonNull(stakingProductSubscribeLimit.getBalanceRuleJson())) {
            HoldPositionVerifyRule holdPositionVerifyRule = JsonUtil.defaultGson().fromJson(stakingProductSubscribeLimit.getBalanceRuleJson(), HoldPositionVerifyRule.class);
            if (holdPositionVerifyRule != null) {
                holdPositionVerify(orgId, userId, holdPositionVerifyRule.getPositionToken(), new BigDecimal(holdPositionVerifyRule.getPositionVolume()));
            }
        }
        //  avg hold position
        if (Objects.nonNull(stakingProductSubscribeLimit.getVerifyAvgBalance()) && stakingProductSubscribeLimit.getVerifyAvgBalance().equals(1)
                && Objects.nonNull(stakingProductSubscribeLimit.getBalanceRuleJson())) {
            HoldPositionVerifyRule holdPositionVerifyRule = JsonUtil.defaultGson().fromJson(stakingProductSubscribeLimit.getBalanceRuleJson(), HoldPositionVerifyRule.class);
            if (holdPositionVerifyRule != null) {
                avgHoldPositionVerify(orgId, userId, holdPositionVerifyRule);
            }
        }
    }

    /**
     * kyc 实名验证
     *
     * @param orgId
     * @param userId
     */
    public void userKycVerify(Long orgId, Long userId) throws BrokerException {
        UserVerify dbUserVerify = userVerifyMapper.getByUserId(userId);
        if (dbUserVerify != null) {
            if (!dbUserVerify.isSeniorVerifyPassed()) {
                throw new BrokerException(BrokerErrorCode.ACTIVITY_FAIL_IN_KYC_LEVEL_REQUIREMENT);
            }
        } else {
            throw new BrokerException(BrokerErrorCode.ACTIVITY_FAIL_IN_KYC_LEVEL_REQUIREMENT);
        }
    }

    /**
     * 手机绑定验证
     *
     * @param orgId
     * @param userId
     */
    public void userBindMobileVerify(Long orgId, Long userId) throws BrokerException {
        User user = userService.getUser(userId);
        if (user != null) {
            if (StringUtils.isEmpty(user.getMobile())) {
                throw new BrokerException(BrokerErrorCode.ACTIVITY_FAIL_IN_PHONE_BINDING_REQUIREMENT);
            }
        } else {
            throw new BrokerException(BrokerErrorCode.ACTIVITY_FAIL_IN_PHONE_BINDING_REQUIREMENT);
        }
    }

    /**
     * vip等级验证
     *
     * @param orgId      机构ID
     * @param userId     用户ID
     * @param levelLimit 配置的vip等级ID，多个等级ID之间以","分隔
     */
    public void userLevelVerify(Long orgId, Long userId, String levelLimit) throws BrokerException {
        if (StringUtils.isEmpty(levelLimit)) {
            return;
        }
        List<String> levelConfigList = Arrays.asList(levelLimit.split(","));
        Set<String> levelConfigSet = new HashSet<>(levelConfigList);
        if (CollectionUtils.isEmpty(levelConfigSet)) {
            return;
        }

        //获取到当前用户的所有level config id
        List<UserLevel> userLevelList = this.userLevelMapper.queryMyAvailableConfigs(orgId, userId);
        if (CollectionUtils.isEmpty(userLevelList)) {
            log.warn("userLevelVerify userLevelList is empty org id->{} user id->{}", orgId, userId);
            throw new BrokerException(BrokerErrorCode.ACTIVITY_FAIL_IN_USER_LEVEL_REQUIREMENT);
        }

        // 校验用户level是否在配置的level范围内
        boolean limit = userLevelList.stream().noneMatch(userLevel -> levelConfigSet.contains(String.valueOf(userLevel.getLevelConfigId())));
        if (limit) {
            throw new BrokerException(BrokerErrorCode.ACTIVITY_FAIL_IN_USER_LEVEL_REQUIREMENT);
        }
    }

    /**
     * 持仓验证
     *
     * @param orgId  机构ID
     * @param userId 用户ID
     * @param token  币种
     * @param amount 持币金额
     */
    public void holdPositionVerify(Long orgId, Long userId, String token, BigDecimal amount) throws BrokerException {
        Long accountId = accountService.getMainAccountId(Header.newBuilder().setOrgId(orgId).setUserId(userId).build());
        if (accountId == null || accountId.equals(0L)) {
            log.warn("holdPositionVerify get user account id is null,org id -> {}, user id -> {}", orgId, userId);
            throw new BrokerException(BrokerErrorCode.ACTIVITY_FAIL_IN_TOKEN_HOLDING_REQUIREMENT);
        }

        if (StringUtils.isEmpty(token) || amount.compareTo(BigDecimal.ZERO) <= 0) {
            throw new BrokerException(BrokerErrorCode.PARAM_ERROR);
        }

        // get user balance
        GetBalanceDetailRequest request = GetBalanceDetailRequest.newBuilder()
                .setBaseRequest(BaseReqUtil.getBaseRequest(orgId))
                .setAccountId(accountId)
                .addAllTokenId(Collections.singletonList(token))
                .build();
        BalanceDetailList balanceDetailList = grpcBalanceService.getBalanceDetail(request);
        List<BalanceDetail> balanceDetails = balanceDetailList.getBalanceDetailsList();

        if ( CollectionUtils.isEmpty(balanceDetails)) {
            log.warn("holdPositionVerify get user balance details is null,org id -> {}, user id -> {}", orgId, userId);
            throw new BrokerException(BrokerErrorCode.ACTIVITY_FAIL_IN_TOKEN_HOLDING_REQUIREMENT);
        }
        for (BalanceDetail balance : balanceDetails) {
            BigDecimal total = DecimalUtil.toBigDecimal(balance.getTotal());
            if (total.compareTo(amount) >= 0) {
                return;
            }
        }
        throw new BrokerException(BrokerErrorCode.ACTIVITY_FAIL_IN_TOKEN_HOLDING_REQUIREMENT);
    }

    /**
     * 平均持仓验证
     *
     * @param orgId                  机构ID
     * @param userId                 用户ID
     * @param holdPositionVerifyRule 验证规则
     */
    public void avgHoldPositionVerify(Long orgId, Long userId, HoldPositionVerifyRule holdPositionVerifyRule) throws BrokerException {

        if (StringUtil.isEmpty(holdPositionVerifyRule.getVerifyAvgBalanceToken()) || StringUtils.isEmpty(holdPositionVerifyRule.getVerifyAvgBalanceVolume())
                || holdPositionVerifyRule.getVerifyAvgBalanceStartTime() == null || holdPositionVerifyRule.getVerifyAvgBalanceEndTime() == null) {
            return;
        }

        Long accountId = accountService.getAccountId(orgId, userId);
        if (accountId == null || accountId.equals(0L)) {
            return;
        }

        LocalDateTime startTime = LocalDateTime.ofInstant(new Date(holdPositionVerifyRule.getVerifyAvgBalanceStartTime()).toInstant(), ZoneId.of("UTC"));
        LocalDateTime endTime = LocalDateTime.ofInstant(new Date(holdPositionVerifyRule.getVerifyAvgBalanceEndTime()).toInstant(), ZoneId.of("UTC"));
        String startTimeStr = startTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        String endTimeStr = endTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        long days = startTime.until(endTime, ChronoUnit.DAYS);

        // 持仓合计
        BigDecimal total = statisticsBalanceService.getTokenBalanceSnapshotTotalByAccountId(orgId, accountId
                , holdPositionVerifyRule.getVerifyAvgBalanceToken(), startTimeStr, endTimeStr);
        // 计算平均持仓
        BigDecimal totalValue = total.divide(new BigDecimal(days), 18, RoundingMode.DOWN).setScale(8, RoundingMode.DOWN);
        if (totalValue.compareTo(new BigDecimal(holdPositionVerifyRule.getVerifyAvgBalanceVolume())) < 0) {
            throw new BrokerException(BrokerErrorCode.ACTIVITY_FAIL_IN_AVERAGE_TOKEN_HOLDING_REQUIREMENT);
        }
    }
}
