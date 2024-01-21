package io.bhex.broker.server.grpc.server.service;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.Resource;

import io.bhex.base.rc.RcBalanceResponse;
import io.bhex.base.rc.UserBizPermission;
import io.bhex.base.rc.UserRequest;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.server.domain.FrozenReason;
import io.bhex.broker.server.domain.FrozenStatus;
import io.bhex.broker.server.domain.FrozenType;
import io.bhex.broker.server.grpc.client.service.GrpcExchangeRCService;
import io.bhex.broker.server.model.FrozenUserRecord;
import io.bhex.broker.server.primary.mapper.FrozenUserRecordMapper;
import lombok.extern.slf4j.Slf4j;

/**
 * @ProjectName:
 * @Package:
 * @Author: yuehao  <hao.yue@bhex.com>
 * @CreateDate: 2021/2/24 上午10:17
 * @Copyright（C）: 2018 BHEX Inc. All rights reserved.
 */

@Slf4j
@Service
public class RiskControlBalanceService {

    private static final String USER_RISK_KEY = "USER_RISK_KEY:%s_%s";

    private static Map<String, UserBizPermission> USER_RISK_CONTROL_CONFIG = new HashMap<>();

    @Resource
    private GrpcExchangeRCService grpcExchangeRCService;

    @Resource
    private FrozenUserRecordMapper frozenUserRecordMapper;

    @Resource
    private UserService userService;


//    @PostConstruct
//    public void init() {
//        //初始化平台用户风控限制到内存
//        userRiskConfigCache();
//    }

//    @Scheduled(cron = "0/5 * * * * ?")
//    public void task() {
//        userRiskConfigCache();
//    }

//    public void userRiskConfigCache() {
//        long startTime = System.currentTimeMillis();
//        int limit = 300;
//        int page = 0;
//        while (true) {
//            ListUserBizPermissionResponse response = this.grpcExchangeRCService.getAllUserBizPermissionList(ListRequest.newBuilder()
//                    .setLimit(limit)
//                    .setOffset(page * limit)
//                    .build());
//            if (response.getUserBizPermissionCount() == 0) {
//                break;
//            }
//            List<UserBizPermission> userBizPermissionList = response.getUserBizPermissionList();
//            if (CollectionUtils.isEmpty(userBizPermissionList)) {
//                break;
//            }
//            USER_RISK_CONTROL_CONFIG.putAll(userBizPermissionList.stream().collect(Collectors.toMap(s -> String.format(USER_RISK_KEY, s.getOrgId(), s.getUserId()), s -> s)));
//            if (response.getUserBizPermissionCount() < 300) {
//                break;
//            } else {
//                page++;
//            }
//        }
//        long endTime = System.currentTimeMillis();
//        log.info("risk controller cache use time {}", (endTime - startTime));
//    }

//    public UserBizPermission query(long orgId, long userId) {
//        String key = String.format(USER_RISK_KEY, orgId, userId);
//        return USER_RISK_CONTROL_CONFIG.get(key);
//    }

    /**
     * 风控拦截
     *
     * @author yuehao
     */
    public void checkRiskControlLimit(Long orgId, Long userId, FrozenType frozenType) {
        if (orgId == null || orgId == 0) {
            log.info("checkRcBalance fail org id is null orgId {} userId {}", orgId, userId);
            throw new BrokerException(BrokerErrorCode.REQUEST_INVALID);
        }
        if (userId == null || userId == 0) {
            log.info("checkRcBalance fail user id is null orgId {} userId {}", orgId, userId);
            throw new BrokerException(BrokerErrorCode.REQUEST_INVALID);
        }
        checkRcBalance(orgId, userId);
        checkWithdrawFrozen(orgId, userId, frozenType);
    }

    /**
     * 平台风控黑名单拦截(禁止出金[otc,红包,提币,用户间转账])
     *
     * @param orgId
     * @param userId
     */
    public void checkRcBalance(Long orgId, Long userId) {
        try {
            RcBalanceResponse rcBalanceResponse = grpcExchangeRCService.getUserRcBalance(UserRequest.newBuilder().setUserId(userId).build());
            if (StringUtils.isEmpty(rcBalanceResponse.getLocked())) {
                log.info("rcBalanceResponse RISK_CONTROL_INTERCEPTION_LIMIT !!!! orgId {} userId {} locked {}", orgId, userId, rcBalanceResponse.getLocked());
                throw new BrokerException(BrokerErrorCode.RISK_CONTROL_INTERCEPTION_LIMIT);
            }
            if (new BigDecimal(rcBalanceResponse.getLocked()).compareTo(BigDecimal.ZERO) < 0) {
                log.info("rcBalanceResponse RISK_CONTROL_INTERCEPTION_LIMIT !!!! orgId {} userId {} locked {}", orgId, userId, rcBalanceResponse.getLocked());
                throw new BrokerException(BrokerErrorCode.RISK_CONTROL_INTERCEPTION_LIMIT);
            }
        } catch (BrokerException ex) {
            throw ex;
        } catch (Exception ex) {
            log.error("checkRcBalance fail orgId {} userId {} error {}", orgId, userId, ex);
        }
    }

    /**
     * 券商出金拦截(管理员操作解绑GA 找回密码 修改资金密码 用户解绑GA 用户解绑邮箱 用户解绑手机 重新绑定邮箱 重新绑定手机 重新绑定GA) [otc,红包,提币]
     *
     * @param orgId
     * @param userId
     * @param frozenType
     */
    private void checkWithdrawFrozen(Long orgId, Long userId, FrozenType frozenType) {
        Long currentTimestamp = System.currentTimeMillis();
        FrozenUserRecord frozenUserRecord = frozenUserRecordMapper.getByUserIdAndFrozenType(orgId, userId, frozenType.type());

        //在杠杆风控黑名单禁止出金
        if (frozenUserRecord != null && frozenUserRecord.getStatus() == FrozenStatus.UNDER_FROZEN.status()
                && frozenUserRecord.getFrozenReason() == FrozenReason.MARGIN_BLACK_LIST.reason()
                && (currentTimestamp <= frozenUserRecord.getEndTime() && currentTimestamp >= frozenUserRecord.getStartTime())) {
            log.info("checkWithdrawFrozen user:{} frozenType {} reason {} is under withdraw frozen, cannot withdraw", userId, frozenType, frozenUserRecord.getFrozenReason());
            throw new BrokerException(BrokerErrorCode.IN_MARGIN_RISK_BLACK);
        }
        if (frozenUserRecord != null && frozenUserRecord.getStatus() == FrozenStatus.UNDER_FROZEN.status()
                && (currentTimestamp <= frozenUserRecord.getEndTime() && currentTimestamp >= frozenUserRecord.getStartTime())) {
            log.info("checkWithdrawFrozen user:{} frozenType {} reason {} is under withdraw frozen, cannot withdraw", userId, frozenType, frozenUserRecord.getFrozenReason());
            throw new BrokerException(BrokerErrorCode.BROKER_RISK_CONTROL_INTERCEPTION_LIMIT);
        }

        if (userService.getUser(userId).getIsVirtual() == 1) {
            throw new BrokerException(BrokerErrorCode.BROKER_RISK_CONTROL_INTERCEPTION_LIMIT);
        }

        //todo 子用户也禁止提币 新系统上线添加拦截
    }
}
