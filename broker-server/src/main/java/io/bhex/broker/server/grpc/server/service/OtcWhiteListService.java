package io.bhex.broker.server.grpc.server.service;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Resource;

import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.server.domain.OTCUserInfo;
import io.bhex.broker.server.grpc.client.service.GrpcOtcService;
import io.bhex.broker.server.model.Account;
import io.bhex.broker.server.model.OtcWhiteList;
import io.bhex.broker.server.model.User;
import io.bhex.broker.server.model.UserVerify;
import io.bhex.broker.server.primary.mapper.AccountMapper;
import io.bhex.broker.server.primary.mapper.OtcWhiteListLimitMapper;
import io.bhex.broker.server.primary.mapper.OtcWhiteListMapper;
import io.bhex.broker.server.primary.mapper.UserMapper;
import io.bhex.broker.server.primary.mapper.UserVerifyMapper;
import io.bhex.ex.otc.BatchUpdatePaymentVisibleRequest;
import io.bhex.ex.otc.OTCPaymentInfo;
import io.bhex.ex.otc.OTCUser;
import io.bhex.ex.otc.QueryOtcPaymentTermListRequest;
import io.bhex.ex.otc.QueryOtcPaymentTermListResponse;
import lombok.extern.slf4j.Slf4j;

/**
 * otc用户白名单
 *
 * @author lizhen
 * @date 2018-11-14
 */
@Slf4j
@Service
public class OtcWhiteListService {

    @Resource
    private OtcWhiteListMapper otcWhiteListMapper;

    @Resource
    private UserMapper userMapper;

    @Resource
    private GrpcOtcService grpcOtcService;

    @Resource
    private UserVerifyService userVerifyService;

    @Resource
    private GrpcOtcService getGrpcOtcService;

    @Resource
    private AccountMapper accountMapper;

    @Resource
    private UserVerifyMapper userVerifyMapper;

    @Resource
    private OtcWhiteListLimitMapper otcWhiteListLimitMapper;

    public List<OtcWhiteList> getOtcWhiteList(Long orgId) {
        OtcWhiteList example = OtcWhiteList.builder()
                .build();
        if (orgId != null && orgId > 0) {
            example.setOrgId(orgId);
        }
        return otcWhiteListMapper.select(example);
    }

    @Transactional(rollbackFor = Throwable.class)
    public int deleteOtcWhiteList(Long orgId, Long userId) {
        Account mainAccount = this.accountMapper.getMainAccount(orgId, userId);
        if (mainAccount == null || mainAccount.getAccountId().equals(0)) {
            throw new BrokerException(BrokerErrorCode.ACCOUNT_NOT_EXIST);
        }
        QueryOtcPaymentTermListResponse response
                = this.getGrpcOtcService.queryOtcPaymentTermList(QueryOtcPaymentTermListRequest.newBuilder().setAccountId(mainAccount.getAccountId()).setPage(0).setSize(100).build());
        if (response.getPaymentInfoCount() > 0) {
            List<Long> idList = new ArrayList<>();
            for (int i = 0; i < response.getPaymentInfoList().size(); i++) {
                try {
                    OTCPaymentInfo s = response.getPaymentInfoList().get(i);
                    log.info("OTCPaymentInfo info id {} accountId {} type {} realName {} ", s.getId(), s.getAccountId(), s.getPaymentType(), s.getRealName());
                    Account account = null;
                    try {
                        account = this.accountMapper.getAccountByAccountId(s.getAccountId());
                    } catch (Exception ex) {
                        throw new BrokerException(BrokerErrorCode.ACCOUNT_NOT_EXIST);
                    }
                    if (account != null && account.getUserId() > 0) {
                        UserVerify userVerify = UserVerify.decrypt(userVerifyMapper.getByUserId(account.getUserId()));
                        if (userVerify == null) {
                            log.info("cleanOtcPaymentTerm userVerify is null userId {} ", account.getUserId());
                            continue;
                        }
                        String realName = "";
                        if (userVerify != null && StringUtils.isNoneBlank(userVerify.getFirstName())) {
                            realName = userVerify.getFirstName();
                        }
                        if (userVerify != null && StringUtils.isNoneBlank(userVerify.getSecondName())) {
                            realName = realName + userVerify.getSecondName();
                        }

                        String realName_ = "";
                        if (userVerify != null && StringUtils.isNoneBlank(userVerify.getFirstName())) {
                            realName_ = userVerify.getFirstName();
                        }
                        if (userVerify != null && StringUtils.isNoneBlank(userVerify.getSecondName())) {
                            realName_ = realName_ + "_" + userVerify.getSecondName();
                        }
                        if (StringUtils.isBlank(s.getRealName())) {
                            idList.add(s.getId());
                        } else {
                            if (StringUtils.deleteWhitespace(s.getRealName()).equalsIgnoreCase(StringUtils.deleteWhitespace(realName_))) {
                                log.info("Do not deal with cleanOtcPaymentTerm id {} orgId {} userId {} kycName {} kycName_ {} payRealName {} ", s.getId(), account.getOrgId(), account.getUserId(), realName.trim(), realName_, s.getRealName().trim());
                                continue;
                            }
                            if (StringUtils.deleteWhitespace(s.getRealName()).equalsIgnoreCase(StringUtils.deleteWhitespace(realName))) {
                                log.info("Not equal to cleanOtcPaymentTerm id {} orgId {} userId {} kycName {} payRealName {} ", s.getId(), account.getOrgId(), account.getUserId(), realName.trim(), s.getRealName().trim());
                                continue;
                            }
                            idList.add(s.getId());
                            log.info("different name !!! userId {} payRealName {} kycName {}", account.getUserId(), s.getRealName(), realName);
                        }
                    }

                } catch (Exception ex) {
                    log.info("clean user otc payment term userId {} error {}", userId, ex);
                    throw ex;
                }
            }
            if (org.apache.commons.collections4.CollectionUtils.isNotEmpty(idList)) {
                Set<Long> idSetList = new HashSet<>(idList);
                Joiner joiner = Joiner.on(',');
                joiner.skipNulls();
                String idString = joiner.join(idSetList);
                if (StringUtils.isNoneBlank(idString)) {
                    log.info("cleanOtcPaymentTerm IDString {}", idString);
                    //根据ID批量更新
                    try {
                        this.getGrpcOtcService.batchUpdatePaymentVisible(BatchUpdatePaymentVisibleRequest.newBuilder().setIds(idString).build());
                    } catch (Exception e) {
                        log.info("clean user otc payment term userId {} error {}", userId, e);
                        throw e;
                    }
                }
            }
        }

        OtcWhiteList example = OtcWhiteList.builder()
                .orgId(orgId)
                .userId(userId)
                .build();
        return otcWhiteListMapper.delete(example);
    }

    public int addOtcWhiteList(Long orgId, Long userId) {
        if (otcWhiteListLimitMapper.query(orgId) != null) {
            throw new BrokerException(BrokerErrorCode.OTC_PERMISSION_DENIED);
        }
        User user = userMapper.getByOrgAndUserId(orgId, userId);
        if (Objects.isNull(user)) {
            throw new BrokerException(BrokerErrorCode.USER_NOT_EXIST);
        }
        OtcWhiteList otcWhiteList = OtcWhiteList.builder()
                .orgId(orgId)
                .userId(userId)
                .build();
        return otcWhiteListMapper.insert(otcWhiteList);
    }

    public List<OTCUserInfo> getWhiteListUser(Long orgId, Long userId, int page, int size) {
        if (page <= 0) {
            page = 1;
        }
        if (userId == null || userId <= 0) {
            userId = null;
        }
        List<User> userList = userMapper.getOtcWhiteListUser(orgId, userId, (page - 1) * size, size);
        if (CollectionUtils.isEmpty(userList)) {
            return Lists.newArrayList();
        }

        Set<Long> userIds = userList.stream().map(i -> i.getUserId()).collect(Collectors.toSet());
        List<UserVerify> verifies = userVerifyService.listByUserIds(userIds);
        Map<Long, UserVerify> verifyMap = verifies.stream().collect(Collectors.toMap(i -> i.getUserId(), i -> i, (o, n) -> o));

        List<OTCUser> otcUserList = grpcOtcService.listUser(userIds, orgId);
        Map<Long, OTCUser> otcUserMap = otcUserList.stream().collect(Collectors.toMap(i -> i.getUserId(), i -> i, (o, n) -> o));

        return userList.stream().map(i -> {
            UserVerify verify = verifyMap.get(i.getUserId());
            OTCUser otcUser = otcUserMap.get(i.getUserId());
            return OTCUserInfo.builder().user(i).verify(verify).otcUser(otcUser).build();
        }).collect(Collectors.toList());


    }
}
