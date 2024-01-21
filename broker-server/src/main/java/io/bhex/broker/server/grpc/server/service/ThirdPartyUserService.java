package io.bhex.broker.server.grpc.server.service;

import com.google.common.base.Strings;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.grpc.account.CreateThirdPartyTokenResponse;
import io.bhex.broker.grpc.account.GetThirdPartyAccountResponse;
import io.bhex.broker.grpc.common.Header;
import io.bhex.broker.grpc.security.QueryUserTokenRequest;
import io.bhex.broker.grpc.security.QueryUserTokenResponse;
import io.bhex.broker.grpc.user.RegisterResponse;
import io.bhex.broker.server.domain.AccountType;
import io.bhex.broker.server.domain.BrokerServerConstants;
import io.bhex.broker.server.grpc.client.service.GrpcSecurityService;
import io.bhex.broker.server.model.Account;
import io.bhex.broker.server.model.MasterKeyUserConfig;
import io.bhex.broker.server.model.ThirdPartyUser;
import io.bhex.broker.server.model.User;
import io.bhex.broker.server.primary.mapper.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Date;

@Slf4j
@Service
public class ThirdPartyUserService {

    @Resource
    private ThirdPartyUserMapper thirdPartyUserMapper;

    @Resource
    private AccountMapper accountMapper;

    @Resource(name = "stringRedisTemplate")
    protected StringRedisTemplate redisTemplate;

    @Resource
    private UserService userService;

    @Resource
    private UserMapper userMapper;

    @Resource
    private GrpcSecurityService grpcSecurityService;

    @Resource
    private CommonIniService commonIniService;

    @Resource
    private MasterKeyUserConfigMapper masterKeyUserConfigMapper;

    /**
     * 创建虚拟用户 存储关系
     *
     * @param header
     * @param thirdUserId
     */
    public Long virtualUserRegister(Header header, String thirdUserId) {
        //检查是否拥有权限
        User user = userMapper.getByUserId(header.getUserId());
        if (user == null || user.getIsVip().equals(0)) {
            throw new BrokerException(BrokerErrorCode.NO_PERMISSION_TO_CREATE_VIRTUAL_ACCOUNT);
        }
        if (commonIniService.getBooleanValueOrDefault(header.getOrgId(), BrokerServerConstants.CONTRACT_FINANCING_KEY, false)) {
            MasterKeyUserConfig masterKeyUserConfig = masterKeyUserConfigMapper.selectMasterKeyUserConfigByUserId(header.getOrgId(), header.getUserId());
            if (masterKeyUserConfig == null || masterKeyUserConfig.getCreateAccount() == 0) {
                throw new BrokerException(BrokerErrorCode.NO_PERMISSION_TO_CREATE_VIRTUAL_ACCOUNT);
            }
        }

        ThirdPartyUser thirdPartyUserInfo = this.thirdPartyUserMapper.getByMasterUserIdAndThirdUserId(header.getOrgId(), header.getUserId(), thirdUserId);
        if (thirdPartyUserInfo != null) {
            log.info("Third user {}  is exist", thirdUserId);
            return thirdPartyUserInfo.getUserId();
//            throw new BrokerException(BrokerErrorCode.THIRD_USER_EXIST);
        }
        RegisterResponse registerResponse;
        try {
            registerResponse = userService.thirdPartyUserRegister(header, true);
            if (registerResponse == null || registerResponse.getUser() == null) {
                log.info("Virtual user register fail thirdUserId {}", thirdUserId);
                throw new BrokerException(BrokerErrorCode.BIND_THIRD_USER_ERROR);
            }
        } catch (Exception ex) {
            log.error("Virtual user register fail ex {}", ex);
            throw new BrokerException(BrokerErrorCode.BIND_THIRD_USER_ERROR);
        }

        ThirdPartyUser thirdPartyUser = ThirdPartyUser
                .builder()
                .masterUserId(header.getUserId())
                .thirdUserId(thirdUserId)
                .userId(registerResponse.getUser().getUserId())
                .orgId(header.getOrgId())
                .created(new Date())
                .updated(new Date())
                .build();
        this.thirdPartyUserMapper.insert(thirdPartyUser);
        return registerResponse.getUser().getUserId();
    }

    public CreateThirdPartyTokenResponse virtualUserLogin(Header header, String thirdUserId, Long userId, int accountType) {
        //检查是否拥有权限
        User user = userMapper.getByUserId(header.getUserId());
        if (user == null || user.getIsVip().equals(0)) {
            throw new BrokerException(BrokerErrorCode.NO_PERMISSION_TO_CREATE_VIRTUAL_ACCOUNT);
        }
        User virtualUser = getVirtualUser(header.getOrgId(), header.getUserId(), thirdUserId, userId);
        if (virtualUser.getUserStatus() != 1) {
            throw new BrokerException(BrokerErrorCode.USER_STATUS_FORBIDDEN);
        }
        QueryUserTokenResponse response
                = grpcSecurityService.getAuthorizeToken(QueryUserTokenRequest.newBuilder().setHeader(header).setUserId(virtualUser.getUserId()).build());
        if (StringUtils.isEmpty(response.getToken())) {
            log.info("Query third user token fail token is empty . thirdUserId {} ", thirdUserId);
            throw new BrokerException(BrokerErrorCode.GET_THIRD_TOKEN_ERROR);
        }

        Account account;
        if (AccountType.FUTURES.value() == accountType) {
            account = this.accountMapper.queryAccountByUserIdAndAccountType(header.getOrgId(), virtualUser.getUserId(), AccountType.FUTURES.value());
        } else if (AccountType.OPTION.value() == accountType) {
            account = this.accountMapper.queryAccountByUserIdAndAccountType(header.getOrgId(), virtualUser.getUserId(), AccountType.OPTION.value());
        } else {
            account = this.accountMapper.queryAccountByUserIdAndAccountType(header.getOrgId(), virtualUser.getUserId(), AccountType.MAIN.value());
        }

        return CreateThirdPartyTokenResponse
                .newBuilder()
                .setAuToken(response.getToken())
                .setUserId(virtualUser.getUserId())
                .setAccountId((account != null && account.getAccountId() != null) ? account.getAccountId() : 0L)
                .build();
    }

    public GetThirdPartyAccountResponse getVirtualUser(Header header, String thirdUserId, Long userId) {
        //检查是否拥有权限
        User user = userMapper.getByUserId(header.getUserId());
        if (user == null || user.getIsVip().equals(0)) {
            throw new BrokerException(BrokerErrorCode.NO_PERMISSION_TO_CREATE_VIRTUAL_ACCOUNT);
        }
        User virtualUser = getVirtualUser(header.getOrgId(), header.getUserId(), thirdUserId, userId);
        if (virtualUser.getUserStatus() != 1) {
            throw new BrokerException(BrokerErrorCode.USER_STATUS_FORBIDDEN);
        }
        return GetThirdPartyAccountResponse.newBuilder().setUserId(virtualUser.getUserId()).build();
    }

    public User getVirtualUser(Long orgId, Long masterUserId, String thirdUserId, Long userId) {
        ThirdPartyUser thirdPartyUser = null;
        if (!Strings.isNullOrEmpty(thirdUserId)) {
            thirdPartyUser = this.thirdPartyUserMapper.getByMasterUserIdAndThirdUserId(orgId, masterUserId, thirdUserId);
        } else if (userId != null && userId > 0) {
            thirdPartyUser = this.thirdPartyUserMapper.getByMasterUserIdAndUserId(orgId, masterUserId, userId);
        }
        if (thirdPartyUser == null) {
            log.warn("cannot find third party user with orgId:{}, masterUserId:{} thirdUserId:{}, userId:{}",
                    orgId, masterUserId, thirdUserId, userId);
            throw new BrokerException(BrokerErrorCode.USER_NOT_EXIST);
        }
        User user = userMapper.getByOrgAndUserId(orgId, thirdPartyUser.getUserId());
        if (user == null) {
            throw new BrokerException(BrokerErrorCode.USER_NOT_EXIST);
        }
        return user;
    }

}
