/**********************************
 *@项目名称: broker-server
 *@文件名称: io.bhex.broker.server.grpc.server.service
 *@Date 2018/8/23
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
package io.bhex.broker.server.grpc.server.service;

import com.google.common.base.Strings;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import io.bhex.base.account.*;
import io.bhex.base.account.SyncTransferResponse.ResponseCode;
import io.bhex.base.idgen.api.ISequenceGenerator;
import io.bhex.base.margin.TokenConfig;
import io.bhex.base.margin.cross.CrossLoanPosition;
import io.bhex.base.margin.cross.GetMarginPositionStatusReply;
import io.bhex.base.margin.cross.MarginCrossPositionStatusEnum;
import io.bhex.base.proto.BaseRequest;
import io.bhex.base.proto.Decimal;
import io.bhex.base.proto.DecimalUtil;
import io.bhex.base.quote.Rate;
import io.bhex.base.token.SymbolDetail;
import io.bhex.base.token.TokenDetail;
import io.bhex.base.token.TokenOptionInfo;
import io.bhex.base.token.TokenTypeEnum;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.common.util.JsonUtil;
import io.bhex.broker.core.domain.BaseResult;
import io.bhex.broker.grpc.account.Balance;
import io.bhex.broker.grpc.account.*;
import io.bhex.broker.grpc.admin.*;
import io.bhex.broker.grpc.common.AccountTypeEnum;
import io.bhex.broker.grpc.common.Header;
import io.bhex.broker.grpc.common.Platform;
import io.bhex.broker.grpc.margin.GetAvailWithdrawAmountResponse;
import io.bhex.broker.grpc.staking.GetStakingAssetListReply;
import io.bhex.broker.grpc.staking.GetStakingAssetListRequest;
import io.bhex.broker.grpc.staking.StakingProductType;
import io.bhex.broker.server.domain.AccountType;
import io.bhex.broker.server.domain.*;
import io.bhex.broker.server.elasticsearch.service.IBalanceFlowHistoryService;
import io.bhex.broker.server.grpc.client.service.GrpcAccountService;
import io.bhex.broker.server.grpc.client.service.GrpcBalanceService;
import io.bhex.broker.server.grpc.client.service.GrpcBatchTransferService;
import io.bhex.broker.server.grpc.client.service.GrpcTokenService;
import io.bhex.broker.server.grpc.server.service.aspect.UserActionLogAnnotation;
import io.bhex.broker.server.grpc.server.service.staking.StakingProductService;
import io.bhex.broker.server.model.Account;
import io.bhex.broker.server.model.BalanceBatchOperatePositionTask;
import io.bhex.broker.server.model.*;
import io.bhex.broker.server.primary.mapper.*;
import io.bhex.broker.server.util.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.ibatis.session.RowBounds;
import org.joda.time.DateTime;
import org.springframework.core.task.TaskExecutor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;
import tk.mybatis.mapper.entity.Example;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
@Service
public class AccountService {

    public static final BigDecimal MIN_TOKEN_BALANCE_VALUE = new BigDecimal("0.00000001");

    public static final String CREATE_SUB_ACCOUNT_UNLIMITED = "unlimitedUser-createSubAccount";

    private static final int DEFAULT_ACCOUNT_INDEX = 0;

    // 期权资产钱包
    public static final String BTC_TOKEN_ID = "BTC";
//    public static final String BUSDT_TOKEN_ID = "BUSDT";

    public static final String[] TEST_TOKENS = {"TBTC", "TUSDT", "TETH", "BUSDT", "CBTC"};

//    public static final String[] IGNORE_TOKENS = {"YANT", "BT"};

    public static final String ORG_API_TRANSFER_KEY = "orgApiTransfer";


    public static final String ORG_API_TRANSFER_TOKENS_KEY = "transferTokens";

    @Resource
    private GrpcAccountService grpcAccountService;

    @Resource
    private AccountMapper accountMapper;

    @Resource
    private GrpcBalanceService grpcBalanceService;

    @Resource
    private BasicService basicService;

    @Resource
    private UserMapper userMapper;

    @Resource
    private OptionPriceService optionPriceService;

    @Resource
    private ISequenceGenerator sequenceGenerator;

    @Resource
    private GrpcBatchTransferService grpcBatchTransferService;

    @Resource
    private BalanceMappingMapper balanceMappingMapper;

    @Resource
    private BalanceTransferMapper balanceTransferMapper;

    @Resource
    private AssetFuturesService assetFuturesService;

    @Resource
    private BalanceBatchTransferMapper balanceBatchTransferMapper;

    @Resource
    private BalanceBatchTransferTaskMapper balanceBatchTransferTaskMapper;

    @Resource
    private BalanceBatchLockPositionMapper balanceBatchLockPositionMapper;

    @Resource
    private BalanceBatchUnlockPositionMapper balanceBatchUnlockPositionMapper;

    @Resource
    private BalanceLockPositionMapper balanceLockPositionMapper;

    @Resource
    private BalanceUnlockPositionMapper balanceUnlockPositionMapper;

    @Resource
    private BalanceBatchOperatePositionTaskMapper balanceBatchOperatePositionTaskMapper;

    @Resource
    private SubBusinessSubjectService subBusinessSubjectService;

    @Resource
    private CommonIniService commonIniService;

    @Resource
    private BaseBizConfigService baseBizConfigService;

    @Resource(name = "asyncTaskExecutor")
    private TaskExecutor taskExecutor;

    @Resource
    private SubAccountTransferLimitLogMapper subAccountTransferLimitLogMapper;

    @Resource
    private UserTransferRecordMapper userTransferRecordMapper;

    @Resource
    private ThirdPartyUserService thirdPartyUserService;

    @Resource
    private MasterKeyUserConfigMapper masterKeyUserConfigMapper;

    @Resource
    private IBalanceFlowHistoryService IBalanceFlowHistoryService;

    @Resource
    private TransferLimitMapper transferLimitMapper;

    @Resource(name = "stringRedisTemplate")
    private StringRedisTemplate redisTemplate;

    @Resource
    private MarginPositionService marginPositionService;

    @Resource
    private GrpcTokenService grpcTokenService;

    @Resource
    private UserSecurityService userSecurityService;

    @Resource
    private FundAccountMapper fundAccountMapper;

    @Resource
    StakingProductService stakingProductService;

    @Resource
    private MarginService marginService;

    private static final Cache<String, Long> ACCOUNT_ID_CACHE = CacheBuilder.newBuilder()
            .initialCapacity(1024)
            .expireAfterWrite(30, TimeUnit.MINUTES).build();

    private static final Cache<String, Long> USER_ID_BY_ACCO_CACHE = CacheBuilder.newBuilder()
            .initialCapacity(1024)
            .expireAfterWrite(60, TimeUnit.MINUTES).build();

    private static final List<BusinessSubject> PC_BALANCE_FLOW_SUBJECTS = Lists.newArrayList(
            BusinessSubject.TRANSFER, BusinessSubject.CONVERT, BusinessSubject.MERGE_BALANCE,
            BusinessSubject.OTC_BUY_COIN, BusinessSubject.OTC_SELL_COIN,
            BusinessSubject.ACTIVITY_AWARD, BusinessSubject.INVITATION_REFERRAL_BONUS,
            // BusinessSubject.INVITATION_REFERRAL_BONUS,
            BusinessSubject.REGISTER_BONUS, BusinessSubject.AIRDROP,
            BusinessSubject.MINE_REWARD, BusinessSubject.GUILD_MASTER_REWARD,
            BusinessSubject.ICO_UNLOCK, BusinessSubject.POINT_CARD_TRADE_BONUS_TOKEN,
            BusinessSubject.POINT_CARD_TRADE_PAID, BusinessSubject.POINT_CARD_TRADE_REFUND,
            BusinessSubject.MAKER_REWARD, BusinessSubject.SETTLEMENT,
            BusinessSubject.INTEREST_WEALTH_MANAGEMENT_PRODUCTS, BusinessSubject.COMMISSION,
            BusinessSubject.INEX_MAPPING, BusinessSubject.INEX_FEE_RETURN,
            BusinessSubject.INEX_ALGEBRA_UNLOCK, BusinessSubject.INEX_TEAM_UNLOCK,
            BusinessSubject.INEX_TRADE_UNLOCK, BusinessSubject.INEX_AIRDROP_LOCK,
            BusinessSubject.INEX_ACTIVATION,
            BusinessSubject.ADMIN_TRANSFER, BusinessSubject.SAAS_FEE,
            BusinessSubject.PAY_GENERAL_PAY, BusinessSubject.PAY_FROM_LOCK_PAY,
            BusinessSubject.PAY_REFUND_PAY, BusinessSubject.PAY_COMMODITY_PAY,
            BusinessSubject.USER_ACCOUNT_TRANSFER, BusinessSubject.MASTER_TRANSFER_IN,
            BusinessSubject.MASTER_TRANSFER_OUT, BusinessSubject.MAKER_BONUS, BusinessSubject.LEADER_QUIT_UNLOCK_REPURCHASE,
            BusinessSubject.SEND_RED_PACKET, BusinessSubject.RECEIVER_RED_PACKET, BusinessSubject.REFUND_RED_PACKET,
            BusinessSubject.LEADER_INCOME, BusinessSubject.LEADER_INCOME_RELEASE, BusinessSubject.LEADER_INCOME_CIRCULATION
    );

    /*
     * 这里获取的是币币交易账户的accountId
     */
    public Long getUserId(Long accountId) {
        Account account = accountMapper.getAccountByAccountId(accountId);
        if (account == null) {
            throw new BrokerException(BrokerErrorCode.USER_NOT_EXIST);
        }
        return account.getUserId();
    }

    public Long getUserIdByAccountType(Long orgId, int accountType) {
        GetAccountByTypeRequest request = GetAccountByTypeRequest.newBuilder()
                .setBaseRequest(BaseReqUtil.getBaseRequest(orgId))
                .setOrgId(orgId)
                .setType(io.bhex.base.account.AccountType.valueOf(accountType))
                .build();
        GetAccountByTypeReply response = grpcAccountService.getUserIdByAccountType(request);
        if (Strings.isNullOrEmpty(response.getBrokerUserId())) {
            throw new BrokerException(BrokerErrorCode.USER_NOT_EXIST);
        }
        return Long.valueOf(response.getBrokerUserId());
    }

    public Long getAccountId(Long orgId, Long userId) {
        if (orgId == null || orgId == 0) {
            User user = userMapper.getByUserId(userId);
            orgId = user.getOrgId();
        }
        return getAccountId(orgId, userId, AccountType.MAIN, DEFAULT_ACCOUNT_INDEX);
    }

    /*
     * 这里获取的是币币交易账户的accountId
     */
    public Long getMainAccountId(Header header) {
        return getAccountId(header.getOrgId(), header.getUserId(), AccountType.MAIN,
                DEFAULT_ACCOUNT_INDEX);
    }

    public Long getMainIndexAccountId(Header header, int index) {
        return getAccountId(header.getOrgId(), header.getUserId(), AccountType.MAIN, index);
    }

    /*
     * 这里获取的是期权交易账户的accountId
     */
    public Long getOptionAccountId(Header header) {
        // log.info("getOptionAccountId. header:{}", TextFormat.shortDebugString(header));
        return getAccountId(header.getOrgId(), header.getUserId(), AccountType.OPTION,
                DEFAULT_ACCOUNT_INDEX);
    }

    public Long getOptionIndexAccountId(Header header, int index) {
        // log.info("getOptionAccountId. header:{}", TextFormat.shortDebugString(header));
        return getAccountId(header.getOrgId(), header.getUserId(), AccountType.OPTION, index);
    }

    /*
     * 这里获取的是期货交易账户的accountId
     */
    public Long getFuturesAccountId(Header header) {
        return getAccountId(header.getOrgId(), header.getUserId(), AccountType.FUTURES,
                DEFAULT_ACCOUNT_INDEX);
    }

    public Long getFuturesIndexAccountId(Header header, int index) {
        return getAccountId(header.getOrgId(), header.getUserId(), AccountType.FUTURES, index);
    }

    /*
     * 获取杠杆账户accountId
     */
    public Long getMarginAccountId(Header header) {
        return getAccountId(header.getOrgId(), header.getUserId(), AccountType.MARGIN, DEFAULT_ACCOUNT_INDEX);
    }

    public Long getMarginIndexAccountId(Header header, int index) {
        return getAccountId(header.getOrgId(), header.getUserId(), AccountType.MARGIN, index);
    }

    public Long getMarginIndexAccountIdNoThrow(Header header, int index) {
        return getMarginAccountIdNoThrow(header.getOrgId(), header.getUserId(), index);
    }

    public Long checkMarginAccountId(Header header, Long accountId) {
        if (header.getUserId() > 0) {
            accountId = getMarginAccountId(header);
        } else {
            Account account = accountMapper.getAccountByAccountId(accountId);
            if (account == null) {
                throw new BrokerException(BrokerErrorCode.ACCOUNT_NOT_EXIST);
            }
            if (account.getAccountType().compareTo(AccountType.MARGIN.value()) != 0) {
                throw new BrokerException(BrokerErrorCode.ACCOUNT_NOT_EXIST);
            }
            //如果userId不一致，就报错
            if (!account.getUserId().equals(header.getUserId())) {
                throw new BrokerException(BrokerErrorCode.ACCOUNT_NOT_EXIST);
            }
        }
        return accountId;
    }

    public Long checkMarginAccountIdNoThrow(Header header, Long accountId) {
        if (header.getUserId() > 0) {
            accountId = getMarginAccountIdNoThrow(header.getOrgId(), header.getUserId(), DEFAULT_ACCOUNT_INDEX);
        } else {
            Account account = accountMapper.getAccountByAccountId(accountId);
            if (account == null || account.getAccountType().compareTo(AccountType.MARGIN.value()) != 0) {
                return null;
            }
        }
        return accountId;
    }

    public void evictAccountIdCache() {
        ACCOUNT_ID_CACHE.invalidateAll();
    }

    public Long getAccountId(Long orgId, Long userId, AccountType accountType) {
        return getAccountId(orgId, userId, accountType, DEFAULT_ACCOUNT_INDEX);
    }

    /**
     * 这里查询的都是index=0的账户
     */
    public Long getAccountId(Long orgId, Long userId, AccountType accountType, int index) {
        String key = String.format("broker:account:%s:%s:%s:%s", orgId, userId, accountType, index);
        Long accountId;
        try {
            accountId = ACCOUNT_ID_CACHE.get(key, () -> {
                Account account = accountMapper
                        .getAccountByType(orgId, userId, accountType.value(), index);
                if (account == null || account.getAccountId() == 0) {
                    throw new BrokerException(BrokerErrorCode.ACCOUNT_NOT_EXIST);
                }
                return account.getAccountId();
            });
        } catch (Exception e) {
            log.warn(
                    "getAccountId by orgId:{} userId:{} accountTpe:{}, index:{} error, please check",
                    orgId, userId, accountType, index, e);
            throw new BrokerException(BrokerErrorCode.ACCOUNT_NOT_EXIST);
        }
        return accountId;
    }

    public Long getMarginAccountIdNoThrow(Long orgId, Long userId, int index) {
        String key = String.format("broker:account:%s:%s:%s:%s", orgId, userId, AccountType.MARGIN, index);
        Long accountId = null;
        try {
            accountId = ACCOUNT_ID_CACHE.get(key, () -> {
                Account account = accountMapper
                        .getAccountByType(orgId, userId, AccountType.MARGIN.value(), index);
                if (account == null || account.getAccountId() == 0) {
                    throw new BrokerException(BrokerErrorCode.ACCOUNT_NOT_EXIST);
                }
                return account.getAccountId();
            });
        } catch (Exception e) {
        }
        return accountId;
    }

    /**
     * cache获取用户账户 如果不存在返回0
     *
     * @param orgId
     * @param userId
     * @param accountType
     * @param index
     * @return
     */
    public Long getAccountIdFromCache(Long orgId, Long userId, AccountType accountType, int index) {
        String key = String.format("broker:account:%s:%s:%s:%s", orgId, userId, accountType, index);
        Long accountId;
        try {
            accountId = ACCOUNT_ID_CACHE.get(key, () -> {
                Account account = accountMapper
                        .getAccountByType(orgId, userId, accountType.value(), index);
                if (account == null || account.getAccountId() == 0) {
                    return 0L;
                }
                return account.getAccountId();
            });
        } catch (Exception e) {
            log.warn(
                    "getAccountId by orgId:{} userId:{} accountTpe:{}, index:{} error, please check",
                    orgId, userId, accountType, index, e);
            return 0L;
        }
        return accountId;
    }

    /**
     * 根据accountId获取UserId
     *
     * @param orgId
     * @param accountId
     * @return
     */
    public Long getUserIdByAccountId(Long orgId, Long accountId) {
        String key = String.format("broker:userByAcco:%s:%s", orgId, accountId);
        Long userIdReq;
        try {
            userIdReq = USER_ID_BY_ACCO_CACHE.get(key, () -> {
                Account account = accountMapper.getAccountByOrgIdAndAccountId(orgId, accountId);
                if (account == null || account.getAccountId() == 0) {
                    throw new BrokerException(BrokerErrorCode.ACCOUNT_NOT_EXIST);
                }
                return account.getUserId();
            });
        } catch (Exception e) {
            log.warn("getUserIdByAccoId by orgId:{} accountId:{}, error, please check",
                    orgId, accountId, e);
            throw new BrokerException(BrokerErrorCode.ACCOUNT_NOT_EXIST);
        }
        return userIdReq;
    }

    /**
     * 根据accountid获取user信息
     *
     * @param orgId
     * @param accountId
     * @return
     */
    public User getUserInfoByAccountId(Long orgId, Long accountId) {
        Account account = accountMapper.getAccountByOrgIdAndAccountId(orgId, accountId);
        if (account == null || account.getAccountId() == 0) {
            throw new BrokerException(BrokerErrorCode.ACCOUNT_NOT_EXIST);
        }

        User user = userMapper.getByOrgAndUserId(account.getOrgId(), account.getUserId());
        if (user == null || user.getUserId() == 0) {
            throw new BrokerException(BrokerErrorCode.USER_NOT_EXIST);
        }
        return user;
    }

    /**
     * 检查accountID是否是币币默认账户
     *
     * @param orgId
     * @param accountId
     * @return
     */
    public boolean checkAccountIdMain(Long orgId, Long accountId) {
        Account account = accountMapper.getAccountByOrgIdAndAccountId(orgId, accountId);
        if (account == null || account.getAccountId() == 0) {
            throw new BrokerException(BrokerErrorCode.ACCOUNT_NOT_EXIST);
        }

        if (account.getAccountType() == AccountType.MAIN.value()) {
            return true;
        } else {
            return false;
        }

    }

    /**
     * 判断accountID 是否是有效的资金账户 是资金账户且可见有效
     *
     * @param orgId
     * @param accountId
     * @return
     */
    public boolean checkValidFundAccountByAccountId(Long orgId, Long accountId) {
        Example example = new Example(FundAccount.class);
        example.createCriteria().andEqualTo("orgId", orgId).andEqualTo("accountId", accountId);

        FundAccount fundAccount = fundAccountMapper.selectOneByExample(example);
        if (fundAccount == null || fundAccount.getIsShow() == 0) {
            return false;
        }
        return true;
    }

    /**
     * 创建子账户，返回账户信息 TODO: 需要限定只有合约账户和币币账户能创建子账号
     */
    @UserActionLogAnnotation(actionType = BrokerServerConstants.ACTION_TYPE_CREATE_SUB_ACCOUNT,
            action = "type:{#accountType} name:{#accountName}")
    public Account createSubAccount(Header header, AccountType accountType, boolean authorizedOrg,
                                    String accountName) {
        if (authorizedOrg) {
            boolean authorizedOrgOpen = commonIniService.getBooleanValueOrDefault(header.getOrgId(),
                    BrokerServerConstants.SUB_ACCOUNT_AUTHORIZED_ORG_KEY, false);
            if (!authorizedOrgOpen) {
                log.error("org:{} not open SUB_ACCOUNT_AUTHORIZED_ORG", header.getOrgId());
                throw new BrokerException(BrokerErrorCode.NO_SUB_ACCOUNT_AUTHORIZED_ORG_CONFIG);
            }
        }

        //如果是大客户 并且是非open api的请求 拒绝
        User user = this.userMapper.getByUserId(header.getUserId());
        if (user.getIsVip().equals(1) && header.getPlatform() != Platform.OPENAPI) {
            throw new BrokerException(BrokerErrorCode.PARAM_INVALID);
        }
        Account account = accountMapper.getAccountByType(header.getOrgId(), header.getUserId(), accountType.value(), 0);
        int nextAccountIndex = 0;
        if (account != null) {
            nextAccountIndex = accountMapper.getMaxIndex(header.getOrgId(), header.getUserId(), accountType.value()) + 1;
        }

        if (!commonIniService
                .getStringValueOrDefault(header.getOrgId(), CREATE_SUB_ACCOUNT_UNLIMITED, "")
                .contains(String.valueOf(header.getUserId()))
                && nextAccountIndex > 5) {
            throw new BrokerException(BrokerErrorCode.SUB_ACCOUNT_NUMBER_EXCEED_UPPER_LIMIT);
        }
        CreateSpecialUserAccountRequest request = CreateSpecialUserAccountRequest.newBuilder()
                .setBaseRequest(BaseReqUtil.getBaseRequest(header.getOrgId()))
                .setOrgId(header.getOrgId())
                .setBrokerUserId(String.valueOf(header.getUserId()))
                .setAccountType(getBhAccountType(accountType))
                .setAccountIndex(nextAccountIndex)
                .build();
        CreateSpecialUserAccountReply reply = grpcAccountService.createSubAccount(request);
        Long timestamp = System.currentTimeMillis();
        Account subAccount = Account.builder()
                .orgId(header.getOrgId())
                .userId(header.getUserId())
                .accountId(reply.getAccountId())
                .accountName(accountName)
                .accountType(accountType.value())
                .isAuthorizedOrg(authorizedOrg ? 1 : 0)
                .accountIndex(nextAccountIndex)
                .accountStatus(1)
                .created(timestamp)
                .updated(timestamp)
                .build();
        accountMapper.insertSelective(subAccount);
       /* // 杠杆开通子账户需要saveUserContract
        if (AccountType.MARGIN.equals(accountType)) {
            SaveUserContractRequest req = SaveUserContractRequest.newBuilder()
                    .setOrgId(header.getOrgId())
                    .setUserId(header.getUserId())
                    .setOpen(UserContractOpenType.ON_OPEN_TYPE)
                    .setName(AccountType.MARGIN.type())
                    .build();
            userService.saveUserContract(req);
        }*/
        return subAccount;
    }

    public Account createSubAccountByOrgId(Long orgId, Long userId, AccountType accountType,
                                           String accountName, Integer index) {
        CreateSpecialUserAccountRequest request = CreateSpecialUserAccountRequest.newBuilder()
                .setBaseRequest(BaseReqUtil.getBaseRequest(orgId))
                .setOrgId(orgId)
                .setBrokerUserId(String.valueOf(userId))
                .setAccountType(getBhAccountType(accountType))
                .setAccountIndex(index)
                .build();
        CreateSpecialUserAccountReply reply = grpcAccountService.createSubAccount(request);
        Long timestamp = System.currentTimeMillis();
        Account subAccount = Account.builder()
                .orgId(orgId)
                .userId(userId)
                .accountId(reply.getAccountId())
                .accountName(accountName)
                .accountType(accountType.value())
                .accountIndex(index)
                .accountStatus(1)
                .created(timestamp)
                .updated(timestamp)
                .build();
        accountMapper.insertSelective(subAccount);
        return subAccount;
    }

    public List<Account> querySubAccountList(Header header, AccountType accountType) {
        List<Account> accountList = accountMapper
                .queryByUserId(header.getOrgId(), header.getUserId());
        if (!BrokerService.checkModule(header, FunctionModule.OPTION)) {
            accountList = accountList.stream()
                    .filter(account -> account.getAccountType() != AccountType.OPTION.value())
                    .collect(Collectors.toList());
        }
        if (!BrokerService.checkModule(header, FunctionModule.FUTURES)) {
            accountList = accountList.stream()
                    .filter(account -> account.getAccountType() != AccountType.FUTURES.value())
                    .collect(Collectors.toList());
        }
        if (!BrokerService.checkModule(header, FunctionModule.MARGIN)) {
            accountList = accountList.stream()
                    .filter(account -> account.getAccountType() != AccountType.MARGIN.value())
                    .collect(Collectors.toList());
        }
        return accountList.stream()
                .filter(account -> account.getAccountType() == accountType.value())
                .collect(Collectors.toList());
    }

    public List<Account> queryAllSubAccount(Header header) {
        List<Account> accountList = accountMapper
                .queryByUserId(header.getOrgId(), header.getUserId());
        if (!BrokerService.checkModule(header, FunctionModule.OPTION)) {
            accountList = accountList.stream()
                    .filter(account -> account.getAccountType() != AccountType.OPTION.value())
                    .collect(Collectors.toList());
        }
        if (!BrokerService.checkModule(header, FunctionModule.FUTURES)) {
            accountList = accountList.stream()
                    .filter(account -> account.getAccountType() != AccountType.FUTURES.value())
                    .collect(Collectors.toList());
        }
        if (!BrokerService.checkModule(header, FunctionModule.MARGIN)) {
            accountList = accountList.stream()
                    .filter(account -> account.getAccountType() != AccountType.MARGIN.value())
                    .collect(Collectors.toList());
        }
        return accountList;
    }

    private io.bhex.base.account.AccountType getBhAccountType(AccountType accountType) {
        switch (accountType) {
            case MAIN:
                return io.bhex.base.account.AccountType.GENERAL_ACCOUNT;
            case OPTION:
                return io.bhex.base.account.AccountType.OPTION_ACCOUNT;
            case FUTURES:
                return io.bhex.base.account.AccountType.FUTURES_ACCOUNT;
            case MARGIN:
                return io.bhex.base.account.AccountType.MARGIN_CROSS_ACCOUNT;
            default:
                return null;
        }
    }

    // querySubAccountBalance
    @Deprecated
    public QueryAccountResponse queryAccount(Header header) {
        List<io.bhex.broker.server.model.Account> localAccountList = accountMapper
                .queryByUserId(header.getOrgId(), header.getUserId());
        List<io.bhex.broker.grpc.account.Account> accountList = Lists.newArrayList();
        if (localAccountList.size() > 0) {
            accountList = localAccountList.stream()
                    .map(account -> getAccount(account, Lists.newArrayList()))
                    .collect(Collectors.toList());
        }
        return QueryAccountResponse.newBuilder().addAllAccount(accountList).build();
    }

    public GetAccountResponse getAccount(Header header, AccountTypeEnum accountTypeEnum,
                                         Integer accountIndex) {
        Account localAccount = accountMapper.getAccountByType(header.getOrgId(), header.getUserId(),
                AccountType.fromAccountTypeEnum(accountTypeEnum).value(), accountIndex);
        if (localAccount == null) {
            throw new BrokerException(BrokerErrorCode.ACCOUNT_NOT_EXIST);
        }
        List<Balance> balanceList = queryBalance(header, localAccount.getAccountId(),
                Lists.newArrayList(), false);
        return GetAccountResponse.newBuilder().setAccount(getAccount(localAccount, balanceList))
                .build();
    }

    public String getAvailableBalance(Header header, AccountTypeEnum accountTypeEnum,
                                      Integer accountIndex, String tokenId) {
        Account localAccount = accountMapper.getAccountByType(header.getOrgId(), header.getUserId(),
                AccountType.fromAccountTypeEnum(accountTypeEnum).value(), accountIndex);
        if (localAccount == null) {
            throw new BrokerException(BrokerErrorCode.ACCOUNT_NOT_EXIST);
        }
        GetOtcAvailableRequest getOtcAvailableRequest = GetOtcAvailableRequest.newBuilder()
                .setOrgId(header.getOrgId())
                .setAccountId(localAccount.getAccountId())
                .setTokenId(tokenId)
                .build();

        return grpcBalanceService.getOtcAvailable(getOtcAvailableRequest).getAvailable();
    }

    public void userAccountTransfer(Header header, AccountTypeEnum fromAccountTypeEnum, int fromIndex,
                                    AccountTypeEnum toAccountTypeEnum, int toIndex, String tokenId, BigDecimal amount) {

        String lockKey = null;
        boolean lock = false;
        try {

            Long fromAccountId = getAccountId(header.getOrgId(), header.getUserId(), AccountType.fromAccountTypeEnum(fromAccountTypeEnum), fromIndex);
            Account fromAccount = accountMapper.getAccountByAccountId(fromAccountId);
            if (fromAccount == null) {
                throw new BrokerException(BrokerErrorCode.ACCOUNT_NOT_EXIST);
            }

            if (fromAccount.getAccountIndex() != null && fromAccount.getAccountIndex() > 0) {
                if (fromAccount.getIsForbid().equals(ForbidType.OPEN.value())) {
                    if (fromAccount.getForbidEndTime() != null && fromAccount.getForbidEndTime() > 0 && fromAccount.getForbidEndTime() > new Date().getTime()) {
                        throw new BrokerException(BrokerErrorCode.SUB_ACCOUNT_TRANSFER_FAILED);
                    }
                }
            }

            checkRcSubAccount(header, fromAccountId, tokenId, amount);

            Long toAccountId = getAccountId(header.getOrgId(), header.getUserId(), AccountType.fromAccountTypeEnum(toAccountTypeEnum), toIndex);
            if (fromAccountId == null || toAccountId == null) {
                throw new BrokerException(BrokerErrorCode.ACCOUNT_NOT_EXIST);
            }
            Account toAccount = accountMapper.getAccountByAccountId(toAccountId);
            if (toAccount == null) {
                throw new BrokerException(BrokerErrorCode.ACCOUNT_NOT_EXIST);
            }
            if (toAccount.getIsAuthorizedOrg() == 1 && header.getPlatform() == Platform.ORG_API) {
                log.error("org:{} orgapi UserAccountTransfer not allowed", header.getOrgId());
                throw new BrokerException(BrokerErrorCode.SUB_ACCOUNT_TRANSFER_FAILED);
            }
            if (toAccount.getAccountIndex() != null && toAccount.getAccountIndex() > 0) {
                if (toAccount.getIsForbid().equals(ForbidType.OPEN.value())) {
                    if (toAccount.getForbidEndTime() != null && toAccount.getForbidEndTime() > 0 && toAccount.getForbidEndTime() > new Date().getTime()) {
                        throw new BrokerException(BrokerErrorCode.SUB_ACCOUNT_TRANSFER_FAILED);
                    }
                }
            }

            if ((fromAccountTypeEnum != AccountTypeEnum.COIN && toAccountTypeEnum != AccountTypeEnum.COIN)
                    || (fromIndex > 0 && toIndex > 0)) {
                log.error("{}-{} transfer {}-{} from {}-{} to {}-{},one of the transfer account must be the main account",
                        header.getOrgId(), header.getUserId(), tokenId, amount,
                        fromAccountTypeEnum, fromIndex, toAccountTypeEnum, toIndex);
                throw new BrokerException(BrokerErrorCode.ACCOUNT_TRANSFER_MUST_BE_MAIN_ACCOUNT);
            }
            boolean isMarginTransfer = false;
            Long calAccountId = 0L;
            //来源账户为杠杆，表示出金
            if (fromAccountTypeEnum == AccountTypeEnum.MARGIN) {
                GetMarginPositionStatusReply positionStatusReply = marginService.getMarginPositionStatus(header, fromAccountId);
                if (positionStatusReply.getCurStatus() == MarginCrossPositionStatusEnum.POSITION_FORCE_CLOSING) {
                    log.warn("userAccountTransfer margin withdraw orgId:{} userId:{} accountId:{} status is FORCE_CLOSING cannot create order", header.getOrgId(), header.getUserId(), fromAccountId);
                    throw new BrokerException(BrokerErrorCode.MARGIN_ACCOUNT_IS_FORCE_CLOSE);
                }
                lockKey = String.format(BrokerServerConstants.MARGIN_WITHDRAW_LOCK, header.getOrgId(), fromAccountId);
                lock = RedisLockUtils.tryLockAlways(redisTemplate, lockKey, 2000, 10);
                if (!lock) {
                    throw new BrokerException(BrokerErrorCode.MARGIN_WITHDRAW_FAILED);
                }
                //获取可出金数量
                GetAvailWithdrawAmountResponse availWithdrawAmountResponse = marginPositionService.getAvailWithdrawAmount(header, tokenId, fromAccountId);
                BigDecimal availWithdrawAmount = new BigDecimal(availWithdrawAmountResponse.getAvailWithdrawAmount());
                if (availWithdrawAmount.compareTo(amount) < 0) {//可出金数量小于请求数量
                    log.error("margin avail withdraw not enough orgId:{} userId:{} accountId:{} avail:{}  amount:{}  ",
                            header.getOrgId(), header.getUserId(), fromAccountId, availWithdrawAmount, amount);
                    throw new BrokerException(BrokerErrorCode.MARGIN_AVAIL_WITHDRAW_NOT_ENOUGH_FAILED);
                }
                isMarginTransfer = true;
                calAccountId = fromAccountId;

            }
            if (toAccountTypeEnum == AccountTypeEnum.MARGIN) {
                TokenConfig tc = basicService.getOrgMarginToken(header.getOrgId(), tokenId);
                if (tc == null || tc.getIsOpen() != 1) {
                    throw new BrokerException(BrokerErrorCode.TRANSFER_SYMBOL_NOT_ALLOWED);
                }
                isMarginTransfer = true;
                calAccountId = toAccountId;
            }
            log.info("{}-{} transfer {}-{} from {}-{} to {}-{}", header.getOrgId(), header.getUserId(), tokenId, amount,
                    fromAccountTypeEnum, fromIndex, toAccountTypeEnum, toIndex);
            UserAccountSafeTransferReq request = UserAccountSafeTransferReq.newBuilder()
                    .setOrgId(header.getOrgId())
                    .setBrokerUserId(String.valueOf(header.getUserId()))
                    .setSourceType(getBhAccountType(AccountType.fromAccountTypeEnum(fromAccountTypeEnum)))
                    .setSourceIndex(fromIndex)
                    .setDestType(getBhAccountType(AccountType.fromAccountTypeEnum(toAccountTypeEnum)))
                    .setDestIndex(toIndex)
                    .setTokenId(tokenId)
                    .setQuantity(DecimalUtil.fromBigDecimal(amount))
                    .build();
            SyncTransferResponse response = grpcAccountService.userAccountTransfer(request);
            SyncTransferResponse.ResponseCode code = response.getCode();
            if (code == SyncTransferResponse.ResponseCode.SUCCESS) {
                if (isMarginTransfer) {
                    marginPositionService.calculateSafeByAccount(header.getOrgId(), calAccountId);
                    //杠杆入金，更新保证金记录，用于后续计算杠杆收益
                    if (toAccountTypeEnum == AccountTypeEnum.MARGIN) {
                        marginPositionService.calTransferMarginAsset(header.getOrgId(), header.getUserId(), calAccountId, tokenId, amount, 0);
                    }
                }
                return;
            }
            if (code == SyncTransferResponse.ResponseCode.ACCOUNT_NOT_EXIST
                    || code == SyncTransferResponse.ResponseCode.FROM_ACCOUNT_NOT_EXIST
                    || code == SyncTransferResponse.ResponseCode.TO_ACCOUNT_NOT_EXIST) {
                throw new BrokerException(BrokerErrorCode.ACCOUNT_NOT_EXIST);
            } else if (code == SyncTransferResponse.ResponseCode.BALANCE_INSUFFICIENT) {
                throw new BrokerException(BrokerErrorCode.INSUFFICIENT_BALANCE);
            } else {
                throw new BrokerException(BrokerErrorCode.GRPC_SERVER_SYSTEM_ERROR);
            }
        } finally {
            if (lockKey != null && lock) {
                RedisLockUtils.releaseLock(redisTemplate, lockKey);
            }
        }
    }

    //已过时 @2021-03-08 风控规则：2.禁止这个子账户单次转账大于1000万usdt 3.这个账户usdt资产小于等于1.7亿usdt 以后禁止转账提现；
    private void checkRcSubAccount(Header header, Long accountId, String transferTokenId, BigDecimal transferAmount) {
        if (header.getUserId() == 842145604923681280L) { //1.禁止这个子账户交易 7070的子账号
            throw new BrokerException(BrokerErrorCode.FEATURE_SUSPENDED);
        }

//        BigDecimal fxRate = basicService.getFXRate(header.getOrgId(), transferTokenId, "USDT");
//        BigDecimal transferAmountInUsdt = fxRate.multiply(transferAmount);
//        if (transferAmountInUsdt.compareTo(new BigDecimal(10000000)) > 0) {
//            log.warn("checkRcSubAccount. more than maxTransferAmount aid:{} token:{} amount:{}",
//                    accountId, transferTokenId, transferAmount);
//            throw new BrokerException(BrokerErrorCode.BALANCE_TRANSFER_FAILED);
//        }
//
//        List<Balance> balances = queryBalance(header, accountId, Lists.newArrayList());
//        if (CollectionUtils.isEmpty(balances)) {
//            throw new BrokerException(BrokerErrorCode.FINANCE_INSUFFICIENT_BALANCE);
//        }
//
//        List<String> tokenIds = balances.stream().map(Balance::getTokenId).distinct().collect(Collectors.toList());
//        GetTokenIdsRequest tokenIdsRequest = GetTokenIdsRequest.newBuilder()
//                .setBaseRequest(GrpcRequestUtil.getBaseRequest(header))
//                .addAllTokenIds(tokenIds)
//                .build();
//        List<TokenDetail> tokenDetailList = grpcTokenService.queryTokenListByIds(tokenIdsRequest).getTokenDetailsList();
//        Set<String> streamTokenSet = tokenDetailList.stream()
//                .filter(t -> t.getIsMainstream())
//                .map(t -> t.getTokenId())
//                .collect(Collectors.toSet());
//
//        BigDecimal totalInUsdt = BigDecimal.ZERO;
//        for (Balance balance : balances) {
//            if (!streamTokenSet.contains(balance.getTokenId())) {
//                continue;
//            }
//            totalInUsdt = totalInUsdt.add(new BigDecimal(balance.getUsdtValue()));
//        }
//        //剩余的最小余额必须是主流币
//        if (totalInUsdt.subtract(transferAmountInUsdt).compareTo(new BigDecimal(150000000)) < 0) {
//            log.warn("checkRcSubAccount. more than maxTransferAmount aid:{} token:{} transferAmountInUnit:{} totalInUnit:{}",
//                    accountId, transferTokenId, transferAmountInUsdt, totalInUsdt);
//            throw new BrokerException(BrokerErrorCode.FINANCE_INSUFFICIENT_BALANCE);
//        }
    }

    public GetCanTransferAmountResp getCanTransferAmount(Header header, AccountTypeEnum accountType,
                                                         int accountIndex, String tokenId) {
        Long accountId = getAccountId(header.getOrgId(), header.getUserId(),
                AccountType.fromAccountTypeEnum(accountType), accountIndex);
        if (accountId == null || accountId == 0) {
            throw new BrokerException(BrokerErrorCode.ACCOUNT_NOT_EXIST);
        }
        GetCanTransferReq req = GetCanTransferReq.newBuilder()
                .setBaseRequest(BaseReqUtil.getBaseRequest(header.getOrgId()))
                .setOrgId(header.getOrgId()).setAccountId(accountId).setTokenId(tokenId).build();
        GetCanTransferResp resp = grpcAccountService.getCanTransferAmount(req);
        return GetCanTransferAmountResp.newBuilder().setAmount(resp.getAmount()).build();
    }

    public QueryBalanceResponse queryBalance(Header header, AccountTypeEnum accountTypeEnum,
                                             Integer accountIndex, List<String> tokenIds) {
        Long accountId = getAccountId(header.getOrgId(), header.getUserId(),
                AccountType.fromAccountTypeEnum(accountTypeEnum), accountIndex);
        List<Balance> balanceList = queryBalance(header, accountId, tokenIds);
        return QueryBalanceResponse.newBuilder().addAllBalances(balanceList).build();
    }

    public List<io.bhex.broker.grpc.account.Account> queryBalanceByAccountType(Header header,
                                                                               AccountType accountType, boolean filterIndex0) {
        return queryBalanceByAccountType(header, accountType, filterIndex0, false);
    }

    public List<io.bhex.broker.grpc.account.Account> queryBalanceByAccountType(Header header,
                                                                               AccountType accountType, boolean filterIndex0, boolean withShortDeadline) {
        List<io.bhex.broker.server.model.Account> localAccountList
                = accountMapper.queryByUserIdAndAccountType(header.getOrgId(), header.getUserId(),
                accountType.value());
        if (filterIndex0) {
            localAccountList = localAccountList.stream()
                    .filter(account -> account.getAccountIndex() > 0).collect(Collectors.toList());
        }
        GetBalanceDetailByAccountTypeRequest request = GetBalanceDetailByAccountTypeRequest
                .newBuilder()
                .setBaseRequest(GrpcRequestUtil.getBaseRequest(header))
                .setAccountType(getBhAccountType(accountType).getNumber())
                .build();
        List<BalanceDetail> balanceDetails = null;
        if (withShortDeadline) {
            balanceDetails = grpcBalanceService
                    .queryUserBalanceByAccountTypeWithShortDeadline(request).getBalanceDetailsList();
        } else {
            balanceDetails = grpcBalanceService.queryUserBalanceByAccountType(request)
                    .getBalanceDetailsList();
        }
        if (filterIndex0) {
            balanceDetails = balanceDetails.stream()
                    .filter(balanceDetail -> balanceDetail.getAccountIndex() > 0)
                    .collect(Collectors.toList());
        }
        Map<Long, List<BalanceDetail>> groupedBalance = balanceDetails.stream()
                .collect(Collectors.groupingBy(BalanceDetail::getAccountId));
        List<io.bhex.broker.grpc.account.Account> resultAccountList = Lists.newArrayList();
        for (Account account : localAccountList) {
            if (groupedBalance.containsKey(account.getAccountId())) {
                resultAccountList
                        .add(getAccount(account, groupedBalance.get(account.getAccountId()).stream()
                                .map(balanceDetail -> getBalance(header.getOrgId(), balanceDetail, null))
                                .collect(Collectors.toList())));
            } else {
                resultAccountList.add(getAccount(account, Lists.newArrayList()));
            }
        }
        return resultAccountList;
    }


    public List<Balance> queryBalance(Header header, Long accountId, List<String> tokenIds) {
        return queryBalance(header, accountId, tokenIds, false);
    }

    public List<Balance> queryBalance(Header header, Long accountId, List<String> tokenIds,
                                      boolean withShortDeadline) {
        tokenIds = tokenIds.stream().filter(tokenId -> !Strings.isNullOrEmpty(tokenId))
                .collect(Collectors.toList());
        GetBalanceDetailRequest request = GetBalanceDetailRequest.newBuilder()
                .setBaseRequest(GrpcRequestUtil.getBaseRequest(header))
                .setAccountId(accountId)
                .addAllTokenId(tokenIds)
                .build();
        List<BalanceDetail> balanceDetails;
        if (withShortDeadline) {
            balanceDetails = grpcBalanceService.getBalanceDetailWithShortDeadline(request)
                    .getBalanceDetailsList();
        } else {
            balanceDetails = grpcBalanceService.getBalanceDetail(request).getBalanceDetailsList();
        }
        GetPositionRequest getPositionRequest = GetPositionRequest.newBuilder()
                .setBrokerId(header.getOrgId())
                .setAccountId(accountId)
                .build();
        Map<String, Position> tmpPositionMap = new HashMap<>();
        try {
            PositionResponseList positionResponseList = null;
            if (withShortDeadline) {
                positionResponseList = grpcBalanceService
                        .getBalancePositionWithShortDeadline(getPositionRequest);
            } else {
                positionResponseList = grpcBalanceService.getBalancePosition(getPositionRequest);
            }
            List<Position> positionList = positionResponseList.getPositionListList();
            tmpPositionMap = positionList.stream()
                    .collect(Collectors.toMap(Position::getTokenId, position -> position, (q, p) -> q));
        } catch (Exception e) {
            log.error("get user(accountId:{}) balance position info error", accountId, e);
            //
        }
        List<Balance> balanceList = Lists.newArrayList();
        if (balanceDetails != null) {
            Map<String, Position> positionMap = tmpPositionMap;
            balanceList = balanceDetails.stream()
                    .filter(balanceDetail -> DecimalUtil.toBigDecimal(balanceDetail.getTotal()).compareTo(MIN_TOKEN_BALANCE_VALUE) > 0)
                    .map(balanceDetail -> getBalance(header.getOrgId(), balanceDetail, positionMap.get(balanceDetail.getTokenId())))
//                    .filter(balanceDetail -> Stream.of(IGNORE_TOKENS).noneMatch(tokenId -> tokenId.equalsIgnoreCase(balanceDetail.getTokenId())))
                    .collect(Collectors.toList());
        }
        return balanceList;
    }

    public Balance queryTokenBalance(Long orgId, Long accountId, String tokenId) {
        Balance balance = null;
        try {
            GetBalanceDetailRequest request = GetBalanceDetailRequest.newBuilder()
                    .setBaseRequest(BaseReqUtil.getBaseRequest(orgId))
                    .setAccountId(accountId)
                    .addTokenId(tokenId)
                    .build();
            List<BalanceDetail> balanceDetailList = grpcBalanceService.getBalanceDetail(request)
                    .getBalanceDetailsList();
            if (balanceDetailList.size() > 0) {
                balance = getBalance(orgId, balanceDetailList.get(0), null);
            }
        } catch (Exception e) {
            log.error(" queryTokenBalance exception: orgId:{} accountId:{} tokenId:{}", orgId,
                    accountId, tokenId, e);
        }
        return balance;
    }

    public QueryBalanceFlowResponse queryBalanceFlow(Header header, AccountTypeEnum accountType, Integer accountIndex,
                                                     List<String> tokenIds, List<Integer> balanceFlowTypes, Long fromFlowId, Long endFlowId, Long startTime, Long endTime,
                                                     Integer limit, boolean fromEsHistory) {
        tokenIds = tokenIds.stream().filter(StringUtils::isNoneEmpty).distinct()
                .collect(Collectors.toList());
        Long accountId;
        List<BusinessSubject> querySubjects = new ArrayList<>(PC_BALANCE_FLOW_SUBJECTS);
        if (accountType == AccountTypeEnum.COIN) {
            accountId = getMainIndexAccountId(header, accountIndex);
        } else if (accountType == AccountTypeEnum.OPTION) {
            // 如果为期权资产记录查询，则要展示交易 记录（仅展示数字货币的交易记录）交易手续费
            querySubjects.add(BusinessSubject.TRADE);
            querySubjects.add(BusinessSubject.FEE);
            if (CollectionUtils.isEmpty(tokenIds)) {
                tokenIds = basicService.getOptionCoinTokenIds();
            }
            accountId = getOptionIndexAccountId(header, accountIndex);
        } else if (accountType == AccountTypeEnum.FUTURE) {
            // 如果是期货资产记录查询，展示交易 记录交易手续费、资金费率结算记录，期货盈亏记录、资产划转记录
            querySubjects.add(BusinessSubject.TRADE);
            querySubjects.add(BusinessSubject.FEE);
            querySubjects.add(BusinessSubject.FUNDING_SETTLEMENT);
            querySubjects.add(BusinessSubject.PNL);
            querySubjects.add(BusinessSubject.USER_ACCOUNT_TRANSFER);
            querySubjects.add(BusinessSubject.CONTRACT_PRESENT);
            accountId = getFuturesIndexAccountId(header, accountIndex);
        } else if (accountType == AccountTypeEnum.MARGIN) {
            querySubjects.add(BusinessSubject.PAY_FROM_LOCK_PAY);
            querySubjects.add(BusinessSubject.MARGIN_CROSS_LOAN);
            querySubjects.add(BusinessSubject.MARGIN_CROSS_REPAY);
            querySubjects.add(BusinessSubject.MARGIN_CROSS_INTEREST);
            accountId = getMarginIndexAccountIdNoThrow(header, accountIndex);
        } else {
            // 默认为币币账户
            accountId = getMainAccountId(header);
        }
        // 如果请求里面有queryBalanceFlowType 那么查询条件设置为要查询的数据
        if (balanceFlowTypes != null && !balanceFlowTypes.isEmpty()) {
            List<BusinessSubject> forQuerySubjects = balanceFlowTypes.stream()
                    .map(BusinessSubject::forNumber)
                    .filter(Objects::nonNull)
                    .filter(querySubjects::contains)
                    .collect(Collectors.toList());
            if (forQuerySubjects != null && !forQuerySubjects.isEmpty()) {
                querySubjects = forQuerySubjects;
            }
        }

        // 如果账户ID为空直接返回
        if (accountId == null || accountId.equals(0l)) {
            return QueryBalanceFlowResponse.getDefaultInstance();
        }
        long sevenDaysBefore = System.currentTimeMillis() - 7 * 24 * 3600 * 1000;
        if (header.getOrgId() == 7070 && (startTime == null || startTime == 0 || startTime < sevenDaysBefore)) {
            startTime = sevenDaysBefore;
        }
        List<BalanceFlow> balanceFlowList = Lists.newArrayList();
        if (fromEsHistory) {
            List<io.bhex.broker.server.elasticsearch.entity.BalanceFlow> esBalanceFlowList =
                    IBalanceFlowHistoryService
                            .queryWithCondition(header, accountId, tokenIds, querySubjects, startTime,
                                    endTime, fromFlowId, endFlowId, limit);
            balanceFlowList = esBalanceFlowList.stream()
                    .map(flow -> BalanceFlow.newBuilder()
                            .setFlowId(flow.getBalanceFlowId())
                            .setAccountId(flow.getAccountId())
                            .setTokenId(flow.getTokenId())
                            .setTokenName(basicService.getTokenName(flow.getOrgId(), flow.getTokenId()))
                            .setBalanceFlowType(BusinessSubject.forNumber(flow.getBusinessSubject()).name())
                            .setChanged(flow.getChanged().stripTrailingZeros().toPlainString())
                            .setTotal(flow.getTotal().stripTrailingZeros().toPlainString())
//                            .setCreated(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS").parse(flow.getCreatedAt(), new ParsePosition(1)).getTime())
                            .setCreated(flow.getCreatedAt().getTime())
                            .setBusinessSubject(flow.getBusinessSubject())
                            .setSubBusinessSubject(flow.getSecondBusinessSubject())
                            .setFlowName(getBalanceFlowName(header.getOrgId(), flow.getBusinessSubject(),
                                    flow.getSecondBusinessSubject(), header.getLanguage()))
                            .build())
                    .collect(Collectors.toList());
        } else {
            GetBalanceFlowsWithPageRequest request = GetBalanceFlowsWithPageRequest.newBuilder()
                    .setBaseRequest(GrpcRequestUtil.getBaseRequest(header))
                    .setAccountId(accountId)
                    .addAllTokenIds(tokenIds.stream().filter(tokenId -> !Strings.isNullOrEmpty(tokenId))
                            .collect(Collectors.toList()))
                    .addAllBusinessSubject(querySubjects)
                    .setFromFlowId(fromFlowId)
                    .setEndFlowId(endFlowId)
                    .setStartTime(startTime)
                    .setEndTime(endTime)
                    .setLimit(limit)
                    .build();
            BalanceFlowsReply reply = grpcBalanceService.queryBalanceFlow(request);
            if (reply.getBalanceFlowDetailsCount() == 0) {
                return queryBalanceFlow(header, accountType, accountIndex, tokenIds, balanceFlowTypes,
                        fromFlowId, endFlowId, startTime, endTime, limit, true);
            }
            List<BalanceFlowDetail> balanceFlowDetailList = reply.getBalanceFlowDetailsList();
            if (balanceFlowDetailList != null && balanceFlowDetailList.size() > 0) {
                balanceFlowList = balanceFlowDetailList.stream()
                        .map(flow -> BalanceFlow.newBuilder()
                                .setFlowId(flow.getBalanceFlowId())
                                .setAccountId(flow.getAccountId())
                                .setTokenId(flow.getTokenInfo().getTokenId())
                                .setTokenName(basicService.getTokenName(header.getOrgId(), flow.getTokenInfo().getTokenId()))
                                .setBalanceFlowType(flow.getBusinessSubject().toString())
                                .setChanged(DecimalUtil.toBigDecimal(flow.getChanged()).stripTrailingZeros()
                                        .toPlainString())
                                .setTotal(DecimalUtil.toBigDecimal(flow.getTotal()).stripTrailingZeros()
                                        .toPlainString())
                                .setCreated(flow.getCreatedTime())
                                .setBusinessSubject(flow.getBusinessSubjectValue())
                                .setSubBusinessSubject(flow.getSecondBusinessSubject())
                                .setFlowName(
                                        getBalanceFlowName(header.getOrgId(), flow.getBusinessSubjectValue(),
                                                flow.getSecondBusinessSubject(), header.getLanguage()))
                                .build())
                        .collect(Collectors.toList());
            }
            if (reply.getBalanceFlowDetailsCount() < limit) {
                Long nestFromId = reply.getBalanceFlowDetailsList()
                        .get(reply.getBalanceFlowDetailsCount() - 1).getBalanceFlowId();
                List<io.bhex.broker.server.elasticsearch.entity.BalanceFlow> esBalanceFlowList =
                        IBalanceFlowHistoryService
                                .queryWithCondition(header, accountId, tokenIds, querySubjects, startTime,
                                        endTime, nestFromId, endFlowId,
                                        limit - reply.getBalanceFlowDetailsCount());
                balanceFlowList.addAll(esBalanceFlowList.stream()
                        .map(flow -> BalanceFlow.newBuilder()
                                .setFlowId(flow.getBalanceFlowId())
                                .setAccountId(flow.getAccountId())
                                .setTokenId(flow.getTokenId())
                                .setTokenName(basicService.getTokenName(flow.getOrgId(), flow.getTokenId()))
                                .setBalanceFlowType(
                                        BusinessSubject.forNumber(flow.getBusinessSubject()).name())
                                .setChanged(flow.getChanged().stripTrailingZeros().toPlainString())
                                .setTotal(flow.getTotal().stripTrailingZeros().toPlainString())
//                                .setCreated(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS").parse(flow.getCreatedAt(), new ParsePosition(1)).getTime())
                                .setCreated(flow.getCreatedAt().getTime())
                                .setBusinessSubject(flow.getBusinessSubject())
                                .setSubBusinessSubject(flow.getSecondBusinessSubject())
                                .setFlowName(
                                        getBalanceFlowName(header.getOrgId(), flow.getBusinessSubject(),
                                                flow.getSecondBusinessSubject(), header.getLanguage()))
                                .build())
                        .collect(Collectors.toList()));
            }
        }
        return QueryBalanceFlowResponse.newBuilder().addAllBalanceFlows(balanceFlowList).build();
    }

    private String getBalanceFlowName(Long orgId, Integer subject, Integer subBusinessSubject,
                                      String language) {
        String flowName = "";
        if (subBusinessSubject > 0) {
            flowName = subBusinessSubjectService
                    .getSubBusinessSubjectName(orgId, subject, subBusinessSubject, language);
        }
        return Strings.nullToEmpty(flowName);
    }

    public GetBrokerAccountListResponse getBrokerAccountList(Long brokerId, Long fromId,
                                                             Long beginTime, Long endTime, int limit) {
        Example example = Example.builder(Account.class)
                .orderByAsc("id")
                .build();

        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("orgId", brokerId);
        if (fromId != null && fromId > 0) {
            criteria.andGreaterThan("id", fromId);
        }

        if (beginTime != null && beginTime > 0) {
            criteria.andGreaterThanOrEqualTo("created", beginTime);
        }

        if (endTime != null && endTime > 0) {
            criteria.andLessThanOrEqualTo("created", endTime);
        }
        criteria.andEqualTo("accountType", AccountType.MAIN.value());

        List<Account> accountList = accountMapper
                .selectByExampleAndRowBounds(example, new RowBounds(0, limit));

        return GetBrokerAccountListResponse.newBuilder()
                .addAllAccounts(getSimpleAccountList(accountList))
                .build();
    }

    public GetBrokerAccountCountResponse getBrokerAccountCount(Long brokerId, Long beginTime,
                                                               Long endTime) {

        Example example = Example.builder(Account.class)
                .orderByAsc("id")
                .build();

        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("orgId", brokerId);
        if (beginTime != null && beginTime > 0) {
            criteria.andGreaterThanOrEqualTo("created", beginTime);
        }

        if (endTime != null && endTime > 0) {
            criteria.andLessThanOrEqualTo("created", endTime);
        }
        int count = accountMapper.selectCountByExample(example);

        return GetBrokerAccountCountResponse.newBuilder()
                .setCount(count)
                .build();
    }

    public VerifyBrokerAccountResponse verifyBrokerAccount(Long brokerId, List<Long> accountList) {
        if (CollectionUtils.isEmpty(accountList)) {
            return VerifyBrokerAccountResponse.getDefaultInstance();
        }

        Example example = Example.builder(Account.class)
                .orderByAsc("id")
                .build();

        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("orgId", brokerId);
        criteria.andIn("accountId", accountList);

        List<Account> list = accountMapper.selectByExample(example);
        return VerifyBrokerAccountResponse.newBuilder()
                .addAllAccounts(getSimpleAccountList(list))
                .build();
    }

    private Balance getBalance(Long orgId, BalanceDetail balanceDetail, Position position) {
        Rate rate = null;
        if (balanceDetail.getToken().getTokenType() != TokenTypeEnum.BH_CARD) {
            rate = basicService.getV3Rate(orgId, balanceDetail.getTokenId());
        }
        BigDecimal btcRate = (rate == null || Stream.of(TEST_TOKENS)
                .anyMatch(testToken -> testToken.equalsIgnoreCase(balanceDetail.getTokenId())))
                ? BigDecimal.ZERO : DecimalUtil.toBigDecimal(rate.getRatesMap().get("BTC"));
        BigDecimal usdtRate = (rate == null || Stream.of(TEST_TOKENS)
                .anyMatch(testToken -> testToken.equalsIgnoreCase(balanceDetail.getTokenId())))
                ? BigDecimal.ZERO : DecimalUtil.toBigDecimal(rate.getRatesMap().get("USDT"));
        return Balance.newBuilder()
                .setTokenId(balanceDetail.getToken().getTokenId())
                .setTokenName(basicService.getTokenName(orgId, balanceDetail.getToken().getTokenId()))
                .setFree(DecimalUtil.toBigDecimal(balanceDetail.getAvailable()).stripTrailingZeros()
                        .toPlainString())
                .setLocked(DecimalUtil.toBigDecimal(balanceDetail.getLocked()).stripTrailingZeros()
                        .toPlainString())
                .setTotal(DecimalUtil.toBigDecimal(balanceDetail.getTotal()).stripTrailingZeros()
                        .toPlainString())
                .setBtcValue(DecimalUtil.toBigDecimal(balanceDetail.getTotal()).multiply(btcRate)
                        .setScale(8, RoundingMode.DOWN).stripTrailingZeros().toPlainString())
                .setUsdtValue(DecimalUtil.toBigDecimal(balanceDetail.getTotal()).multiply(usdtRate)
                        .setScale(2, RoundingMode.DOWN).stripTrailingZeros().toPlainString())
                .setPosition(position == null ? "0"
                        : new BigDecimal(position.getTotal()).stripTrailingZeros().toPlainString())
                .setAvailablePosition(position == null ? "0"
                        : new BigDecimal(position.getAvailable()).stripTrailingZeros().toPlainString())
                .setLockedPosition(position == null ? "0"
                        : new BigDecimal(position.getLocked()).stripTrailingZeros().toPlainString())
                .setBalanceCreateAt(balanceDetail.getCreatedAt())
                .setBalanceUpdateAt(balanceDetail.getUpdatedAt())
                .build();
    }

    private io.bhex.broker.grpc.account.Account getAccount(
            io.bhex.broker.server.model.Account account, List<Balance> balanceList) {
        balanceList = balanceList == null ? Lists.newArrayList() : balanceList;
        BigDecimal btcValue = balanceList.stream()
                .filter(balance -> Stream.of(TEST_TOKENS)
                        .noneMatch(testToken -> testToken.equalsIgnoreCase(balance.getTokenId())))
                .map(a -> new BigDecimal(a.getBtcValue()))
                .reduce(BigDecimal::add).orElse(BigDecimal.ZERO);
        BigDecimal usdtValue = balanceList.stream()
                .filter(balance -> Stream.of(TEST_TOKENS)
                        .noneMatch(testToken -> testToken.equalsIgnoreCase(balance.getTokenId())))
                .map(a -> new BigDecimal(a.getUsdtValue()))
                .reduce(BigDecimal::add).orElse(BigDecimal.ZERO);
        return io.bhex.broker.grpc.account.Account.newBuilder()
                .setAccountId(account.getAccountId())
                .setAccountName(Strings.nullToEmpty(account.getAccountName()))
                .setAccountType(account.getAccountType())
                .setAccountIndex(account.getAccountIndex())
                .setAuthorizedOrg(account.getIsAuthorizedOrg() == 1)
                .addAllBalance(balanceList)
                .setBtcValue(btcValue.setScale(BrokerServerConstants.BTC_PRECISION, RoundingMode.HALF_DOWN).stripTrailingZeros().toPlainString())
                .setUsdtValue(usdtValue.setScale(BrokerServerConstants.USDT_PRECISION, RoundingMode.HALF_DOWN).stripTrailingZeros().toPlainString())
                .build();
    }

    public List<SimpleAccount> getSimpleAccountList(List<Account> accountList) {
        if (CollectionUtils.isEmpty(accountList)) {
            return Lists.newArrayList();
        }
        List<SimpleAccount> list = Lists.newArrayList();
        for (Account account : accountList) {
            list.add(getSimpleAccount(account));
        }
        return list;
    }

    public SimpleAccount getSimpleAccount(Account account) {
        return SimpleAccount.newBuilder()
                .setId(account.getId())
                .setAccountId(account.getAccountId())
                .setUserId(account.getUserId())
                .build();
    }

    public BigDecimal exchangeToken(Long orgId, String fromToken, String targetToken,
                                    BigDecimal fromAmount) {
        if (StringUtils.isBlank(fromToken) || StringUtils.isEmpty(targetToken)) {
            return BigDecimal.ZERO;
        }

        if (fromAmount == null || fromAmount.compareTo(BigDecimal.ZERO) <= 0) {
            return BigDecimal.ZERO;
        }

        if (fromToken.equals(targetToken)) {
            return fromAmount;
        }

        Rate rate = basicService.getV3Rate(orgId, fromToken);
        if (rate == null) {
            return BigDecimal.ZERO;
        }

        // 如果转换的目标就是BTC USDT ETH , 直接用来源的Token获取汇率，用数量乘以响应的汇率值，得到的数量就是转换的数量
        if (BrokerServerConstants.FX_RATE_BASE_TOKENS.contains(targetToken)) {
            switch (targetToken) {
                case "BTC":
                case "USDT":
                case "ETH":
                    return fromAmount
                            .multiply(DecimalUtil.toBigDecimal(rate.getRatesMap().get(targetToken)));
                default:
                    return BigDecimal.ZERO;
            }
        }
        // 来源币种兑换BTC的价格
        BigDecimal btcPrice = rate.getRatesMap().get("BTC") == null ? BigDecimal.ZERO
                : DecimalUtil.toBigDecimal(rate.getRatesMap().get("BTC"));
        if (btcPrice.compareTo(BigDecimal.ZERO) <= 0) {
            return BigDecimal.ZERO;
        }

        // 计算来源币种折算成BTC的数量
        BigDecimal fromBtcAmount = btcPrice.multiply(fromAmount);

        // 如果转换的目标不是BTC ,用转换的目标去获取汇率
        //  （1 / 目标Token的BTC价格） * BTC数量
        Rate targetRate = basicService.getV3Rate(orgId, targetToken);
        BigDecimal targetBtcPrice = targetRate.getRatesMap().get("BTC") == null ? BigDecimal.ZERO
                : DecimalUtil.toBigDecimal(targetRate.getRatesMap().get("BTC"));
        if (targetBtcPrice.compareTo(BigDecimal.ZERO) <= 0) {
            return BigDecimal.ZERO;
        }
        return BigDecimal.ONE.divide(targetBtcPrice, 18, RoundingMode.DOWN).multiply(fromBtcAmount);
    }


    public BalanceFlow balanceFlowDetailToBalanceFlow(BalanceFlowDetail flow) {
        return BalanceFlow.newBuilder()
                .setFlowId(flow.getBalanceFlowId())
                .setAccountId(flow.getAccountId())
                .setTokenId(flow.getTokenInfo().getTokenId())
                .setTokenName(basicService.getTokenName(flow.getOrgId(), flow.getTokenInfo().getTokenId()))
                .setBalanceFlowType(flow.getBusinessSubject().toString())
                .setChanged(
                        DecimalUtil.toBigDecimal(flow.getChanged()).stripTrailingZeros().toPlainString())
                .setTotal(
                        DecimalUtil.toBigDecimal(flow.getTotal()).stripTrailingZeros().toPlainString())
                .setCreated(flow.getCreatedTime())
                .build();
    }

    // 通过tokenid 获取 symbol id
    private Symbol getSymbolIdByTokenId(String tokenId, Long orgId) {
        ImmutableMap<String, Symbol> optionSymbolMap = basicService.getTokenSymbolOptionMap();
        Symbol symbol = optionSymbolMap.get(orgId + "_" + tokenId);

        return symbol;
    }

    // 只返回为期权的token detail
    private TokenDetail getTokenDetailByTokenId(String tokenId) {
        ImmutableMap<String, TokenDetail> optionTokenDetailMap = basicService
                .getTokenDetailOptionMap();
        return optionTokenDetailMap.get(tokenId);
    }

    public OptionAssetListResponse getOptionAssetList(OptionAssetRequest request) {
        if (!BrokerService.checkModule(request.getHeader(), FunctionModule.OPTION)) {
            return OptionAssetListResponse.getDefaultInstance();
        }
        Header header = request.getHeader();
        AccountTypeEnum accountTypeEnum = request.getAccountType();
        Integer accountIndex = request.getAccountIndex();
//        Long accountId = header.getPlatform() == Platform.OPENAPI && accountIndex != 0 ?
//                getAccountId(header.getOrgId(), header.getUserId(), AccountType.fromAccountTypeEnum(accountTypeEnum), accountIndex) : getOptionAccountId(header);
        Long accountId = getOptionAccountId(header);
        return getOptionAssetList(accountId, request.getHeader().getOrgId(),
                request.getTokenIdsList(), request.getFilterExplore());
    }

    public OptionAssetListResponse getOptionAssetList(Long optionAccountId, Long orgId,
                                                      List<String> tokenIds, boolean filterExplore) {
        return getOptionAssetList(optionAccountId, orgId, tokenIds, filterExplore, false);
    }

    public OptionAssetListResponse getOptionAssetList(Long optionAccountId, Long orgId,
                                                      List<String> tokenIds, boolean filterExplore, boolean withShortDeadline) {
        if (!BrokerService.checkModule(orgId, FunctionModule.OPTION)) {
            return OptionAssetListResponse.getDefaultInstance();
        }
        tokenIds = CollectionUtils.isEmpty(tokenIds) ? new ArrayList<>() : tokenIds;
        OptionAssetReq bhRequest = OptionAssetReq.newBuilder()
                .setBaseRequest(BaseReqUtil.getBaseRequest(orgId))
                .setAccountId(optionAccountId)
                .addAllTokenIds(tokenIds)
                .setFilterExplore(filterExplore)
                .build();
        OptionAssetList optionAssetList;
        if (withShortDeadline) {
            optionAssetList = grpcBalanceService.getOptionAssetListWithShortDeadline(bhRequest);
        } else {
            optionAssetList = grpcBalanceService.getOptionAssetList(bhRequest);
        }
        List<OptionAssetListResponse.OptionAsset> assetList = new ArrayList<>();
        optionAssetList.getOptionAssetList().forEach(optionAsset -> {
            Symbol symbol = getSymbolIdByTokenId(optionAsset.getTokenId(), orgId);
            // 持仓量为0的期权不显示
            if (Objects.nonNull(symbol) && !(
                    BigDecimal.ZERO.compareTo(new BigDecimal(optionAsset.getTotal())) == 0)) {
                String symbolId = symbol.getSymbolId();
                Long exchangeId = symbol.getExchangeId();

                //当前价格
                BigDecimal currentOptionPrice = optionPriceService
                        .getCurrentOptionPrice(symbolId, exchangeId, orgId);
                //全部数量
                BigDecimal total = new BigDecimal(optionAsset.getTotal());
                //期权估值
                BigDecimal valuation = optionPriceService.getValuation(total, currentOptionPrice);
                //保证金
                BigDecimal margin = new BigDecimal(optionAsset.getMargin());
                //持仓均价
                BigDecimal cost = new BigDecimal(optionAsset.getCost());
                //账户权益
                BigDecimal equity = optionPriceService.getEquity(total, margin, currentOptionPrice);
                //持仓盈亏
                BigDecimal profit = optionPriceService.getProfit(total, cost, currentOptionPrice);
                //盈亏百分比
                BigDecimal profitPercentage = optionPriceService
                        .getProfitPercentage(total, cost, margin, currentOptionPrice);

                assetList.add(OptionAssetListResponse.OptionAsset.newBuilder()
                        .setTokenId(optionAsset.getTokenId())
                        .setTokenName(optionAsset.getTokenName())
                        .setTokenFullName(optionAsset.getTokenFullName())
                        .setIsCall(optionAsset.getIsCall())
                        .setCurrentPrice(currentOptionPrice
                                .setScale(BrokerServerConstants.OPTION_PRICE_PRECISION,
                                        BigDecimal.ROUND_DOWN).toPlainString())
                        .setQuantity(DecimalUtil.toTrimString(total
                                .setScale(BrokerServerConstants.OPTION_QUANTITY_PRECISION,
                                        BigDecimal.ROUND_DOWN)))
                        .setValuation(DecimalUtil.toTrimString(valuation
                                .setScale(BrokerServerConstants.BASE_OPTION_PRECISION,
                                        BigDecimal.ROUND_DOWN)))
                        .setMargin(DecimalUtil.toTrimString(margin
                                .setScale(BrokerServerConstants.BASE_OPTION_PRECISION,
                                        BigDecimal.ROUND_DOWN)))
                        .setEquity(DecimalUtil.toTrimString(equity
                                .setScale(BrokerServerConstants.BASE_OPTION_PRECISION,
                                        BigDecimal.ROUND_DOWN)))
                        .setProfit(profit.setScale(BrokerServerConstants.OPTION_PROFIT_PRECISION,
                                BigDecimal.ROUND_DOWN).toPlainString())
                        .setProfitPercentage(profitPercentage
                                .setScale(BrokerServerConstants.OPTION_PROFIT_PRECISION,
                                        BigDecimal.ROUND_DOWN).toPlainString())
                        .setUnit(symbol.getQuoteTokenId())
                        .build());
            }

        });
        OptionAssetListResponse reply = OptionAssetListResponse.newBuilder()
                .addAllOptionAsset(assetList)
                .build();

        return reply;
    }

    public OptionAccountDetailListResponse getOptionAccountDetail(
            OptionAccountDetailRequest request) {
        if (!BrokerService.checkModule(request.getHeader(), FunctionModule.OPTION)) {
            return OptionAccountDetailListResponse.getDefaultInstance();
        }
        Header header = request.getHeader();
        AccountTypeEnum accountTypeEnum = request.getAccountType();
        Integer accountIndex = request.getAccountIndex();
//        Long accountId = header.getPlatform() == Platform.OPENAPI && accountIndex != 0 ?
//                getAccountId(header.getOrgId(), header.getUserId(), AccountType.fromAccountTypeEnum(accountTypeEnum), accountIndex) : getOptionAccountId(header);
        Long accountId = getOptionAccountId(header);
        return getOptionAccountDetail(request.getHeader().getOrgId(), accountId,
                request.getTokenIdsList(), request.getFilterExplore());
    }

    public OptionAccountDetailListResponse getOptionAccountDetail(Long orgId, Long optionAccountId,
                                                                  List<String> tokenIds, boolean filterExplore) {
        return getOptionAccountDetail(orgId, optionAccountId, tokenIds, filterExplore, false);
    }

    // 获取期权钱包内的余额
    public OptionAccountDetailListResponse getOptionAccountDetail(Long orgId, Long optionAccountId,
                                                                  List<String> tokenIds, boolean filterExplore, boolean withShortDeadline) {
        if (!BrokerService.checkModule(orgId, FunctionModule.OPTION)) {
            return OptionAccountDetailListResponse.getDefaultInstance();
        }
        tokenIds = CollectionUtils.isEmpty(tokenIds) ? new ArrayList<>() : tokenIds;
        OptionAccountDetailReq bhRequest = OptionAccountDetailReq.newBuilder()
                .setBaseRequest(BaseReqUtil.getBaseRequest(orgId))
                .setAccountId(optionAccountId)
                .addAllTokenIds(tokenIds)
                .setFilterExplore(filterExplore)
                .build();
        OptionAccountDetailList detailList;
        if (withShortDeadline) {
            detailList = grpcBalanceService.getOptionAccountDetailWithShortDeadline(bhRequest);
        } else {
            detailList = grpcBalanceService.getOptionAccountDetail(bhRequest);
        }
        List<OptionAccountDetailListResponse.OptionAccountDetail> accountDetails = new ArrayList<>();
        detailList.getOptionAccountDetailList().forEach(detail -> {
            TokenDetail tokenDetail = getTokenDetailByTokenId(detail.getTokenId());
            if (Objects.isNull(tokenDetail) || (Objects.nonNull(tokenDetail) && !TokenCategory
                    .isOption(tokenDetail.getType().name()))) {
                // 换算为USDT价格
                Rate rate = basicService.getV3Rate(orgId, detail.getTokenId());
                BigDecimal btcUsdtPrice = rate == null ? BigDecimal.ZERO
                        : DecimalUtil.toBigDecimal(rate.getRatesMap().get("USDT"));
                BigDecimal usdtValue = new BigDecimal(detail.getAvailable())
                        .add(new BigDecimal(detail.getLocked())).multiply(btcUsdtPrice);

                accountDetails.add(OptionAccountDetailListResponse.OptionAccountDetail.newBuilder()
                        .setTokenId(detail.getTokenId())
                        .setTokenName(detail.getTokenName())
                        .setTokenFullName(detail.getTokenFullName())
                        .setTotal(DecimalUtil.toTrimString(new BigDecimal(detail.getTotal())
                                .setScale(BrokerServerConstants.BASE_OPTION_PRECISION,
                                        BigDecimal.ROUND_DOWN)))
                        .setAvailable(DecimalUtil.toTrimString(new BigDecimal(detail.getAvailable())
                                .setScale(BrokerServerConstants.BASE_OPTION_PRECISION,
                                        BigDecimal.ROUND_DOWN)))
                        .setLocked(DecimalUtil.toTrimString(new BigDecimal(detail.getLocked())
                                .setScale(BrokerServerConstants.BASE_OPTION_PRECISION,
                                        BigDecimal.ROUND_DOWN)))
                        .setMargin(DecimalUtil.toTrimString(new BigDecimal(detail.getMargin())
                                .setScale(BrokerServerConstants.BASE_OPTION_PRECISION,
                                        BigDecimal.ROUND_DOWN)))
                        .setUsdtValue(DecimalUtil.toTrimString(usdtValue
                                .setScale(BrokerServerConstants.USDT_PRECISION,
                                        BigDecimal.ROUND_DOWN)))
                        .build());
            }
        });

        return OptionAccountDetailListResponse.newBuilder()
                .addAllOptionAccountDetail(accountDetails)
                .build();
    }


    // 总资产折合（USDT计价）
    public GetAllAssetInfoResponse getAllAssetInfo(GetAllAssetInfoRequest request) {
        boolean filterExplore = request.getFilterExplore();
        Long mainAccountId = getMainAccountId(request.getHeader());
        Long optionAccountId = getOptionAccountId(request.getHeader());
        Long futuresAccountId = getFuturesAccountId(request.getHeader());
        Long marginAccountId = null;
        try {
            marginAccountId = getMarginAccountIdNoThrow(request.getHeader().getOrgId(), request.getHeader().getUserId(), DEFAULT_ACCOUNT_INDEX);
        } catch (Exception e) {
        }

        Long orgId = request.getHeader().getOrgId();
        GetAllAssetInfoResponse.Builder responseBuilder = GetAllAssetInfoResponse.newBuilder();
        if (Objects.nonNull(mainAccountId) && Objects.nonNull(optionAccountId)) {
            Rate rate = basicService.getV3Rate(orgId, BTC_TOKEN_ID);
            BigDecimal btcUsdtRate = rate == null ? BigDecimal.ZERO
                    : DecimalUtil.toBigDecimal(rate.getRatesMap().get("USDT"));
            rate = basicService.getV3Rate(orgId, "USDT");
            BigDecimal usdtBtcRate = rate == null ? BigDecimal.ZERO
                    : DecimalUtil.toBigDecimal(rate.getRatesMap().get("BTC"));

            BigDecimal coinBTCTotal = BigDecimal.ZERO;
            BigDecimal coinUSDTTotal = BigDecimal.ZERO;

            BigDecimal optionBTCTotal = BigDecimal.ZERO;
            BigDecimal optionUSDTTotal = BigDecimal.ZERO;

            BigDecimal optionCoinBTCTotal = BigDecimal.ZERO;
            BigDecimal optionCoinUSDTTotal = BigDecimal.ZERO;

            BigDecimal financeBTCTotal = BigDecimal.ZERO;
            BigDecimal financeUSDTTotal = BigDecimal.ZERO;

            BigDecimal marginBTCTotal = BigDecimal.ZERO;
            BigDecimal marginUSDTToal = BigDecimal.ZERO;

            BigDecimal stakingBTCTotal = BigDecimal.ZERO;
            BigDecimal stakingUSDTTotal = BigDecimal.ZERO;


            // 1.获取钱包资产列表;计算资产折合USDT
            List<Balance> balances = queryBalance(request.getHeader(), mainAccountId,
                    Lists.newArrayList(), true);
            if (filterExplore) {   //todo: 过滤BUSDT
                balances = balances.stream()
                        .filter(balance -> Stream.of(TEST_TOKENS)
                                .noneMatch(testToken -> testToken.equalsIgnoreCase(balance.getTokenId())))
//                        .filter(t -> !t.getTokenId().equalsIgnoreCase(ExploreUtil.BUSDT))
                        .collect(Collectors.toList());
            }
            for (Balance balance : balances) {
                coinBTCTotal = coinBTCTotal.add(new BigDecimal(balance.getBtcValue()));
                coinUSDTTotal = coinUSDTTotal.add(new BigDecimal(balance.getUsdtValue()));
            }
            if (marginAccountId != null) {
                //杠杆资产
                List<Balance> marginBalances = queryBalance(request.getHeader(), marginAccountId, Lists.newArrayList(), true);

                if (filterExplore) {
                    marginBalances = marginBalances.stream()
                            .filter(balance -> Stream.of(TEST_TOKENS)
                                    .noneMatch(testToken -> testToken.equalsIgnoreCase(balance.getTokenId())))
//                        .filter(t -> !t.getTokenId().equalsIgnoreCase(ExploreUtil.BUSDT))
                            .collect(Collectors.toList());
                }
                for (Balance marginBalance : marginBalances) {
                    marginBTCTotal = marginBTCTotal.add(new BigDecimal(marginBalance.getBtcValue()));
                    marginUSDTToal = marginUSDTToal.add(new BigDecimal(marginBalance.getUsdtValue()));
                }

                //获取已借资产值
                Pair<BigDecimal, BigDecimal> loanAsset = this.getLoanAsset(orgId, marginAccountId);
                BigDecimal btcLoanAsset = loanAsset.getLeft();
                BigDecimal usdtLoanAsset = loanAsset.getRight();

                //杠杆资产减去已借资产值，取得净资产值
                marginBTCTotal = marginBTCTotal.subtract(btcLoanAsset);
                marginUSDTToal = marginUSDTToal.subtract(usdtLoanAsset);

            }
            //期货资产
            AssetFutures assetFutures = assetFuturesService
                    .getAssetFutures(request.getHeader(), futuresAccountId);
            BigDecimal futuresCoinUSDTTotal = (assetFutures != null ? assetFutures
                    .getFuturesCoinUSDTTotal() : BigDecimal.ZERO);
            BigDecimal futuresCoinBTCTotal = (assetFutures != null ? assetFutures
                    .getFuturesCoinBTCTotal() : BigDecimal.ZERO);

            BigDecimal activeCoinUSDTTotal = BigDecimal.ZERO;
            BigDecimal activeCoinBTCTotal = BigDecimal.ZERO;

            //队长活动账户资产
//            if (hobbitActiveAccountId != null && hobbitActiveAccountId > 0) {
//                AssetHobbitActive assetActive = assetFuturesService.getAssetActive(request.getHeader(), hobbitActiveAccountId);
//                activeCoinUSDTTotal = (assetActive != null ? assetActive
//                        .getActiveUSDTTotal() : BigDecimal.ZERO);
//                activeCoinBTCTotal = (assetActive != null ? assetActive
//                        .getActiveCoinBTCTotal() : BigDecimal.ZERO);
//            }

            // TODO 2.获取期权资产列表
            // 仓位权益折合: 所以期权的仓位权益总和
//            OptionAssetListResponse optionAssetList = getOptionAssetList(optionAccountId, orgId,
//                    null, filterExplore, true);
//            List<OptionAssetListResponse.OptionAsset> optionAssets = optionAssetList
//                    .getOptionAssetList();
//
//            for (OptionAssetListResponse.OptionAsset optionAsset : optionAssets) {
////                if (Stream.of(TEST_TOKENS).anyMatch(testToken -> testToken.equalsIgnoreCase(optionAsset.getTokenId()))) {
////                    continue;
////                }
//                // 非USDT需要汇率转换
//                Symbol symbol = getSymbolIdByTokenId(optionAsset.getTokenId(), orgId);
//                String quoteTokenId = symbol.getQuoteTokenId();
//                Rate quoteRate = basicService.getV3Rate(orgId, quoteTokenId);
//                BigDecimal price = quoteRate == null ? BigDecimal.ZERO
//                        : DecimalUtil.toBigDecimal(quoteRate.getRatesMap().get("USD"));
//                optionUSDTTotal = optionUSDTTotal
//                        .add(new BigDecimal(optionAsset.getEquity()).multiply(price));
//            }
//            optionBTCTotal = optionUSDTTotal.multiply(usdtBtcRate);

            // 3.获取期权钱包资产
            // 期权资产估值: 可用+冻结+仓位权益折合
//            OptionAccountDetailListResponse optionAccountDetail = getOptionAccountDetail(
//                    request.getHeader().getOrgId(), optionAccountId, null, filterExplore, true);
//            List<OptionAccountDetailListResponse.OptionAccountDetail> optionAccountDetails = optionAccountDetail
//                    .getOptionAccountDetailList();
//
//            for (OptionAccountDetailListResponse.OptionAccountDetail optionCoin : optionAccountDetails) {
//                // 非USDT需要汇率转换
//                Rate tokenRate = basicService.getV3Rate(orgId, optionCoin.getTokenId());
//                BigDecimal price = tokenRate == null ? BigDecimal.ZERO
//                        : DecimalUtil.toBigDecimal(tokenRate.getRatesMap().get("USD"));
//                optionCoinUSDTTotal = optionCoinUSDTTotal
//                        .add(new BigDecimal(optionCoin.getAvailable()).multiply(price));
//                optionCoinUSDTTotal = optionCoinUSDTTotal
//                        .add(new BigDecimal(optionCoin.getLocked()).multiply(price));
//            }
//            optionCoinBTCTotal = optionCoinUSDTTotal.multiply(usdtBtcRate);

            //币多多资产 包含在理财资产中
            /*List<FinanceWallet> financeWalletList = financeWalletService
                    .queryWalletBalance(request.getHeader().getOrgId(),
                            request.getHeader().getUserId());
            financeBTCTotal = financeWalletList.stream().map(FinanceWallet::getBtcValue)
                    .reduce(BigDecimal.ZERO, BigDecimal::add);
            financeUSDTTotal = financeWalletList.stream().map(FinanceWallet::getUsdtValue)
                    .reduce(BigDecimal.ZERO, BigDecimal::add);*/

            //理财资产
            GetStakingAssetListRequest getStakingAssetListRequest = GetStakingAssetListRequest.newBuilder()
                    .setOrgId(request.getHeader().getOrgId())
                    .setUserId(request.getHeader().getUserId())
                    .setAccountId(mainAccountId)
                    .build();
            GetStakingAssetListReply getStakingAssetListReply = stakingProductService.getStakingAssetList(getStakingAssetListRequest);
            stakingBTCTotal = getStakingAssetListReply.getAssetsList().stream().filter(p -> p.getProductType() != StakingProductType.LOCK_POSITION_VALUE).map(p -> new BigDecimal(p.getBtcValue()))
                    .reduce(BigDecimal.ZERO, BigDecimal::add);
            stakingUSDTTotal = getStakingAssetListReply.getAssetsList().stream().filter(p -> p.getProductType() != StakingProductType.LOCK_POSITION_VALUE).map(p -> new BigDecimal(p.getUsdtValue()))
                    .reduce(BigDecimal.ZERO, BigDecimal::add);

            // 4.计算子账户资产
            List<io.bhex.broker.grpc.account.Account> mainAccountList = queryBalanceByAccountType(
                    request.getHeader(), AccountType.MAIN, true);
            BigDecimal subAccountBtcValue = mainAccountList.stream()
                    .map(account -> new BigDecimal(account.getBtcValue()))
                    .reduce(BigDecimal.ZERO, BigDecimal::add);
            BigDecimal subAccountUSDTValue = mainAccountList.stream()
                    .map(account -> new BigDecimal(account.getUsdtValue()))
                    .reduce(BigDecimal.ZERO, BigDecimal::add);

            List<io.bhex.broker.grpc.account.Account> futuresAccountList = queryBalanceByAccountType(
                    request.getHeader(), AccountType.FUTURES, true);
            BigDecimal futuresSubAccountBtcValue = futuresAccountList.stream()
                    .map(account -> new BigDecimal(account.getBtcValue()))
                    .reduce(BigDecimal.ZERO, BigDecimal::add);
            BigDecimal futuresSubAccountUSDTValue = futuresAccountList.stream()
                    .map(account -> new BigDecimal(account.getUsdtValue()))
                    .reduce(BigDecimal.ZERO, BigDecimal::add);

//            List<io.bhex.broker.grpc.account.Account> marginAccountList = queryBalanceByAccountType(request.getHeader(), AccountType.MARGIN, true);
//            BigDecimal marginSubBtcAccount = marginAccountList.stream()
//                    .map(margin -> new BigDecimal(margin.getBtcValue()))
//                    .reduce(BigDecimal.ZERO, BigDecimal::add);
//            BigDecimal marginSubAccountUSDTValue = marginAccountList.stream()
//                    .map(margin -> new BigDecimal(margin.getUsdtValue()))
//                    .reduce(BigDecimal.ZERO, BigDecimal::add);

            // 全部资产总和
            BigDecimal total = coinUSDTTotal
                    .add(optionUSDTTotal).add(optionCoinUSDTTotal)
                    .add(futuresCoinUSDTTotal).add(financeUSDTTotal)
                    .add(subAccountUSDTValue).add(futuresSubAccountUSDTValue)
                    .add(activeCoinUSDTTotal)
                    .add(marginUSDTToal)
//                    .add(marginSubAccountUSDTValue)
                    .add(stakingUSDTTotal);

            BigDecimal totalBtc = coinBTCTotal
                    .add(optionBTCTotal).add(optionCoinBTCTotal)
                    .add(futuresCoinBTCTotal).add(financeBTCTotal)
                    .add(subAccountBtcValue).add(futuresSubAccountBtcValue)
                    .add(activeCoinBTCTotal)
                    .add(marginBTCTotal)
//                    .add(marginSubBtcAccount)
                    .add(stakingBTCTotal);

            responseBuilder.setCoinAsset(DecimalUtil.toTrimString(coinUSDTTotal
                    .setScale(BrokerServerConstants.USDT_PRECISION, BigDecimal.ROUND_DOWN)));
            responseBuilder.setCoinBtcAsset(DecimalUtil.toTrimString(coinBTCTotal
                    .setScale(BrokerServerConstants.BTC_PRECISION, BigDecimal.ROUND_DOWN)));
            responseBuilder.setOptionAsset(DecimalUtil.toTrimString(optionUSDTTotal
                    .setScale(BrokerServerConstants.USDT_PRECISION, BigDecimal.ROUND_DOWN)));
            responseBuilder.setOptionBtcAsset(DecimalUtil.toTrimString(optionBTCTotal
                    .setScale(BrokerServerConstants.BTC_PRECISION, BigDecimal.ROUND_DOWN)));
            responseBuilder.setOptionCoinAsset(DecimalUtil.toTrimString(optionCoinUSDTTotal
                    .setScale(BrokerServerConstants.USDT_PRECISION, BigDecimal.ROUND_DOWN)));
            responseBuilder.setOptionCoinBtcAsset(DecimalUtil.toTrimString(optionCoinBTCTotal
                    .setScale(BrokerServerConstants.BTC_PRECISION, BigDecimal.ROUND_DOWN)));
            responseBuilder.setFuturesCoinAsset(DecimalUtil.toTrimString(futuresCoinUSDTTotal
                    .setScale(BrokerServerConstants.USDT_PRECISION, BigDecimal.ROUND_DOWN)));
            responseBuilder.setFuturesCoinBtcAsset(DecimalUtil.toTrimString(futuresCoinBTCTotal
                    .setScale(BrokerServerConstants.BTC_PRECISION, BigDecimal.ROUND_DOWN)));
            responseBuilder.setFinanceAsset(DecimalUtil.toTrimString(financeUSDTTotal
                    .setScale(BrokerServerConstants.USDT_PRECISION, BigDecimal.ROUND_DOWN)));
            responseBuilder.setFinanceBtcAsset(DecimalUtil.toTrimString(financeBTCTotal
                    .setScale(BrokerServerConstants.BTC_PRECISION, BigDecimal.ROUND_DOWN)));
            responseBuilder.setTotalAsset(DecimalUtil.toTrimString(total
                    .setScale(BrokerServerConstants.USDT_PRECISION, BigDecimal.ROUND_DOWN)));
            responseBuilder.setTotalBtcAsset(DecimalUtil.toTrimString(totalBtc
                    .setScale(BrokerServerConstants.BTC_PRECISION, BigDecimal.ROUND_DOWN)));
            responseBuilder.setActiveCoinAsset(DecimalUtil.toTrimString(activeCoinUSDTTotal
                    .setScale(BrokerServerConstants.USDT_PRECISION, BigDecimal.ROUND_DOWN)));
            responseBuilder.setActiveCoinBtcAsset(DecimalUtil.toTrimString(activeCoinBTCTotal
                    .setScale(BrokerServerConstants.BTC_PRECISION, BigDecimal.ROUND_DOWN)));
            responseBuilder.setStakingAsset(DecimalUtil.toTrimString(stakingUSDTTotal
                    .setScale(BrokerServerConstants.USDT_PRECISION, BigDecimal.ROUND_DOWN)));
            responseBuilder.setStakingBtcAsset(DecimalUtil.toTrimString(stakingBTCTotal
                    .setScale(BrokerServerConstants.BTC_PRECISION, BigDecimal.ROUND_DOWN)));

            responseBuilder.addAllSubAccountBalance(mainAccountList);
            responseBuilder.addAllSubAccountBalance(futuresAccountList);
        } else {
            log.error("User does not exist. userId:{}", request.getHeader().getUserId());
            throw new BrokerException(BrokerErrorCode.USER_NOT_EXIST);
        }
        return responseBuilder.build();
    }

    public GetOptionTradeableResponse getOptionTradeAble(GetOptionTradeableRequest request) {
        if (!BrokerService.checkModule(request.getHeader(), FunctionModule.OPTION)) {
            return GetOptionTradeableResponse.getDefaultInstance();
        }
        Long optionAccountId = getOptionAccountId(request.getHeader());
        List<String> baseTokenIds = request.getTokenIdsList();
        Long orgId = request.getHeader().getOrgId();

        if (CollectionUtils.isEmpty(baseTokenIds)) {
            return GetOptionTradeableResponse.newBuilder().build();
        }

        //期权资产(base token balance)
        Map<String, OptionAssetList.OptionAsset> baseBalanceMap = getOptionBaseBalanceMap(
                optionAccountId, request.getHeader().getOrgId());

        //base token ids
        List<TokenDetail> baseTokens = baseTokenIds
                .stream()
                .map(tokenId -> basicService.getTokenDetailOptionMap().get(tokenId))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        //quote token ids
        List<String> quoteTokenIds = getQuoteTokenIds(orgId, baseTokenIds);

        //当前资产余额（币) (quote token balance)
        Map<String, OptionAccountDetailList.OptionAccountDetail> quoteTokenBalanceMap = getOptionQuoteBalanceMap(
                optionAccountId, quoteTokenIds, request.getHeader().getOrgId());

        //期权资产、当前资产余额 --计算--> 可卖期权，当前资产余额
        List<OptionBalance> result = baseTokens
                .stream()
                .map(token -> {
                    String baseTokenId = token.getTokenId();
                    Symbol symbol = basicService.getTokenSymbolOptionMap()
                            .get(String.format("%s_%s", orgId, baseTokenId));
                    if (symbol == null) {
                        return null;
                    }
                    SymbolDetail symbolDetail = basicService
                            .getSymbolDetailOption(symbol.getExchangeId(), symbol.getSymbolId());
                    if (symbolDetail == null) {
                        return null;
                    }

                    OptionAssetList.OptionAsset baseBalance = baseBalanceMap.get(baseTokenId);
                    OptionAccountDetailList.OptionAccountDetail quoteBalance = quoteTokenBalanceMap
                            .get(symbol.getQuoteTokenId());

                    BigDecimal baseBalanceTotal = (baseBalance != null ? new BigDecimal(
                            baseBalance.getTotal()) : BigDecimal.ZERO);
                    BigDecimal baseBalanceAvailable = (baseBalance != null ? new BigDecimal(
                            baseBalance.getAvailable()) : BigDecimal.ZERO);
                    BigDecimal baseAvailPosition = (baseBalance != null ? new BigDecimal(
                            baseBalance.getAvailPosition()) : BigDecimal.ZERO);
                    BigDecimal baseBalanceMargin = (baseBalance != null ? new BigDecimal(
                            baseBalance.getMargin()) : BigDecimal.ZERO);

                    BigDecimal quoteBalanceTotal = (quoteBalance != null ? new BigDecimal(
                            quoteBalance.getTotal()) : BigDecimal.ZERO);
                    BigDecimal quoteBalanceAvailable = (quoteBalance != null ? new BigDecimal(
                            quoteBalance.getAvailable()) : BigDecimal.ZERO);

                    return buildOptionBalance(
                            symbolDetail,
                            baseTokenId,
                            token.getTokenFullName(),
                            token.getTokenFullName(),
                            baseBalanceTotal,
                            baseBalanceAvailable,
                            baseAvailPosition,
                            baseBalanceMargin,
                            quoteBalanceTotal,
                            quoteBalanceAvailable
                    );
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        return GetOptionTradeableResponse.newBuilder().addAllOptionBalance(result).build();
    }

    /**
     * 期权资产(base token balance)
     *
     * @param optionAccountId option account id
     * @return base token balance
     */
    private Map<String, OptionAssetList.OptionAsset> getOptionBaseBalanceMap(Long optionAccountId, Long orgId) {
        OptionAssetReq baseRequest = OptionAssetReq.newBuilder()
                .setBaseRequest(BaseReqUtil.getBaseRequest(orgId))
                .setAccountId(optionAccountId)
                .build();
        List<OptionAssetList.OptionAsset> baseBalances = grpcBalanceService
                .getOptionAssetList(baseRequest)
                .getOptionAssetList();
        return Optional.ofNullable(baseBalances)
                .orElse(new ArrayList<>())
                .stream()
                .collect(
                        Collectors
                                .toMap(OptionAssetList.OptionAsset::getTokenId, asset -> asset, (p, q) -> p)
                );
    }

    /**
     * 当前资产余额（币) (quote token balance)
     *
     * @param optionAccountId option account id
     * @param quoteTokenIds   quote token ids
     * @return quote token balance
     */
    private Map<String, OptionAccountDetailList.OptionAccountDetail> getOptionQuoteBalanceMap(
            Long optionAccountId, List<String> quoteTokenIds, Long orgId) {
        OptionAccountDetailReq quoteRequest = OptionAccountDetailReq.newBuilder()
                .setBaseRequest(BaseReqUtil.getBaseRequest(orgId))
                .setAccountId(optionAccountId)
                .addAllTokenIds(quoteTokenIds)
                .build();
        List<OptionAccountDetailList.OptionAccountDetail> quoteBalances = grpcBalanceService
                .getOptionAccountDetail(quoteRequest)
                .getOptionAccountDetailList();
        return Optional.ofNullable(quoteBalances)
                .orElse(new ArrayList<>())
                .stream()
                .collect(
                        Collectors.toMap(OptionAccountDetailList.OptionAccountDetail::getTokenId,
                                detail -> detail, (p, q) -> p)
                );
    }

    private List<String> getQuoteTokenIds(Long orgId, List<String> baseTokenIds) {
        return baseTokenIds
                .stream()
                .map(baseTokenId -> {
                    Symbol symbol = basicService.getTokenSymbolOptionMap()
                            .get(String.format("%s_%s", orgId, baseTokenId));
                    return symbol != null ? symbol.getQuoteTokenId() : null;
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    /**
     * 获取可卖期权
     */
    private OptionBalance buildOptionBalance(SymbolDetail symbolDetail, String baseTokenId,
                                             String baseTokenName,
                                             String baseTokenFullName,
                                             BigDecimal baseTotal,
                                             BigDecimal baseAvailable,
                                             BigDecimal baseAvailPosition,
                                             BigDecimal baseMargin,
                                             BigDecimal quoteTotal,
                                             BigDecimal quoteAvailable) {
        BigDecimal maxPayOff = BigDecimal.ZERO;
        TokenOptionInfo tokenOptionInfo = basicService.getTokenOptionInfo(baseTokenId);
        if (tokenOptionInfo != null) {
            maxPayOff = DecimalUtil.toBigDecimal(tokenOptionInfo.getMaxPayOff());
        }

        SellAbleValueDetail sellAbleValueDetail = getSellAbleValue(quoteAvailable, maxPayOff,
                baseTotal, baseAvailable);
        BigDecimal quoteLocked = quoteTotal.subtract(quoteAvailable);

        int basePrecision = BrokerServerConstants.OPTION_QUANTITY_PRECISION;
        if (symbolDetail != null) {
            basePrecision = DecimalUtil.toBigDecimal(symbolDetail.getBasePrecision())
                    .stripTrailingZeros().scale();
        }

        BigDecimal sellAble = sellAbleValueDetail.getSellAbleQuantity()
                .setScale(basePrecision, RoundingMode.DOWN).stripTrailingZeros();
        quoteAvailable = quoteAvailable
                .setScale(BrokerServerConstants.BASE_OPTION_PRECISION, RoundingMode.DOWN)
                .stripTrailingZeros();
        quoteLocked = quoteLocked
                .setScale(BrokerServerConstants.BASE_OPTION_PRECISION, RoundingMode.DOWN)
                .stripTrailingZeros();

        return OptionBalance.newBuilder()
                //option base
                .setTokenId(baseTokenId)//期权 token id
                .setTokenName(baseTokenName)//期权 token name
                .setTokenFullName(baseTokenFullName)//期权 token full name
                .setSellable(DecimalUtil.toTrimString(sellAble)) //可卖期权
                .setAvailPosition(DecimalUtil.toTrimString(baseAvailPosition))//期权可平量
                .setOptionTotal(DecimalUtil.toTrimString(baseTotal))//期权total
                .setOptionAvailable(DecimalUtil.toTrimString(baseAvailable))//期权available
                .setMaxOff(DecimalUtil.toTrimString(maxPayOff))//最大赔付（收益）
                .setRemainQuantity(
                        DecimalUtil.toTrimString(sellAbleValueDetail.getRemainQuantity()))//余额可卖张数
                .setCloseQuantity(
                        DecimalUtil.toTrimString(sellAbleValueDetail.getCloseQuantity()))//平仓期权的张数
                .setMargin(DecimalUtil.toTrimString(baseMargin)) //期权，可用保证金
                //option quote
                .setAvailable(DecimalUtil.toTrimString(quoteAvailable)) //当前资产余额（币), 可用
                .setLocked(DecimalUtil.toTrimString(quoteLocked)) //当前资产余额（币) ,锁定

                .build();
    }

    /**
     * 可卖期权 = 可用coin余额(quoteBalance available)/最大收益(maxPayOff) + k
     * <p>
     * k = if (total > 0, baseBalance available, 0)
     * <p>
     * 可平量kpl: when total > 0:      kpl = baseBalance available when total < 0:      kpl =
     * max(abs(total) - long_on_book, 0)
     *
     * @param quoteAvailable 可用coin余额(USDT, BTC...)
     * @param maxPayOff      期权最大收益
     * @param baseTotal      期权的total
     * @param baseAvailable  期权的available
     */
    private SellAbleValueDetail getSellAbleValue(BigDecimal quoteAvailable, BigDecimal maxPayOff,
                                                 BigDecimal baseTotal, BigDecimal baseAvailable) {
        if (maxPayOff == null || maxPayOff.compareTo(BigDecimal.ZERO) == 0) {
            return SellAbleValueDetail.builder()
                    .remainQuantity(BigDecimal.ZERO)
                    .closeQuantity(BigDecimal.ZERO)
                    .sellAbleQuantity(BigDecimal.ZERO)
                    .build();
        }

        BigDecimal closeQuantity =
                baseTotal.compareTo(BigDecimal.ZERO) > 0 ? baseAvailable : BigDecimal.ZERO;
        BigDecimal remainQuantity = quoteAvailable.divide(maxPayOff, 18, RoundingMode.DOWN);
        BigDecimal sellAbleQuantity = (remainQuantity).add(closeQuantity);
        return SellAbleValueDetail.builder()
                .remainQuantity(remainQuantity)
                .closeQuantity(closeQuantity)
                .sellAbleQuantity(sellAbleQuantity)
                .build();
    }

    private boolean checkTokenTransferWhiteList(Header header, List<String> transferTokens) {
        List<String> permitTokens = Lists.newArrayList(commonIniService
                .getStringValueOrDefault(header.getOrgId(), ORG_API_TRANSFER_TOKENS_KEY, "")
                .split(","));
        if (permitTokens.containsAll(transferTokens)) {
            return true;
        }
        return false;
    }

    public BaseResult accountAuthorizedOrgTransfer(Header header, String clientOrderId,
                                                   Long sourceUserId, Long sourceAccountId, Long targetUserId, Long targetAccountId,
                                                   String tokenId, String amount, boolean fromSourceLock, boolean toTargetLock,
                                                   Integer businessSubject, Integer subBusinessSubject) {
        // 默认关闭券商的transfer功能
        if (!commonIniService
                .getBooleanValueOrDefault(header.getOrgId(), ORG_API_TRANSFER_KEY, Boolean.FALSE)) {
            return BaseResult.fail(BrokerErrorCode.RETURN_403);
        }

        boolean authorizedOrgOpen = commonIniService.getBooleanValueOrDefault(header.getOrgId(),
                BrokerServerConstants.SUB_ACCOUNT_AUTHORIZED_ORG_KEY, false);
        if (!authorizedOrgOpen) {
            log.error("org:{} not open SUB_ACCOUNT_AUTHORIZED_ORG", header.getOrgId());
            return BaseResult.fail(BrokerErrorCode.NO_SUB_ACCOUNT_AUTHORIZED_ORG_CONFIG);
        }

        String accountsStr = commonIniService.getStringValueOrDefault(header.getOrgId(),
                BrokerServerConstants.ORG_CENTRAL_COUNTER_PARTY, "");
        if (StringUtils.isEmpty(accountsStr)) {
            log.error(
                    "accountAuthorizedOrgTransfer error, org:{} no ORG_CENTRAL_COUNTER_PARTY config",
                    header.getOrgId());
            return BaseResult.fail(BrokerErrorCode.NO_CENTRAL_COUNTER_PARTY_CONFIG);
        }
        List<Long> ccpAccounts = Arrays.stream(accountsStr.split(","))
                .map(aid -> Long.parseLong(aid)).collect(Collectors.toList());
        if (!ccpAccounts.contains(sourceAccountId) && !ccpAccounts.contains(targetAccountId)) {
            log.warn(
                    "accountAuthorizedOrgTransfer error, sourceAccount:{}&targetAccount:{} not in CCP config",
                    sourceAccountId, targetAccountId);
            return BaseResult.fail(BrokerErrorCode.NO_CENTRAL_COUNTER_PARTY_CONFIG);
        }
        // 校验二级流水
        if (subBusinessSubject != null && subBusinessSubject > 0
                && subBusinessSubjectService
                .getSubBusinessSubject(header.getOrgId(), businessSubject, subBusinessSubject)
                == null) {
            return BaseResult.fail(BrokerErrorCode.ERROR_SUB_BUSINESS_SUBJECT);
        }

        Account sourceAccount = accountMapper.getAccountByAccountId(sourceAccountId);
        if (sourceAccount == null || !sourceAccount.getUserId().equals(sourceUserId)) {
            return BaseResult.fail(BrokerErrorCode.PARAM_INVALID);
        }
        if (!ccpAccounts.contains(sourceAccountId) && sourceAccount.getIsAuthorizedOrg() == 0) {
            log.warn("sourceAccount:{} NOT_AUTHORIZED_ACCOUNT", sourceAccountId);
            return BaseResult.fail(BrokerErrorCode.NOT_AUTHORIZED_ACCOUNT);
        }
        Account targetAccount = accountMapper.getAccountByAccountId(targetAccountId);
        if (targetAccount == null || !targetAccount.getUserId().equals(targetUserId)) {
            return BaseResult.fail(BrokerErrorCode.PARAM_INVALID);
        }
        if (sourceAccount.getAccountType() != targetAccount.getAccountType()) {
            log.warn("source:{} and target:{} accountType not same", sourceAccountId,
                    targetAccountId);
            return BaseResult.fail(BrokerErrorCode.PARAM_INVALID);
        }

        BalanceTransfer balanceTransfer = balanceTransferMapper
                .getByClientOrderId(header.getOrgId(), clientOrderId);
        if (balanceTransfer == null) {
            Long transferId = sequenceGenerator.getLong();
            balanceTransfer = BalanceTransfer.builder()
                    .orgId(header.getOrgId())
                    .clientOrderId(clientOrderId)
                    .transferId(transferId)
                    .subject(businessSubject)
                    .subSubject(subBusinessSubject)
                    .sourceUserId(sourceUserId)
                    .sourceAccountId(sourceAccountId)
                    .targetUserId(targetUserId)
                    .targetAccountId(targetAccountId)
                    .tokenId(tokenId)
                    .amount(new BigDecimal(amount))
                    .fromSourceLock(fromSourceLock ? 1 : 0)
                    .toTargetLock(toTargetLock ? 1 : 0)
                    .status(0)
                    .response("")
                    .created(System.currentTimeMillis())
                    .updated(System.currentTimeMillis())
                    .build();
            balanceTransferMapper.insertSelective(balanceTransfer);
        } else {
            if (balanceTransfer.getStatus() == 200) {
                return BaseResult.success();
            }
        }
        SyncTransferRequest transferRequest = SyncTransferRequest.newBuilder()
                .setBaseRequest(BaseRequest.newBuilder().setOrganizationId(header.getOrgId()).build())
                .setClientTransferId(balanceTransfer.getTransferId())
                .setSourceOrgId(header.getOrgId())
                .setSourceFlowSubject(BusinessSubject.forNumber(balanceTransfer.getSubject()))
                .setSourceFlowSecondSubject(subBusinessSubject == null ? 0 : subBusinessSubject)
                .setSourceAccountId(balanceTransfer.getSourceAccountId())
                .setTokenId(balanceTransfer.getTokenId())
                .setAmount(balanceTransfer.getAmount().stripTrailingZeros().toPlainString())
                .setTargetOrgId(balanceTransfer.getOrgId())
                .setTargetAccountId(balanceTransfer.getTargetAccountId())
                .setTargetFlowSubject(BusinessSubject.forNumber(balanceTransfer.getSubject()))
                .setTargetFlowSecondSubject(subBusinessSubject)
                .setFromPosition(balanceTransfer.getFromSourceLock() == 1)
                .setToPosition(balanceTransfer.getToTargetLock() == 1)
                .build();
        SyncTransferResponse response = grpcBatchTransferService.syncTransfer(transferRequest);
        BalanceTransfer updateObj = BalanceTransfer.builder()
                .id(balanceTransfer.getId())
                .status(response.getCodeValue())
                .response(JsonUtil.defaultGson().toJson(response))
                .updated(System.currentTimeMillis())
                .build();
        balanceTransferMapper.updateByPrimaryKeySelective(updateObj);
        if (response.getCode() == SyncTransferResponse.ResponseCode.SUCCESS) {
            return BaseResult.success();
        }
        log.warn("transfer clientOrderId:{} get Error(code:{}, msg:{})", clientOrderId,
                response.getCode().name(), response.getMsg());
        return handleTransferResponse(response, false);
    }

    public BaseResult balanceTransfer(Header header, String clientOrderId,
                                      Long sourceUserId, Long targetUserId, String tokenId, String amount, boolean fromSourceLock,
                                      boolean toTargetLock,
                                      Integer businessSubject, Integer subBusinessSubject, Long sourceAccountId,
                                      Long targetAccountId) {
        if (header.getUserId() == 842145604923681280L || sourceUserId.equals(842145604923681280L) || targetUserId.equals(842145604923681280L)) { //1.禁止这个子账户交易 7070的子账号
            throw new BrokerException(BrokerErrorCode.FEATURE_SUSPENDED);
        }

        String lockKey = String
                .format(BrokerServerConstants.TRANSFER_LIMIT_LOCK, header.getOrgId(), sourceUserId,
                        tokenId);
        try {
            // 默认关闭券商的transfer功能
            if (!commonIniService
                    .getBooleanValueOrDefault(header.getOrgId(), ORG_API_TRANSFER_KEY, Boolean.FALSE)) {
                return BaseResult.fail(BrokerErrorCode.RETURN_403);
//            throw new BrokerException(BrokerErrorCode.RETURN_403);
            }
            // 校验二级流水
            if (subBusinessSubject != null && subBusinessSubject > 0
                    && subBusinessSubjectService
                    .getSubBusinessSubject(header.getOrgId(), businessSubject, subBusinessSubject)
                    == null) {
                return BaseResult.fail(BrokerErrorCode.ERROR_SUB_BUSINESS_SUBJECT);
//            throw new BrokerException(BrokerErrorCode.ERROR_SUB_BUSINESS_SUBJECT);
            }
            // 校验是否为运营账号转出
            Long operationUserId = getUserIdByAccountType(header.getOrgId(),
                    io.bhex.base.account.AccountType.OPERATION_ACCOUNT_VALUE);
            if (!operationUserId.equals(sourceUserId)) {
                // 校验币种白名单
                if (!checkTokenTransferWhiteList(header, Lists.newArrayList(tokenId))) {
                    log.warn(
                            "{} invoke transfer:{} with sourceUserId:{} operationUserId:{} tokenId:{} not in whiteList",
                            header.getOrgId(), clientOrderId, sourceUserId, operationUserId, tokenId);
                    return BaseResult.fail(BrokerErrorCode.TRANSFER_SYMBOL_NOT_ALLOWED);
                }
            }

            BalanceTransfer balanceTransfer = balanceTransferMapper
                    .getByClientOrderId(header.getOrgId(), clientOrderId);
            Long dailyStartTime = TimesUtils.getDailyStartTime(System.currentTimeMillis());
            if (balanceTransfer == null) {
                Long transferId = sequenceGenerator.getLong();
                if (sourceAccountId == 0L && targetAccountId == 0L) {
                    sourceAccountId = getAccountId(header.getOrgId(), sourceUserId);
                    targetAccountId = getAccountId(header.getOrgId(), targetUserId);
                } else {
                    if (sourceAccountId > 0) {
                        Long tmpSourceUserId = getUserIdByAccountId(header.getOrgId(),
                                sourceAccountId);
                        if (!tmpSourceUserId.equals(sourceUserId)) {
                            log.warn(
                                    "{} invoke transfer:{} with  UserId:{} of the sourceAccountId:{} is not equals sourceUserId:{} ",
                                    header.getOrgId(), clientOrderId, tmpSourceUserId, sourceAccountId,
                                    sourceUserId);
                            return BaseResult.fail(BrokerErrorCode.ACCOUNT_NOT_EXIST);
                        }
                    } else {
                        sourceAccountId = getAccountId(header.getOrgId(), sourceUserId);
                    }
                    if (targetAccountId > 0) {
                        Long tmpTargetUserId = getUserIdByAccountId(header.getOrgId(),
                                targetAccountId);
                        if (!tmpTargetUserId.equals(targetUserId)) {
                            log.warn(
                                    "{} invoke transfer:{} with  UserId:{} of the targetaccountId:{} is not equals targetUserId:{} ",
                                    header.getOrgId(), clientOrderId, tmpTargetUserId, targetAccountId,
                                    targetAccountId);
                            return BaseResult.fail(BrokerErrorCode.ACCOUNT_NOT_EXIST);
                        }
                    } else {
                        targetAccountId = getAccountId(header.getOrgId(), targetUserId);
                    }
                }
                //加锁判断提币限额
                boolean lock = RedisLockUtils.tryLockAlways(redisTemplate, lockKey, 30 * 1000, 30);
                if (!lock) {
                    //todo 变更错误号
                    return BaseResult.fail(BrokerErrorCode.TRANSFER_LIMIT_FAILED);
                }
                //检验是否触发限制
                if (!checkTransferlimit(header.getOrgId(), sourceUserId, dailyStartTime, tokenId,
                        new BigDecimal(amount), clientOrderId)) {
                    log.warn(
                            "{} invoke transfer:userId:{} {} with tokenId:{} amount:{} greater than limit",
                            header.getOrgId(), sourceUserId, clientOrderId, tokenId, amount);
                    return BaseResult.fail(BrokerErrorCode.TRANSFER_LIMIT_FAILED);
                }
                balanceTransfer = BalanceTransfer.builder()
                        .orgId(header.getOrgId())
                        .clientOrderId(clientOrderId)
                        .transferId(transferId)
                        .subject(businessSubject)
                        .subSubject(subBusinessSubject)
                        .sourceUserId(sourceUserId)
                        .sourceAccountId(sourceAccountId)
                        .targetUserId(targetUserId)
                        .targetAccountId(targetAccountId)
                        .tokenId(tokenId)
                        .amount(new BigDecimal(amount))
                        .fromSourceLock(fromSourceLock ? 1 : 0)
                        .toTargetLock(toTargetLock ? 1 : 0)
                        .status(0)
                        .response("")
                        .created(System.currentTimeMillis())
                        .updated(System.currentTimeMillis())
                        .build();
                balanceTransferMapper.insertSelective(balanceTransfer);
            } else {
                if (balanceTransfer.getStatus() == 200) {
                    return BaseResult.success();
                }
            }
            SyncTransferRequest transferRequest = SyncTransferRequest.newBuilder()
                    .setBaseRequest(BaseReqUtil.getBaseRequest(header.getOrgId()))
                    .setClientTransferId(balanceTransfer.getTransferId())
                    .setSourceOrgId(header.getOrgId())
                    .setSourceFlowSubject(BusinessSubject.forNumber(balanceTransfer.getSubject()))
                    .setSourceFlowSecondSubject(subBusinessSubject == null ? 0 : subBusinessSubject)
                    .setSourceAccountId(balanceTransfer.getSourceAccountId())
                    .setTokenId(balanceTransfer.getTokenId())
                    .setAmount(balanceTransfer.getAmount().stripTrailingZeros().toPlainString())
                    .setTargetOrgId(balanceTransfer.getOrgId())
                    .setTargetAccountId(balanceTransfer.getTargetAccountId())
                    .setTargetFlowSubject(BusinessSubject.forNumber(balanceTransfer.getSubject()))
                    .setTargetFlowSecondSubject(subBusinessSubject)
                    .setFromPosition(balanceTransfer.getFromSourceLock() == 1)
                    .setToPosition(balanceTransfer.getToTargetLock() == 1)
                    .build();
            SyncTransferResponse response = grpcBatchTransferService.syncTransfer(transferRequest);
            BalanceTransfer updateObj = BalanceTransfer.builder()
                    .id(balanceTransfer.getId())
                    .status(response.getCodeValue())
                    .response(JsonUtil.defaultGson().toJson(response))
                    .updated(System.currentTimeMillis())
                    .build();
            balanceTransferMapper.updateByPrimaryKeySelective(updateObj);
            if (response.getCode() == SyncTransferResponse.ResponseCode.SUCCESS) {
                //更新redis
                String quotaKey = String
                        .format(BrokerServerConstants.TRANSFER_USER_DAY_QUOTA, header.getOrgId(),
                                sourceUserId, tokenId, dailyStartTime);
                BigDecimal usedQuota = CommonUtil
                        .toBigDecimal(redisTemplate.opsForValue().get(quotaKey), BigDecimal.ZERO);
                redisTemplate.opsForValue().set(quotaKey,
                        usedQuota.add(new BigDecimal(amount)).stripTrailingZeros().toPlainString());
                return BaseResult.success();
            }
            log.warn("transfer clientOrderId:{} get Error(code:{}, msg:{})", clientOrderId,
                    response.getCode().name(), response.getMsg());
            return handleTransferResponse(response, false);
        } finally {
            RedisLockUtils.releaseLock(redisTemplate, lockKey);
        }

    }

    public List<BalanceBatchTransferTask> queryBatchBalanceTransferTask(Header header, Integer type,
                                                                        Long fromId, Integer limit) {
        return balanceBatchTransferTaskMapper.queryTaskList(header.getOrgId(), type, fromId, limit);
    }

    public BalanceBatchTransferResult getBalanceBatchTransferResult(Header header,
                                                                    String clientOrderId, Integer type) {
        BalanceBatchTransferTask transferTask = balanceBatchTransferTaskMapper
                .getByClientOrderId(header.getOrgId(), clientOrderId, type);
        if (transferTask == null) {
            throw new BrokerException(BrokerErrorCode.DB_RECORD_NOT_EXISTS);
        }
        List<BalanceBatchTransfer> batchTransferList = new ArrayList<>();
        if (transferTask.getStatus() == 1) {
            BalanceBatchTransfer queryObj = BalanceBatchTransfer.builder()
                    .orgId(header.getOrgId())
                    .clientOrderId(clientOrderId)
                    .build();
            batchTransferList = balanceBatchTransferMapper.select(queryObj);
        } else if (System.currentTimeMillis() > transferTask.getCreated() + 60 * 1000) {
            CompletableFuture.runAsync(() -> {
                try {
                    balanceBatchTransfer(header, clientOrderId, type, 0);
                } catch (Exception e) {
                    log.error(" getBalanceBatchTransferResult error", e);
                }
            }, taskExecutor);
        }
        return BalanceBatchTransferResult.builder()
                .transferTask(transferTask)
                .transferList(batchTransferList)
                .build();
    }

    @Transactional(isolation = Isolation.READ_COMMITTED, propagation = Propagation.REQUIRED, rollbackFor = Throwable.class)
    public BalanceBatchTransferTask airDrop(Header header, String clientOrderId, String desc,
                                            List<BalanceBatchTransfer> batchTransferList) {
        batchTransferList = batchTransferList.stream()
                .map(transfer -> {
                    BalanceBatchTransfer batchTransfer = transfer.toBuilder().build();
                    batchTransfer.setSubject(BusinessSubject.AIRDROP_VALUE);
                    batchTransfer.setSourceAccountType(
                            io.bhex.base.account.AccountType.OPERATION_ACCOUNT.getNumber());
                    return batchTransfer;
                }).collect(Collectors.toList());
        return balanceBatchTransfer(header, clientOrderId, BalanceBatchTransferType.AIR_DROP.type(),
                desc, batchTransferList);
    }

    /**
     * 即可用作批量转账，有可用作空投
     */
    @Transactional(isolation = Isolation.READ_COMMITTED, propagation = Propagation.REQUIRED, rollbackFor = Throwable.class)
    public BalanceBatchTransferTask balanceBatchTransfer(Header header, String clientOrderId,
                                                         Integer type, String desc, List<BalanceBatchTransfer> batchTransferList) {
        // 默认关闭券商的transfer功能
        if (type == BalanceBatchTransferType.TRANSFER.type() && !commonIniService
                .getBooleanValueOrDefault(header.getOrgId(), ORG_API_TRANSFER_KEY, Boolean.FALSE)) {
            throw new BrokerException(BrokerErrorCode.RETURN_403);
        }
        if (type == BalanceBatchTransferType.TRANSFER.type()) {
            Long operationUserId = getUserIdByAccountType(header.getOrgId(),
                    io.bhex.base.account.AccountType.OPERATION_ACCOUNT_VALUE);
            if (!checkTokenTransferWhiteList(header, batchTransferList.stream()
                    .filter(item -> !item.getSourceUserId().equals(operationUserId))
                    .map(BalanceBatchTransfer::getTokenId).distinct()
                    .collect(Collectors.toList()))) {
                log.warn("{} invoke batchTransfer:{} with invalid sourceUserId and invalid token",
                        header.getOrgId(), clientOrderId);
                throw new BrokerException(BrokerErrorCode.TRANSFER_SYMBOL_NOT_ALLOWED);
            }
        }
        BalanceBatchTransferTask transferTask = balanceBatchTransferTaskMapper
                .lockByClientOrderId(header.getOrgId(), clientOrderId, type);
        if (transferTask == null) {
            Long currentTimestamp = System.currentTimeMillis();
            transferTask = BalanceBatchTransferTask.builder()
                    .orgId(header.getOrgId())
                    .clientOrderId(clientOrderId)
                    .type(type)
                    .desc(desc)
                    .status(0)
                    .created(currentTimestamp)
                    .updated(currentTimestamp)
                    .build();
            balanceBatchTransferTaskMapper.insertSelective(transferTask);
            Long taskId = transferTask.getId();
            batchTransferList = batchTransferList.stream()
                    .map(transfer ->
                    {
                        BalanceBatchTransfer batchTransfer = transfer.toBuilder().build();
                        batchTransfer.setTaskId(taskId);
                        batchTransfer.setOrgId(header.getOrgId());
                        batchTransfer.setClientOrderId(clientOrderId);
                        batchTransfer.setType(type);
                        batchTransfer.setTransferId(0L);
                        batchTransfer.setStatus(0);
                        batchTransfer.setResponse("");

                        if (transfer.getSourceUserId() > 0) {
                            if (transfer.getSourceAccountId() == null
                                    || transfer.getSourceAccountId() == 0) {
                                try {
                                    Long sourceAccountId = getAccountId(header.getOrgId(),
                                            transfer.getSourceUserId());
                                    batchTransfer.setSourceAccountId(sourceAccountId);
                                } catch (Exception e) {
                                    batchTransfer.setStatus(BrokerErrorCode.USER_NOT_EXIST.code());
                                }
                            } else {
                                try {
                                    Long tmpSourceUserId = getUserIdByAccountId(header.getOrgId(),
                                            transfer.getSourceAccountId());
                                    if (!tmpSourceUserId.equals(transfer.getSourceUserId())) {
                                        batchTransfer
                                                .setStatus(BrokerErrorCode.ACCOUNT_NOT_EXIST.code());
                                    }
                                } catch (Exception e) {
                                    batchTransfer.setStatus(BrokerErrorCode.ACCOUNT_NOT_EXIST.code());
                                }
                            }
                        }
                        if (transfer.getTargetUserId() > 0 && batchTransfer.getStatus() == 0) {
                            if (transfer.getTargetAccountId() == null
                                    || transfer.getTargetAccountId() == 0) {
                                try {
                                    Long targetAccountId = getAccountId(header.getOrgId(),
                                            transfer.getTargetUserId());
                                    batchTransfer.setTargetAccountId(targetAccountId);
                                } catch (Exception e) {
                                    batchTransfer.setStatus(BrokerErrorCode.USER_NOT_EXIST.code());
                                }
                            } else {
                                try {
                                    Long tmpTargetUserId = getUserIdByAccountId(header.getOrgId(),
                                            transfer.getTargetAccountId());
                                    if (!tmpTargetUserId.equals(transfer.getTargetUserId())) {
                                        batchTransfer
                                                .setStatus(BrokerErrorCode.ACCOUNT_NOT_EXIST.code());
                                    }
                                } catch (Exception e) {
                                    batchTransfer.setStatus(BrokerErrorCode.ACCOUNT_NOT_EXIST.code());
                                }
                            }
                        }
                        if (batchTransfer.getStatus() == 0) {
                            batchTransfer.setTransferId(sequenceGenerator.getLong());
                        }
                        batchTransfer.setCreated(currentTimestamp);
                        batchTransfer.setUpdated(currentTimestamp);
                        return batchTransfer;
                    }).collect(Collectors.toList());
            balanceBatchTransferMapper.insertList(batchTransferList);
            CompletableFuture.runAsync(() -> {
                try {
                    Thread.sleep(1000);
                    balanceBatchTransfer(header, clientOrderId, type, 0);
                } catch (Exception e) {
                    log.error(" submit balanceBatchTransferAsync error", e);
                }
            }, taskExecutor);
        } else if (transferTask.getStatus() == 0
                && System.currentTimeMillis() > transferTask.getCreated() + 60 * 1000) {
            CompletableFuture.runAsync(() -> {
                try {
                    balanceBatchTransfer(header, clientOrderId, type, 0);
                } catch (Exception e) {
                    log.error(" submit balanceBatchTransferAsync error", e);
                }
            }, taskExecutor);
        }
        return transferTask;
    }

    private void balanceBatchTransfer(Header header, String clientOrderId, Integer type,
                                      int index) {
        try {
            BalanceBatchTransfer queryObj = BalanceBatchTransfer.builder()
                    .orgId(header.getOrgId())
                    .clientOrderId(clientOrderId)
                    .type(type)
                    .status(0)
                    .build();
            List<BalanceBatchTransfer> batchTransferList = balanceBatchTransferMapper
                    .select(queryObj);
            batchTransferList.forEach(transfer -> balanceBatchTransfer(transfer, 0, type));
            BalanceBatchTransferTask transferTask = balanceBatchTransferTaskMapper
                    .getByClientOrderId(header.getOrgId(), clientOrderId, type);
            BalanceBatchTransferTask updateObj = BalanceBatchTransferTask.builder()
                    .id(transferTask.getId())
                    .status(1)
                    .updated(System.currentTimeMillis())
                    .build();
            balanceBatchTransferTaskMapper.updateByPrimaryKeySelective(updateObj);
        } catch (Exception e) {
            log.error("balanceBatchTransfer with async:(orgId:{}, clientOrderId:{}) error",
                    header.getOrgId(), clientOrderId, e);
            if (index == 0) {
                balanceBatchTransfer(header, clientOrderId, type, ++index);
            } else {
                throw e;
            }
        }
    }

    private void balanceBatchTransfer(BalanceBatchTransfer transfer, int index, Integer type) {
        String lockKey = String
                .format(BrokerServerConstants.TRANSFER_LIMIT_LOCK, transfer.getOrgId(),
                        transfer.getSourceUserId(), transfer.getTokenId());
        try {
            transfer = balanceBatchTransferMapper.selectByPrimaryKey(transfer.getId());
            if (transfer.getStatus() == 0) {
                if (transfer.getSubSubject() != null && transfer.getSubSubject() > 0
                        && subBusinessSubjectService
                        .getSubBusinessSubject(transfer.getOrgId(), transfer.getSubject(),
                                transfer.getSubSubject()) == null) {
                    BalanceBatchTransfer updateObj = BalanceBatchTransfer.builder()
                            .id(transfer.getId())
                            .status(BrokerErrorCode.ERROR_SUB_BUSINESS_SUBJECT.code())
                            .updated(System.currentTimeMillis())
                            .build();
                    balanceBatchTransferMapper.updateByPrimaryKeySelective(updateObj);
                    return;
                }
                Long dailyStartTime = TimesUtils.getDailyStartTime(transfer.getCreated());
                //加锁判断转账限额
                if (type == 0) {
                    boolean lock = RedisLockUtils
                            .tryLockAlways(redisTemplate, lockKey, 30 * 1000, 30);
                    //超过转账限额，转账失败
                    if (!lock ||
                            !checkTransferlimit(transfer.getOrgId(), transfer.getSourceUserId(),
                                    dailyStartTime, transfer.getTokenId(), transfer.getAmount(),
                                    transfer.getClientOrderId())) {
                        BalanceBatchTransfer updateObj = BalanceBatchTransfer.builder()
                                .id(transfer.getId())
                                .status(BrokerErrorCode.TRANSFER_LIMIT_FAILED.code())
                                .updated(System.currentTimeMillis())
                                .build();
                        balanceBatchTransferMapper.updateByPrimaryKeySelective(updateObj);
                        return;
                    }

                }

                SyncTransferRequest transferRequest = SyncTransferRequest.newBuilder()
                        .setBaseRequest(BaseReqUtil.getBaseRequest(transfer.getOrgId()))
                        .setClientTransferId(transfer.getTransferId())
                        .setSourceOrgId(transfer.getOrgId())
                        .setSourceFlowSubject(BusinessSubject.forNumber(transfer.getSubject()))
                        .setSourceFlowSecondSubject(transfer.getSubSubject())
                        .setSourceAccountType(
                                io.bhex.base.account.AccountType.forNumber(transfer.getSourceAccountType()))
                        .setSourceAccountId(transfer.getSourceAccountId())
                        .setTokenId(transfer.getTokenId())
                        .setAmount(transfer.getAmount().stripTrailingZeros().toPlainString())
                        .setTargetOrgId(transfer.getOrgId())
                        .setTargetAccountId(transfer.getTargetAccountId())
                        .setTargetAccountType(
                                io.bhex.base.account.AccountType.forNumber(transfer.getTargetAccountType()))
                        .setTargetFlowSubject(BusinessSubject.forNumber(transfer.getSubject()))
                        .setTargetFlowSecondSubject(transfer.getSubSubject())
                        .setFromPosition(transfer.getFromSourceLock() == 1)
                        .setToPosition(transfer.getToTargetLock() == 1)
                        .build();
                SyncTransferResponse response = grpcBatchTransferService
                        .syncTransfer(transferRequest);
                if (response.getCode() == SyncTransferResponse.ResponseCode.PROCESSING) {
                    throw new RuntimeException(
                            "SyncTransferRequest get processing status, exception for retry");
                }
                BalanceBatchTransfer updateObj = BalanceBatchTransfer.builder()
                        .id(transfer.getId())
                        .status(response.getCodeValue())
                        .response(JsonUtil.defaultGson().toJson(response))
                        .updated(System.currentTimeMillis())
                        .build();
                balanceBatchTransferMapper.updateByPrimaryKeySelective(updateObj);
                //处理成功&为转账
                if (response.getCode() == ResponseCode.SUCCESS && type == 0) {
                    //更新redis
                    String quotaKey = String
                            .format(BrokerServerConstants.TRANSFER_USER_DAY_QUOTA, transfer.getOrgId(),
                                    transfer.getSourceUserId(), transfer.getTokenId(), dailyStartTime);
                    BigDecimal usedQuota = CommonUtil
                            .toBigDecimal(redisTemplate.opsForValue().get(quotaKey), BigDecimal.ZERO);
                    redisTemplate.opsForValue().set(quotaKey,
                            usedQuota.add(transfer.getAmount()).stripTrailingZeros().toPlainString());
                }
            }
        } catch (
                Exception e) {
            log.error("batch balanceTransfer:{} error", JsonUtil.defaultGson().toJson(transfer), e);
            if (index == 0) {
                balanceBatchTransfer(transfer, ++index, type);
            } else {
                throw e;
            }
        } finally {
            if (type == 0) {
                RedisLockUtils.releaseLock(redisTemplate, lockKey);
            }

        }
    }

    public BaseResult balanceMapping(Header header, String clientOrderId,
                                     Long sourceUserId, String sourceTokenId, String sourceAmount, boolean fromSourceLock,
                                     boolean toTargetLock,
                                     Long targetUserId, String targetTokenId, String targetAmount, boolean fromTargetLock,
                                     boolean toSourceLock,
                                     Integer businessSubject) {
        if (!commonIniService
                .getBooleanValueOrDefault(header.getOrgId(), ORG_API_TRANSFER_KEY, Boolean.FALSE)) {
            return BaseResult.fail(BrokerErrorCode.RETURN_403);
        }
        BalanceMapping balanceMapping = balanceMappingMapper
                .getByClientOrderId(header.getOrgId(), clientOrderId);
        if (balanceMapping == null) {
            Long sourceTransferId = sequenceGenerator.getLong();
            Long targetTransferId = sequenceGenerator.getLong();
            Long sourceAccountId = getAccountId(header.getOrgId(), sourceUserId);
            Long targetAccountId = getAccountId(header.getOrgId(), targetUserId);
            balanceMapping = BalanceMapping.builder()
                    .orgId(header.getOrgId())
                    .clientOrderId(clientOrderId)
                    .sourceTransferId(sourceTransferId)
                    .sourceSubject(businessSubject)
                    .sourceUserId(sourceUserId)
                    .sourceAccountId(sourceAccountId)
                    .sourceTokenId(sourceTokenId)
                    .sourceAmount(new BigDecimal(sourceAmount))
                    .fromSourceLock(fromSourceLock ? 1 : 0)
                    .toTargetLock(toTargetLock ? 1 : 0)
                    .targetTransferId(targetTransferId)
                    .targetSubject(businessSubject)
                    .targetUserId(targetUserId)
                    .targetAccountId(targetAccountId)
                    .targetTokenId(targetTokenId)
                    .targetAmount(new BigDecimal(targetAmount))
                    .fromTargetLock(fromTargetLock ? 1 : 0)
                    .toSourceLock(toSourceLock ? 1 : 0)
                    .status(0)
                    .response("")
                    .created(System.currentTimeMillis())
                    .updated(System.currentTimeMillis())
                    .build();
            balanceMappingMapper.insertSelective(balanceMapping);
        } else {
            if (balanceMapping.getStatus() == 200) {
                return BaseResult.success();
            }
        }
        List<SyncTransferRequest> mappingRequests = Lists.newArrayList(
                SyncTransferRequest.newBuilder()
                        .setClientTransferId(balanceMapping.getSourceTransferId())
                        .setSourceOrgId(balanceMapping.getOrgId())
                        .setSourceFlowSubject(BusinessSubject.forNumber(balanceMapping.getSourceSubject()))
                        .setSourceAccountId(balanceMapping.getSourceAccountId())
                        .setTokenId(balanceMapping.getSourceTokenId())
                        .setAmount(balanceMapping.getSourceAmount().stripTrailingZeros().toPlainString())
                        .setTargetOrgId(balanceMapping.getOrgId())
                        .setTargetAccountId(balanceMapping.getTargetAccountId())
                        .setTargetFlowSubject(BusinessSubject.forNumber(balanceMapping.getSourceSubject()))
                        .setFromPosition(balanceMapping.getFromSourceLock() == 1)
                        .setToPosition(balanceMapping.getToTargetLock() == 1)
                        .build(),
                SyncTransferRequest.newBuilder()
                        .setClientTransferId(balanceMapping.getTargetTransferId())
                        .setSourceOrgId(balanceMapping.getOrgId())
                        .setSourceFlowSubject(BusinessSubject.forNumber(balanceMapping.getSourceSubject()))
                        .setSourceAccountId(balanceMapping.getTargetAccountId())
                        .setTokenId(balanceMapping.getTargetTokenId())
                        .setAmount(balanceMapping.getTargetAmount().stripTrailingZeros().toPlainString())
                        .setTargetOrgId(balanceMapping.getOrgId())
                        .setTargetAccountId(balanceMapping.getSourceAccountId())
                        .setTargetFlowSubject(BusinessSubject.forNumber(balanceMapping.getSourceSubject()))
                        .setFromPosition(balanceMapping.getFromTargetLock() == 1)
                        .setToPosition(balanceMapping.getToSourceLock() == 1)
                        .build()
        );
        BatchSyncTransferRequest batchSyncTransferRequest = BatchSyncTransferRequest
                .newBuilder()
                .setBaseRequest(BaseReqUtil.getBaseRequest(header.getOrgId()))
                .addAllSyncTransferRequestList(mappingRequests)
                .build();
        SyncTransferResponse response = grpcBatchTransferService
                .batchSyncTransfer(batchSyncTransferRequest);
        BalanceMapping updateObj = BalanceMapping.builder()
                .id(balanceMapping.getId())
                .status(response.getCodeValue())
                .response(JsonUtil.defaultGson().toJson(response))
                .updated(System.currentTimeMillis())
                .build();
        balanceMappingMapper.updateByPrimaryKeySelective(updateObj);
        if (response.getCode() == SyncTransferResponse.ResponseCode.SUCCESS) {
            return BaseResult.success();
        }
        log.warn("mapping clientOrderId:{} get Error(code:{}, msg:{})", clientOrderId,
                response.getCode().name(), response.getMsg());
        return handleTransferResponse(response, true);
    }

    private BaseResult handleTransferResponse(SyncTransferResponse response, boolean isMapping) {
        if (response.getCode() == SyncTransferResponse.ResponseCode.BALANCE_INSUFFICIENT
                || response.getCode() == SyncTransferResponse.ResponseCode.LOCK_BALANCE_INSUFFICIENT
                || response.getCode() == SyncTransferResponse.ResponseCode.NO_POSITION) {
            return BaseResult.fail(BrokerErrorCode.TRANSFER_INSUFFICIENT_BALANCE);
//            throw new BrokerException(BrokerErrorCode.TRANSFER_INSUFFICIENT_BALANCE);
        } else if (response.getCode() == SyncTransferResponse.ResponseCode.FROM_ACCOUNT_NOT_EXIST
                || response.getCode() == SyncTransferResponse.ResponseCode.TO_ACCOUNT_NOT_EXIST
                || response.getCode() == SyncTransferResponse.ResponseCode.REQUEST_PARA_ERROR
                || response.getCode() == SyncTransferResponse.ResponseCode.ACCOUNT_NOT_EXIST
                || response.getCode() == SyncTransferResponse.ResponseCode.ACCOUNT_NOT_SAME_SHARD) {
            return BaseResult.fail(BrokerErrorCode.ACCOUNT_NOT_EXIST);
//            throw new BrokerException(BrokerErrorCode.PARAM_INVALID);
        } else {
            return BaseResult.fail(isMapping ? BrokerErrorCode.MAPPING_TRANSFER_FAILED
                    : BrokerErrorCode.BALANCE_TRANSFER_FAILED);
//            throw new BrokerException(BrokerErrorCode.BALANCE_TRANSFER_FAILED);
        }
    }

    @Transactional(isolation = Isolation.READ_COMMITTED, propagation = Propagation.REQUIRED, rollbackFor = Throwable.class)
    public BalanceBatchOperatePositionTask batchLockPosition(Header header, String clientOrderId,
                                                             String desc, List<BalanceBatchLockPosition> batchLockPositionList) {
        BalanceBatchOperatePositionTask task = balanceBatchOperatePositionTaskMapper
                .lockByClientOrderId(header.getOrgId(), clientOrderId,
                        BalancePositionOperateType.LOCK.type());
        if (task == null) {
            Long currentTimestamp = System.currentTimeMillis();
            task = BalanceBatchOperatePositionTask.builder()
                    .orgId(header.getOrgId())
                    .clientOrderId(clientOrderId)
                    .type(BalancePositionOperateType.LOCK.type())
                    .desc(desc)
                    .status(0)
                    .created(currentTimestamp)
                    .updated(currentTimestamp)
                    .build();
            balanceBatchOperatePositionTaskMapper.insertSelective(task);
            Long taskId = task.getId();
            batchLockPositionList = batchLockPositionList.stream()
                    .map(item ->
                    {
                        BalanceBatchLockPosition lockPosition = item.toBuilder().build();
                        lockPosition.setTaskId(taskId);
                        lockPosition.setOrgId(header.getOrgId());
                        lockPosition.setClientOrderId(clientOrderId);
                        lockPosition.setLockId(0L);
                        lockPosition.setRemainUnlockedAmount(BigDecimal.ZERO);
                        lockPosition.setStatus(0);
                        lockPosition.setResponse("");
                        try {
                            if (item.getAccountId() == 0L) {
                                Long accountId = getAccountId(header.getOrgId(), item.getUserId());
                                lockPosition.setAccountId(accountId);
                            } else {
                                List<Account> accountList = accountMapper.queryByUserId(header.getOrgId(), item.getUserId());
                                Account lockAccount = accountList.stream().filter(account -> account.getAccountId().equals(item.getAccountId())).findFirst().orElse(null);
                                if (lockAccount == null) {
                                    lockPosition.setStatus(BrokerErrorCode.ACCOUNT_NOT_EXIST.code());
                                }
                                if (lockAccount.getAccountType() != AccountType.MAIN.value() && lockAccount.getIsAuthorizedOrg() == 0) {
                                    log.error("lock error accoutType is not main or AuthorizedOrg is false: accountId:{} type:{} AuthorizedOrg:{} ",
                                            lockAccount.getAccountId(), lockAccount.getAccountType(), lockAccount.getIsAuthorizedOrg());
                                    lockPosition.setStatus(BrokerErrorCode.USER_ACCOUNT_LOCK_FILLED.code());
                                } else {
                                    lockPosition.setAccountId(item.getAccountId());
                                }
                            }

                        } catch (Exception e) {
                            lockPosition.setStatus(BrokerErrorCode.USER_NOT_EXIST.code());
                        }

                        if (lockPosition.getStatus() == 0) {
                            lockPosition.setLockId(sequenceGenerator.getLong());
                        }
                        if (item.getToPositionLocked() == 1) {
                            lockPosition.setRemainUnlockedAmount(lockPosition.getAmount());
                        }
                        lockPosition.setCreated(currentTimestamp);
                        lockPosition.setUpdated(currentTimestamp);
                        return lockPosition;
                    }).collect(Collectors.toList());
            balanceBatchLockPositionMapper.insertList(batchLockPositionList);
            CompletableFuture.runAsync(() -> {
                try {
                    Thread.sleep(1000);
                    balanceBatchLockPosition(header, clientOrderId, 0);
                } catch (Exception e) {
                    log.error(" submit batchLockPosition error", e);
                }
            }, taskExecutor);
        } else if (task.getStatus() == 0
                && System.currentTimeMillis() > task.getCreated() + 60 * 1000) {
            CompletableFuture.runAsync(() -> {
                try {
                    balanceBatchLockPosition(header, clientOrderId, 0);
                } catch (Exception e) {
                    log.error(" submit batchLockPosition error", e);
                }
            }, taskExecutor);
        }
        return task;
    }

    private void balanceBatchLockPosition(Header header, String clientOrderId, int index) {
        try {
            BalanceBatchLockPosition queryObj = BalanceBatchLockPosition.builder()
                    .orgId(header.getOrgId())
                    .clientOrderId(clientOrderId)
                    .status(0)
                    .build();
            List<BalanceBatchLockPosition> batchLockPositionList = balanceBatchLockPositionMapper
                    .select(queryObj);
            batchLockPositionList
                    .forEach(lockPosition -> balanceBatchLockPosition(lockPosition, 0));
            BalanceBatchOperatePositionTask task = balanceBatchOperatePositionTaskMapper
                    .getByClientOrderId(header.getOrgId(), clientOrderId,
                            BalancePositionOperateType.LOCK.type());
            BalanceBatchOperatePositionTask updateObj = BalanceBatchOperatePositionTask.builder()
                    .id(task.getId())
                    .status(1)
                    .updated(System.currentTimeMillis())
                    .build();
            balanceBatchOperatePositionTaskMapper.updateByPrimaryKeySelective(updateObj);
        } catch (Exception e) {
            log.error("balanceBatchLockPosition with async:(orgId:{}, clientOrderId:{}) error",
                    header.getOrgId(), clientOrderId, e);
            if (index == 0) {
                balanceBatchLockPosition(header, clientOrderId, ++index);
            } else {
                throw e;
            }
        }
    }

    private void balanceBatchLockPosition(BalanceBatchLockPosition lockPosition, int index) {
        try {
            lockPosition = balanceBatchLockPositionMapper.selectByPrimaryKey(lockPosition.getId());
            if (lockPosition.getStatus() == 0) {
                LockBalanceRequest request = LockBalanceRequest
                        .newBuilder()
                        .setBaseRequest(
                                BaseRequest.newBuilder().setOrganizationId(lockPosition.getOrgId())
                                        .setBrokerUserId(lockPosition.getUserId()).build())
                        .setClientReqId(lockPosition.getLockId())
                        .setAccountId(lockPosition.getAccountId())
                        .setTokenId(lockPosition.getTokenId())
                        .setLockAmount(lockPosition.getAmount().stripTrailingZeros().toPlainString())
                        .setLockedToPositionLocked(lockPosition.getToPositionLocked() == 1)
                        .setLockReason(lockPosition.getDesc())
                        .build();
                LockBalanceReply reply = grpcBalanceService.lockBalance(request);
                BalanceBatchLockPosition updateObj = BalanceBatchLockPosition.builder()
                        .id(lockPosition.getId())
                        .status(reply.getCodeValue())
                        .response(JsonUtil.defaultGson().toJson(reply))
                        .updated(System.currentTimeMillis())
                        .build();
                balanceBatchLockPositionMapper.updateByPrimaryKeySelective(updateObj);
            }
        } catch (Exception e) {
            log.error("batch lock position:{} error", JsonUtil.defaultGson().toJson(lockPosition),
                    e);
            if (index == 0) {
                balanceBatchLockPosition(lockPosition, ++index);
            } else {
                throw e;
            }
        }
    }

    @Transactional(isolation = Isolation.READ_COMMITTED, propagation = Propagation.REQUIRED, rollbackFor = Throwable.class)
    public BalanceBatchOperatePositionTask batchUnlockPosition(Header header, String clientOrderId,
                                                               String desc, List<BalanceBatchUnlockPosition> batchUnlockPositionList) {
        BalanceBatchOperatePositionTask task = balanceBatchOperatePositionTaskMapper
                .lockByClientOrderId(header.getOrgId(), clientOrderId,
                        BalancePositionOperateType.UNLOCK.type());
        if (task == null) {
            Long currentTimestamp = System.currentTimeMillis();
            task = BalanceBatchOperatePositionTask.builder()
                    .orgId(header.getOrgId())
                    .clientOrderId(clientOrderId)
                    .type(BalancePositionOperateType.UNLOCK.type())
                    .desc(desc)
                    .status(0)
                    .created(currentTimestamp)
                    .updated(currentTimestamp)
                    .build();
            balanceBatchOperatePositionTaskMapper.insertSelective(task);
            Long taskId = task.getId();
            batchUnlockPositionList = batchUnlockPositionList.stream()
                    .map(item ->
                    {
                        BalanceBatchUnlockPosition unlockPosition = item.toBuilder().build();
                        unlockPosition.setTaskId(taskId);
                        unlockPosition.setOrgId(header.getOrgId());
                        unlockPosition.setClientOrderId(clientOrderId);
                        unlockPosition.setUnlockId(0L);
                        unlockPosition.setLockId(0L);
                        unlockPosition.setStatus(0);
                        unlockPosition.setResponse("");
                        try {
                            if (item.getAccountId() == null || item.getAccountId() == 0L) {
                                Long accountId = getAccountId(header.getOrgId(), item.getUserId());
                                unlockPosition.setAccountId(accountId);
                            } else {
                                Long tmpSourceUserId = getUserIdByAccountId(header.getOrgId(),
                                        item.getAccountId());
                                if (!tmpSourceUserId.equals(item.getUserId())) {
                                    unlockPosition.setStatus(BrokerErrorCode.ACCOUNT_NOT_EXIST.code());
                                } else {
                                    unlockPosition.setAccountId(item.getAccountId());
                                }
                            }

                        } catch (Exception e) {
                            unlockPosition.setStatus(BrokerErrorCode.USER_NOT_EXIST.code());
                        }
                        if (unlockPosition.getStatus() == 0) {
                            unlockPosition.setUnlockId(sequenceGenerator.getLong());
                        }
                        if (unlockPosition.getFromPositionLocked() == 1) {
                            unlockPosition.setLockId(item.getLockId());
                        }
                        unlockPosition.setCreated(currentTimestamp);
                        unlockPosition.setUpdated(currentTimestamp);
                        return unlockPosition;
                    }).collect(Collectors.toList());
            balanceBatchUnlockPositionMapper.insertList(batchUnlockPositionList);
            CompletableFuture.runAsync(() -> {
                try {
                    Thread.sleep(1000);
                    balanceBatchUnlockPosition(header, clientOrderId, 0);
                } catch (Exception e) {
                    log.error(" submit batchUnlockPosition error", e);
                }
            }, taskExecutor);
        } else if (task.getStatus() == 0
                && System.currentTimeMillis() > task.getCreated() + 60 * 1000) {
            CompletableFuture.runAsync(() -> {
                try {
                    balanceBatchUnlockPosition(header, clientOrderId, 0);
                } catch (Exception e) {
                    log.error(" submit batchUnlockPosition error", e);
                }
            }, taskExecutor);
        }
        return task;
    }

    private void balanceBatchUnlockPosition(Header header, String clientOrderId, int index) {
        try {
            BalanceBatchUnlockPosition queryObj = BalanceBatchUnlockPosition.builder()
                    .orgId(header.getOrgId())
                    .clientOrderId(clientOrderId)
                    .status(0)
                    .build();
            List<BalanceBatchUnlockPosition> batchUnlockPositionList = balanceBatchUnlockPositionMapper
                    .select(queryObj);
            batchUnlockPositionList
                    .forEach(unlockPosition -> balanceBatchUnlockPosition(unlockPosition, 0));
            BalanceBatchOperatePositionTask task = balanceBatchOperatePositionTaskMapper
                    .getByClientOrderId(header.getOrgId(), clientOrderId,
                            BalancePositionOperateType.UNLOCK.type());
            BalanceBatchOperatePositionTask updateObj = BalanceBatchOperatePositionTask.builder()
                    .id(task.getId())
                    .status(1)
                    .updated(System.currentTimeMillis())
                    .build();
            balanceBatchOperatePositionTaskMapper.updateByPrimaryKeySelective(updateObj);
        } catch (Exception e) {
            log.error("balanceBatchUnlockPosition with async:(orgId:{}, clientOrderId:{}) error",
                    header.getOrgId(), clientOrderId, e);
            if (index == 0) {
                balanceBatchUnlockPosition(header, clientOrderId, ++index);
            } else {
                throw e;
            }
        }
    }

    private void balanceBatchUnlockPosition(BalanceBatchUnlockPosition unlockPosition, int index) {
        try {
            unlockPosition = balanceBatchUnlockPositionMapper
                    .selectByPrimaryKey(unlockPosition.getId());
            if (unlockPosition.getStatus() == 0) {
                UnlockBalanceRequest request = UnlockBalanceRequest
                        .newBuilder()
                        .setBaseRequest(
                                BaseRequest.newBuilder().setOrganizationId(unlockPosition.getOrgId())
                                        .setBrokerUserId(unlockPosition.getUserId()).build())
                        .setClientReqId(unlockPosition.getUnlockId()) // 幂等性
                        .setOriginClientReqId(unlockPosition.getLockId())
                        .setAccountId(unlockPosition.getAccountId())
                        .setTokenId(unlockPosition.getTokenId())
                        .setUnlockAmount(
                                unlockPosition.getAmount().stripTrailingZeros().toPlainString())
                        .setUnlockFromPositionLocked(unlockPosition.getFromPositionLocked() == 1)
                        .setUnlockReason(unlockPosition.getDesc())
                        .build();
                UnlockBalanceResponse response = grpcBalanceService.unLockBalance(request);
                BalanceBatchUnlockPosition updateObj = BalanceBatchUnlockPosition.builder()
                        .id(unlockPosition.getId())
                        .status(response.getCodeValue())
                        .response(JsonUtil.defaultGson().toJson(response))
                        .updated(System.currentTimeMillis())
                        .build();
                balanceBatchUnlockPositionMapper.updateByPrimaryKeySelective(updateObj);
            }
        } catch (Exception e) {
            log.error("batch unlock position:{} error",
                    JsonUtil.defaultGson().toJson(unlockPosition), e);
            if (index == 0) {
                balanceBatchUnlockPosition(unlockPosition, ++index);
            } else {
                throw e;
            }
        }
    }

    public BalanceBatchOperatePositionResult<BalanceBatchLockPosition> getBalanceBatchLockPositionResult(
            Header header, String clientOrderId) {
        BalanceBatchOperatePositionTask operateTask = balanceBatchOperatePositionTaskMapper
                .getByClientOrderId(header.getOrgId(), clientOrderId,
                        BalancePositionOperateType.LOCK.type());
        if (operateTask == null) {
            throw new BrokerException(BrokerErrorCode.DB_RECORD_NOT_EXISTS);
        }
        List<BalanceBatchLockPosition> batchLockPositionList = new ArrayList<>();
        if (operateTask.getStatus() == 1) {
            BalanceBatchLockPosition queryObj = BalanceBatchLockPosition.builder()
                    .orgId(header.getOrgId())
                    .clientOrderId(clientOrderId)
                    .build();
            batchLockPositionList = balanceBatchLockPositionMapper.select(queryObj);
        } else if (System.currentTimeMillis() > operateTask.getCreated() + 60 * 1000) {
            CompletableFuture.runAsync(() -> {
                try {
                    balanceBatchLockPosition(header, clientOrderId, 0);
                } catch (Exception e) {
                    log.error(" getBalanceBatchLockPositionResult error", e);
                }
            }, taskExecutor);
        }
        return BalanceBatchOperatePositionResult.<BalanceBatchLockPosition>builder()
                .operateTask(operateTask)
                .operateItemList(batchLockPositionList)
                .build();
    }

    public BalanceBatchOperatePositionResult<BalanceBatchUnlockPosition> getBalanceBatchUnlockPositionResult(
            Header header, String clientOrderId) {
        BalanceBatchOperatePositionTask operateTask = balanceBatchOperatePositionTaskMapper
                .getByClientOrderId(header.getOrgId(), clientOrderId,
                        BalancePositionOperateType.UNLOCK.type());
        if (operateTask == null) {
            throw new BrokerException(BrokerErrorCode.DB_RECORD_NOT_EXISTS);
        }
        List<BalanceBatchUnlockPosition> batchUnlockPositionList = new ArrayList<>();
        if (operateTask.getStatus() == 1) {
            BalanceBatchUnlockPosition queryObj = BalanceBatchUnlockPosition.builder()
                    .orgId(header.getOrgId())
                    .clientOrderId(clientOrderId)
                    .build();
            batchUnlockPositionList = balanceBatchUnlockPositionMapper.select(queryObj);
        } else if (System.currentTimeMillis() > operateTask.getCreated() + 60 * 1000) {
            CompletableFuture.runAsync(() -> {
                try {
                    balanceBatchUnlockPosition(header, clientOrderId, 0);
                } catch (Exception e) {
                    log.error(" getBalanceBatchUnlockPositionResult error", e);
                }
            }, taskExecutor);
        }
        return BalanceBatchOperatePositionResult.<BalanceBatchUnlockPosition>builder()
                .operateTask(operateTask)
                .operateItemList(batchUnlockPositionList)
                .build();
    }

    @Transactional(isolation = Isolation.READ_COMMITTED, propagation = Propagation.REQUIRED, rollbackFor = Throwable.class)
    public BaseResult balanceLockPosition(Header header, String clientOrderId,
                                          BalanceLockPositionObj lockPositionObj) {
        BalanceLockPosition lockPosition = balanceLockPositionMapper
                .getByClientOrderId(header.getOrgId(), clientOrderId);
        if (lockPosition == null) {
            lockPosition = newBalanceLockPosition(header, clientOrderId, lockPositionObj);
            balanceLockPositionMapper.insert(lockPosition);
        }
        return balanceLockPosition(lockPosition, 0);
    }

    @Transactional(isolation = Isolation.READ_COMMITTED, propagation = Propagation.REQUIRED, rollbackFor = Throwable.class)
    public BaseResult balanceUnlockPosition(Header header, String clientOrderId,
                                            BalanceUnlockPositionObj unlockPositionObj) {
        BalanceUnlockPosition unlockPosition = balanceUnlockPositionMapper
                .getByClientOrderId(header.getOrgId(), clientOrderId);
        if (unlockPosition == null) {
            unlockPosition = newBalanceUnlockPosition(header, clientOrderId, unlockPositionObj);
            balanceUnlockPositionMapper.insert(unlockPosition);
        }
        return balanceUnlockPosition(unlockPosition, 0);
    }

    private BalanceLockPosition newBalanceLockPosition(Header header, String clientOrderId,
                                                       BalanceLockPositionObj lockPositionObj) {
        BalanceLockPosition lockPosition = BalanceLockPosition.builder().build();
        lockPosition.setOrgId(header.getOrgId());
        lockPosition.setUserId(lockPositionObj.getUserId());
        lockPosition.setTokenId(lockPositionObj.getTokenId());
        lockPosition.setClientOrderId(clientOrderId);
        lockPosition.setDesc(lockPositionObj.getDesc());
        // TODO: 暂时不使用客户端的lockId
        lockPosition.setLockId(0L);
        lockPosition.setAmount(new BigDecimal(lockPositionObj.getAmount()));
        lockPosition.setRemainUnlockedAmount(BigDecimal.ZERO);
        lockPosition.setToPositionLocked(lockPositionObj.getToPositionLocked() ? 1 : 0);
        lockPosition.setStatus(0);
        lockPosition.setResponse("");

        try {
            if (lockPositionObj.getAccountId() == 0L) {
                Long accountId = getAccountId(header.getOrgId(), lockPositionObj.getUserId());
                lockPosition.setAccountId(accountId);
            } else {
                List<Account> accountList = accountMapper.queryByUserId(header.getOrgId(), lockPositionObj.getUserId());
                Account lockAccount = accountList.stream().filter(account -> account.getAccountId().equals(lockPositionObj.getAccountId())).findFirst().orElse(null);
                if (lockAccount == null) {
                    lockPosition.setStatus(BrokerErrorCode.ACCOUNT_NOT_EXIST.code());
                }
                if (lockAccount.getAccountType() != AccountType.MAIN.value() && lockAccount.getIsAuthorizedOrg() == 0) {
                    log.error("lock error accoutType is not main or AuthorizedOrg is false: accountId:{} type:{} AuthorizedOrg:{} ",
                            lockAccount.getAccountId(), lockAccount.getAccountType(), lockAccount.getIsAuthorizedOrg());
                    lockPosition.setStatus(BrokerErrorCode.USER_ACCOUNT_LOCK_FILLED.code());
                } else {
                    lockPosition.setAccountId(lockPositionObj.getAccountId());
                }
            }
        } catch (Exception e) {
            log.error(String.format("get account error. org_id: %s user_id: %s",
                    header.getOrgId(), lockPositionObj.getUserId()), e);
            lockPosition.setStatus(BrokerErrorCode.USER_NOT_EXIST.code());
        }

        if (lockPosition.getStatus() == 0) {
            lockPosition.setLockId(sequenceGenerator.getLong());
        }

        if (lockPosition.getToPositionLocked() == 1) {
            lockPosition.setRemainUnlockedAmount(lockPosition.getAmount());
        }

        Long currentTimestamp = System.currentTimeMillis();
        lockPosition.setCreated(currentTimestamp);
        lockPosition.setUpdated(currentTimestamp);

        return lockPosition;
    }

    public BalanceUnlockPosition newBalanceUnlockPosition(Header header, String clientOrderId,
                                                          BalanceUnlockPositionObj unlockPositionObj) {
        BalanceUnlockPosition unlockPosition = BalanceUnlockPosition.builder().build();
        unlockPosition.setOrgId(header.getOrgId());
        unlockPosition.setClientOrderId(clientOrderId);
        unlockPosition.setUserId(unlockPositionObj.getUserId());
        unlockPosition.setDesc(unlockPositionObj.getDesc());
        unlockPosition.setTokenId(unlockPositionObj.getTokenId());
        unlockPosition.setAmount(new BigDecimal(unlockPositionObj.getAmount()));
        unlockPosition.setFromPositionLocked(unlockPositionObj.getFromPositionLocked() ? 1 : 0);
        unlockPosition.setUnlockId(0L);
        unlockPosition.setLockId(0L);
        unlockPosition.setStatus(0);
        unlockPosition.setResponse("");

        try {
            if (unlockPositionObj.getAccountId() == 0L) {
                Long accountId = getAccountId(header.getOrgId(), unlockPositionObj.getUserId());
                unlockPosition.setAccountId(accountId);
            } else {
                Long tmpSourceUserId = getUserIdByAccountId(header.getOrgId(),
                        unlockPositionObj.getAccountId());
                if (!tmpSourceUserId.equals(unlockPositionObj.getUserId())) {
                    unlockPosition.setStatus(BrokerErrorCode.ACCOUNT_NOT_EXIST.code());
                } else {
                    unlockPosition.setAccountId(unlockPositionObj.getAccountId());
                }
            }
        } catch (Exception e) {
            log.error(String.format("get account error. org_id: %s user_id: %s",
                    header.getOrgId(), unlockPositionObj.getUserId()), e);
            unlockPosition.setStatus(BrokerErrorCode.USER_NOT_EXIST.code());
        }

        if (unlockPosition.getStatus() == 0) {
            unlockPosition.setUnlockId(sequenceGenerator.getLong());
        }

        if (unlockPosition.getFromPositionLocked() == 1) {
            unlockPosition.setLockId(unlockPositionObj.getLockId());
        }

        Long currentTimestamp = System.currentTimeMillis();
        unlockPosition.setCreated(currentTimestamp);
        unlockPosition.setUpdated(currentTimestamp);
        return unlockPosition;
    }

    private BaseResult balanceLockPosition(BalanceLockPosition lockPosition, int index) {
        try {
            lockPosition = balanceLockPositionMapper.selectByPrimaryKey(lockPosition.getId());
            if (lockPosition.getStatus() == 200) {
                return BaseResult.success();
            }

            BaseRequest baseRequest = BaseRequest.newBuilder()
                    .setOrganizationId(lockPosition.getOrgId())
                    .setBrokerUserId(lockPosition.getUserId())
                    .build();

            LockBalanceRequest request = LockBalanceRequest.newBuilder()
                    .setBaseRequest(baseRequest)
                    .setClientReqId(lockPosition.getLockId())
                    .setAccountId(lockPosition.getAccountId())
                    .setTokenId(lockPosition.getTokenId())
                    .setLockAmount(lockPosition.getAmount().stripTrailingZeros().toPlainString())
                    .setLockedToPositionLocked(lockPosition.getToPositionLocked() == 1)
                    .setLockReason(lockPosition.getDesc())
                    .build();

            LockBalanceReply reply = grpcBalanceService.lockBalance(request);
            int status = reply.getCodeValue();
            if (reply.getCode() == LockBalanceReply.ReplyCode.REPEAT_LOCK) {
                status = 200;
            }

            // 更新锁仓结果到数据库
            BalanceLockPosition updateLockPosition = BalanceLockPosition.builder().build();
            updateLockPosition.setId(lockPosition.getId());
            updateLockPosition.setStatus(status);
            updateLockPosition.setResponse(JsonUtil.defaultGson().toJson(reply));
            updateLockPosition.setUpdated(System.currentTimeMillis());
            balanceLockPositionMapper.updateByPrimaryKeySelective(updateLockPosition);

            if (status == 200) {
                return BaseResult.success();
            }
            if (reply.getCode() == LockBalanceReply.ReplyCode.INSUFFICIENT_BALANCE) {
                return BaseResult.fail(BrokerErrorCode.INSUFFICIENT_BALANCE);
//                throw new BrokerException(BrokerErrorCode.INSUFFICIENT_BALANCE);
            }
            return BaseResult.fail(BrokerErrorCode.USER_ACCOUNT_LOCK_FILLED);
        } catch (Exception e) {
            log.error("lock position:{} error", JsonUtil.defaultGson().toJson(lockPosition), e);
            if (index == 0) {
                return balanceLockPosition(lockPosition, ++index);
            } else {
                return BaseResult.fail(BrokerErrorCode.USER_ACCOUNT_LOCK_FILLED);
//                throw e;
            }
        }
    }

    private BaseResult balanceUnlockPosition(BalanceUnlockPosition unlockPosition, int index) {
        try {
            unlockPosition = balanceUnlockPositionMapper.selectByPrimaryKey(unlockPosition.getId());
            if (unlockPosition.getStatus() == 200) {
                return BaseResult.success();
            }
            BaseRequest baseRequest = BaseRequest.newBuilder()
                    .setOrganizationId(unlockPosition.getOrgId())
                    .setBrokerUserId(unlockPosition.getUserId())
                    .build();

            UnlockBalanceRequest request = UnlockBalanceRequest.newBuilder()
                    .setBaseRequest(baseRequest)
                    .setClientReqId(unlockPosition.getUnlockId())
                    .setOriginClientReqId(unlockPosition.getLockId())
                    .setAccountId(unlockPosition.getAccountId())
                    .setTokenId(unlockPosition.getTokenId())
                    .setUnlockAmount(unlockPosition.getAmount().stripTrailingZeros().toPlainString())
                    .setUnlockFromPositionLocked(unlockPosition.getFromPositionLocked() == 1)
                    .setUnlockReason(unlockPosition.getDesc())
                    .build();

            UnlockBalanceResponse response = grpcBalanceService.unLockBalance(request);
            int status = response.getCodeValue();
            if (response.getCode() == UnlockBalanceResponse.ReplyCode.REPEAT_UNLOCKED) {
                status = 200;
            }

            // 更新解锁结果到数据库
            BalanceUnlockPosition updateUnlockPosition = BalanceUnlockPosition.builder().build();
            updateUnlockPosition.setId(unlockPosition.getId());
            updateUnlockPosition.setStatus(status);
            updateUnlockPosition.setResponse(JsonUtil.defaultGson().toJson(response));
            updateUnlockPosition.setUpdated(System.currentTimeMillis());
            balanceUnlockPositionMapper.updateByPrimaryKeySelective(updateUnlockPosition);

            if (status == 200) {
                return BaseResult.success();
            }
            if (response.getCode() == UnlockBalanceResponse.ReplyCode.INSUFFICIENT_LOCKED_VALUE
                    || response.getCode() == UnlockBalanceResponse.ReplyCode.NO_POSITION) {
                return BaseResult.fail(BrokerErrorCode.INSUFFICIENT_BALANCE);
//                throw new BrokerException(BrokerErrorCode.INSUFFICIENT_BALANCE);
            }
            return BaseResult.fail(BrokerErrorCode.USER_ACCOUNT_UNLOCK_FILLED);
        } catch (Exception e) {
            log.error("unlock position: {} error", JsonUtil.defaultGson().toJson(unlockPosition),
                    e);
            if (index == 0) {
                return balanceUnlockPosition(unlockPosition, ++index);
            } else {
                return BaseResult.fail(BrokerErrorCode.USER_ACCOUNT_UNLOCK_FILLED);
//                throw e;
            }
        }
    }

    @Transactional(isolation = Isolation.READ_COMMITTED, propagation = Propagation.REQUIRED, rollbackFor = Throwable.class)
    public void subAccountTransferLimit(Header header, Long accountId, Long limitTime) {
        Account account = accountMapper.getAccountByAccountId(accountId);
        if (account == null) {
            throw new BrokerException(BrokerErrorCode.ACCOUNT_NOT_EXIST);
        }

        if (!account.getUserId().equals(header.getUserId())) {
            throw new BrokerException(BrokerErrorCode.PARAM_INVALID);
        }

        if (limitTime < new DateTime().getMillis()) {
            throw new BrokerException(BrokerErrorCode.END_TIME_MUST_GREATER_CONFIG_TIME);
        }

        if (limitTime < account.getForbidEndTime()) {
            throw new BrokerException(BrokerErrorCode.END_TIME_MUST_GREATER_CONFIG_TIME);
        }

        this.accountMapper
                .updateForbidEndTimeByAccountId(new Date().getTime(), limitTime, accountId);

        SubAccountTransferLimitLog subAccountTransferLimitLog = new SubAccountTransferLimitLog();
        subAccountTransferLimitLog.setUserId(header.getUserId());
        subAccountTransferLimitLog.setSubAccountId(accountId);
        subAccountTransferLimitLog.setBeforeJson(new Gson().toJson(account));
        account.setForbidEndTime(limitTime);
        account.setForbidStartTime(new Date().getTime());
        subAccountTransferLimitLog.setAfterJson(new Gson().toJson(account));
        subAccountTransferLimitLog.setCreated(new Date());
        this.subAccountTransferLimitLogMapper.insertSelective(subAccountTransferLimitLog);
    }

    @Transactional(isolation = Isolation.READ_COMMITTED, propagation = Propagation.REQUIRED, rollbackFor = Throwable.class)
    public void closeSubAccountTransferLimit(Header header, Long accountId) {
        Account account = accountMapper.getAccountByAccountId(accountId);
        if (account == null) {
            throw new BrokerException(BrokerErrorCode.ACCOUNT_NOT_EXIST);
        }
        if (!account.getUserId().equals(header.getUserId())) {
            throw new BrokerException(BrokerErrorCode.PARAM_INVALID);
        }

        if (account.getIsForbid().equals(ForbidType.CLOSE.value())) {
            return;
        }
        this.accountMapper.closeForbidByAccountId(accountId);
        SubAccountTransferLimitLog subAccountTransferLimitLog = new SubAccountTransferLimitLog();
        subAccountTransferLimitLog.setUserId(header.getUserId());
        subAccountTransferLimitLog.setSubAccountId(accountId);
        subAccountTransferLimitLog.setBeforeJson(new Gson().toJson(account));
        account.setForbidEndTime(0L);
        account.setForbidStartTime(0L);
        account.setIsForbid(0);
        subAccountTransferLimitLog.setAfterJson(new Gson().toJson(account));
        subAccountTransferLimitLog.setCreated(new Date());
        this.subAccountTransferLimitLogMapper.insert(subAccountTransferLimitLog);
    }

    public BaseResult thirdPartyUserTransfer(Header header, String clientOrderId,
                                             boolean transferIn,
                                             String thirdUserId, Long userId, AccountType accountType,
                                             String tokenId, String amount) {
        User user = userMapper.getByUserId(header.getUserId());
        if (user == null || user.getIsVip().equals(0)) {
            return BaseResult.fail(BrokerErrorCode.OPERATION_HAS_NO_PERMISSION);
        }
        if (commonIniService.getBooleanValueOrDefault(header.getOrgId(),
                BrokerServerConstants.CONTRACT_FINANCING_KEY, false)) {
            MasterKeyUserConfig masterKeyUserConfig = masterKeyUserConfigMapper
                    .selectMasterKeyUserConfigByUserId(header.getOrgId(), header.getUserId());
            if (masterKeyUserConfig == null || (transferIn
                    && masterKeyUserConfig.getTransferIn() == 0) || (!transferIn
                    && masterKeyUserConfig.getTransferOut() == 0)) {
                return BaseResult.fail(BrokerErrorCode.OPERATION_HAS_NO_PERMISSION);
            }
        }
        User virtualUser = thirdPartyUserService
                .getVirtualUser(header.getOrgId(), header.getUserId(), thirdUserId, userId);
        if (virtualUser.getUserStatus() != 1) {
            return BaseResult.fail(BrokerErrorCode.USER_STATUS_FORBIDDEN);
        }
        if (transferIn) {
            return userTransfer(header, clientOrderId, header.getUserId(), AccountType.MAIN,
                    virtualUser.getUserId(), accountType, false,
                    BusinessSubject.MASTER_TRANSFER_IN_VALUE, 0, tokenId, amount);
        } else {
            return userTransfer(header, clientOrderId, virtualUser.getUserId(), accountType,
                    header.getUserId(), AccountType.MAIN, false,
                    BusinessSubject.MASTER_TRANSFER_OUT_VALUE, 0, tokenId, amount);
        }
    }

    public BaseResult institutionalUserTransfer(Header header, String clientOrderId, Long userId,
                                                String tokenId, String amount, boolean toPosition,
                                                Integer businessSubject, Integer subBusinessSubject) {
        User user = userMapper.getByOrgAndUserId(header.getOrgId(), header.getUserId());
        if (user.getUserType() != UserType.INSTITUTIONAL_USER.value()) {
            return BaseResult.fail(BrokerErrorCode.FEATURE_NOT_OPEN);
        }
        if (subBusinessSubject != null && subBusinessSubject > 0
                && subBusinessSubjectService
                .getSubBusinessSubject(header.getOrgId(), businessSubject, subBusinessSubject)
                == null) {
            log.warn(
                    "userId:{} clientOrderId:{} use a invalid BusinessSubject:{} sub_BusinessSubject:{}",
                    header.getUserId(), clientOrderId, businessSubject, subBusinessSubject);
            return BaseResult.fail(BrokerErrorCode.ERROR_SUB_BUSINESS_SUBJECT);
        }
        if (businessSubject != 3 && businessSubject != 70) {
            log.warn("userId:{} clientOrderId:{} use a invalid business subject:{}",
                    header.getUserId(), clientOrderId, businessSubject);
            return BaseResult.fail(BrokerErrorCode.PARAM_INVALID);
        }
        return userTransfer(header, clientOrderId, header.getUserId(), AccountType.MAIN, userId,
                AccountType.MAIN, toPosition,
                businessSubject, subBusinessSubject, tokenId, amount);
    }

    public BaseResult userTransfer(Header header, String clientOrderId, Long fromUserId,
                                   AccountType fromAccountType,
                                   Long toUserId, AccountType toAccountType, boolean toPosition,
                                   Integer businessSubject, Integer subBusinessSubject, String tokenId, String amount) {
        return userTransfer(header, clientOrderId, fromUserId, fromAccountType,
                DEFAULT_ACCOUNT_INDEX, toUserId, toAccountType, DEFAULT_ACCOUNT_INDEX, toPosition,
                businessSubject, subBusinessSubject, tokenId, amount);
    }

    public BaseResult userTransfer(Header header, String clientOrderId,
                                   Long fromUserId, AccountType fromAccountType, Integer fromAccountIndex,
                                   Long toUserId, AccountType toAccountType, Integer toAccountIndex, boolean toPosition,
                                   Integer businessSubject, Integer subBusinessSubject, String tokenId, String amount) {
        Long fromAccountId = getAccountId(header.getOrgId(), fromUserId, fromAccountType,
                fromAccountIndex);
        Long toAccountId = getAccountId(header.getOrgId(), toUserId, toAccountType, toAccountIndex);
        return userTransfer(header, clientOrderId, fromUserId, fromAccountId, toUserId, toAccountId,
                toPosition, businessSubject, subBusinessSubject, tokenId, amount);
    }

    public BaseResult userTransfer(Header header, String clientOrderId,
                                   Long sourceUserId, Long sourceAccountId, Long targetUserId, Long targetAccountId,
                                   boolean toPosition,
                                   Integer businessSubject, Integer subBusinessSubject, String tokenId, String amount) {
        UserTransferRecord userTransferRecord = userTransferRecordMapper
                .getByClientOrderIdAndSourceUserId(header.getOrgId(), header.getUserId(),
                        clientOrderId);
        if (userTransferRecord == null) {
            Long transferId = sequenceGenerator.getLong();
            userTransferRecord = UserTransferRecord.builder()
                    .orgId(header.getOrgId())
                    .userId(header.getUserId())
                    .clientOrderId(clientOrderId)
                    .transferId(transferId)
                    .subject(businessSubject)
                    .subSubject(subBusinessSubject)
                    .subSubject(0)
                    .sourceUserId(sourceUserId)
                    .sourceAccountId(sourceAccountId)
                    .targetUserId(targetUserId)
                    .targetAccountId(targetAccountId)
                    .tokenId(tokenId)
                    .amount(new BigDecimal(amount))
                    .status(0)
                    .response("")
                    .created(System.currentTimeMillis())
                    .updated(System.currentTimeMillis())
                    .build();
            userTransferRecordMapper.insertSelective(userTransferRecord);
        } else {
            if (userTransferRecord.getStatus() == 200) {
                return BaseResult.success();
            }
        }
        SyncTransferRequest transferRequest = SyncTransferRequest.newBuilder()
                .setBaseRequest(BaseReqUtil.getBaseRequest(header.getOrgId()))
                .setClientTransferId(userTransferRecord.getTransferId())
                .setSourceOrgId(header.getOrgId())
                .setSourceAccountId(userTransferRecord.getSourceAccountId())
                .setTargetOrgId(userTransferRecord.getOrgId())
                .setTargetAccountId(userTransferRecord.getTargetAccountId())
                .setSourceFlowSubject(BusinessSubject.forNumber(userTransferRecord.getSubject()))
                .setSourceFlowSecondSubject(subBusinessSubject)
                .setTargetFlowSubject(BusinessSubject.forNumber(userTransferRecord.getSubject()))
                .setTargetFlowSecondSubject(subBusinessSubject)
                .setToPosition(toPosition)
                .setTokenId(userTransferRecord.getTokenId())
                .setAmount(userTransferRecord.getAmount().stripTrailingZeros().toPlainString())
                .build();
        SyncTransferResponse response = grpcBatchTransferService.syncTransfer(transferRequest);
        UserTransferRecord updateObj = UserTransferRecord.builder()
                .id(userTransferRecord.getId())
                .status(response.getCodeValue())
                .response(JsonUtil.defaultGson().toJson(response))
                .updated(System.currentTimeMillis())
                .build();
        userTransferRecordMapper.updateByPrimaryKeySelective(updateObj);
        if (response.getCode() == SyncTransferResponse.ResponseCode.SUCCESS) {
            return BaseResult.success();
        }
        log.warn("userTransfer clientOrderId:{} get Error(code:{}, msg:{})", clientOrderId,
                response.getCode().name(), response.getMsg());
        return handleTransferResponse(response, false);
    }

    public BaseResult<List<Balance>> getThirdPartyUserBalance(Header header, String thirdUserId,
                                                              Long userId, AccountType accountType) {
        User user = userMapper.getByUserId(header.getUserId());
        if (user == null || user.getIsVip().equals(0)) {
            return BaseResult.fail(BrokerErrorCode.NO_PERMISSION_TO_CREATE_VIRTUAL_ACCOUNT);
        }
        User virtualUser = thirdPartyUserService
                .getVirtualUser(header.getOrgId(), header.getUserId(), thirdUserId, userId);
        if (virtualUser.getUserStatus() != 1) {
            return BaseResult.fail(BrokerErrorCode.USER_STATUS_FORBIDDEN);
        }
        Long accountId = getAccountId(header.getOrgId(), virtualUser.getUserId(), accountType,
                DEFAULT_ACCOUNT_INDEX);
        List<Balance> balanceList = queryBalance(header, accountId, Lists.newArrayList());
        return BaseResult.success(balanceList);
    }

    public boolean checkTransferlimit(Long orgId, Long userId, Long createTime, String tokenId,
                                      BigDecimal amount, String clientOrderId) {
        TransferLimit transferLimit = transferLimitMapper.getTransferLimit(orgId, tokenId);
        if (transferLimit == null) {
            return true;
        }
        //校验单笔限额
        if (transferLimit.getMaxAmount().compareTo(BigDecimal.ZERO) == 1
                && amount.compareTo(transferLimit.getMaxAmount()) == 1) {
            log.warn(
                    "checkTransferlimit clientOrderId: {},orgId:{}, tokenId:{} amount：{} greater than maxAomount:{} ",
                    clientOrderId, orgId, tokenId, amount, transferLimit.getMaxAmount());
            return false;
        }
        //校验每日限额
        if (transferLimit.getDayQuota().compareTo(BigDecimal.ZERO) == 0) {
            return true;
        }
        try {
            String quotaKey = String
                    .format(BrokerServerConstants.TRANSFER_USER_DAY_QUOTA, orgId, userId, tokenId,
                            createTime);
            if (!redisTemplate.hasKey(quotaKey)) {
                redisTemplate.opsForValue().set(quotaKey, "0", Duration.ofHours(24));
            }
            BigDecimal usedQuota = CommonUtil
                    .toBigDecimal(redisTemplate.opsForValue().get(quotaKey), BigDecimal.ZERO);
            if (usedQuota.add(amount).compareTo(transferLimit.getDayQuota()) > 0) {
                return false;
            }
            return true;
        } catch (Exception e) {
            log.warn("error with checkTransferlimit orgId:{} userId:{} clientorderId:{} ", orgId,
                    userId, clientOrderId, e);
            return false;
        }
    }

    public Account addAccountByType(Long orgId, Long userId, io.bhex.base.account.AccountType accountType) {
        if (orgId == null || userId == null || accountType == null) {
            return null;
        }
        Account account = accountMapper.getAccountByType(orgId, userId, accountType.getNumber(), 0);
        if (account != null) {
            return account;
        }
        AddAccountRequest addAccountRequest = AddAccountRequest
                .newBuilder()
                .setOrgId(orgId)
                .setBrokerUserId(String.valueOf(userId))
                .setType(accountType)
                .build();
        AddAccountReply reply = grpcAccountService.addAccount(addAccountRequest);
        Account newAccount = Account.builder()
                .orgId(orgId)
                .userId(userId)
                .accountId(reply.getAccountId())
                .accountName("")
                .accountType(accountType.getNumber())
                .created(System.currentTimeMillis())
                .updated(System.currentTimeMillis())
                .build();
        accountMapper.insertRecord(newAccount);
        return newAccount;
    }

    public List<BalanceBatchTransfer> getAirDropDetailResult(Header header,
                                                             Integer subSubject,
                                                             Integer status,
                                                             Long fromId,
                                                             Long endId,
                                                             Long startTime,
                                                             Long endTime,
                                                             Integer limit) {
        try {
            Example example = Example.builder(BalanceBatchTransfer.class).orderByDesc("id").build();
            Example.Criteria criteria = example.createCriteria();
            criteria.andEqualTo("orgId", header.getOrgId());
            criteria.andEqualTo("subject", BusinessSubject.AIRDROP_VALUE);
            if (subSubject != null && subSubject != 0) {
                criteria.andEqualTo("subSubject", subSubject);
            }
            if (status != null && status != 0) {
                criteria.andEqualTo("status", status);
            }
            if (fromId != null && fromId > 0) {
                criteria.andGreaterThan("id", fromId);
            }
            if (endId != null && endId > 0) {
                criteria.andLessThan("id", endId);
            }
            if (startTime != null && startTime > 0) {
                criteria.andGreaterThan("created", startTime);
            }
            if (endTime != null && endTime > 0) {
                criteria.andLessThan("created", endTime);
            }
            //增加limit查询上线
            limit = limit > 500 ? 500 : limit;
            return balanceBatchTransferMapper.selectByExampleAndRowBounds(example, new RowBounds(0, limit));
        } catch (Exception e) {
            log.warn("getAirDropDetailResult error: orgId:{} subSubject:{} status:{} ", header.getOrgId(),
                    subSubject, status, e);
            return new ArrayList<>();
        }
    }

    /**
     * 查询批量转账明细
     */
    public List<BalanceBatchTransfer> getBalanceBatchTransferDetail(Header header, Long toId, Integer limit) {
        if (limit <= 0) {
            return new ArrayList<>();
        }

        try {
            Example example = Example.builder(BalanceBatchTransfer.class).orderByAsc("id").build();
            Example.Criteria criteria = example.createCriteria();
            criteria.andEqualTo("orgId", header.getOrgId());
            if (toId != null && toId > 0) {
                criteria.andGreaterThan("id", toId);
            }
            //增加limit查询上限
            limit = limit > 500 ? 500 : limit;
            return balanceBatchTransferMapper.selectByExampleAndRowBounds(example, new RowBounds(0, limit));
        } catch (Exception e) {
            log.warn("getBalanceBatchTransferDetail error: orgId:{} toId:{} ", header.getOrgId(), toId, e);
            return new ArrayList<>();
        }
    }

    /**
     * 查询转账明细
     */
    public List<BalanceTransfer> getBalanceTransferDetail(Header header, Long toId, Integer limit) {
        if (limit <= 0) {
            return new ArrayList<>();
        }

        try {
            Example example = Example.builder(BalanceTransfer.class).orderByAsc("id").build();
            Example.Criteria criteria = example.createCriteria();
            criteria.andEqualTo("orgId", header.getOrgId());
            if (toId != null && toId > 0) {
                criteria.andGreaterThan("id", toId);
            }
            //增加limit查询上限
            limit = limit > 500 ? 500 : limit;
            return balanceTransferMapper.selectByExampleAndRowBounds(example, new RowBounds(0, limit));
        } catch (Exception e) {
            log.warn("getBalanceTransferDetail error: orgId:{} toId:{} ", header.getOrgId(), toId, e);
            return new ArrayList<>();
        }
    }

    public CheckBindFundAccountResponse checkBindFundAccount(CheckBindFundAccountRequest request) {
        //0=正常，1=账户不存在 2=已是资金账户 3=未绑定手机或GA 4=非币币主账户
        User user;

        try {
            if (!checkAccountIdMain(request.getOrgId(), request.getAccountId())) {
                return CheckBindFundAccountResponse.newBuilder().setCode(4).build();
            }

            user = this.getUserInfoByAccountId(request.getOrgId(), request.getAccountId());

        } catch (Exception e) {
            return CheckBindFundAccountResponse.newBuilder().setCode(1).build();
        }

        if (user.getBindGA() != 1 && StringUtils.isEmpty(user.getMobile())) {
            return CheckBindFundAccountResponse.newBuilder().setCode(3).build();
        }

        Example example = new Example(FundAccount.class);
        example.createCriteria().andEqualTo("orgId", request.getOrgId()).andEqualTo("accountId", request.getAccountId());
        int count = fundAccountMapper.selectCountByExample(example);
        if (count > 0) {
            return CheckBindFundAccountResponse.newBuilder().setCode(2).build();
        }

        return CheckBindFundAccountResponse.newBuilder()
                .setCode(0)
                .setResponse(CheckBindFundAccountResponse.CheckResponse
                        .newBuilder()
                        .setBindGa(user.getBindGA() == 1 ? true : false)
                        .setBindTradePwd(user.getBindTradePwd() == 1 ? true : false)
                        .setEmail(user.getEmail())
                        .setNationalCode(user.getNationalCode())
                        .setMobile(user.getMobile())
                        .setUserId(user.getUserId())
                        .build())
                .build();
    }

    public BindFundAccountResponse bindFundAccount(BindFundAccountRequest request) {
        //0=正常，1=账户不存在 2=已是资金账户 3=未绑定手机或GA 4=非币币账户 5=ga验证错误
        if (request.getAuthType() == AuthType.GA.value()) {
            Header header = Header.newBuilder()
                    .setOrgId(request.getOrgId())
                    .setUserId(request.getUserId())
                    .setRequestTime(System.currentTimeMillis())
                    .build();
            try {
                userSecurityService.validGACode(header, request.getUserId(), request.getVerifyCode());
            } catch (Exception e) {
                log.warn("bindFundAccount verify ga error", e);
                return BindFundAccountResponse.newBuilder().setCode(5).build();
            }
        }

        try {
            if (!checkAccountIdMain(request.getOrgId(), request.getAccountId())) {
                return BindFundAccountResponse.newBuilder().setCode(4).build();
            }

            this.getUserInfoByAccountId(request.getOrgId(), request.getAccountId());

        } catch (Exception e) {
            return BindFundAccountResponse.newBuilder().setCode(1).build();
        }

        Example example = new Example(FundAccount.class);
        example.createCriteria().andEqualTo("orgId", request.getOrgId()).andEqualTo("accountId", request.getAccountId());
        int count = fundAccountMapper.selectCountByExample(example);
        if (count > 0) {
            return BindFundAccountResponse.newBuilder().setCode(2).build();
        }

        FundAccount fundAccount = new FundAccount();
        fundAccount.setAccountId(request.getAccountId());
        fundAccount.setOrgId(request.getOrgId());
        fundAccount.setUserId(request.getUserId());
        fundAccount.setTag(request.getTag());
        fundAccount.setRemark(request.getRemark());
        fundAccount.setIsShow(0);
        fundAccount.setCreatedAt(System.currentTimeMillis());
        fundAccount.setUpdatedAt(System.currentTimeMillis());

        fundAccountMapper.insertSelective(fundAccount);

        return BindFundAccountResponse.newBuilder().setCode(0).build();
    }

    public QueryFundAccountResponse queryFundAccount(QueryFundAccountRequest request) {
        Example example = new Example(FundAccount.class);
        example.createCriteria().andEqualTo("orgId", request.getOrgId());
        example.setOrderByClause("id desc");

        List<FundAccount> fundAccounts = fundAccountMapper.selectByExample(example);

        List<QueryFundAccountResponse.FundAccount> fundAccountList = new ArrayList<>();

        fundAccounts.forEach(fundAccount -> {
            QueryFundAccountResponse.FundAccount account = QueryFundAccountResponse.FundAccount.newBuilder()
                    .setAccountId(fundAccount.getAccountId())
                    .setCreatedAt(fundAccount.getCreatedAt())
                    .setUpdatedAt(fundAccount.getUpdatedAt())
                    .setId(fundAccount.getId())
                    .setOrgId(fundAccount.getOrgId())
                    .setUserId(fundAccount.getUserId())
                    .setIsShow(fundAccount.getIsShow())
                    .setRemark(fundAccount.getRemark())
                    .setTag(fundAccount.getTag())
                    .build();
            fundAccountList.add(account);
        });

        return QueryFundAccountResponse.newBuilder().addAllFundAccountList(fundAccountList).build();

    }

    public SetFundAccountShowResponse setFundAccountShow(SetFundAccountShowRequest request) {

        Example example = new Example(FundAccount.class);
        example.createCriteria().andEqualTo("orgId", request.getOrgId()).andEqualTo("accountId", request.getAccountId());

        FundAccount fundAccount = fundAccountMapper.selectOneByExample(example);

        if (fundAccount != null) {
            if (request.getIsShow() == 1) {
                fundAccount.setIsShow(1);
            } else {
                fundAccount.setIsShow(0);
            }

            fundAccount.setUpdatedAt(System.currentTimeMillis());

            fundAccountMapper.updateByPrimaryKeySelective(fundAccount);
        }
        return SetFundAccountShowResponse.newBuilder().setCode(0).build();
    }

    public QueryFundAccountShowResponse queryFundAccountShow(QueryFundAccountShowRequest request) {

        Example example = new Example(FundAccount.class);
        example.createCriteria().andEqualTo("orgId", request.getOrgId()).andEqualTo("isShow", 1);
        List<FundAccount> fundAccountList = fundAccountMapper.selectByExample(example);


        List<QueryFundAccountShowResponse.FundAccountShow> list = new ArrayList<>();

        for (FundAccount fundAccount : fundAccountList) {

            QueryFundAccountShowResponse.FundAccountShow fundAccountShow = QueryFundAccountShowResponse.FundAccountShow.newBuilder()
                    .setAccountId(fundAccount.getAccountId())
                    .setId(fundAccount.getId())
                    .setIsShow(fundAccount.getIsShow())
                    .setOrgId(fundAccount.getOrgId())
                    .setRemark(fundAccount.getRemark())
                    .setTag(fundAccount.getTag())
                    .setUserId(fundAccount.getUserId())
                    .build();

            list.add(fundAccountShow);
        }

        return QueryFundAccountShowResponse.newBuilder().addAllFundAccounts(list).build();
    }

    //获取 已借资产
    private Pair<BigDecimal, BigDecimal> getLoanAsset(Long orgId, Long accountId) {
        List<CrossLoanPosition> crossLoanPositions = marginService.queryCrossLoanPosition(orgId, accountId, "");

        BigDecimal btcLoanAsset = BigDecimal.ZERO;
        BigDecimal usdtLoanAsset = BigDecimal.ZERO;

        for (CrossLoanPosition crossLoanPosition : crossLoanPositions) {

            Rate rate = basicService.getV3Rate(orgId, crossLoanPosition.getTokenId());
            BigDecimal btcRate = (rate == null || Stream.of(TEST_TOKENS)
                    .anyMatch(testToken -> testToken.equalsIgnoreCase(crossLoanPosition.getTokenId())))
                    ? BigDecimal.ZERO : DecimalUtil.toBigDecimal(rate.getRatesMap().get("BTC"));
            BigDecimal usdtRate = (rate == null || Stream.of(TEST_TOKENS)
                    .anyMatch(testToken -> testToken.equalsIgnoreCase(crossLoanPosition.getTokenId())))
                    ? BigDecimal.ZERO : DecimalUtil.toBigDecimal(rate.getRatesMap().get("USDT"));

            Decimal loanTotal = crossLoanPosition.getLoanTotal();

            btcLoanAsset = btcLoanAsset.add(DecimalUtil.toBigDecimal(loanTotal).multiply(btcRate).setScale(8, RoundingMode.DOWN));
            usdtLoanAsset = usdtLoanAsset.add(DecimalUtil.toBigDecimal(loanTotal).multiply(usdtRate).setScale(2, RoundingMode.DOWN));

        }

        return Pair.of(btcLoanAsset, usdtLoanAsset);

    }
}