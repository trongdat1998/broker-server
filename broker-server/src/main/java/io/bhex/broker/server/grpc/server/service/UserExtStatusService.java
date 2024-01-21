package io.bhex.broker.server.grpc.server.service;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import io.bhex.base.common.CancelBaseConfigRequest;
import io.bhex.base.common.EditBaseConfigsRequest;
import io.bhex.base.common.SwtichStatus;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.grpc.bwlist.EditUserBlackWhiteConfigRequest;
import io.bhex.broker.grpc.bwlist.UserBlackWhiteListType;
import io.bhex.broker.grpc.bwlist.UserBlackWhiteType;
import io.bhex.broker.grpc.common.Header;
import io.bhex.broker.grpc.user.*;
import io.bhex.broker.server.domain.BaseConfigConstants;
import io.bhex.broker.server.domain.SwitchStatus;
import io.bhex.broker.server.domain.UserBlackWhiteListConfig;
import io.bhex.broker.server.model.BaseConfigInfo;
import io.bhex.broker.server.primary.mapper.UserMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import javax.annotation.Resource;
import java.util.*;


/**
 * @Description:
 * @Date: 2020/1/8 下午3:15
 * @Author: liwei
 * @Copyright（C）: 2018 BlueHelix Inc. All rights reserved.
 */
@Slf4j
@Service
public class UserExtStatusService {
    @Resource
    private UserMapper userMapper;
    @Resource
    private UserBlackWhiteListConfigService blackWhiteListConfigService;
    @Resource
    private BaseBizConfigService baseBizConfigService;
    @Resource
    private UserSecurityService userSecurityService;

    @Resource
    private BaseConfigService baseConfigService;

    private static final int BW_LIST_SIZE = 10; //一个人的黑白名单一个条目最多一条

    private static Map<UserExtStatusName, String> baseConfigGroupMap = new HashMap<>();

    static {
        baseConfigGroupMap.put(UserExtStatusName.SPOT_TRADE, BaseConfigConstants.FROZEN_USER_COIN_TRADE_GROUP);
        baseConfigGroupMap.put(UserExtStatusName.OPTION_TRADE, BaseConfigConstants.FROZEN_USER_OPTION_TRADE_GROUP);
        baseConfigGroupMap.put(UserExtStatusName.CONTRACT_TRADE, BaseConfigConstants.FROZEN_USER_FUTURE_TRADE_GROUP);
        baseConfigGroupMap.put(UserExtStatusName.OTC_TRADE, BaseConfigConstants.FROZEN_USER_OTC_TRADE_GROUP);
        baseConfigGroupMap.put(UserExtStatusName.FINANCE_TRADE, BaseConfigConstants.FROZEN_USER_BONUS_TRADE_GROUP);
        baseConfigGroupMap.put(UserExtStatusName.WITHDRAW_BROKER_AUDIT, BaseConfigConstants.FORCE_AUDIT_USER_WITHDRAW_GROUP);
    }

    public QueryUserExtStatusResponse queryUserExtStatus(QueryUserExtStatusRequest request) {
        long orgId = request.getHeader().getOrgId();
        long userId = request.getHeader().getUserId();
        io.bhex.broker.server.model.User user = userMapper.getByOrgAndUserId(orgId, userId);
        if (user == null) {
            throw new BrokerException(BrokerErrorCode.USER_NOT_EXIST);
        }
        Map<String, Integer> result = new HashMap<>();

        List<UserExtStatusName> queryNames = request.getStatusNameList();
        if (CollectionUtils.isEmpty(queryNames)) {
            queryNames = Lists.newArrayList(UserExtStatusName.values());
        }

        boolean hasMoneyOutFlowStatus = false;
        boolean hasWithdrawWhiteListStatus = false;
        List<String> baseConfigGroups = new ArrayList<>();
        for (UserExtStatusName extStatusName : queryNames) {
            if (extStatusName == UserExtStatusName.USER_STATUS) {
                result.put(extStatusName.name(), user != null && user.getUserStatus() == 1 ? 1 : 0);
            } else if (extStatusName == UserExtStatusName.MONEY_OUTFLOW) {
                hasMoneyOutFlowStatus = true;
            } else if (extStatusName == UserExtStatusName.WITHDRAW_WHITE_LIST) {
                hasWithdrawWhiteListStatus = true;
            } else if (extStatusName != UserExtStatusName.UNRECOGNIZED) {
                baseConfigGroups.add(baseConfigGroupMap.get(extStatusName));
            }
        }

        if (hasMoneyOutFlowStatus || hasWithdrawWhiteListStatus) {
            List<UserBlackWhiteListConfig> blackWhiteListConfigs = blackWhiteListConfigService.getBlackWhiteConfigs(orgId,
                    UserBlackWhiteListType.WITHDRAW_BLACK_WHITE_LIST_TYPE_VALUE, 0, BW_LIST_SIZE, 0, userId);
            if (CollectionUtils.isEmpty(blackWhiteListConfigs)) {
                blackWhiteListConfigs = Lists.newArrayList();
            }

            if (hasMoneyOutFlowStatus) { //反向开关
                boolean openInDb = blackWhiteListConfigs.stream()
                        .anyMatch(c -> c.getBwType() == UserBlackWhiteType.BLACK_CONFIG_VALUE && c.getStatus() == 1);
                result.put(UserExtStatusName.MONEY_OUTFLOW.name(), openInDb ? 0 : 1);
            }

            if (hasWithdrawWhiteListStatus) {
                boolean openInDb = blackWhiteListConfigs.stream()
                        .anyMatch(c -> c.getBwType() == UserBlackWhiteType.WHITE_CONFIG_VALUE && c.getStatus() == 1);
                result.put(UserExtStatusName.WITHDRAW_WHITE_LIST.name(), openInDb ? 1 : 0);
            }
        }

        if (!CollectionUtils.isEmpty(baseConfigGroups)) {
            List<BaseConfigInfo> configInfos = baseBizConfigService.getConfigsByGroupsAndKey(orgId, baseConfigGroups, userId + "");
            if (CollectionUtils.isEmpty(configInfos)) {
                configInfos = Lists.newArrayList();
            }
            for (UserExtStatusName extStatusName : baseConfigGroupMap.keySet()) {
                if (!baseConfigGroups.contains(baseConfigGroupMap.get(extStatusName))) {
                    continue;
                }
                Optional<BaseConfigInfo> configInfoOptional = configInfos.stream()
                        .filter(c -> c.getConfGroup().equals(baseConfigGroupMap.get(extStatusName)))
                        .findFirst();
                SwitchStatus dbSwitchStatus;
                if (!configInfoOptional.isPresent()) {
                    dbSwitchStatus = SwitchStatus.builder().existed(false).open(false).build();
                } else {
                    dbSwitchStatus = SwitchStatus.builder().existed(true).open(configInfoOptional.get().getConfValue().equalsIgnoreCase("true")).build();
                }

                if (extStatusName == UserExtStatusName.WITHDRAW_BROKER_AUDIT) {
                    result.put(extStatusName.name(), dbSwitchStatus.isOpen() ? 1 : 0);
                } else { //反向开关 数据库没有 前端可用
                    result.put(extStatusName.name(), dbSwitchStatus.isOpen() ? 0 : 1);
                }
            }
        }

        return QueryUserExtStatusResponse.newBuilder().setRet(0).putAllStatusMap(result).build();
    }

    public ResetUserExtStatusResponse resetUserExtStatus(ResetUserExtStatusRequest request) {
        Header header = request.getHeader();
        long orgId = header.getOrgId();
        long userId = header.getUserId();
        io.bhex.broker.server.model.User user = userMapper.getByOrgAndUserId(orgId, userId);
        if (user == null) {
            throw new BrokerException(BrokerErrorCode.USER_NOT_EXIST);
        }
        int status = request.getStatus();
        UserExtStatusName statusName = request.getStatusName();

        boolean result = false;
        String source = !StringUtils.isEmpty(header.getSource()) ? header.getSource() : header.getPlatform().name();
        switch (statusName) {
            case USER_STATUS:
                result = userSecurityService.updateUserStatus(userId, status == 1 ? true : false);
                break;


            case SPOT_TRADE: //反向开关
                result = editBaseConfig(orgId, userId, BaseConfigConstants.FROZEN_USER_COIN_TRADE_GROUP,
                        status == 1 ? SwtichStatus.SWITCH_CLOSE : SwtichStatus.SWITCH_OPEN, source);
                break;
            case OPTION_TRADE: //反向开关
                result = editBaseConfig(orgId, userId, BaseConfigConstants.FROZEN_USER_OPTION_TRADE_GROUP,
                        status == 1 ? SwtichStatus.SWITCH_CLOSE : SwtichStatus.SWITCH_OPEN, source);
                break;
            case CONTRACT_TRADE: //反向开关
                result = editBaseConfig(orgId, userId, BaseConfigConstants.FROZEN_USER_FUTURE_TRADE_GROUP,
                        status == 1 ? SwtichStatus.SWITCH_CLOSE : SwtichStatus.SWITCH_OPEN, source);
                break;
            case OTC_TRADE: //反向开关
                result = editBaseConfig(orgId, userId, BaseConfigConstants.FROZEN_USER_OTC_TRADE_GROUP,
                        status == 1 ? SwtichStatus.SWITCH_CLOSE : SwtichStatus.SWITCH_OPEN, source);
                break;
            case FINANCE_TRADE: //反向开关
                result = editBaseConfig(orgId, userId, BaseConfigConstants.FROZEN_USER_BONUS_TRADE_GROUP,
                        status == 1 ? SwtichStatus.SWITCH_CLOSE : SwtichStatus.SWITCH_OPEN, source);
                break;
            case WITHDRAW_BROKER_AUDIT: //正向
                result = editBaseConfig(orgId, userId, BaseConfigConstants.FORCE_AUDIT_USER_WITHDRAW_GROUP,
                         status == 1 ? SwtichStatus.SWITCH_OPEN : SwtichStatus.SWITCH_CLOSE, source);
                break;

            case MARGIN_TRADE: //反向开关
                result = editBaseConfig(orgId, userId, BaseConfigConstants.FROZEN_USER_MARGIN_TRADE_GROUP,
                        status == 1 ? SwtichStatus.SWITCH_CLOSE : SwtichStatus.SWITCH_OPEN, source);
                break;

            case MONEY_OUTFLOW: //反向开关
                result = editUserBlackWhiteConfig(orgId, userId, UserBlackWhiteListType.WITHDRAW_BLACK_WHITE_LIST_TYPE,
                        UserBlackWhiteType.BLACK_CONFIG, status == 1 ? 0 : 1, source); //数据库存的是禁止出金
                break;
            case WITHDRAW_WHITE_LIST: //正向
                result = editUserBlackWhiteConfig(orgId, userId, UserBlackWhiteListType.WITHDRAW_BLACK_WHITE_LIST_TYPE,
                        UserBlackWhiteType.WHITE_CONFIG, status, source);
                break;

            case API_SPOT_TRADE: //反向开关
                result = editBaseConfig(orgId, userId, BaseConfigConstants.FROZEN_USER_API_COIN_TRADE_GROUP,
                        status == 1 ? SwtichStatus.SWITCH_CLOSE : SwtichStatus.SWITCH_OPEN, source);
            default:
                break;
        }
        log.info("org:{} user:{} switch:{} status:{} result:{}", orgId, userId, statusName, status, result);
        return ResetUserExtStatusResponse.newBuilder().setStatus(result ? 0 : 1).setStatus(status).build();
    }

    public SetAllowedTradingSymbolsResponse setAllowedTradingSymbols(SetAllowedTradingSymbolsRequest request) {
        Header header = request.getHeader();
        io.bhex.broker.server.model.User user = userMapper.getByOrgAndUserId(header.getOrgId(), header.getUserId());
        if (user == null) {
            throw new BrokerException(BrokerErrorCode.USER_NOT_EXIST);
        }
        EditBaseConfigsRequest configsRequest = EditBaseConfigsRequest.newBuilder()
                .addConfig(EditBaseConfigsRequest.Config.newBuilder()
                        .setStatus(request.getSymbolIdCount() > 0 ? 1 : 0)
                        .setOrgId(header.getOrgId())
                        .setKey(header.getUserId() + "")
                        .setAdminUserName(request.getHeader().getSource())
                        .setGroup(BaseConfigConstants.ALLOW_TRADING_SYMBOLS_GROUP)
                        .setValue(String.join(",", request.getSymbolIdList()))
                        .build())
                .build();
        boolean r = baseConfigService.editConfigs(configsRequest);
        return SetAllowedTradingSymbolsResponse.newBuilder().setRet(r ? 0 : 1).build();
    }

    public RcDisableUserTradeResponse disableUserTrade(RcDisableUserTradeRequest request) {
        RcDisableUserTradeRequest.SwitchType switchType = request.getSwitchType();
        Header header = request.getHeader();
        io.bhex.broker.server.model.User user = userMapper.getByOrgAndUserId(header.getOrgId(), header.getUserId());
        if (user == null) {
            throw new BrokerException(BrokerErrorCode.USER_NOT_EXIST);
        }

        List<RcDisableUserTradeRequest.SwitchType> cancelSwitches = Lists.newArrayList();
        if (switchType == RcDisableUserTradeRequest.SwitchType.ALLOW_ALL) {
            cancelSwitches = Lists.newArrayList(RcDisableUserTradeRequest.SwitchType.OPEN,
                    RcDisableUserTradeRequest.SwitchType.CLOSE);
        } else if (request.getSymbolIdCount() == 0){
            cancelSwitches = Lists.newArrayList(request.getSwitchType());
        }

        if(!CollectionUtils.isEmpty(cancelSwitches)) {
            for (RcDisableUserTradeRequest.SwitchType st : cancelSwitches) {
                CancelBaseConfigRequest cancelRequest = CancelBaseConfigRequest.newBuilder()
                        .setOrgId(header.getOrgId())
                        .setGroup(BaseConfigConstants.DISABLE_USER_TRADING_SYMBOLS_GROUP)
                        .setKey(header.getUserId() + "-" + st.name())
                        .setAdminUserName(header.getSource())
                        .build();
                baseConfigService.cancelConfig(cancelRequest);
            }
        } else {
            EditBaseConfigsRequest configsRequest = EditBaseConfigsRequest.newBuilder()
                    .addConfig(EditBaseConfigsRequest.Config.newBuilder()
                            .setStatus(request.getSymbolIdCount() > 0 ? 1 : 0)
                            .setOrgId(header.getOrgId())
                            .setKey(header.getUserId() + "-" + switchType.name())
                            .setAdminUserName(request.getHeader().getSource())
                            .setGroup(BaseConfigConstants.DISABLE_USER_TRADING_SYMBOLS_GROUP)
                            .setValue(String.join(",", request.getSymbolIdList()))
                            .build())
                    .build();
            baseConfigService.editConfigs(configsRequest);
        }

        return RcDisableUserTradeResponse.newBuilder().setRet(0).build();
    }

    private boolean editBaseConfig(long orgId, long userId, String group, SwtichStatus swtichStatus, String adminUserName) {
        EditBaseConfigsRequest request = EditBaseConfigsRequest.newBuilder()
                .addConfig(EditBaseConfigsRequest.Config.newBuilder()
                        .setStatus(swtichStatus == SwtichStatus.SWITCH_OPEN ? 1 : 0)
                        .setOrgId(orgId)
                        .setKey(userId + "")
                        .setAdminUserName(Strings.nullToEmpty(adminUserName))
                        .setGroup(group)
                        .setSwitchStatus(swtichStatus)
                        .build())
                .build();
        boolean r = baseConfigService.editConfigs(request);
        return r;
    }

    private boolean editUserBlackWhiteConfig(long orgId, long userId, UserBlackWhiteListType listType,
                                             UserBlackWhiteType bwType, int swtichStatus, String source) {
        EditUserBlackWhiteConfigRequest request = EditUserBlackWhiteConfigRequest.newBuilder()
                        .setStatus(swtichStatus)
                        .setHeader(Header.newBuilder().setOrgId(orgId).build())
                        .addUserIds(userId)
                        .setListType(listType)
                        .setBwType(bwType)
                        .setExtraInfo("{\"global\":true,\"tokens\":[]}")
                        .setReason(Strings.nullToEmpty(source))
                        .build();
        boolean r = blackWhiteListConfigService.edit(request);
        return r;
    }
}
