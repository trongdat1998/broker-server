/*
 ************************************
 * @项目名称: broker
 * @文件名称: GrcpClientConfig
 * @Date 2018/05/22
 * @Author will.zhao@bhex.io
 * @Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 * 注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 **************************************
 */
package io.bhex.broker.server.grpc.client.config;

import io.bhex.base.account.*;
import io.bhex.base.admin.AdminOrgContractServiceGrpc;
import io.bhex.base.admin.AdminOrgInfoServiceGrpc;
import io.bhex.base.admin.common.BrokerTradeFeeSettingServiceGrpc;
import io.bhex.base.broker.BrokerMqStreamServiceGrpc;
import io.bhex.base.clear.CommissionServiceGrpc;
import io.bhex.base.common.BaseConfigServiceGrpc;
import io.bhex.base.common.MailServiceGrpc;
import io.bhex.base.common.MessageServiceGrpc;
import io.bhex.base.common.SmsServiceGrpc;
import io.bhex.base.es.BalanceFlowServiceGrpc;
import io.bhex.base.es.TradeDetailServiceGrpc;
import io.bhex.base.grpc.client.channel.IGrpcClientPool;
import io.bhex.base.margin.MarginConfigServiceGrpc;
import io.bhex.base.margin.cross.MarginCrossServiceGrpc;
import io.bhex.base.proto.TimeServiceGrpc;
import io.bhex.base.proto.statistics.*;
import io.bhex.base.quote.QuoteServiceGrpc;
import io.bhex.base.rc.ExchangeRCServiceGrpc;
import io.bhex.base.token.BrokerExchangeTokenServiceGrpc;
import io.bhex.base.token.SymbolServiceGrpc;
import io.bhex.base.token.TokenServiceGrpc;
import io.bhex.broker.common.entity.GrpcChannelInfo;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.grpc.app_push.AppPushServiceGrpc;
import io.bhex.broker.grpc.security.SecurityServiceGrpc;
import io.bhex.broker.server.BrokerServerProperties;
import io.bhex.broker.server.grpc.interceptor.RouteAuthCredentials;
import io.bhex.ex.otc.*;
import io.bhex.guild.grpc.guild.GuildServiceGrpc;
import io.grpc.Channel;

import lombok.extern.slf4j.Slf4j;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Slf4j
public class GrpcClientConfig {

    static final String BH_SERVER_CHANNEL_NAME = "bhServer";
    static final String BH_SUB_SERVER_CHANNEL_NAME = "bhSubServer";
    static final String SECURITY_SERVER_CHANNEL_NAME = "securityServer";
    static final String COMMON_SERVER_CHANNEL_NAME = "commonServer";
    static final String QUOTE_SERVER_CHANNEL_NAME = "quoteDataServer";
    static final String OTC_SERVER_CHANNEL_NAME = "otcServer";
    static final String SAAS_SERVER_CHANNEL_NAME = "saasAdminServer";
    // static final String CLEAR_SERVER_CHANNEL_NAME = "clearServer";
    static final String GUILD_SERVER_CHANNEL_NAME = "guildServer";
    static final String MARGIN_SERVER_CHANNEL_NAME = "marginServer";


    @Resource
    BrokerServerProperties brokerServerProperties;

    @Resource
    IGrpcClientPool pool;

    Long stubDeadline;

    Long shortStubDeadline;

    @PostConstruct
    public void init() throws Exception {
        stubDeadline = brokerServerProperties.getGrpcClient().getStubDeadline();
        shortStubDeadline = brokerServerProperties.getGrpcClient().getShortStubDeadline();
        List<GrpcChannelInfo> channelInfoList = brokerServerProperties.getGrpcClient().getChannelInfo();
        for (GrpcChannelInfo channelInfo : channelInfoList) {
            pool.setShortcut(channelInfo.getChannelName(), channelInfo.getHost(), channelInfo.getPort());
        }
    }

    public TokenServiceGrpc.TokenServiceBlockingStub tokenServiceBlockingStub(Long orgId) {
        Channel channel = pool.borrowChannel(BH_SERVER_CHANNEL_NAME);
        return TokenServiceGrpc.newBlockingStub(channel).withDeadlineAfter(stubDeadline, TimeUnit.MILLISECONDS);
    }

    public BrokerExchangeTokenServiceGrpc.BrokerExchangeTokenServiceBlockingStub brokerExchangeTokenServiceBlockingStub(Long orgId) {
        Channel channel = pool.borrowChannel(BH_SERVER_CHANNEL_NAME);
        return BrokerExchangeTokenServiceGrpc.newBlockingStub(channel).withDeadlineAfter(stubDeadline, TimeUnit.MILLISECONDS);
    }

    public SymbolServiceGrpc.SymbolServiceBlockingStub symbolServiceBlockingStub(Long orgId) {
        Channel channel = pool.borrowChannel(BH_SERVER_CHANNEL_NAME);
        return SymbolServiceGrpc.newBlockingStub(channel).withDeadlineAfter(stubDeadline, TimeUnit.MILLISECONDS);
    }

    public RiskBalanceServiceGrpc.RiskBalanceServiceBlockingStub riskBalanceServiceBlockingStub(Long orgId) {
        Channel channel = pool.borrowChannel(BH_SERVER_CHANNEL_NAME);
        return RiskBalanceServiceGrpc.newBlockingStub(channel).withDeadlineAfter(stubDeadline, TimeUnit.MILLISECONDS);
    }

    public AccountServiceGrpc.AccountServiceBlockingStub accountServiceBlockingStub(Long orgId) {
        Channel channel = pool.borrowChannel(BH_SERVER_CHANNEL_NAME);
        return AccountServiceGrpc.newBlockingStub(channel).withDeadlineAfter(stubDeadline, TimeUnit.MILLISECONDS);
    }

    public BalanceServiceGrpc.BalanceServiceBlockingStub balanceServiceBlockingStub(Long orgId) {
        Channel channel = pool.borrowChannel(BH_SERVER_CHANNEL_NAME);
        return BalanceServiceGrpc.newBlockingStub(channel).withDeadlineAfter(stubDeadline, TimeUnit.MILLISECONDS);
    }

    public BalanceServiceGrpc.BalanceServiceBlockingStub shortDeadlineBalanceServiceBlockingStub(Long orgId) {
        Channel channel = pool.borrowChannel(BH_SERVER_CHANNEL_NAME);
        return BalanceServiceGrpc.newBlockingStub(channel).withDeadlineAfter(shortStubDeadline, TimeUnit.MILLISECONDS);
    }

    public ColdWalletServiceGrpc.ColdWalletServiceBlockingStub coldWalletServiceBlockingStub(Long orgId) {
        Channel channel = pool.borrowChannel(BH_SERVER_CHANNEL_NAME);
        return ColdWalletServiceGrpc.newBlockingStub(channel).withDeadlineAfter(stubDeadline, TimeUnit.MILLISECONDS);
    }

    public DepositServiceGrpc.DepositServiceBlockingStub depositServiceBlockingStub(Long orgId) {
        Channel channel = pool.borrowChannel(BH_SERVER_CHANNEL_NAME);
        return DepositServiceGrpc.newBlockingStub(channel).withDeadlineAfter(stubDeadline, TimeUnit.MILLISECONDS);
    }

    public FeeServiceGrpc.FeeServiceBlockingStub feeServiceBlockingStub(Long orgId) {
        Channel channel = pool.borrowChannel(BH_SERVER_CHANNEL_NAME);
        return FeeServiceGrpc.newBlockingStub(channel).withDeadlineAfter(stubDeadline, TimeUnit.MILLISECONDS);
    }

    public BrokerTradeFeeSettingServiceGrpc.BrokerTradeFeeSettingServiceBlockingStub tradeFeeSettingServiceBlockingStub(Long orgId) {
        Channel channel = pool.borrowChannel(BH_SERVER_CHANNEL_NAME);
        return BrokerTradeFeeSettingServiceGrpc.newBlockingStub(channel).withDeadlineAfter(stubDeadline, TimeUnit.MILLISECONDS);
    }

    public WithdrawalServiceGrpc.WithdrawalServiceBlockingStub withdrawalServiceBlockingStub(Long orgId) {
        Channel channel = pool.borrowChannel(BH_SERVER_CHANNEL_NAME);
        return WithdrawalServiceGrpc.newBlockingStub(channel).withDeadlineAfter(stubDeadline, TimeUnit.MILLISECONDS);
    }

    public OrderServiceGrpc.OrderServiceBlockingStub orderServiceBlockingStub(Long orgId) {
        Channel channel = pool.borrowChannel(BH_SERVER_CHANNEL_NAME);
        return OrderServiceGrpc.newBlockingStub(channel).withDeadlineAfter(stubDeadline, TimeUnit.MILLISECONDS);
    }

    public ExchangeRCServiceGrpc.ExchangeRCServiceBlockingStub exchangeRCServiceBlockingStub() {
        Channel channel = pool.borrowChannel(BH_SERVER_CHANNEL_NAME);
        return ExchangeRCServiceGrpc.newBlockingStub(channel).withDeadlineAfter(stubDeadline, TimeUnit.MILLISECONDS);
    }

    public OrderServiceGrpc.OrderServiceBlockingStub shortDeadlineOrderServiceBlockingStub(Long orgId) {
        Channel channel = pool.borrowChannel(BH_SERVER_CHANNEL_NAME);
        return OrderServiceGrpc.newBlockingStub(channel).withDeadlineAfter(shortStubDeadline, TimeUnit.MILLISECONDS);
    }

    public OrderServiceGrpc.OrderServiceStub orderServiceStub(Long orgId) {
        Channel channel = pool.borrowChannel(BH_SERVER_CHANNEL_NAME);
        return OrderServiceGrpc.newStub(channel).withDeadlineAfter(stubDeadline, TimeUnit.MILLISECONDS);
    }

    public OptionServerGrpc.OptionServerBlockingStub optionServerBlockingStub(Long orgId) {
        Channel channel = pool.borrowChannel(BH_SERVER_CHANNEL_NAME);
        return OptionServerGrpc.newBlockingStub(channel).withDeadlineAfter(stubDeadline, TimeUnit.MILLISECONDS);
    }

    public OptionServerGrpc.OptionServerBlockingStub shortDeadlineOptionServerBlockingStub(Long orgId) {
        Channel channel = pool.borrowChannel(BH_SERVER_CHANNEL_NAME);
        return OptionServerGrpc.newBlockingStub(channel).withDeadlineAfter(shortStubDeadline, TimeUnit.MILLISECONDS);
    }

    public BatchTransferServiceGrpc.BatchTransferServiceBlockingStub batchTransferServiceBlockingStub(Long orgId) {
        Channel channel = pool.borrowChannel(BH_SERVER_CHANNEL_NAME);
        return BatchTransferServiceGrpc.newBlockingStub(channel)
                .withDeadlineAfter(stubDeadline, TimeUnit.MILLISECONDS);
    }

    public SecurityServiceGrpc.SecurityServiceBlockingStub securityServiceBlockingStub() {
        Channel channel = pool.borrowChannel(SECURITY_SERVER_CHANNEL_NAME);
        return SecurityServiceGrpc.newBlockingStub(channel).withDeadlineAfter(stubDeadline, TimeUnit.MILLISECONDS);
    }

    public SmsServiceGrpc.SmsServiceBlockingStub smsServiceBlockingStub(Long orgId) {
        Channel channel = pool.borrowChannel(COMMON_SERVER_CHANNEL_NAME);
        return SmsServiceGrpc.newBlockingStub(channel).withDeadlineAfter(stubDeadline, TimeUnit.MILLISECONDS);
    }

    public MessageServiceGrpc.MessageServiceBlockingStub messageServiceBlockingStub(Long orgId) {
        Channel channel = pool.borrowChannel(COMMON_SERVER_CHANNEL_NAME);
        return MessageServiceGrpc.newBlockingStub(channel).withDeadlineAfter(stubDeadline, TimeUnit.MILLISECONDS);
    }

    public MailServiceGrpc.MailServiceBlockingStub mailServiceBlockingStub(Long orgId) {
        Channel channel = pool.borrowChannel(COMMON_SERVER_CHANNEL_NAME);
        return MailServiceGrpc.newBlockingStub(channel).withDeadlineAfter(stubDeadline, TimeUnit.MILLISECONDS);
    }

    public QuoteServiceGrpc.QuoteServiceBlockingStub quoteServiceBlockingStub(Long orgId) {
        Channel channel = pool.borrowChannel(QUOTE_SERVER_CHANNEL_NAME);
        return QuoteServiceGrpc.newBlockingStub(channel).withDeadlineAfter(stubDeadline, TimeUnit.MILLISECONDS);
    }

    public OTCUserServiceGrpc.OTCUserServiceBlockingStub otcUserServiceBlockingStub(Long orgId) {
        Channel channel = pool.borrowChannel(OTC_SERVER_CHANNEL_NAME);
        return OTCUserServiceGrpc.newBlockingStub(channel).withDeadlineAfter(stubDeadline, TimeUnit.MILLISECONDS);
    }

    public OTCItemServiceGrpc.OTCItemServiceBlockingStub otcItemServiceBlockingStub(Long orgId) {
        Channel channel = pool.borrowChannel(OTC_SERVER_CHANNEL_NAME);
        return OTCItemServiceGrpc.newBlockingStub(channel).withDeadlineAfter(stubDeadline, TimeUnit.MILLISECONDS);
    }

    public OTCOrderServiceGrpc.OTCOrderServiceBlockingStub otcOrderServiceBlockingStub(Long orgId) {
        Channel channel = pool.borrowChannel(OTC_SERVER_CHANNEL_NAME);
        return OTCOrderServiceGrpc.newBlockingStub(channel).withDeadlineAfter(stubDeadline, TimeUnit.MILLISECONDS);
    }

    public OTCMessageServiceGrpc.OTCMessageServiceBlockingStub otcMessageServiceBlockingStub(Long orgId) {
        Channel channel = pool.borrowChannel(OTC_SERVER_CHANNEL_NAME);
        return OTCMessageServiceGrpc.newBlockingStub(channel).withDeadlineAfter(stubDeadline, TimeUnit.MILLISECONDS);
    }

    public OTCPaymentTermServiceGrpc.OTCPaymentTermServiceBlockingStub otcPaymentTermServiceBlockingStub(Long orgId) {
        Channel channel = pool.borrowChannel(OTC_SERVER_CHANNEL_NAME);
        return OTCPaymentTermServiceGrpc.newBlockingStub(channel).withDeadlineAfter(stubDeadline, TimeUnit.MILLISECONDS);
    }

    public OTCPaymentTermServiceGrpc.OTCPaymentTermServiceBlockingStub otcPaymentTermServiceBlockingStub() {
        Channel channel = pool.borrowChannel(OTC_SERVER_CHANNEL_NAME);
        return OTCPaymentTermServiceGrpc.newBlockingStub(channel).withDeadlineAfter(stubDeadline, TimeUnit.MILLISECONDS);
    }

    public OTCConfigServiceGrpc.OTCConfigServiceBlockingStub otcConfigServiceBlockingStub(Long orgId) {
        Channel channel = pool.borrowChannel(OTC_SERVER_CHANNEL_NAME);
        return OTCConfigServiceGrpc.newBlockingStub(channel).withDeadlineAfter(stubDeadline, TimeUnit.MILLISECONDS);
    }

    public OTCConfigServiceGrpc.OTCConfigServiceBlockingStub otcConfigServiceBlockingStub() {
        Channel channel = pool.borrowChannel(OTC_SERVER_CHANNEL_NAME);
        return OTCConfigServiceGrpc.newBlockingStub(channel).withDeadlineAfter(stubDeadline, TimeUnit.MILLISECONDS);
    }

    public BalanceChangeRecordServiceGrpc.BalanceChangeRecordServiceBlockingStub balanceChangeRecordStub(Long orgId) {
        Channel channel = pool.borrowChannel(BH_SERVER_CHANNEL_NAME);
        return BalanceChangeRecordServiceGrpc.newBlockingStub(channel).withDeadlineAfter(stubDeadline, TimeUnit.MILLISECONDS);
    }

    public OrgServiceGrpc.OrgServiceBlockingStub orgServiceStub(Long orgId) {
        Channel channel = pool.borrowChannel(BH_SERVER_CHANNEL_NAME);
        return OrgServiceGrpc.newBlockingStub(channel).withDeadlineAfter(stubDeadline, TimeUnit.MILLISECONDS);
    }

    public TimeServiceGrpc.TimeServiceBlockingStub timeServiceBlockingStub(Long orgId) {
        Channel channel = pool.borrowChannel(BH_SERVER_CHANNEL_NAME);
        return TimeServiceGrpc.newBlockingStub(channel).withDeadlineAfter(stubDeadline, TimeUnit.MILLISECONDS);
    }

    public AdminOrgContractServiceGrpc.AdminOrgContractServiceBlockingStub orgContractServiceBlockingStub(Long orgId) {
        Channel channel = pool.borrowChannel(SAAS_SERVER_CHANNEL_NAME);
        return AdminOrgContractServiceGrpc.newBlockingStub(channel).withDeadlineAfter(stubDeadline, TimeUnit.MILLISECONDS);
    }

    public GuildServiceGrpc.GuildServiceBlockingStub guildServiceBlockingStub(Long orgId) {
        Channel channel = pool.borrowChannel(GUILD_SERVER_CHANNEL_NAME);
        return GuildServiceGrpc.newBlockingStub(channel).withDeadlineAfter(stubDeadline, TimeUnit.MILLISECONDS);
    }

    public UserAccountTransferGrpc.UserAccountTransferBlockingStub accountTransferServiceBlockingStub(Long orgId) {
        Channel channel = pool.borrowChannel(BH_SERVER_CHANNEL_NAME);
        return UserAccountTransferGrpc.newBlockingStub(channel).withDeadlineAfter(stubDeadline, TimeUnit.MILLISECONDS);
    }

    public SubscribeServiceGrpc.SubscribeServiceStub subscribeServiceStub(Long orgId) {
        Channel channel = pool.robChannel(BH_SUB_SERVER_CHANNEL_NAME);
        return SubscribeServiceGrpc.newStub(channel);
    }

    public FuturesServerGrpc.FuturesServerBlockingStub futuresServerStub(Long orgId) {
        Channel channel = pool.borrowChannel(BH_SERVER_CHANNEL_NAME);
        return FuturesServerGrpc.newBlockingStub(channel).withDeadlineAfter(stubDeadline, TimeUnit.MILLISECONDS);
    }

    public FuturesServerGrpc.FuturesServerBlockingStub shortDeadlineFuturesServerStub(Long orgId) {
        Channel channel = pool.borrowChannel(BH_SERVER_CHANNEL_NAME);
        return FuturesServerGrpc.newBlockingStub(channel).withDeadlineAfter(shortStubDeadline, TimeUnit.MILLISECONDS);
    }

    public BaseConfigServiceGrpc.BaseConfigServiceBlockingStub baseConfigServiceBlockingStub(Long orgId) {
        Channel channel = pool.borrowChannel(BH_SERVER_CHANNEL_NAME);
        return BaseConfigServiceGrpc.newBlockingStub(channel).withDeadlineAfter(stubDeadline, TimeUnit.MILLISECONDS);
    }

    public PayServiceGrpc.PayServiceBlockingStub payServiceBlockingStub(Long orgId) {
        Channel channel = pool.borrowChannel(BH_SERVER_CHANNEL_NAME);
        return PayServiceGrpc.newBlockingStub(channel).withDeadlineAfter(stubDeadline, TimeUnit.MILLISECONDS);
    }

    public MarginConfigServiceGrpc.MarginConfigServiceBlockingStub marginConfigServiceBlockingStub(Long orgId) {
        Channel channel = pool.borrowChannel(MARGIN_SERVER_CHANNEL_NAME);
        return MarginConfigServiceGrpc.newBlockingStub(channel).withDeadlineAfter(stubDeadline, TimeUnit.MILLISECONDS);
    }

    public MarginCrossServiceGrpc.MarginCrossServiceBlockingStub marginCrossServiceBlockingStub(Long orgId) {
        Channel channel = pool.borrowChannel(BH_SERVER_CHANNEL_NAME);
        return MarginCrossServiceGrpc.newBlockingStub(channel).withDeadlineAfter(stubDeadline, TimeUnit.MILLISECONDS);
    }

    public AdminOrgInfoServiceGrpc.AdminOrgInfoServiceBlockingStub orgInfoServiceBlockingStub(Long orgId) {
        Channel channel = pool.borrowChannel(SAAS_SERVER_CHANNEL_NAME);
        return AdminOrgInfoServiceGrpc.newBlockingStub(channel).withDeadlineAfter(stubDeadline, TimeUnit.MILLISECONDS);
    }

}


