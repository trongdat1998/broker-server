package io.bhex.broker.server.model;

import io.bhex.broker.grpc.red_packet.RedPacketReceiveUserType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import java.math.BigDecimal;

@Data
@Builder(builderClassName = "Builder", toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "tb_red_packet_receive_detail")
public class RedPacketReceiveDetail {

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;
    private Long orgId;
    private Long redPacketId;
    private Long themeId; // 红包主题, 包含了红包的背景图和slogan
    private String backgroundUrl; // 背景图, 冗余 现在好像用不到
    @Deprecated
    private String slogan; // 标语, 冗余 现在好像用不到
    private Long senderUserId; // 冗余
    private Long senderAccountId;
    private String senderAvatar;
    private String senderNickname;
    private String senderUsername;
    private String tokenId;
    private String tokenName;
    private Integer assignIndex;
    private BigDecimal receiveAmount;
    private String fxRate;
    private BigDecimal equivalentUsdtAmount; // 折合USDT
    private Integer receiverType;
    private Long receiverUserId; // 领取人
    private Long receiverAccountId; // 领确认accountId
    private String receiverAvatar; // 冗余用户的头像
    private String receiverNickname;
    private String receiverUsername;
    private Long transferId;
    private Integer status; // 0 未认领  1 已认领  -1 已失效
    private Integer redPacketHandled; // 0 未处理  1 已处理
    private Integer transferStatus; // 0 未转账 1 已转账
    private Long opened;
    private String ip;
    private String platform;
    private String userAgent;
    private String language;
    private String appBaseHeader;
    private String channel;
    private String source;
    private Long created;
    private Long updated;
    private transient Boolean hasBeenOpened; // 是否已经打开过该红包

    public io.bhex.broker.grpc.red_packet.RedPacketReceiveDetail convertGrpcObj() {
        return io.bhex.broker.grpc.red_packet.RedPacketReceiveDetail.newBuilder()
                .setId(this.id)
                .setOrgId(this.orgId)
                .setRedPacketId(this.redPacketId)
                .setThemeId(this.themeId)
                .setBackgroundUrl(this.backgroundUrl)
                .setSlogan(this.slogan)
                .setSenderUserId(this.senderUserId)
                .setSenderAccountId(this.senderAccountId)
                .setSenderAvatar(this.senderAvatar)
                .setSenderNickname(this.senderNickname)
                .setSenderUsername(this.senderUsername)
                .setTokenId(this.tokenId)
                .setTokenName(this.tokenName)
                .setAmount(this.receiveAmount.stripTrailingZeros().toPlainString())
                .setEquivalentUsdtAmount(this.equivalentUsdtAmount.stripTrailingZeros().toPlainString())
                .setReceiverType(RedPacketReceiveUserType.valueOf(this.receiverType))
                .setReceiverUserId(this.receiverUserId == null ? 0L : this.receiverUserId)
                .setReceiverAccountId(this.receiverAccountId == null ? 0L : this.receiverAccountId)
                .setReceiverAvatar(this.receiverAvatar)
                .setReceiverNickname(this.receiverNickname)
                .setReceiverUsername(this.receiverUsername)
                .setCreated(this.opened)
                .setUpdated(this.updated)
                .build();
    }

}
