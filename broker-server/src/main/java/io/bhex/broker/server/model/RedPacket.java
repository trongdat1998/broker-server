package io.bhex.broker.server.model;

import com.google.common.base.Strings;
import io.bhex.broker.grpc.red_packet.RedPacketReceiveUserType;
import io.bhex.broker.grpc.red_packet.RedPacketType;
import io.bhex.broker.server.domain.RedPacketStatus;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import java.math.BigDecimal;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "tb_red_packet")
public class RedPacket {

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;
    private Long orgId;
    private Long userId;
    private Long accountId; // 冗余
    private Long chatId; // 对于聊天群组发的红包使用
    private String avatar; // 冗余用户的头像
    private String nickname; // 冗余用户的昵称
    private String username;
    private String inviteUrl;
    private String inviteCode; // 用户的邀请码
    private Long themeId; // 红包主题, 包含了红包的背景图和slogan
    private String backgroundUrl; // 背景图, 冗余 现在好像用不到
    private String slogan; // 标语, 冗余 现在好像用不到
    private Integer redPacketType; // 红包类型 0 普通红包 红包个数 + 单个红包金额 1 拼手气红包 总金额 + 红包个数
    private Integer needPassword; // 是否设置口令
    private String password; // 口令
    private String tokenId; // 币种
    private String tokenName;
    private Integer totalCount; // 个数
    private BigDecimal amount; // 普通红包单个金额
    private BigDecimal totalAmount; // 总金额 普通红包总金额 或者 拼手气红包总金额
    private BigDecimal equivalentUsdtAmount; // 折合USDT
    private String fxRate;
    private Long sendTransferId;
    private Long refundTransferId;
    private Integer receiveUserType; // 0 不限制 1 仅限新用户  2 仅限老用户
    private String amountAssignInfo; // 冗余一下金额分配信息
    private Long expired; // 失效时间
    private Integer remainCount;
    private BigDecimal remainAmount;
    private BigDecimal refundAmount; // 退还金额
    private Integer status; // 0 进行中 1 已领完 1 已退款
    private String ip;
    private String platform;
    private String userAgent;
    private String language;
    private String appBaseHeader;
    private String channel;
    private String source;
    private Long created;
    private Long updated;

    public io.bhex.broker.grpc.red_packet.RedPacket convertGrpcObj() {
        int status = this.status;
        BigDecimal refundAmount = this.refundAmount;
        // 处理status 和 refundAmount
        if ((this.remainCount == 0 || this.remainAmount.compareTo(BigDecimal.ZERO) == 0)
                && this.status != RedPacketStatus.OVER.status()) {
            // 处理已抢完但是状态非over
            this.status = RedPacketStatus.OVER.status();
            refundAmount = BigDecimal.ZERO;
        } else if (this.expired < System.currentTimeMillis()
                && (this.remainCount > 0 || this.remainAmount.compareTo(BigDecimal.ZERO) > 0)
                && this.status != RedPacketStatus.REFUNDED.status()) {
            // 处理失效未抢完
            status = RedPacketStatus.REFUNDING.status();
            refundAmount = this.remainAmount;
        }

        return io.bhex.broker.grpc.red_packet.RedPacket.newBuilder()
                .setId(this.id)
                .setOrgId(this.orgId)
                .setUserId(this.userId)
                .setAccountId(this.accountId)
                .setAvatar(this.avatar)
                .setNickname(this.nickname)
                .setUsername(this.username)
                .setInviteUrl(this.inviteUrl)
                .setInviteCode(this.inviteCode)
                .setThemeId(this.themeId)
                .setBackgroundUrl(this.backgroundUrl)
                .setSlogan(this.slogan)
                .setRedPacketType(RedPacketType.forNumber(this.redPacketType))
                .setNeedPassword(this.needPassword == 1)
                .setPassword(Strings.nullToEmpty(this.password))
                .setTokenId(this.tokenId)
                .setTokenName(this.tokenName)
                .setTotalCount(this.totalCount)
                .setAmount(this.amount.stripTrailingZeros().toPlainString())
                .setTotalAmount(this.totalAmount.stripTrailingZeros().toPlainString())
                .setEquivalentUsdtAmount(this.equivalentUsdtAmount.stripTrailingZeros().toPlainString())
                .setReceiveUserType(RedPacketReceiveUserType.forNumber(this.receiveUserType))
                .setExpired(this.expired)
                .setRemainCount(this.remainCount)
                .setRemainAmount(this.remainAmount.stripTrailingZeros().toPlainString())
                .setRefundAmount(refundAmount.stripTrailingZeros().toPlainString())
                .setStatus(status)
                .setCreated(this.created)
                .setUpdated(this.updated)
                .build();
    }

}
