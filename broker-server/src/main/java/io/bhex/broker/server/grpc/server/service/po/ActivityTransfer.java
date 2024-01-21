package io.bhex.broker.server.grpc.server.service.po;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Builder
@AllArgsConstructor
@NoArgsConstructor
@Data
public class ActivityTransfer {

    /**
     * orgId
     */
    private Long orgId;
    /**
     * to账户
     */
    private Long toAccountId;
    /**
     * source账户
     */
    private Long sourceAccountId;
    /**
     * 用户获得tokenId
     */
    private String toTokenId;
    /**
     * source账户获得tokenId
     */
    private String sourceTokenId;
    /**
     * 用户获得token数量
     */
    private String toAmount;
    /**
     * source账户获得token数量
     */
    private String sourceAmount;

    /**
     * 赠送金额
     */
    private String lockAmount;
    /**
     * 赠送tokenId
     */
    private String lockTokenId;
    /**
     * 0到可用 非0到锁仓
     */
    private Integer isLock;
    /**
     * true 有赠送 false 无赠送
     */
    private Boolean haveGift;
    /**
     * 用户转账clientOrderId;
     */
    private Long userClientOrderId;
    /**
     * 运营账户clientOrderId;
     */
    private Long ieoAccountClientOrderId;
    /**
     * 赠送clientOrderId;
     */
    private Long giftClientOrderId;
}
