package io.bhex.broker.server.util;

import io.bhex.base.proto.DecimalUtil;
import io.bhex.base.token.TokenOptionInfo;
import io.bhex.broker.server.domain.BrokerServerConstants;
import io.bhex.broker.server.domain.SellAbleValueDetail;

import java.math.BigDecimal;
import java.math.RoundingMode;

public class OptionUtil {

    public static io.bhex.broker.grpc.basic.TokenOptionInfo toTokenOptionInfo(TokenOptionInfo tokenOption) {
        return io.bhex.broker.grpc.basic.TokenOptionInfo.newBuilder()
                .setTokenId(tokenOption.getTokenId())
                .setStrikePrice(DecimalUtil.toTrimString(tokenOption.getStrikePrice()))
                .setIssueDate(tokenOption.getIssueDate())
                .setSettlementDate(tokenOption.getSettlementDate())
                .setIsCall(tokenOption.getIsCall())
                .setMaxPayOff(DecimalUtil.toTrimString(tokenOption.getMaxPayOff()))
                .setCoinToken(tokenOption.getCoinToken())
                .setIndexToken(tokenOption.getIndexToken())
                .setUnderlyingId(tokenOption.getUnderlyingId())
                .build();
    }

    /**
     * 可卖期权 = 可用coin余额(quoteBalance available)/最大收益(maxPayOff) + k
     * <p>
     * k = if (total > 0, baseBalance available, 0)
     * <p>
     * 可平量kpl:
     * when total > 0:      kpl = baseBalance available
     * when total < 0:      kpl = max(abs(total) - long_on_book, 0)
     *
     * @param quoteAvailable 可用coin余额(USDT, BTC...)
     * @param maxPayOff      期权最大收益
     * @param baseTotal      期权的total
     * @param baseAvailable  期权的available
     */
    public static SellAbleValueDetail getSellAbleValue(BigDecimal quoteAvailable, BigDecimal maxPayOff, BigDecimal baseTotal, BigDecimal baseAvailable) {
        if (maxPayOff == null || maxPayOff.compareTo(BigDecimal.ZERO) == 0) {
            return SellAbleValueDetail.builder()
                    .remainQuantity(BigDecimal.ZERO)
                    .closeQuantity(BigDecimal.ZERO)
                    .sellAbleQuantity(BigDecimal.ZERO)
                    .build();
        }

        BigDecimal closeQuantity = baseTotal.compareTo(BigDecimal.ZERO) > 0 ? baseAvailable : BigDecimal.ZERO;
        BigDecimal remainQuantity = quoteAvailable.divide(maxPayOff, 18, RoundingMode.DOWN);
        BigDecimal sellAbleQuantity = (remainQuantity).add(closeQuantity);
        return SellAbleValueDetail.builder()
                .remainQuantity(remainQuantity)
                .closeQuantity(closeQuantity)
                .sellAbleQuantity(sellAbleQuantity)
                .build();
    }

    public static String toScaleValue(BigDecimal value) {
        return DecimalUtil.toTrimString(value.setScale(BrokerServerConstants.BASE_OPTION_PRECISION, BigDecimal.ROUND_DOWN));
    }
}
