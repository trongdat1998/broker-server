package io.bhex.broker.server.util;

import io.bhex.base.proto.Decimal;
import io.bhex.base.proto.DecimalUtil;

import java.math.BigDecimal;

/**
 * 格式化BigDecimal对象或者平台proto的Decimal对象
 */
public class DecimalStringFormatter {

    private final int scale;
    private final int roundingMode;
    private final boolean stripTrailingZeros;

    public DecimalStringFormatter(int scale, int roundingMode, boolean stripTrailingZeros) {
        this.scale = scale;
        this.roundingMode = roundingMode;
        this.stripTrailingZeros = stripTrailingZeros;
    }

    public DecimalStringFormatter(int scale) {
        this(scale, BigDecimal.ROUND_DOWN, true);
    }

    public DecimalStringFormatter(Decimal precision) {
        this(DecimalUtil.toBigDecimal(precision).stripTrailingZeros().scale());
    }

    public DecimalStringFormatter(BigDecimal precision) {
        this(precision.stripTrailingZeros().scale());
    }

    /**
     * 将BigDecimal对象格式化成字符串
     * @param numeral BigDecimal对象
     * @return 格式化后的字符串
     */
    public String format(BigDecimal numeral) {
        BigDecimal formatNumeral = numeral.setScale(scale, roundingMode);
        if (stripTrailingZeros) {
            formatNumeral = formatNumeral.stripTrailingZeros();
        }
        return formatNumeral.toPlainString();
    }

    /**
     * 将平台proto的Decimal对象格式化成字符串
     * @param protoNumeral 平台proto定义的Decimal对象
     * @return 格式化后的字符串
     */
    public String format(Decimal protoNumeral) {
        return format(DecimalUtil.toBigDecimal(protoNumeral));
    }

    /**
     * 将BigDecimal对象取倒数之后再格式化成字符串
     * @param numeral BigDecimal对象
     * @return 格式化后的字符串
     */
    public String reciprocalFormat(BigDecimal numeral) {
        BigDecimal reciprocalNumeral;
        if (numeral.compareTo(BigDecimal.ZERO) != 0) {
            reciprocalNumeral = BigDecimal.ONE.divide(numeral, scale, roundingMode);
        } else {
            reciprocalNumeral = numeral;
        }
        if (stripTrailingZeros) {
            reciprocalNumeral = reciprocalNumeral.stripTrailingZeros();
        }
        return reciprocalNumeral.toPlainString();
    }

    /**
     * 将平台proto的Decimal对象取倒数之后格式化成字符串
     * @param protoNumeral 平台proto定义的Decimal对象
     * @return 格式化后的字符串
     */
    public String reciprocalFormat(Decimal protoNumeral) {
        return reciprocalFormat(DecimalUtil.toBigDecimal(protoNumeral));
    }
}
