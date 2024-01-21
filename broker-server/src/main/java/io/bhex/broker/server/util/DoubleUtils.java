package io.bhex.broker.server.util;


import java.math.BigDecimal;
import java.math.RoundingMode;

public class DoubleUtils {

    private static final double DEFAULT_VALUE = 0;

    /**
     * 截取小数点后多少位 ex: value:0.123456789 scale:4 ---> 0.1234
     *
     * @param value 截断值
     * @param scale 位数
     * @return
     */
    public static double round(double value, int scale) {
        return round(value, DEFAULT_VALUE, scale, RoundingMode.DOWN);
    }

    public static double round(double value, double defaultValue, int scale) {
        return round(value, defaultValue, scale, RoundingMode.DOWN);
    }

    /**
     * 四舍五入小数点后位数，负数绝对值后  五舍六入 ex: value:0.12345678 scale:4 ---> 0.1235
     *
     * @param value
     * @param scale
     * @return
     */
    public static double halfUp(double value, int scale) {
        return round(value, DEFAULT_VALUE, scale, RoundingMode.HALF_UP);
    }

    public static double halfUp(double value, double defaultValue, int scale) {
        return round(value, defaultValue, scale, RoundingMode.HALF_UP);
    }


    private static double round(double value, double defaultValue, int scale, RoundingMode mode) {

        try {
            BigDecimal decimal = new BigDecimal(value);
            decimal.setScale(scale, mode);
            return decimal.doubleValue();
        } catch (Exception e) {
            e.printStackTrace();
            return defaultValue;
        }

    }
}
