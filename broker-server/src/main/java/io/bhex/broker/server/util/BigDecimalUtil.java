package io.bhex.broker.server.util;

import io.bhex.base.constants.ProtoConstants;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.math.BigDecimal;


@Slf4j
public class BigDecimalUtil {

    /**
     * 校验参数是否超过18位精度
     *
     * @param value
     */
    public static void checkParamScale(String... value) {
        for (int i = 0; i < value.length; i++) {
            if (StringUtils.isNotEmpty(value[i])) {
                BigDecimal val = new BigDecimal(value[i]).stripTrailingZeros();
                if (val.scale() > ProtoConstants.PRECISION) {
                    log.warn("[check param scale error ] scale over limit ! {} ", value[i]);
                    throw new BrokerException(BrokerErrorCode.PARAM_INVALID);
                }
            }
        }
    }

}
