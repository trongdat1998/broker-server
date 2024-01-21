/**********************************
 *@项目名称: server-parent
 *@文件名称: io.bhex.broker.server.util
 *@Date 2019/1/28
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
package io.bhex.broker.server.util;

import com.google.common.base.Strings;
import lombok.extern.slf4j.Slf4j;
import java.math.BigDecimal;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import org.apache.commons.lang3.StringUtils;

import java.util.Objects;

@Slf4j
public class CommonUtil {

    public static String getFXRateKey(Long orgId, String tokenId) {
        return orgId + "_" + tokenId;
    }

    public static<T> T nullToDefault(T value,T defaultValue){
        if(Objects.isNull(value)){
            return defaultValue;
        }

        return value;
    }

    public static BigDecimal toBigDecimal(String string) {
        return toBigDecimal(string, null);
    }

    public static BigDecimal toBigDecimal(String string, BigDecimal defaultValue) {
        if (StringUtils.isEmpty(string)) {
            return defaultValue;
        }

        try {
            return new BigDecimal(string);
        } catch (Exception e) {
            log.warn("string: {} toBigDecimal error: {}", string, e.getMessage());
        }
        return defaultValue;
    }

    public static String formatMessage(Message message){

        try{
            return JsonFormat.printer().print(message);
        }catch (Exception e){
            log.error(e.getMessage(),e);
            return "";
        }
    }


    public static String getBriefString(String text, int length) {
        if (Strings.isNullOrEmpty(text) || text.length() <= length) {
            return text;
        }

        return String.format("%s......total %d chars", text.substring(0, length - 1), text.length());
    }

    public static String bigDecimalToString(BigDecimal data){
        if(Objects.isNull(data)){
            return "";
        }

        return data.stripTrailingZeros().toPlainString();

    }
}
