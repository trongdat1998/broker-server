package io.bhex.broker.server.util;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.util
 * @Author: ming.xu
 * @CreateDate: 27/08/2018 4:07 PM
 * @Copyright（C）: 2018 BHEX Inc. All rights reserved.
 */
public class BooleanUtil {

    public static Boolean toBool(Integer n) {
        if (null != n) {
            if (n.intValue() == 1) {
                return true;
            }
        }
        return false;
    }

    public static Integer toInteger(Boolean b) {
        if (null != b) {
            if (b.booleanValue()) {
                return 1;
            }
        }
        return 0;
    }

}
