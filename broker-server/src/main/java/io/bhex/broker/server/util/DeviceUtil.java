package io.bhex.broker.server.util;

import io.bhex.broker.grpc.common.DeviceTypeEnum;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.util
 * @Author: ming.xu
 * @CreateDate: 14/09/2018 5:49 PM
 * @Copyright（C）: 2018 BHEX Inc. All rights reserved.
 */
public class DeviceUtil {

    private static final Integer PLATFORM_PC = 1;

    private static final Integer PLATFORM_APP = 2;

    public static Integer getPlatInfo(DeviceTypeEnum deviceTypeEnum) {
        switch (deviceTypeEnum) {
            case WEB:
                return PLATFORM_PC;
            case ANDROID:
            case IOS:
                return PLATFORM_APP;
            default:
                return 0;
        }
    }
}
