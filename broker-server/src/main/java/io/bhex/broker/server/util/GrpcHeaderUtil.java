/**********************************
 *@项目名称: server-parent
 *@文件名称: io.bhex.broker.server.util
 *@Date 2019-02-18
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
package io.bhex.broker.server.util;

import io.bhex.broker.common.util.JsonUtil;
import io.bhex.broker.grpc.common.Header;
import io.bhex.broker.grpc.common.Platform;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GrpcHeaderUtil {

    public static String getAppBaseInfo(Header header) {
        String appBaseHeaderInfo = "";
        if (header.getPlatform() == Platform.MOBILE) {
            try {
                appBaseHeaderInfo = JsonUtil.defaultProtobufJsonPrinter().print(header.getAppBaseHeader());
            } catch (Exception e) {
                log.error("withdraw get app request base header info error", e);
            }
        }
        return appBaseHeaderInfo;
    }

    public static boolean isBhopReqeust(Header header) {
        return header.getOrgId() >= 7000;
    }

}
