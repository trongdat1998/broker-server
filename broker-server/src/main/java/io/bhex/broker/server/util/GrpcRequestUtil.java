/**********************************
 *@项目名称: server-parent
 *@文件名称: io.bhex.broker.server.util
 *@Date 2018/9/4
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
package io.bhex.broker.server.util;

import io.bhex.base.proto.BaseRequest;
import io.bhex.broker.grpc.common.Header;

public class GrpcRequestUtil {

    public static BaseRequest getBaseRequest(Header header) {
        return BaseRequest.newBuilder()
                .setOrganizationId(header.getOrgId())
                .setBrokerUserId(header.getUserId())
                .setRequestTime(System.currentTimeMillis())
                .setInvokeType(BaseRequest.InvokerTypeEnum.AGENT_OF_USER)
                .setRequestTime(header.getRequestTime() > 0 ? header.getRequestTime() : System.currentTimeMillis())
                .build();
    }

    public static BaseRequest getBaseRequest(Long orgId) {
        return BaseRequest.newBuilder()
                .setOrganizationId(orgId)
                .setRequestTime(System.currentTimeMillis())
                .setInvokeType(BaseRequest.InvokerTypeEnum.AGENT_OF_USER)
                .build();
    }

}
