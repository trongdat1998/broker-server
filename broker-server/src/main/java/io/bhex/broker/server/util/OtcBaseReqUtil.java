package io.bhex.broker.server.util;

import io.bhex.ex.proto.BaseRequest;

import java.util.Optional;

/**
 * @author wangsc
 * @description 产生baseRequest
 * @date 2020-05-31 22:15
 */
public class OtcBaseReqUtil {

    public static BaseRequest getBaseRequest(Long orgId) {
        //兼容proxy为false的情况,不做校验避免orgId为null或者为0时失败
        return BaseRequest.newBuilder().setOrgId(orgId).build();
    }

    /**
     * 避免可能的baseRequest为null造成的null指针
     * @param baseRequest
     * @return
     */
    public static Long getOrgIdByBaseReq(BaseRequest baseRequest){
        return Optional.ofNullable(baseRequest).map(BaseRequest::getOrgId).orElse(0L);
    }
}
