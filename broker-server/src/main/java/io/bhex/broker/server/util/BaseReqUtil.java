package io.bhex.broker.server.util;

import io.bhex.base.proto.BaseRequest;
import io.bhex.broker.grpc.common.Header;

import java.util.Optional;

/**
 * @author wangsc
 * @description 产生baseRequest
 * @date 2020-05-31 22:15
 */
public class BaseReqUtil {

    public static BaseRequest getBaseRequest(Long orgId){
        //兼容proxy为false的情况,不做校验避免orgId为null或者为0时失败
        return BaseRequest.newBuilder().setOrganizationId(orgId).build();
    }

    public static BaseRequest getBaseRequestByHeader(Header header){
        //兼容proxy为false的情况,不做校验避免orgId为null或者为0时失败
        return BaseRequest.newBuilder().setOrganizationId(getOrgIdByHeader(header)).build();
    }

    /**
     * 避免可能的baseRequest为null造成的null指针
     * @param baseRequest
     * @return
     */
    public static Long getOrgIdByBaseReq(BaseRequest baseRequest){
        return Optional.ofNullable(baseRequest).map(BaseRequest::getOrganizationId).orElse(0L);
    }

    public static Long getOrgIdByHeader(Header header){
        return Optional.ofNullable(header).map(Header::getOrgId).orElse(0L);
    }

}
