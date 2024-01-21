package io.bhex.broker.server.push.bo;

import io.bhex.broker.server.domain.NoticeBusinessType;

import java.util.Map;

/**
 * @author wangsc
 * @description 只是入参的简化
 * @date 2020-09-14 11:37
 */
public final class OrgBusinessPushRecordSimple {
    private final Long orgId;
    private final NoticeBusinessType businessType;

    /**
     * 业务通知的动态参数
     */
    private final Map<String, String> reqParam;
    /**
     * pushUrl的扩展参数
     */
    private final Map<String, String> urlParam;

    public OrgBusinessPushRecordSimple(Long brokerId, NoticeBusinessType businessType, Map<String, String> reqParam, Map<String, String> urlParam) {
        this.businessType = businessType;
        this.orgId = brokerId;
        this.reqParam = reqParam;
        this.urlParam = urlParam;
    }

    public Long getOrgId() {
        return orgId;
    }

    public NoticeBusinessType getBusinessType() {
        return businessType;
    }

    public Map<String, String> getReqParam() {
        return reqParam;
    }

    public Map<String, String> getUrlParam() {
        return urlParam;
    }
}
