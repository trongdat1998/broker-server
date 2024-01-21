package io.bhex.broker.server.push.service;

import io.bhex.broker.common.util.JsonUtil;
import io.bhex.broker.server.model.OrgBusinessPushRecord;
import io.bhex.broker.server.primary.mapper.OrgBusinessPushRecordMapper;
import io.bhex.broker.server.push.bo.OrgBusinessPushRecordSimple;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

/**
 * @author wangsc
 * @description
 * @date 2020-09-12 12:19
 */
@Service
public class OrgBusinessPushRecordService {
    @Resource
    private OrgBusinessPushRecordMapper orgBusinessPushRecordMapper;

    public Long addOrgBusinessPushRecord(OrgBusinessPushRecordSimple recordSimple, Integer rangeType) {
        long currentTimeMillis = System.currentTimeMillis();
        OrgBusinessPushRecord pushRecord = OrgBusinessPushRecord.builder()
                .orgId(recordSimple.getOrgId())
                .businessType(recordSimple.getBusinessType().name())
                .rangeType(rangeType)
                .reqParam(recordSimple.getReqParam() == null ? JsonUtil.defaultGson().toString() : JsonUtil.defaultGson().toJson(recordSimple.getReqParam()))
                .urlParam(recordSimple.getUrlParam() == null ? JsonUtil.defaultGson().toString() : JsonUtil.defaultGson().toJson(recordSimple.getUrlParam()))
                .created(currentTimeMillis)
                .updated(currentTimeMillis)
                .build();
        orgBusinessPushRecordMapper.insertSelective(pushRecord);
        return pushRecord.getId();
    }

    public void updateOrgBusinessPushRecord(Long id, Integer status, String remark, long sendCount) {
        long currentTimeMillis = System.currentTimeMillis();
        OrgBusinessPushRecord pushRecord = OrgBusinessPushRecord.builder()
                .id(id)
                .status(status)
                .remark(remark)
                .sendCount(sendCount)
                .updated(currentTimeMillis)
                .build();
        orgBusinessPushRecordMapper.updateByPrimaryKeySelective(pushRecord);
    }
}
