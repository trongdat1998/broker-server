package io.bhex.broker.server.grpc.server.service;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.gson.JsonObject;
import io.bhex.base.account.BusinessSubject;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.common.util.JsonUtil;
import io.bhex.broker.server.model.SubBusinessSubject;
import io.bhex.broker.server.primary.mapper.SubBusinessSubjectMapper;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.*;
import java.util.stream.Stream;

@Service
public class SubBusinessSubjectService {

    private static final int ONLINE_STATUS = 1;

    private static final int[] ALLOWED_PARENT_SUBJECT = new int[]{BusinessSubject.TRANSFER_VALUE, BusinessSubject.AIRDROP_VALUE};

    private static final String SUB_BUSINESS_SUBJECT_KEY = "%s_%s_%s"; // ${orgId}_${subject}
    private static final String SUB_BUSINESS_SUBJECT_NAME_KEY = "%s_%s_%s_%s"; // ${orgId}_${subject}_${language}

    @Resource
    private SubBusinessSubjectMapper subBusinessSubjectMapper;

    private static ImmutableMap<String, SubBusinessSubject> SUB_BUSINESS_SUBJECT_MAP = ImmutableMap.of();
    private static ImmutableMap<String, String> SUB_BUSINESS_SUBJECT_NAME_MAP = ImmutableMap.of();

    @Scheduled(cron = "0 0/1 * * * ?")
    @PostConstruct
    public void initSubBusinessSubject() {
        Map<String, SubBusinessSubject> tmpSubjectMap = Maps.newHashMap();
        Map<String, String> tmpSubjectNameMap = Maps.newHashMap();

        List<SubBusinessSubject> businessSubjectList = subBusinessSubjectMapper.selectAll();
        for (SubBusinessSubject subject : businessSubjectList) {
            tmpSubjectMap.putIfAbsent(String.format(SUB_BUSINESS_SUBJECT_KEY, subject.getOrgId(), subject.getParentSubject(), subject.getSubject()), subject);
//            if (subject.getStatus() == ONLINE_STATUS) {
//                tmpSubjectMap.putIfAbsent(String.format(SUB_BUSINESS_SUBJECT_KEY, subject.getOrgId(), subject.getParentSubject(), subject.getSubject()), subject);
//            }
            tmpSubjectNameMap.put(String.format(SUB_BUSINESS_SUBJECT_NAME_KEY, subject.getOrgId(), subject.getParentSubject(), subject.getSubject(), subject.getLanguage()), subject.getSubjectName());
        }
        SUB_BUSINESS_SUBJECT_MAP = ImmutableMap.copyOf(tmpSubjectMap);
        SUB_BUSINESS_SUBJECT_NAME_MAP = ImmutableMap.copyOf(tmpSubjectNameMap);
    }

    public SubBusinessSubject getSubBusinessSubject(Long orgId, Integer parentSubject, Integer subject) {
        String key = String.format(SUB_BUSINESS_SUBJECT_KEY, orgId, parentSubject, subject);
        return SUB_BUSINESS_SUBJECT_MAP.get(key);
    }

    public String getSubBusinessSubjectName(Long orgId, Integer parentSubject, Integer subject, String language) {
        String key = String.format(SUB_BUSINESS_SUBJECT_NAME_KEY, orgId, parentSubject, subject, language);
        String name = SUB_BUSINESS_SUBJECT_NAME_MAP.get(key);
        if (Strings.isNullOrEmpty(name) && !language.equals(Locale.US.toString())) {
            key = String.format(SUB_BUSINESS_SUBJECT_NAME_KEY, orgId, parentSubject, subject, Locale.US.toString());
            name = SUB_BUSINESS_SUBJECT_NAME_MAP.get(key);
        }
        return name;
    }

    public List<SubBusinessSubject> querySubBusinessSubject(Long orgId, int parentSubject) {
        SubBusinessSubject.Builder builder = SubBusinessSubject.builder();
        builder.orgId(orgId);
        if (parentSubject > 0) {
            builder.parentSubject(parentSubject);
        }
        return subBusinessSubjectMapper.select(builder.build());
    }

    public void saveSubBusinessSubject(Long orgId, int parentSubject, int subject, Map<String, String> names, Integer status) {
        if (parentSubject != BusinessSubject.TRANSFER_VALUE && parentSubject != BusinessSubject.AIRDROP_VALUE) {
            throw new BrokerException(BrokerErrorCode.PARAM_INVALID);
        }
        Set<String> languages = names.keySet();
        Long currentTimestamp = System.currentTimeMillis();
        for (String language : languages) {
            SubBusinessSubject queryResult = subBusinessSubjectMapper.selectOne(
                    SubBusinessSubject.builder().orgId(orgId).parentSubject(parentSubject).subject(subject).language(language).build());
            if (queryResult != null) {
                SubBusinessSubject subBusinessSubject = SubBusinessSubject.builder()
                        .id(queryResult.getId())
                        .subjectName(names.get(language))
                        .status(status)
                        .updated(currentTimestamp)
                        .build();
                subBusinessSubjectMapper.updateByPrimaryKeySelective(subBusinessSubject);
            } else {
                SubBusinessSubject subBusinessSubject = SubBusinessSubject.builder()
                        .orgId(orgId)
                        .parentSubject(parentSubject)
                        .subject(subject)
                        .subjectName(names.get(language))
                        .language(language)
                        .status(status)
                        .created(currentTimestamp)
                        .updated(currentTimestamp)
                        .build();
                subBusinessSubjectMapper.insertSelective(subBusinessSubject);
            }
        }
    }

}
