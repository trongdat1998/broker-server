package io.bhex.broker.server.grpc.server.service.aspect;

import com.google.protobuf.GeneratedMessageV3;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.server.domain.BaseConfigConstants;
import io.bhex.broker.server.domain.SwitchStatus;
import io.bhex.broker.server.grpc.server.service.BaseBizConfigService;
import io.bhex.broker.server.util.BeanCopyUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.aspectj.lang.annotation.Pointcut;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

@Slf4j
@Aspect
@Component
public class SiteFunctionLimitSwitchAspect  extends AbstractAspect {

    @Resource
    private BaseBizConfigService baseBizConfigService;

    @Pointcut(value = "@annotation(io.bhex.broker.server.grpc.server.service.aspect.SiteFunctionLimitSwitchAnnotation)")
    public void cutService() {
    }

    @Before(value = "cutService()")
    public void beforeAdvice(JoinPoint point) {
        handle(point);
    }



    private void handle(JoinPoint point) {
        long orgId = 0;
        long userId = 0;
        String wholeSiteSwitchKey = "";
        String userSwitchGroupKey = "";
        BrokerErrorCode userSwitchErrorCode = null;
        try {
            Method currentMethod = getCurrentMethod(point);
            SiteFunctionLimitSwitchAnnotation annotation = currentMethod.getAnnotation(SiteFunctionLimitSwitchAnnotation.class);
            wholeSiteSwitchKey = annotation.wholeSiteSwitchKey();
            userSwitchGroupKey = annotation.userSwitchGroupKey();
            userSwitchErrorCode = annotation.userSwitchErrorCode();
            Object[] args = point.getArgs();
            io.bhex.broker.grpc.common.Header header = getHeader(args);
            if (header != null) {
                orgId = header.getOrgId();
                userId = header.getUserId();
            }



            if (StringUtils.isNotEmpty(annotation.orgId())) {
                String orgIdStr = parseOpContent(annotation.orgId(), currentMethod, point.getArgs());
                if (isNum(orgIdStr)) {
                    orgId = Long.parseLong(orgIdStr);
                }
            }

            if (StringUtils.isNotEmpty(annotation.userId())) {
                String userIdStr = parseOpContent(annotation.userId(), currentMethod, point.getArgs());
                if (isNum(userIdStr)) {
                    userId = Long.parseLong(userIdStr);
                }
            }

        } catch (Exception e) {
            log.error("WholeSiteSwitch error", e);
        }

        //log.info("org:{} user:{} siteKey:{} userKey:{}", orgId, userId, wholeSiteSwitchKey, userSwitchGroupKey);

        if (orgId > 0 && !wholeSiteSwitchKey.equals("")) {
            SwitchStatus switchStatus = baseBizConfigService.getConfigSwitchStatus(orgId,
                    BaseConfigConstants.WHOLE_SITE_CONTROL_SWITCH_GROUP, wholeSiteSwitchKey);
            if (switchStatus.isOpen()) {
                log.info("WholeSiteSuspended org:{} userId:{} key:{}", orgId, userId, wholeSiteSwitchKey);
                throw new BrokerException(BrokerErrorCode.FEATURE_SUSPENDED);
            }
        }

        if (orgId > 0 && userId > 0 && !userSwitchGroupKey.equals("")) {
            SwitchStatus switchStatus = baseBizConfigService.getConfigSwitchStatus(orgId,
                    userSwitchGroupKey, userId + "");
            if (switchStatus.isOpen()) {
                log.info("UserActionSuspended org:{} userId:{} key:{}", orgId, userId, userSwitchGroupKey);
                throw new BrokerException(userSwitchErrorCode);
            }
        }
    }

}
