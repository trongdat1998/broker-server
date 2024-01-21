package io.bhex.broker.server.grpc.server.service.aspect;

import com.google.common.base.Strings;
import com.google.protobuf.GeneratedMessageV3;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.server.model.UserActionLog;
import io.bhex.broker.server.primary.mapper.UserActionLogMapper;
import io.bhex.broker.server.util.BeanCopyUtils;
import io.bhex.broker.server.util.GrpcHeaderUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.AfterReturning;
import org.aspectj.lang.annotation.AfterThrowing;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

@Slf4j
@Aspect
@Component
public class UserActionLogAspect extends AbstractAspect {

    @Resource
    private UserActionLogMapper userActionLogMapper;

    @Pointcut(value = "@annotation(io.bhex.broker.server.grpc.server.service.aspect.UserActionLogAnnotation)")
    public void cutService() {
    }

    @AfterReturning(pointcut = "cutService()", returning = "retVal")
    public void afterReturningAdvice(JoinPoint point, Object retVal) {
        handleWithoutException(point, retVal);
    }

    @AfterThrowing(pointcut = "cutService()", throwing = "ex")
    public void afterThrowingAdvice(JoinPoint point, Throwable ex) {
        handleWithoutException(point, ex);
    }

    private void handleWithoutException(JoinPoint point,  Object retVal) {
        try {
            handle(point, retVal);
        } catch (Exception e) {
            log.error("handle user action aspect error", e);
        }
    }

    private void handle(JoinPoint point,  Object retVal) throws Exception {

        Method currentMethod = getCurrentMethod(point);

        UserActionLogAnnotation annotation = currentMethod.getAnnotation(UserActionLogAnnotation.class);
        //log.info("actionType:{}", annotation.actionType());

        UserActionLog actionLog = new UserActionLog();
        Object[] args = point.getArgs();
        io.bhex.broker.grpc.common.Header header = getHeader(args);
        if (header != null) {
            BeanCopyUtils.copyPropertiesIgnoreNull(header, actionLog);
            actionLog.setAppBaseHeader(GrpcHeaderUtil.getAppBaseInfo(header));
            actionLog.setPlatform(header.getPlatform().name());
        }


        String resField = annotation.resultSucField();
        if (retVal instanceof Exception) {
            if (retVal instanceof BrokerException) {
                actionLog.setResultCode(((BrokerException) retVal).getCode());
            } else {
                actionLog.setResultCode(-99);
            }
        } else if (retVal.getClass().isPrimitive()) {
            actionLog.setResultCode(-90);
        } else {
            actionLog.setResultCode(0);
            Class clazz = retVal.getClass();
            Field[] fields = clazz.getDeclaredFields();
            for (Field field : fields) {
                String realResultCodeField = resField;
                if (retVal instanceof GeneratedMessageV3) {
                    realResultCodeField = realResultCodeField + "_";
                }
                if (field.getName().equals(realResultCodeField)) {
                    field.setAccessible(true);
                    String resultCode = field.get(retVal).toString();
                    if (isNum(resultCode)) {
                        actionLog.setResultCode(Integer.parseInt(resultCode));
                    } else if (resultCode.equalsIgnoreCase("true") || resultCode.equalsIgnoreCase("false")) {
                        actionLog.setResultCode(resultCode.equalsIgnoreCase("true") ? 0 : 1);
                    } else {
                        actionLog.setResultCode(-1);
                    }
                } else if (field.getName().equals("userId_") && (actionLog.getUserId() == null || actionLog.getUserId() == 0)) {
                    field.setAccessible(true);
                    String userId = field.get(retVal).toString();
                    if (!Strings.isNullOrEmpty(userId) && isNum(userId)) {
                        actionLog.setUserId(Long.parseLong(userId));
                    }
                }
            }
        }


        if (StringUtils.isNotEmpty(annotation.userId())) {
            String userId = parseOpContent(annotation.userId(), currentMethod, point.getArgs());
            if (isNum(userId)) {
                actionLog.setUserId(Long.parseLong(userId));
            }
        }
        if (StringUtils.isNotEmpty(annotation.orgId())) {
            String orgId = parseOpContent(annotation.orgId(), currentMethod, point.getArgs());
            if (isNum(orgId)) {
                actionLog.setOrgId(Long.parseLong(orgId));
            }
        }
        actionLog.setActionType(annotation.actionType());
        actionLog.setAction(parseOpContent(annotation.action(), currentMethod, point.getArgs()));
        actionLog.setCreated(System.currentTimeMillis());
        actionLog.setUpdated(System.currentTimeMillis());
        actionLog.setStatus(1);
        if (actionLog.getUserId() != null && actionLog.getUserId() > 0) {
            userActionLogMapper.insertSelective(actionLog);
        }
    }





}
