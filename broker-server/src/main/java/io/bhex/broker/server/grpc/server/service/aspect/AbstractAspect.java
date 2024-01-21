package io.bhex.broker.server.grpc.server.service.aspect;

import com.google.protobuf.GeneratedMessageV3;
import io.bhex.broker.grpc.common.Header;
import io.jsonwebtoken.lang.Collections;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.Signature;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.core.LocalVariableTableParameterNameDiscoverer;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.util.CollectionUtils;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public abstract class AbstractAspect {
    protected boolean isNum(String val) {
        return val == null || "".equals(val) ? false : val.matches("^[0-9]*$");
    }

    protected Header getHeader(Object[] args)  throws Exception {
        for (int i = 0; i < args.length; i++) {
            if (args[i] instanceof io.bhex.broker.grpc.common.Header) {
                io.bhex.broker.grpc.common.Header header = (io.bhex.broker.grpc.common.Header) args[i];
                return header;

            } else if (args[i] instanceof GeneratedMessageV3) {
                Class clazz = args[i].getClass();
                Field[] fields = clazz.getDeclaredFields();
                for (Field field : fields) {
                    String fieldValue = field.getName();
                    if (fieldValue.equals("header_")) {
                        field.setAccessible(true);
                        io.bhex.broker.grpc.common.Header header = (io.bhex.broker.grpc.common.Header) field.get(args[i]);
                        return header;
                    }
                }
            }
        }
        return null;
    }

    protected Method getCurrentMethod(JoinPoint point) throws Exception {
        //获取拦截的方法名
        Signature sig = point.getSignature();
        MethodSignature msig = (MethodSignature) sig;
        Object target = point.getTarget();
        Method currentMethod = target.getClass().getMethod(msig.getName(), msig.getParameterTypes());
        return currentMethod;
    }


    protected String parseOpContent(String opContent, Method method, Object[] args) {
        if (StringUtils.isEmpty(opContent)) {
            return "";
        }
        List<String> keys = new ArrayList<>();
        Matcher matcher = PARAM_PLACEHOLDER_PATTERN.matcher(opContent);
        while (matcher.find()) {
            String key = matcher.group();
            log.info("key:{}", key);
            keys.add(key.replace("{", "").replace("}", ""));
            //keys.add(key.replace("{", "#").replace("}", ""));
        }
        if (CollectionUtils.isEmpty(keys)) {
            return opContent;
        }

        List<String> params = new ArrayList<>();
        for (String key : keys) {
            params.add(parseKey(key, method, args));
        }


        return render(opContent, params);
    }

    private String parseKey(String key, Method method, Object[] args) {
        if (key == null || !key.startsWith("#")) {
            return key;
        }



        //获取被拦截方法参数名列表(使用Spring支持类库)
        LocalVariableTableParameterNameDiscoverer u = new LocalVariableTableParameterNameDiscoverer();
        String[] paraNameArr = u.getParameterNames(method);

        //使用SPEL进行key的解析
        ExpressionParser parser = new SpelExpressionParser();
        //SPEL上下文
        StandardEvaluationContext context = new StandardEvaluationContext();
        //把方法参数放入SPEL上下文中
        for (int i = 0; i < paraNameArr.length; i++) {
            context.setVariable(paraNameArr[i], args[i]);
        }
        return parser.parseExpression(key).getValue(context, String.class);
    }

    //private static final Pattern PARAM_PLACEHOLDER_PATTERN = Pattern.compile("\\{([A-Za-z_$]+\\.[A-Za-z_$\\d]*)\\}");

    private static final Pattern PARAM_PLACEHOLDER_PATTERN = Pattern.compile("\\{.*?\\}");

    protected String render(String content, List<String> params) {
        if (Collections.isEmpty(params)) {
            return content;
        }
        Matcher matcher = PARAM_PLACEHOLDER_PATTERN.matcher(content);
        int index = -1;
        while (matcher.find()) {
            String key = matcher.group();
            content = content.replace(key, params.get(++index));
        }
        return content;
    }
}
