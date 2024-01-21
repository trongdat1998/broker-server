package io.bhex.broker.server.grpc.server.service.aspect;


import java.lang.annotation.*;


/**
 * @Description: 标记用户业务日志的方法
 * @Date: 2019/12/6 上午10:34
 * @Author: liwei
 * @Copyright（C）: 2018 BlueHelix Inc. All rights reserved.
 */
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD})
public @interface UserActionLogAnnotation {


    String actionType() default "";

    //请求参数中没有header时，要用spel设置userid和orgid
    String userId() default "";
    String orgId()  default "";

    //spel表达式放在 {#} 中
    String action() default "";

    int byAdmin() default 0;


    String resultSucField() default "ret";
}

