package io.bhex.broker.server.grpc.server.service.aspect;

import io.bhex.broker.common.exception.BrokerErrorCode;

import java.lang.annotation.*;

@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD})
public @interface SiteFunctionLimitSwitchAnnotation {

    //全站开关
    String wholeSiteSwitchKey() default "";

    String userSwitchGroupKey() default "";

    String orgId()  default "";

    String userId() default "";

    BrokerErrorCode userSwitchErrorCode() default BrokerErrorCode.ORDER_FROZEN_BY_ADMIN;

}
