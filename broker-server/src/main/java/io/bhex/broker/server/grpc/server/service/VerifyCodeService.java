/**********************************
 *@项目名称: broker-parent
 *@文件名称: io.bhex.broker.service.common
 *@Date 2018/7/23
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
package io.bhex.broker.server.grpc.server.service;

import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.grpc.common.Header;
import io.bhex.broker.grpc.security.SecurityInvalidVerifyCodeRequest;
import io.bhex.broker.grpc.security.SecurityInvalidVerifyCodeResponse;
import io.bhex.broker.grpc.security.SecurityValidVerifyCodeRequest;
import io.bhex.broker.grpc.security.SecurityValidVerifyCodeResponse;
import io.bhex.broker.server.domain.AuthType;
import io.bhex.broker.server.domain.VerifyCodeType;
import io.bhex.broker.server.grpc.client.service.GrpcSecurityService;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

@Service
public class VerifyCodeService {

    @Resource
    private GrpcSecurityService grpcSecurityService;

    public void validVerifyCode(Header header, VerifyCodeType type, Long verifyCodeOrderId, String verifyCode, String receiver, AuthType authType) {
        SecurityValidVerifyCodeRequest request = SecurityValidVerifyCodeRequest.newBuilder()
                .setHeader(header)
                .setUserId(header.getUserId())
                .setType(type.verifyCodeType())
                .setReceiver(receiver)
                .setOrderId(verifyCodeOrderId)
                .setVerifyCode(verifyCode)
                .build();
        SecurityValidVerifyCodeResponse securityValidVerifyCodeResponse = grpcSecurityService.validVerifyCode(request);

        if (securityValidVerifyCodeResponse.getRet() != 0) {
            if (authType == AuthType.MOBILE) {
                throw new BrokerException(BrokerErrorCode.VERIFY_CODE_ERROR);
            } else {
                throw new BrokerException(BrokerErrorCode.EMAIL_VERIFY_CODE_ERROR);
            }
        }
    }

    public void invalidVerifyCode(Header header, VerifyCodeType type, Long verifyCodeOrderId, String receiver) {
        try {
            SecurityInvalidVerifyCodeRequest request = SecurityInvalidVerifyCodeRequest.newBuilder()
                    .setHeader(header)
                    .setUserId(header.getUserId())
                    .setType(type.verifyCodeType())
                    .setReceiver(receiver)
                    .setOrderId(verifyCodeOrderId)
                    .build();
            SecurityInvalidVerifyCodeResponse response = grpcSecurityService.invalidVerifyCode(request);
        } catch (Exception e) {
            // ignore
        }
    }

}
