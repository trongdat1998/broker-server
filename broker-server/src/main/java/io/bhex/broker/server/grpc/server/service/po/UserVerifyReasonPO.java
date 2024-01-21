package io.bhex.broker.server.grpc.server.service.po;

import lombok.Data;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.grpc.server.service.po
 * @Author: ming.xu
 * @CreateDate: 24/08/2018 3:23 PM
 * @Copyright（C）: 2018 BHEX Inc. All rights reserved.
 */
@Data
public class UserVerifyReasonPO {

    private Long id;

    private String reason;

    private String locale;
}
