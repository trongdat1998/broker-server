package io.bhex.broker.server.grpc.server.service.po;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class ActivityTransferResult {

    public final static String SUCCESS = "success";

    public final static String FAIL = "fail";

    private Boolean result;

    private Integer code;

    private String message;
}
