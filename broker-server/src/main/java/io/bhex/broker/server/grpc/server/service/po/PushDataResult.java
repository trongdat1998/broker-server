package io.bhex.broker.server.grpc.server.service.po;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class PushDataResult {

    private String code;
    private String msg;
    private List<PushResult> data;

}
