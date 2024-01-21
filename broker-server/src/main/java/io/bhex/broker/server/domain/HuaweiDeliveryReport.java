package io.bhex.broker.server.domain;

import lombok.Builder;
import lombok.Data;

import java.util.List;

@Data
@Builder(builderClassName = "Builder", toBuilder = true)
public class HuaweiDeliveryReport {


    private List<Status> statuses;

    @Data
    public static class Status {
        private String biTag; //开头第一个字母是业务标识 B为系统push，C为task push
        private String appid;
        private String token;
        private Integer status;
        private Long timestamp;
        private String requestId;
    }
}
