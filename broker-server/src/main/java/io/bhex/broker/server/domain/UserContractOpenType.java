package io.bhex.broker.server.domain;

import lombok.Builder;
import lombok.Data;

@Data
@Builder(builderClassName = "Builder", toBuilder = true)
public class UserContractOpenType {

    public final static Integer ON_OPEN_TYPE = 1;
    public final static Integer OFF_OPEN_TYPE = 0;

}
