package io.bhex.broker.server.domain;

import lombok.Data;

@Data
public class ThreadPoolConfig {

    private int corePoolSize = 5;
    private int maxPoolSize = 10;
    private int queueCapacity = 20;

}
