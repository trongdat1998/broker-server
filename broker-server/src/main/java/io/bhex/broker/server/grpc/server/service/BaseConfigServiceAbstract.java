package io.bhex.broker.server.grpc.server.service;

public abstract class BaseConfigServiceAbstract {

    protected boolean isEffective(long now, long newStartTime, long newEndTime) {
        if (newStartTime > 0 && now < newStartTime) {
            return false;
        }
        if (newEndTime > 0 && now > newEndTime) {
            return false;
        }
        return true;
    }
}
