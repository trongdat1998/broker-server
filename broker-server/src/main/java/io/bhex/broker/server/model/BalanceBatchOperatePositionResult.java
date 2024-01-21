package io.bhex.broker.server.model;

import lombok.Builder;
import lombok.Data;

import java.util.List;

@Data
@Builder
public class BalanceBatchOperatePositionResult<T> {

    private BalanceBatchOperatePositionTask operateTask;
    private List<T> operateItemList;

}
