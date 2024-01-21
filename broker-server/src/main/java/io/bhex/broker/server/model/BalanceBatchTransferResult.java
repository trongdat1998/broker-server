package io.bhex.broker.server.model;

import lombok.Builder;
import lombok.Data;

import java.util.List;

@Data
@Builder
public class BalanceBatchTransferResult {

    private BalanceBatchTransferTask transferTask;
    private List<BalanceBatchTransfer> transferList;

}
