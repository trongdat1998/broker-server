package io.bhex.broker.server.domain.kyc.tencent;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.List;

@Data
@EqualsAndHashCode(callSuper=true)
public class GetApiTicketResponse extends WebankBaseResponse {

    private List<TicketObject> tickets;
}
