package io.bhex.broker.server.grpc.server.service.po;


import io.bhex.broker.server.model.TradeCompetitionExt;
import lombok.Data;

import java.math.BigDecimal;
import java.util.Date;
import java.util.List;

@Data
public class TradeCompetitionPo {

    private Long id;

    private Long orgId;

    private String competitionCode;

    private String symbolId;

    private Date startTime;

    private Date endTime;

    private Integer type;

    private Integer status;

    private String receiveTokenId;

    private String receiveTokenName;

    private BigDecimal receiveTokenAmount;

    private List<TradeCompetitionExt> ext;

    private String enterIdStr;

    private Integer enterStatus;

    private String enterWhiteStr;

    private Integer enterWhiteStatus;

    private String limitStr;

    private Integer limitStatus;
}
