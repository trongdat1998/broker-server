package io.bhex.broker.server.model;


import lombok.Data;

import javax.persistence.Table;
import java.math.BigDecimal;
import java.util.Date;

@Data
@Table(name = "tb_activity_lock_interest_lock_record")
public class ActivityLockInterestLockRecord {

    private Long id;

    private String project;

    private Long orgId;

    private Long userId;

    private Long accountId;

    private String tokenId;

    private BigDecimal lockAmount;

    private BigDecimal releaseAmount;

    private BigDecimal lastAmount;

//    private Integer isDel;

    private Date createdTime;

    private Date updatedTime;

    private String activitys;
}
