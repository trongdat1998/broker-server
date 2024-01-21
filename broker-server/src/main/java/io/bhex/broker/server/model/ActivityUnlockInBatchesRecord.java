package io.bhex.broker.server.model;

import io.bhex.broker.server.primary.mapper.ActivityUnlockInBatchesRecordMapper;
import lombok.Data;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import java.math.BigDecimal;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.model
 * @Author: ming.xu
 * @CreateDate: 2019/12/30 2:46 PM
 * @Copyright（C）: 2019 BHEX Inc. All rights reserved.
 */
@Data
@Table(name = ActivityUnlockInBatchesRecordMapper.TABLE_NAME)
public class ActivityUnlockInBatchesRecord {

    public static final Integer UNLOCK_INIT = 0;
    public static final Integer UNLOCK_SUCCESS = 1;
    public static final Integer UNLOCK_FAILED = 2;

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;
    private Long brokerId;
    private Long accountId;
    private Long userId;
    private Long projectId;
    private String projectCode;
    private BigDecimal amount;
    private String tokenId;
    private String tokenName;
    private Integer unlockedStatus;  //解锁状态 0 初始化 1 解锁成功 2 解锁失败
    private Long createdTime;
    private Long updatedTime;

    private Long unlockClientOrderId;
}
