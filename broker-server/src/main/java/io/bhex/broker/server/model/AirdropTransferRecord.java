package io.bhex.broker.server.model;

import lombok.Data;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import java.math.BigDecimal;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.admin.model
 * @Author: ming.xu
 * @CreateDate: 10/11/2018 6:49 PM
 * @Copyright（C）: 2018 BHEX Inc. All rights reserved.
 */
@Data
@Table(name = "tb_airdrop_transfer_record")
public class AirdropTransferRecord {

    public static final Integer UNLOCKED_STATUS = 0; //无需锁定
    public static final Integer NEED_LOCK_STATUS = 1; //待锁定
    public static final Integer LOCKED_STATUS = 2; //已锁定

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;
    private Long clientTransferId;  //调用平台转账时的id
    private Integer tmplLineId; //模板中的行号
    private Long brokerId;
    private Long airdropId;
    private Long groupId;
    private Long snapshotTime;
    private Long accountId;
    private String tokenId;
    private BigDecimal tokenAmount;
    private Integer status;
    private Long createdAt;
    private Integer msgNoticed;
    private Integer lockedStatus;
}
