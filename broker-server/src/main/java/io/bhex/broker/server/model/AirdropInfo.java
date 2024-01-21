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
 * @CreateDate: 10/11/2018 6:46 PM
 * @Copyright（C）: 2018 BHEX Inc. All rights reserved.
 */
@Data
@Table(name = "tb_airdrop_info")
public class AirdropInfo {

    public static final Integer AIRDROP_TYPE_CANDY = 1;
    public static final Integer AIRDROP_TYPE_FORK = 2;

//    public static final Integer STATUS_INIT = 0; //初始化 审核中，不可进行空投
//    public static final Integer STATUS_SUCCESS = 1;
//    public static final Integer STATUS_AIRDOP = 2;
//    public static final Integer STATUS_FAILED = 3;
//

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;
    private Integer type; //空头类型 1免费空投 2分叉空投
    private String title;
    private String description;
    private Long brokerId;
    private Long accountId; //发放空头币的账号id
    private Integer userType; //空头用户类型，1全部 2指定account id
    private String userAccountIds;
    // 空投币种数量与持有币种数量的比例关系决定到底给用户空投多少币
    // 如果为 免费空投，则直接按空投数量进行空投
    private String airdropTokenId; //空投币种
    private BigDecimal airdropTokenNum; //空投币种的数量
    private String haveTokenId; //空投比例需要用户持有的币种
    private BigDecimal haveTokenNum; //用户持有设定币的数量
    private Long snapshotTime; //快照时间
    private Long userCount; //发送的用户总数
    private BigDecimal transferAssetAmount; //发送token总数
    private Boolean isScheduleJod; //是否为定时任务
    private Long airdropTime; //定时任务时，发送时间
    private Integer status; //状态 0.初始化，未执行 1.空投完毕 2.空投中 3.空投失败
    private String failedReason; //空投失败说明
    private Long adminId;
    private Long updatedAt;
    private Long createdAt;
    private String userIds;

    private String tmplUrl;
    private Integer lockModel;

}
