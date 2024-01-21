package io.bhex.broker.server.model;

import lombok.Data;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.model
 * @Author: ming.xu
 * @CreateDate: 2019/6/5 3:59 PM
 * @Copyright（C）: 2019 BHEX Inc. All rights reserved.
 */
@Data
@Table(name = "tb_activity_lock_interest_local")
public class ActivityLockInterestLocal {

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;
    private Long brokerId;
    private Long projectId;
    private String projectCode;
    private String projectName;
    private String title;
    private String descript;
    private String language;
    private String lockedPeriod;
    private Long createdTime;
    private Long updatedTime;
    private String fixedInterestRate; //固定利率 用于展示 String
    private String floatingRate; //浮动利率 用户展示 String
    private String circulationStr; //发行量多语言支持
}
