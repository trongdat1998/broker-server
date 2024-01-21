package io.bhex.broker.server.model;

import com.google.common.base.Strings;
import lombok.Data;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import java.util.Objects;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.model
 * @Author: ming.xu
 * @CreateDate: 2019/6/5 11:52 AM
 * @Copyright（C）: 2019 BHEX Inc. All rights reserved.
 */
@Data
@Table(name = "tb_activity_lock_interest_common")
public class ActivityLockInterestCommon {


    private final static int ACTIVITY_TYPE_LOCK = 1;
    private final static int ACTIVITY_TYPE_IEO = 2;
    private final static int ACTIVITY_TYPE_PANIC_BUYING = 3;

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;
    private Long brokerId;
    private String projectCode;
    private String bannerUrl;
    private String description;
    private String wechatUrl;
    private String blockBrowser;
    private String browserTitle;
    private String language;
    //状态 0 删除 1 开启 2 关闭 3 提前结束
    private Integer status;
    private Long createdTime;
    private Long updatedTime;
    private Long endTime;
    private Long startTime;
    //活动类型 1锁仓派息  2IEO 3 抢购 4 自由模式
    private Integer activityType;
    private Long onlineTime;
    private Long resultTime;
    private Long assetUserId;
    private String introduction;
    //关于
    private String about;
    //活动规则
    private String rule;
    //private String descriptionV2;

    public String getIntroductionWithDefault(){
        return Strings.nullToEmpty(this.introduction);
    }

    public String getDescriptionWithDefault(){
        return Strings.nullToEmpty(this.description);
    }

/*    public String getDescriptionV2WithDefault(){
        return Strings.nullToEmpty(this.descriptionV2);
    }*/

    public int getActivityStatus(){

        if (ACTIVITY_TYPE_IEO==this.activityType) {
            return  getIEOProgress();
        } else if (ACTIVITY_TYPE_PANIC_BUYING==this.activityType) {
            return  getPanicBuyingProgress();
        }

        return -1;

    }

    private int getIEOProgress() {
        if(Objects.isNull(activityType)||activityType.intValue()!=2){
            return 0;
        }

        if(this.getStatus().equals(4)){
            return this.status;
        }

        long now = System.currentTimeMillis();
        int progressStatus = 0;
        // 预热
        if (now < this.getStartTime()) {
            progressStatus = 1;
            // 开始购买
        } else if (now < this.getEndTime()) {
            progressStatus = 2;
            // 购买结束，等待公布结果
        } else if (now < this.getResultTime()) {
            progressStatus = 3;
        } else if (now > this.getResultTime()) {
            progressStatus = 3;
        }
        return progressStatus;
    }

    private int getPanicBuyingProgress() {
        if(Objects.isNull(activityType)||activityType.intValue()!=3){
            return 0;
        }

        long now = System.currentTimeMillis();
        int progressStatus = 0;
        // 预热
        if (now < this.getStartTime()) {
            progressStatus = 1;
            // 开始购买
        } else if (now < this.getEndTime()) {
            progressStatus = 2;
            // 购买结束
        } else if (now > this.getEndTime()) {
            progressStatus = 5;
        }
        return progressStatus;
    }

    public String getDescriptionHtmlSafe(){
        return StringEscapeUtils.unescapeHtml4(this.getDescriptionWithDefault());
    }
}
