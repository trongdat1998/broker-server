package io.bhex.broker.server.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * 推送任务基本信息表
 * author: wangshouchao
 * Date: 2020/07/25 06:21:44
 */
@Data
@Builder(builderClassName = "Builder", toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "tb_admin_push_task")
public class AdminPushTask {

    /**
     * auto increase
     */
    @Id
    @GeneratedValue(generator = "JDBC")
    private Long taskId;
    /**
     * broker id
     */
    private Long orgId;
    /**
     * 推送名称
     */
    private String name;
    /**
     * 推送类别1通知
     */
    private Integer pushCategory;
    /**
     * 推送范围类型
     */
    private Integer rangeType;

    /**
     * 可能是个大数据(注意排除)
     */
    private String userIds;
    /**
     * 计划执行类型，0.定时一次执行、 1.周期性每天执行 2.周期性每周执行
     */
    private Integer cycleType;
    /**
     * 每周几执行
     */
    private Integer cycleDayOfWeek;
    /**
     * 第一次预计执行时间
     */
    private Long firstActionTime;
    /**
     * 执行时间
     */
    private Long executeTime;
    /**
     * 0-未执行 1-执行中 2-此轮任务执行完成  3-此task结束  4-取消
     */
    private Integer status;
    /**
     * actionTime(可变动的执行时间,周期性的为下一次执行时间)
     */
    private Long actionTime;
    /**
     * expireTime(失效时间)
     */
    private Long expireTime;
    /**
     * 默认语言
     */
    private String defaultLanguage;
    /**
     * 备注(用于正常取消，和非正常取消的备注)
     */
    private String remark;
    /**
     * created
     */
    private Long created;
    /**
     * updated
     */
    private Long updated;
}