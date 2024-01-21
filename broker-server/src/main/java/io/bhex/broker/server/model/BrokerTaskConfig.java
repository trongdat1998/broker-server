package io.bhex.broker.server.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "tb_broker_task_config")
public class BrokerTaskConfig {

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;

    private Long orgId;

    private Long exchangeId;

    private String symbolId;

    private String tokenId;

    private Integer type; // 1-币币定时开盘 2-。。。

    private Long actionTime; //下次开始执行的时间

    private String actionContent; //执行任务描述 json格式

    private Integer status; //0-失效 1-待执行任务 2-已执行完毕

    private Integer dailyTask; //1-每天执行 0-一次性任务

    private Long created;

    private Long updated;

    private String adminUserName;

    private String remark; // 备注
}
