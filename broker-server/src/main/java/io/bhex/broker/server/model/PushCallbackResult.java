package io.bhex.broker.server.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * author: wangshouchao（注只记录当前reqOrderId的第一条记录）
 * Date: 2020/07/25 06:27:41
 */
@Data
@Builder(builderClassName = "Builder", toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "tb_push_callback_result")
public class PushCallbackResult {

    /**
     * auto increase
     */
    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;
    /**
     * task id
     */
    private Long taskId;
    /**
     * 执行次数
     */
    private Long taskRound;
    /**
     * broker id
     */
    private Long orgId;
    /**
     * 唯一请求id
     */
    private String reqOrderId;
    /**
     * 回报内容
     */
    private String originContent;
    /**
     * 触达状态
     */
    private String deliveryStatus;
    /**
     * 触达时间
     */
    private Long deliveryTime;
    /**
     * created
     */
    private Long created;
}