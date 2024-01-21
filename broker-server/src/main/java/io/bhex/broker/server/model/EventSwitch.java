package io.bhex.broker.server.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Id;
import javax.persistence.Table;
import java.sql.Timestamp;

/**
 * 事件开关
 *
 * @author zhaojiankun
 * @date 2018/09/14
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Table(name = "tb_event_switch")
public class EventSwitch {

    @Id
    private String eventSwitchId;

    private Boolean isOpen;

    private Timestamp closeEndTime;

    private Timestamp createdAt;

    private Timestamp updatedAt;
}
