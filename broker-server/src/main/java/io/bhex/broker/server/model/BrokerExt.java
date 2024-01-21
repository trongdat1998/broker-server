package io.bhex.broker.server.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import javax.persistence.Id;
import javax.persistence.Table;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "tb_broker_ext")
@Slf4j
@Builder(builderClassName = "Builder", toBuilder = true)
public class BrokerExt {

    @Id
    private Long brokerId;
    private String phone;
    private Long createAt;
    private Long updateAt;
}
