package io.bhex.broker.server.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;

import javax.persistence.GeneratedValue;
import javax.persistence.Table;
import java.sql.Timestamp;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "tb_broker_function_config")
public class BrokerFunctionConfig {
    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;

    private Long brokerId;

    private String function;

    private Integer status;

    private Timestamp createdAt;

    private Timestamp updatedAt;
}
