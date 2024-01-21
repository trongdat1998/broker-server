package io.bhex.broker.server.model;


import java.math.BigDecimal;

import javax.persistence.Column;
import javax.persistence.Id;
import javax.persistence.Table;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "tb_agent_config")
public class AgentConfig {

    @Column(name = "id")
    @Id
    private Long id;
    private Long orgId;
    private BigDecimal coinMaxRate;
    private BigDecimal contractMaxRate;
}
