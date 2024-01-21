package io.bhex.broker.server.model;

import lombok.Data;

import javax.persistence.Id;
import javax.persistence.Table;
import java.sql.Timestamp;

/**
 * @author wangsc
 * @description 真实的quoteEngine地址
 * @date 2020-07-02 14:49
 */
@Data
@Table(name = "tb_quote_engine_address")
public class RealQuoteEngineAddress {
    @Id
    private Long id;
    private String platform;
    private String host;
    private Integer port;
    private String realHost;
    private Integer realPort;
    private Timestamp createdAt;
    private Timestamp updatedAt;
}
