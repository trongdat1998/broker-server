package io.bhex.broker.server.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.joda.time.DateTime;
import tk.mybatis.mapper.annotation.KeySql;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Transient;
import java.sql.Timestamp;
import java.util.Date;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "tb_data_push_message")
public class DataPushMessage {

    @Id
    @KeySql(useGeneratedKeys = true)
    private Long id;

    private Long orgId;

    private String clientReqId;

    private String messageType; // USER.REGISTER USER.KYC TRADE.SPOT

    private transient String callbackUrl;

    private String message;

    private Integer messageStatus;

    private Integer retryCount;

    private Date pushTime;

    private Date createTime;

    private Date updateTime;
}
