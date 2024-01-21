package io.bhex.broker.server.model;

import lombok.Data;

import java.io.Serializable;
import java.util.Date;
import javax.persistence.Column;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.validation.constraints.NotNull;

/**
 * tb_activity_trans_log
 *
 * @author
 */
@Data
@Table(name = "tb_activity_trans_log")
public class ActivityTransLog implements Serializable {

    @Column(name = "id")
    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;

    /**
     * 交易id
     */
    @Column(name = "data_id")
    @NotNull
    private Long dataId;

    /**
     * 交易时间
     */
    @Column(name = "trans_time")
    @NotNull
    private Date transTime;

    @Column(name = "create_time")
    @NotNull
    private Date createTime;

    private static final long serialVersionUID = 1L;

}
