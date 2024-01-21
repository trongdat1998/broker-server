package io.bhex.broker.server.model;

import lombok.*;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import java.math.BigDecimal;

/**
 * @author JinYuYuan
 * @description
 * @date 2021-03-03 19:39
 */
@Data
@Table(name = "tb_margin_asset")
public class MarginAssetRecord {

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;

    private Long orgId;

    private Long userId;

    private Long accountId;

    private BigDecimal beginMarginAsset;

    private BigDecimal todayBeginMarginAsset;

    private Integer isLoaning;

    private Long created;

    private Long updated;
}
