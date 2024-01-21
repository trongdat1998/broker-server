package io.bhex.broker.server.model;

import lombok.Data;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import java.math.BigDecimal;

/**
 * @Description:
 * @Date: 2019/8/28 上午11:52
 * @Author: liwei
 * @Copyright（C）: 2018 BlueHelix Inc. All rights reserved.
 */
@Data
@Table(name = "tb_airdrop_tmpl_record")
public class AirdropTmplRecord {

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;

    private Integer tmplLineId; //模板中的行号

    private Long brokerId;

    private Long airdropId;

    private Long groupId;

    private Long userId;

    private Long accountId;

    private String tokenId;

    private BigDecimal tokenAmount;

    private String haveTokenId;

    private BigDecimal haveTokenAmount;

    private BigDecimal transferTokenAmount;

    private Integer status;

    private Long createdAt;
}
