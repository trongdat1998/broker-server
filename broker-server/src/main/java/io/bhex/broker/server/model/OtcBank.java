package io.bhex.broker.server.model;

import java.util.Date;

import javax.persistence.Id;
import javax.persistence.Table;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author lizhen
 * @date 2018-11-04
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Table(name = "tb_otc_bank")
public class OtcBank {

    public static final int AVAILABLE = 1;

    /**
     * id
     */
    @Id
    private Integer id;
    /**
     * 券商ID
     */
    private Long orgId;
    /**
     * 银行代码
     */
    private String code;
    /**
     * 银行名称
     */
    private String name;
    /**
     * 多语言
     */
    private String language;
    /**
     * 状态  1：可用   -1：不可用
     */
    private Integer status;
    /**
     * 创建时间
     */
    private Date createDate;
    /**
     * 修改时间
     */
    private Date updateDate;
}
