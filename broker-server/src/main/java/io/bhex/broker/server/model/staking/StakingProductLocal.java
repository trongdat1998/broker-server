package io.bhex.broker.server.model.staking;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "tb_staking_product_local")
public class StakingProductLocal {

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;

    /**
     * 机构ID
     */
    private Long orgId;

    /**
     * 定期项目ID
     */
    private Long productId;

    /**
     * 项目名称
     */
    private String productName;

    /**
     * 语言
     */
    private String language;

    /**
     * 协议地址
     */
    private String protocolUrl;

    /**
     * 图片地址
     */
    private String backgroundUrl;

    /**
     * 项目详情
     */
    private String details;

    /**
     * 状态 0=无效 1=有效
     */
    private Integer status;

    /**
     * 创建时间
     */
    private Long createdAt;

    /**
     * 修改时间
     */
    private Long updatedAt;


}
