/**********************************
 *@项目名称: broker-parent
 *@文件名称: io.bhex.broker.domain.entity
 *@Date 2018/6/10
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
package io.bhex.broker.server.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import java.math.BigDecimal;
import java.util.Date;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "tb_agent_user")
public class AgentUser {

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;

    private Long orgId;

    private Long userId;

    private Long accountId;

    private Long superiorUserId;

    private Long p1;

    private Long p2;

    private Long p3;

    private Long p4;

    private Long p5;

    private Long p6;

    private Long p7;

    private Long p8;

    private Long p9;

    private Long p10;

    private BigDecimal rate; //上级默认分佣比例

    private BigDecimal defaultRate; //普通用户默认分佣比例

    private BigDecimal childrenDefaultRate; //下级分佣比例

    private BigDecimal contractRate; //期货上级默认分佣比例

    private BigDecimal contractDefaultRate; //期货普通用户默认分佣比例

    private BigDecimal contractChildrenDefaultRate; //期货下级分佣比例

    private Integer level;

    private Integer isAgent;

    private Integer status; // 0无效 1有效 为0时 普通返佣则正常进行返佣

    private Integer isDel;

    private BigDecimal additionalCoinFee;

    private BigDecimal additionalContractFee;

    private BigDecimal childrenDefaultCoinFee;

    private BigDecimal childrenDefaultContractFee;

    private String agentName;

    private String leader;

    private String leaderMobile;

    private String mark;

    private String mobile;

    private String email;

    private Date registerTime;

    private Integer isAbs;

    private Date updated;

    private Date created;
}
