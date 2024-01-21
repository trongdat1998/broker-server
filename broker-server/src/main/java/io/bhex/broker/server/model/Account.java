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

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "tb_account")
public class Account {

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;
    private Long orgId;
    private Long userId;
    private Long accountId;
    private String accountName;
    private Integer accountType;
    private Integer accountIndex; // 索引值，默认为0
    private Integer accountStatus;
    private Long created;
    private Long updated;

    private Integer isForbid; //是否禁止子账户转入转出 默认0
    private Long forbidStartTime;//禁止开始时间 用户请求时间
    private Long forbidEndTime;//禁止结束时间

    private Integer isAuthorizedOrg; //是否授权给机构进行操作，可以划转账户资产 默认0

}
