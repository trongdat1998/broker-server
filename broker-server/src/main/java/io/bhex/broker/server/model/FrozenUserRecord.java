/**********************************
 *@项目名称: server-parent
 *@文件名称: io.bhex.broker.server.domain
 *@Date 2018/11/30
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
package io.bhex.broker.server.model;

import javax.persistence.Id;
import javax.persistence.Table;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder(builderClassName = "Builder")
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "tb_frozen_user")
public class FrozenUserRecord {

    @Id
    private Long id;
    private Long orgId;
    private Long userId;
    private Integer frozenType;
    private Integer frozenReason;
    private Long startTime;
    private Long endTime;
    private Integer status; // 1 冻结生效中  2 解除冻结
    private Long created;
    private Long updated;

}
