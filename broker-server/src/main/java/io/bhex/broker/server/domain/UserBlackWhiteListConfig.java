/**********************************
 *@项目名称: server-parent
 *@文件名称: io.bhex.broker.server.domain
 *@Date 2018/11/30
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
package io.bhex.broker.server.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Id;
import javax.persistence.Table;

/**
 * @Description:
 * @Date: 2019/8/23 下午3:50
 * @Author: liwei
 * @Copyright（C）: 2018 BlueHelix Inc. All rights reserved.
 */
@Data
@Builder(builderClassName = "Builder")
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "tb_user_black_white_list_config")
public class UserBlackWhiteListConfig {

    @Id
    private Long id;
    private Long orgId;
    private Long userId;
    private Integer bwType;
    private Integer listType;
    private String reason;
    private Long startTime;
    private Long endTime;
    private Integer status; // 1 生效中  2 解除冻结
    private String extraInfo;
    private Long created;
    private Long updated;

}
