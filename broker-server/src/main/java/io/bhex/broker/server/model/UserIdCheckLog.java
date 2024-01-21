/**********************************
 *@项目名称: server-parent
 *@文件名称: io.bhex.broker.server.model
 *@Date 2019/1/16
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
package io.bhex.broker.server.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class UserIdCheckLog {

    private Long id;
    private Long orgId;
    private Long userId;
    private String name;
    private String idNo;
    private String response;
    private Long created;

}
