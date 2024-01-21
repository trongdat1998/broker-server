/**********************************
 *@项目名称: broker-parent
 *@文件名称: io.bhex.broker.service.user.entity
 *@Date 2018/8/3
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
public class LoginLog {

    private Long id;
    private Long orgId;
    private Long userId;
    private String ip;
    private String region;
    private Integer status;
    private Integer loginType;
    private String platform;
    private String userAgent;
    private String language;
    private String appBaseHeader;
    private String channel;
    private String source;
    private Long created;
    private Long updated;

}
