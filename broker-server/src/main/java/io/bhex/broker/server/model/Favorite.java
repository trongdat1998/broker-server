/**********************************
 *@项目名称: broker-server
 *@文件名称: io.bhex.broker.server.model
 *@Date 2018/8/23
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
package io.bhex.broker.server.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Id;
import javax.persistence.Table;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "tb_favorite")
public class Favorite {

    @Id
    private Long id;
    private Long orgId;
    private Long userId;
    private Long exchangeId;
    private String symbolId;
    private Integer customOrder;
    private Long created;

}
