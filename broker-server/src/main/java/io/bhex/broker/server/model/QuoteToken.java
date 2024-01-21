/**********************************
 *@项目名称: broker-parent
 *@文件名称: io.bhex.broker.domain.entity
 *@Date 2018/6/29
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
@Table(name = "tb_quote_token")
public class QuoteToken {

    @Id
    private Long id;
    private Long orgId;
    private String tokenId;
    private String tokenName;
    private String tokenIcon;
    private Integer customOrder;
    private Integer status;
    private Long updated;
    private Long created;
    private Integer category;

}
