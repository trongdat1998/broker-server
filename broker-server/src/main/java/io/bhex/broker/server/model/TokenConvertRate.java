/**********************************
 *@项目名称: server-parent
 *@文件名称: io.bhex.broker.server.model
 *@Date 2018/8/29
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
package io.bhex.broker.server.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TokenConvertRate {

    private Long id;
    private Long orgId;
    private String tokenId;
    private String convertTokenId;
    private BigDecimal rateUpRatio;
    private BigDecimal convertRate;
    private BigDecimal lastConvertRate;
    private Long created;
    private Long updated;

}
