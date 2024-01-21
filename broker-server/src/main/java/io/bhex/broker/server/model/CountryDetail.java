/**********************************
 *@项目名称: broker-parent
 *@文件名称: io.bhex.broker.domain.entity
 *@Date 2018/7/6
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
public class CountryDetail {

    private Long id;
    private Long countryId;
    private String countryName;
    private String indexName;
    private String language;
    private Long created;

}
