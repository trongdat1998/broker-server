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
@Table(name = "tb_news")
public class News {

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;
    private Long orgId;
    private Long newsId;
    private String newsPath;
    private Integer status;
    private Long created;
    private Long updated;
    private Integer version;
    private Long published;
    private String languages;
}
