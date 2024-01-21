/**********************************
 *@项目名称: server-parent
 *@文件名称: io.bhex.broker.server.mapper
 *@Date 2019-03-18
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.FinanceTask;
import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface FinanceTaskMapper extends tk.mybatis.mapper.common.Mapper<FinanceTask> {
}
