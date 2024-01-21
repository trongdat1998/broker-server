/**********************************
 *@项目名称: broker-parent
 *@文件名称: io.bhex.broker.mapper
 *@Date 2018/6/29
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.BrokerExchange;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;
import org.springframework.stereotype.Component;

import java.util.List;

@Deprecated
@Mapper
public interface BrokerExchangeMapper extends tk.mybatis.mapper.common.Mapper<BrokerExchange> {

    String TABLE_NAME = " tb_broker_exchange ";

    String COLUMNS = "id, org_id, exchange_id, exchange_name, status, created";

    @Select("SELECT " + COLUMNS + " FROM " + TABLE_NAME + " WHERE status = 1")
    List<BrokerExchange> queryAll();

    @Update("update " + TABLE_NAME + " set status = #{status} WHERE org_id = #{orgId} and exchange_id = #{exchangeId}")
    int updateContractStatus(@Param("orgId") Long orgId, @Param("exchangeId") Long exchangeId, @Param("status") Integer status);

    @Select("SELECT " + COLUMNS + " FROM " + TABLE_NAME + " WHERE org_id = #{orgId} and exchange_id = #{exchangeId}")
    BrokerExchange getContract(@Param("orgId") Long orgId, @Param("exchangeId") Long exchangeId);

}
