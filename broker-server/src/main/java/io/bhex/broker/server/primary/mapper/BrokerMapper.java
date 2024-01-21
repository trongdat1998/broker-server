/**********************************
 *@项目名称: broker-parent
 *@文件名称: io.bhex.broker.mapper
 *@Date 2018/6/29
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.Broker;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;
import org.springframework.stereotype.Component;

import java.util.List;

@Mapper
@Component
public interface BrokerMapper extends tk.mybatis.mapper.common.Mapper<Broker> {

    @Select("SELECT * FROM tb_broker WHERE org_id = #{orgId}")
    Broker getByOrgId(@Param("orgId") Long orgId);

    @Select("SELECT * FROM tb_broker WHERE org_id = #{orgId} and status = 1")
    Broker getByOrgIdAndStatus(@Param("orgId") Long orgId);

    @Select("SELECT * FROM tb_broker WHERE status = 1")
    List<Broker> queryAvailableBroker();

    @Update("update tb_broker set functions = #{functions} where org_id = #{orgId} and status = 1")
    int updateBrokerFunction(@Param("orgId") Long orgId, @Param("functions") String functions);

    @Update("update tb_broker set support_languages = #{supportLanguages} where org_id = #{orgId} and status = 1")
    int updateBrokerLanguage(@Param("orgId") Long orgId, @Param("supportLanguages") String supportLanguages);
}
