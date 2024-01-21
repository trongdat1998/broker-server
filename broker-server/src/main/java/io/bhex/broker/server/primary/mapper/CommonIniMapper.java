package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.domain.CommonIni;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/**********************************
 *@项目名称: server-parent
 *@文件名称: io.bhex.broker.server.mapper
 *@Date 2018/11/30
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
@Mapper
public interface CommonIniMapper extends tk.mybatis.mapper.common.Mapper<CommonIni> {

    String COLUMNS = "id, org_id, ini_name, ini_desc, ini_value, language, created, updated";

    @Select("SELECT " + COLUMNS + " FROM tb_common_ini WHERE org_id = #{orgId} AND ini_name=#{iniName}")
    List<CommonIni> getByOrgIdAndInitName(@Param("orgId") Long orgId, @Param("iniName") String initName);

    @Select("SELECT " + COLUMNS + " FROM tb_common_ini WHERE org_id=#{orgId} AND ini_name=#{iniName} AND `language`=#{language}")
    CommonIni getByOrgIdAndInitNameAndLanguage(@Param("orgId") Long orgId,
                                               @Param("iniName") String initName,
                                               @Param("language") String language);

    @Select("SELECT " + COLUMNS + " FROM tb_common_ini WHERE org_id=#{orgId}")
    List<CommonIni> queryOrgCommonIni(@Param("orgId") Long orgId);

    @Select("SELECT " + COLUMNS + " FROM tb_common_ini WHERE org_id=#{orgId} AND ini_name like 'custom_%'")
    List<CommonIni> queryOrgCustomCommonIni(@Param("orgId") Long orgId);

}
