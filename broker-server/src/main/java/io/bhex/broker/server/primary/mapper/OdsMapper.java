package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.OdsData;
import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import tk.mybatis.mapper.common.Mapper;

import java.util.HashMap;
import java.util.List;

/**
 * @Description:
 * @Date: 2019/11/1 上午10:22
 * @Author: liwei
 * @Copyright（C）: 2018 BlueHelix Inc. All rights reserved.
 */
@org.apache.ibatis.annotations.Mapper
public interface OdsMapper  extends Mapper<OdsData> {

    //sql like select org_id, count(*) as total ,a,b,c from tb_user group by org_id where created between {start} and {end}
    @Select("${sql}")
    List<HashMap<String, Object>> queryGroupDataList(@Param("sql") String sql);

    @Select({"<script>",
            "select * from tb_ods_data where data_key = #{key} and org_id = #{orgId} ",
            "<if test=\"startTime > 0\">AND start_time >= #{startTime}</if> ",
            "<if test=\"endTime > 0\">AND start_time &lt;= #{endTime}</if> ",
            " order by id desc ",
            "<if test=\"limit > 0\"> limit #{limit}</if> ",
            "</script>"})
    List<OdsData> listDataKey(@Param("orgId") long orgId, @Param("key") String key, @Param("limit") int limit, @Param("startTime") long startTime,  @Param("endTime") long endTime);

    @Select("select count(*) from tb_ods_data where data_key = #{key} and org_id = #{orgId}")
    Integer countDataKey(@Param("orgId") long orgId, @Param("key") String key);

    @Select("select count(*) from tb_ods_data where data_key = #{key} and org_id = #{orgId} and date_str = #{dateStr} ")
    Integer countDataKeyByDateStr(@Param("orgId") long orgId, @Param("key") String key, @Param("dateStr") String dateStr);

    @Select({"<script>select * from tb_ods_data where data_key = #{key} and org_id = #{orgId} ",
             "<if test=\"token != null and token != ''\">AND token = #{token}</if> ",
            "<if test=\"startTime > 0\">AND start_time >= #{startTime}</if> ",
            "<if test=\"endTime > 0\">AND start_time &lt;= #{endTime}</if> ",
            " order by id desc ",
             "</script>"})
    List<OdsData> listDataKey4Token(@Param("orgId") long orgId, @Param("key") String key, @Param("token") String token,
                                    @Param("startTime") long startTime,  @Param("endTime") long endTime);


    @Select("select count(*) from tb_ods_data where data_key = #{key} and org_id = #{orgId} and date_str = #{dateStr} and token = #{token}")
    Integer countDataKey4Token(@Param("orgId") long orgId, @Param("key") String key, @Param("token") String token, @Param("dateStr") String dateStr);

    @Select({"<script>select * from tb_ods_data where data_key = #{key} and org_id = #{orgId} ",
             "<if test=\"symbol != null and symbol != ''\">AND symbol = #{symbol}</if> ",
            "<if test=\"startTime > 0\">AND start_time >= #{startTime}</if> ",
            "<if test=\"endTime > 0\">AND start_time &lt;= #{endTime}</if> ",
            " order by id desc ",
             "</script>"})
    List<OdsData> listDataKey4Symbol(@Param("orgId") long orgId, @Param("key") String key, @Param("symbol") String symbol,
                                      @Param("startTime") long startTime,  @Param("endTime") long endTime);


    @Select("select count(*) from tb_ods_data where data_key = #{key} and org_id = #{orgId} and date_str = #{dateStr} and symbol = #{symbol}")
    Integer countDataKey4Symbol(@Param("orgId") long orgId, @Param("key") String key, @Param("symbol") String symbol, @Param("dateStr") String dateStr);


    @Delete("delete from tb_ods_data where data_key = #{key} ")
    Integer deleteDataKey(@Param("key") String key);

    @Delete("delete from tb_ods_data where data_key = #{key}  and org_id = #{orgId} ")
    Integer deleteDataKey2(@Param("orgId") long orgId, @Param("key") String key);

    @Delete("delete from tb_ods_data where start_time < #{startTime} and data_key like '%.H' ")
    Integer deleteUnusedHourData(@Param("startTime") long startTime);
}
