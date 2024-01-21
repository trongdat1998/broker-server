package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.AirdropTmplRecord;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Update;
import org.springframework.stereotype.Component;
import tk.mybatis.mapper.common.Mapper;
import tk.mybatis.mapper.common.special.InsertListMapper;

import java.util.List;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.admin.mapper
 * @Author: ming.xu
 * @CreateDate: 10/11/2018 7:23 PM
 * @Copyright（C）: 2018 BHEX Inc. All rights reserved.
 */
@Component
@org.apache.ibatis.annotations.Mapper
public interface AirdropTmplRecordMapper extends Mapper<AirdropTmplRecord>, InsertListMapper<AirdropTmplRecord> {


    @Update("update tb_airdrop_tmpl_record set status = #{status} where broker_id = #{brokerId} and airdrop_id = #{airdropId} and group_id = #{groupId}")
    int updateStatus(@Param("brokerId") long brokerId, @Param("airdropId") long airdropId, @Param("groupId") long groupId, @Param("status") int status);

    @Update("update tb_airdrop_tmpl_record set group_id = #{groupId} where broker_id = #{brokerId} and airdrop_id = #{airdropId} and tmpl_line_id = #{tmplLineId}")
    int updageGroup(@Param("brokerId") long brokerId, @Param("airdropId") long airdropId, @Param("tmplLineId") int tmplLineId, @Param("groupId") long groupId);

    @Update({"<script>",
            "update tb_airdrop_tmpl_record set group_id = #{groupId} where broker_id = #{brokerId} and airdrop_id = #{airdropId} and tmpl_line_id in "
                    + " <foreach collection=\"tmplLineIds\" index=\"index\" item=\"item\" open=\"(\" separator=\",\" close=\")\">"
                    + "#{item}"
                    + "</foreach>"
                    + "</script>"})
    int updageGroups(@Param("brokerId") long brokerId, @Param("airdropId") long airdropId, @Param("tmplLineIds") List<Integer> tmplLineIds, @Param("groupId") long groupId);

    @Update({"<script>",
            "update tb_airdrop_tmpl_record set status=#{status} where broker_id = #{brokerId} and airdrop_id = #{airdropId} and group_id = #{groupId} and tmpl_line_id in "
                    + " <foreach collection=\"tmplLineIds\" index=\"index\" item=\"item\" open=\"(\" separator=\",\" close=\")\">"
                    + "#{item}"
                    + "</foreach>"
                    + "</script>"})
    int updateStatusByGroup(@Param("brokerId") long brokerId, @Param("airdropId") long airdropId, @Param("groupId") long groupId,
                            @Param("tmplLineIds") List<Integer> tmplLineIds, @Param("status") Integer status);

}
