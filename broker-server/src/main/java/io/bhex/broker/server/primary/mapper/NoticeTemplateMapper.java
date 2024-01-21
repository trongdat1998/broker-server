package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.NoticeTemplate;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.springframework.stereotype.Component;

import java.util.List;

/**********************************
 *@项目名称: server-parent
 *@文件名称: io.bhex.broker.server.mapper
 *@Date 2018/9/9
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
@Mapper
@Component
public interface NoticeTemplateMapper extends tk.mybatis.mapper.common.Mapper<NoticeTemplate> {

    String COLUMNS = "id, org_id, notice_type, business_type, language, template_id, template_content, send_type, sign, subject, created, updated";

    @Select("SELECT " + COLUMNS + " FROM tb_notice_template WHERE is_verify_code_template = 1")
    List<NoticeTemplate> queryVerifyCodeTemplateList();

    @Select("SELECT " + COLUMNS + " FROM tb_notice_template")
    List<NoticeTemplate> queryAll();

    @Select("SELECT " + COLUMNS + " FROM tb_notice_template WHERE org_id = #{orgId} and notice_type=#{noticeType}"
            + " AND business_type=#{businessType} AND language=#{language}")
    NoticeTemplate queryNoticeTemplate(@Param("orgId") long orgId,
                                       @Param("noticeType") Integer noticeType,
                                       @Param("businessType") String businessType,
                                       @Param("language") String language);

}
