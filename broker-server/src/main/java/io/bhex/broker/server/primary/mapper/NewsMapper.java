package io.bhex.broker.server.primary.mapper;

import io.bhex.broker.server.model.News;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import java.util.List;

@Mapper
public interface NewsMapper extends tk.mybatis.mapper.common.Mapper<News> {

    @Update("update tb_news set status = #{status}, updated = #{updated} where id = #{id} and org_id = #{org_id}")
    int updateStatus(@Param("org_id") Long orgId, @Param("id") Long id, @Param("status") Integer status, @Param("updated") Long updated);

    /**
     * 获得发布时间
     * @param orgId
     * @param newsId
     * @return
     */
    @Select("select published from tb_news where org_id = #{org_id} and news_id = #{news_id} limit 1")
    Long getPublished(@Param("org_id") Long orgId, @Param("news_id")Long newsId);

    /**
     * 往大了搜索已经发布的
     *
     * @param orgId
     * @param language
     * @param published
     * @param limit
     * @return
     */
    @Select("select a.id,a.org_id,a.news_id,a.news_path,a.status,a.created,a.updated,a.published,a.version from tb_news a  where a.org_id = #{org_id} and a.status = 1 and a.published > #{published} and find_in_set(#{language},a.languages)>0 order by a.published, a.news_id limit #{limit};")
    List<News> selectByLanguage(@Param("org_id") Long orgId, @Param("language") String language, @Param("published") Long published, @Param("limit") int limit);

    /**
     * 往小了搜索已经发布的
     *
     * @param orgId
     * @param language
     * @param published
     * @param limit
     * @return
     */
    @Select("select a.id,a.org_id,a.news_id,a.news_path,a.status,a.created,a.updated,a.published,a.version from tb_news a  where a.org_id = #{org_id} and a.status = 1 and a.published < #{published} and find_in_set(#{language},a.languages)>0 order by a.published desc, a.news_id desc limit #{limit};")
    List<News> selectByLanguageReverse(@Param("org_id") Long orgId, @Param("language") String language, @Param("published") Long published, @Param("limit") int limit);
}
