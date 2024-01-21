package io.bhex.broker.server.grpc.server.service;

import com.google.gson.Gson;
import io.bhex.base.DateUtil;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.grpc.common.Header;
import io.bhex.broker.grpc.news.*;
import io.bhex.broker.server.model.News;
import io.bhex.broker.server.model.NewsDetails;
import io.bhex.broker.server.model.NewsTemplate;
import io.bhex.broker.server.primary.mapper.NewsDetailsMapper;
import io.bhex.broker.server.primary.mapper.NewsMapper;
import io.bhex.broker.server.primary.mapper.NewsTemplateMapper;
import io.bhex.broker.server.util.RedisLockUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.ibatis.session.RowBounds;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tk.mybatis.mapper.entity.Example;

import javax.annotation.Resource;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Service
public class NewsService {

    @Resource
    NewsMapper newsMapper;

    @Resource
    NewsDetailsMapper newsDetailsMapper;

    @Resource
    NewsTemplateMapper newsTemplateMapper;

    @Resource
    StringRedisTemplate stringRedisTemplate;

    static String LOCK_TO_GET_NEWS_ID = "get_news_id_lock";

    public QueryNewsResponse queryNews(QueryNewsRequest request) {
        Example example;
        if (request.getDesc()) {
            example = Example.builder(News.class).orderByDesc("published").build();
        } else {
            example = Example.builder(News.class).orderByAsc("published").build();
        }
        Example.Criteria criteria = example.createCriteria();
        //往小了搜索
        if (request.getReverse()) {
            criteria.andLessThan("newsId", request.getLatestId());
        } else {//往大了了搜索
            criteria.andGreaterThan("newsId", request.getLatestId());
        }
        criteria.andEqualTo("orgId", request.getHeader().getOrgId());
        if (request.getStatusList() != null && !request.getStatusList().isEmpty()) {
            criteria.andIn("status", request.getStatusList());
        }
        List<News> newsList;
        if (StringUtils.isNotBlank(request.getHeader().getLanguage())) {
            Long published = newsMapper.getPublished(request.getHeader().getOrgId(), request.getLatestId());
            if (request.getReverse()) {
                if (published == null) {
                    published = Long.MAX_VALUE;
                }
                newsList = newsMapper.selectByLanguageReverse(request.getHeader().getOrgId(), request.getHeader().getLanguage(), published, request.getLimit());
            } else {//往大了了搜索
                if (published == null) {
                    published = 0l;
                }
                newsList = newsMapper.selectByLanguage(request.getHeader().getOrgId(), request.getHeader().getLanguage(), published, request.getLimit());
                Collections.reverse(newsList);
            }
        } else {
            newsList = newsMapper.selectByExampleAndRowBounds(example, new RowBounds(0, request.getLimit()));
        }
        List<Long> newsIds = new LinkedList<>();
        Map<Long, News> newsResult = new HashMap<>();
        newsList.stream().forEach(news -> {
            newsIds.add(news.getNewsId());
            newsResult.put(news.getNewsId(), news);
        });

        List<io.bhex.broker.grpc.news.News> newsList1;
        if (!newsIds.isEmpty()) {
            Example example2 = Example.builder(NewsDetails.class).build();
            criteria = example2.createCriteria().andIn("newsId", newsIds);
            criteria.andEqualTo("orgId", request.getHeader().getOrgId());
            //判断是否需要语言属性
            if (StringUtils.isNotBlank(request.getHeader().getLanguage())) {
                criteria.andEqualTo("language", request.getHeader().getLanguage());
            }
            List<NewsDetails> details = newsDetailsMapper.selectByExample(example2);
            Map<Long, List<NewsDetail>> detailMap = new HashMap<>();
            details.stream().forEach(detail -> {
                NewsDetail newsDetail = NewsDetail.newBuilder()
                        .setId(detail.getId())
                        .setNewsId(detail.getNewsId())
                        .setLanguage(detail.getLanguage())
                        .setOrgId(detail.getOrgId())
                        .setSource(detail.getSource())
                        .setTags(detail.getTags())
                        .setTitle(detail.getTitle())
                        .setContent(detail.getContent())
                        .setImages(detail.getImages())
                        .setSummary(detail.getSummary())
                        .build();
                if (detailMap.containsKey(detail.getNewsId())) {
                    detailMap.get(detail.getNewsId()).add(newsDetail);
                } else {
                    List<NewsDetail> details1 = new LinkedList<>();
                    details1.add(newsDetail);
                    detailMap.put(detail.getNewsId(), details1);
                }
            });
            newsList1 = newsList.stream().map(news -> {
                List<NewsDetail> details1 = detailMap.get(news.getNewsId());
                return io.bhex.broker.grpc.news.News.newBuilder()
                        .setId(news.getId())
                        .setOrgId(news.getOrgId())
                        .setNewsId(news.getNewsId())
                        .setNewsPath(news.getNewsPath())
                        .setStatus(news.getStatus())
                        .setCreated(news.getCreated())
                        .setUpdated(news.getUpdated())
                        .setVersion(news.getVersion())
                        .setPublished(news.getPublished())
                        .addAllDetails(details1 == null ? new LinkedList<>() : details1)
                        .build();
            }).collect(Collectors.toList());
        } else {
            newsList1 = new LinkedList<>();
        }

        QueryNewsResponse response = QueryNewsResponse.newBuilder()
                .addAllNews(newsList1)
                .build();
        return response;
    }

    public GetNewsResponse getNews(GetNewsRequest request) {
        Example example = Example.builder(News.class).build();
        example.createCriteria().andEqualTo("newsId", request.getNewsId());

        News news = newsMapper.selectOneByExample(example);

        if (news == null) {
            return GetNewsResponse.newBuilder().build();
        }

        Example example2 = Example.builder(NewsDetails.class).build();
        Example.Criteria criteria = example2.createCriteria();

        criteria.andEqualTo("newsId", request.getNewsId()).andEqualTo("orgId", request.getHeader().getOrgId());
        //判断是否需要语言属性
        if (StringUtils.isNotBlank(request.getHeader().getLanguage())) {
            criteria.andEqualTo("language", request.getHeader().getLanguage());
        }

        List<NewsDetails> details = newsDetailsMapper.selectByExample(example2);
        List<NewsDetail> detailList = new LinkedList<>();
        details.stream().forEach(detail -> {
            NewsDetail newsDetail = NewsDetail.newBuilder()
                    .setId(detail.getId())
                    .setNewsId(detail.getNewsId())
                    .setLanguage(detail.getLanguage())
                    .setOrgId(detail.getOrgId())
                    .setSource(detail.getSource())
                    .setTags(detail.getTags())
                    .setTitle(detail.getTitle())
                    .setContent(detail.getContent())
                    .setImages(detail.getImages())
                    .setSummary(detail.getSummary())
                    .build();
            detailList.add(newsDetail);
        });
        io.bhex.broker.grpc.news.News newsResult = io.bhex.broker.grpc.news.News.newBuilder()
                .setId(news.getId())
                .setOrgId(news.getOrgId())
                .setNewsId(news.getNewsId())
                .setNewsPath(news.getNewsPath())
                .setStatus(news.getStatus())
                .setCreated(news.getCreated())
                .setUpdated(news.getUpdated())
                .setVersion(news.getVersion())
                .setPublished(news.getPublished())
                .addAllDetails(detailList)
                .build();
        GetNewsResponse response = GetNewsResponse.newBuilder()
                .setNews(newsResult)
                .build();
        return response;
    }


    private Long createNewsId(Long orgId) {

        //搜索当日发布的最近的newsId
        Example example = Example.builder(News.class).orderByDesc("created").build();
        Long startTime = DateUtil.startOfDay(System.currentTimeMillis());
        example.createCriteria().andEqualTo("orgId", orgId).andGreaterThanOrEqualTo("created", startTime);
        List<News> newsList = newsMapper.selectByExampleAndRowBounds(example, new RowBounds(0, 1));
        if (newsList == null || newsList.isEmpty()) {
            return Long.valueOf(String.format("%s%s%04d", orgId, DateUtil.now("yyMMdd"), 1));
        }
        News news = newsList.get(0);
        return news.getNewsId() + 1;

    }

    @Transactional
    public CreateNewsResponse createNews(CreateNewsRequest request) {
        //核对数据
        if (request.getNews() == null) {
            log.warn("createNews news field is null");
            throw new BrokerException(BrokerErrorCode.PARAM_INVALID);
        }
        Header header = request.getHeader();
        Long orgId = header.getOrgId();
        List<NewsDetail> details = request.getNews().getDetailsList();
        int tryCount = 10;
        try {
            while (!RedisLockUtils.tryLock(stringRedisTemplate, LOCK_TO_GET_NEWS_ID + ":" + orgId, 1000)) {
                if (tryCount-- > 0) {
                    continue;
                }
                log.error("create news id timeout， try get redis lock failed, orgId: {}", orgId);
                throw new BrokerException(BrokerErrorCode.SYSTEM_ERROR);
            }

            io.bhex.broker.grpc.news.News news = request.getNews();
            //获取当前机构下的id
            Long newsId = createNewsId(header.getOrgId());

            String languages = "";
            if (details != null) {
                languages = StringUtils.join(details.stream().map(detail -> detail.getLanguage()).collect(Collectors.toSet()), ",");
            }


            Long created = System.currentTimeMillis();
            News createNews = News.builder()
                    .created(created)
                    .updated(created)
                    .status(news.getStatus())
                    .orgId(header.getOrgId())
                    .newsId(newsId)
                    .newsPath(news.getNewsPath())
                    .published(news.getPublished())
                    .languages(languages)
                    .version((int) (created % 10000))
                    .build();
            newsMapper.insert(createNews);
            log.info("create news: {}", new Gson().toJson(createNews));

            if (details != null) {
                details.stream().forEach(detail -> {
                    NewsDetails newsDetails = NewsDetails.builder()
                            .newsId(newsId)
                            .content(detail.getContent())
                            .language(detail.getLanguage())
                            .source(detail.getSource())
                            .tags(detail.getTags())
                            .title(detail.getTitle())
                            .images(detail.getImages())
                            .summary(detail.getSummary() == null ? detail.getContent().substring(0, 1000) : detail.getSummary())
                            .orgId(header.getOrgId())
                            .build();
                    newsDetailsMapper.insert(newsDetails);
                });
            }
            CreateNewsResponse response = CreateNewsResponse.newBuilder().build();
            return response;
        } finally {
            RedisLockUtils.releaseLock(stringRedisTemplate, LOCK_TO_GET_NEWS_ID + ":" + orgId);
        }
    }

    @Transactional
    public ModifyNewsResponse modifyNews(ModifyNewsRequest request) {
        io.bhex.broker.grpc.news.News news = request.getNews();
        News existNews = newsMapper.selectByPrimaryKey(news.getId());
        if (existNews == null) {
            throw new BrokerException(BrokerErrorCode.PARAM_INVALID);
        }
        Long newsId = existNews.getNewsId();
        Header header = request.getHeader();
        Long updated = System.currentTimeMillis();

        String languages = "";
        if(news.getDetailsList() != null) {
            languages = StringUtils.join(news.getDetailsList().stream().map(detail -> detail.getLanguage()).collect(Collectors.toSet()), ",");
        }

        News createNews = News.builder()
                .updated(updated)
                .id(news.getId())
                .version((int) (updated % 10000))
                .published(news.getPublished())
                .newsPath(news.getNewsPath())
                .languages(languages)
                .build();
        newsMapper.updateByPrimaryKeySelective(createNews);

        Example example = Example.builder(NewsDetails.class).build();
        example.createCriteria()
                .andEqualTo("orgId", header.getOrgId())
                .andEqualTo("newsId", newsId);
        //删除所有detail， 重新插入
        newsDetailsMapper.deleteByExample(example);

        List<NewsDetail> details = request.getNews().getDetailsList();
        if (details != null) {
            details.stream().forEach(detail -> {
                NewsDetails newsDetails = NewsDetails.builder()
                        .newsId(newsId)
                        .content(detail.getContent())
                        .language(detail.getLanguage())
                        .source(detail.getSource())
                        .tags(detail.getTags())
                        .title(detail.getTitle())
                        .orgId(header.getOrgId())
                        .images(detail.getImages())
                        .summary(detail.getSummary() == null ? detail.getContent().substring(0, 1000) + "..." : detail.getSummary())
                        .build();
                newsDetailsMapper.insert(newsDetails);
            });
        }
        return ModifyNewsResponse.newBuilder().build();
    }

    public UpdateStatusResponse updateStatus(UpdateStatusRequest request) {
        newsMapper.updateStatus(request.getHeader().getOrgId(), request.getId(), request.getStatus(), System.currentTimeMillis());
        return UpdateStatusResponse.newBuilder().build();
    }

    public GetTemplateResponse getTemplate(GetTemplateRequest request) {
        Example example = Example.builder(NewsTemplate.class).build();
        example.createCriteria()
                .andEqualTo("name", request.getName())
                .andEqualTo("orgId", request.getHeader().getOrgId());
        NewsTemplate newsTemplate = newsTemplateMapper.selectOneByExample(example);
        if (newsTemplate == null) {
            example = Example.builder(NewsTemplate.class).build();
            example.createCriteria()
                    .andEqualTo("name", request.getName())
                    .andEqualTo("orgId", 0l);
            newsTemplate = newsTemplateMapper.selectOneByExample(example);
        }
        if (newsTemplate == null) {
            return null;
        }
        return GetTemplateResponse.newBuilder()
                .setOrgId(newsTemplate.getOrgId())
                .setContent(newsTemplate.getContent())
                .setUpdated(newsTemplate.getUpdated())
                .setParams(newsTemplate.getParams())
                .build();
    }

    public SetTemplateResponse setTemplate(SetTemplateRequest request) {
        Example example = Example.builder(NewsTemplate.class).build();
        example.createCriteria()
                .andEqualTo("orgId", request.getHeader().getOrgId())
                .andEqualTo("name", request.getName());
        NewsTemplate newsTemplate = newsTemplateMapper.selectOneByExample(example);
        String content = request.getContent();
        if (newsTemplate == null) {
            try {
                newsTemplateMapper.insert(NewsTemplate.builder()
                        .name(request.getName())
                        .created(System.currentTimeMillis())
                        .updated(System.currentTimeMillis())
                        .orgId(request.getHeader().getOrgId())
                        .params(request.getParams())
                        .content(content).build());
                return SetTemplateResponse.newBuilder().build();
            } catch (Exception e) {
            }
        }
        newsTemplateMapper.updateByExampleSelective(NewsTemplate.builder()
                .updated(System.currentTimeMillis())
                .content(content)
                .params(request.getParams()).build(), example);
        return SetTemplateResponse.newBuilder().build();
    }


}
