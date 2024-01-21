package io.bhex.broker.server.config;

import com.google.api.client.util.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.annotation.Configuration;

@Slf4j
//@Configuration
public class ElasticsearchConfiguration implements FactoryBean<RestHighLevelClient>, InitializingBean, DisposableBean {

//    @Value("${spring.data.elasticsearch.cluster-rest-nodes}")
    private String clusterNodes;

    private RestHighLevelClient restHighLevelClient;

    @Override
    public void destroy() throws Exception {
        try {
            if (restHighLevelClient != null) {
                restHighLevelClient.close();
            }
        } catch (final Exception e) {
            log.error("Error closing ElasticSearch client: ", e);
        }
    }

    @Override
    public RestHighLevelClient getObject() throws Exception {
        return restHighLevelClient;
    }

    @Override
    public Class<?> getObjectType() {
        return RestHighLevelClient.class;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        restHighLevelClient = buildClient();
    }

    @Override
    public boolean isSingleton() {
        return false;
    }

    private RestHighLevelClient buildClient() {
        try {
            HttpHost host = new HttpHost(clusterNodes.split(":")[0], Integer.parseInt(clusterNodes.split(":")[1]), "http");
            restHighLevelClient = new RestHighLevelClient(RestClient.builder(host));
        } catch (Exception e) {
            log.error(e.getMessage());
        }
        return restHighLevelClient;
    }

}
