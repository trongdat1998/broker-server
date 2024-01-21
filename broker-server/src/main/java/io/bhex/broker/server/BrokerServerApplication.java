/**********************************
 *@项目名称: broker-proto
 *@文件名称: io.bhex.broker.server
 *@Date 2018/7/25
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
package io.bhex.broker.server;

import com.github.pagehelper.PageHelper;
import com.google.common.base.Throwables;
import io.bhex.base.grpc.metrics.TomcatConnectPoolMetricsSupport;
import io.bhex.base.idgen.api.ISequenceGenerator;
import io.bhex.base.idgen.enums.DataCenter;
import io.bhex.base.idgen.snowflake.SnowflakeGenerator;
import io.bhex.base.metrics.PrometheusMetricsCollector;
import io.bhex.broker.common.api.client.jiean.JieanApi;
import io.bhex.broker.common.api.client.okhttp.OkHttpPrometheusInterceptor;
import io.bhex.broker.common.api.client.tencent.TencentApi;
import io.bhex.broker.common.grpc.client.aspect.GrpcLogAspect;
import io.bhex.broker.common.grpc.client.aspect.PrometheusMetricsAspect;
import io.bhex.broker.common.objectstorage.AwsObjectStorage;
import io.bhex.broker.common.objectstorage.ObjectStorage;
import io.bhex.broker.common.redis.GsonValueSerializer;
import io.bhex.broker.common.redis.LongKeySerializer;
import io.bhex.broker.common.redis.LongValueSerializer;
import io.bhex.broker.common.redis.StringKeySerializer;
import io.bhex.broker.server.domain.ThreadPoolConfig;
import io.bhex.broker.server.grpc.client.config.GrpcClientConfig;
import io.bhex.broker.server.push.bo.AppPushTaskCache;
import io.lettuce.core.ReadFrom;
import io.prometheus.client.hotspot.DefaultExports;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.data.redis.LettuceClientConfigurationBuilderCustomizer;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.boot.web.embedded.tomcat.TomcatServletWebServerFactory;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;
import org.springframework.boot.web.servlet.server.ConfigurableServletWebServerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.core.task.TaskExecutor;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.web.client.RestTemplate;
import tk.mybatis.mapper.autoconfigure.MapperAutoConfiguration;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ThreadPoolExecutor;

@SpringBootApplication(scanBasePackages = {"io.bhex"}, exclude = {DataSourceAutoConfiguration.class, MapperAutoConfiguration.class})
@EnableScheduling
//为了满足同一类方法间调用，并保证注解仍然有效
@EnableAspectJAutoProxy(proxyTargetClass = true, exposeProxy = true)
@Slf4j
public class BrokerServerApplication {

    static {
        // set max netty directMemory to 1G
        System.setProperty("es.set.netty.runtime.available.processors", "false");
//        System.setProperty("io.netty.maxDirectMemory", String.valueOf(1024 * 1024 * 1024L));
        DefaultExports.initialize();
    }

    @Bean
    public BrokerServerInitializer brokerServerInitializer() {
        return new BrokerServerInitializer();
    }

    @Bean
    public BrokerServerProperties brokerServerProperties() {
        return new BrokerServerProperties();
    }

    @Bean
    @Qualifier
    public GrpcClientConfig grpcClientConfig(BrokerServerProperties brokerServerProperties) {
        return new GrpcClientConfig();
    }

    @Bean
    public GrpcLogAspect grpcLogAspect() {
        return new GrpcLogAspect();
    }

    @Bean
    public PrometheusMetricsAspect prometheusMetricsAspect() {
        return new PrometheusMetricsAspect();
    }

    @Bean
    public LettuceClientConfigurationBuilderCustomizer lettuceCustomizer() {
//        ClusterTopologyRefreshOptions topologyRefreshOptions = ClusterTopologyRefreshOptions.builder()
////                .enablePeriodicRefresh()
////                .enablePeriodicRefresh(Duration.ofSeconds(5))
//                .enableAllAdaptiveRefreshTriggers()
//                .adaptiveRefreshTriggersTimeout(Duration.ofSeconds(5))
//                .build();
//
//        ClusterClientOptions clientOptions = ClusterClientOptions.builder()
//                .topologyRefreshOptions(topologyRefreshOptions)
//                .build();
//        return clientConfigurationBuilder -> clientConfigurationBuilder.clientOptions(clientOptions);
        return clientConfigurationBuilder -> clientConfigurationBuilder.readFrom(ReadFrom.REPLICA_PREFERRED);
    }

    @Bean
    public ISequenceGenerator sequenceGenerator(StringRedisTemplate redisTemplate) {
        long workId;
        try {
            workId = redisTemplate.opsForValue().increment("idGenerator-wordId") % 512;
        } catch (Exception e) {
            workId = RandomUtils.nextLong(0, 512);
            log.error("getIdGeneratorWorkId from redis occurred exception. set a random workId:{}", workId);
        }
        log.info("use workId:{} for IdGenerator", workId);
        return SnowflakeGenerator.newInstance(5, workId);
    }

    @Bean
    public RestTemplate restTemplate() {
        return new RestTemplateBuilder(restTemplate -> restTemplate.getInterceptors().add((request, body, execution) -> {
            String addr = request.getURI().getHost() + ":" + request.getURI().getPort();
            String path = request.getURI().getPath();
//            OkHttpPrometheusInterceptor.STARTED_COUNTER.labels(addr, path).inc();
            final long beginTime = System.currentTimeMillis();
            Throwable throwable = null;
            Integer status = null;
            try {
                ClientHttpResponse response = execution.execute(request, body);
                status = response.getStatusCode().value();
                return response;
            } catch (Exception e) {
                throwable = e;
                Throwables.throwIfUnchecked(e);
                Throwables.throwIfInstanceOf(e, IOException.class);
                throw e;
            } finally {
                String statusDesc = status == null ? "none" : String.valueOf(status);
//                OkHttpPrometheusInterceptor.COMPLETED_SUMMARY.labels(addr, path,
//                        statusDesc,
//                        throwable == null ? "none" : throwable.getClass().getName())
//                        .observe((System.currentTimeMillis() - beginTime) / 1.000D);
                OkHttpPrometheusInterceptor.HISTOGRAM_LATENCY_MILLISECONDS.labels(addr, path, statusDesc)
                        .observe((System.currentTimeMillis() - beginTime) / 1.000D);
            }
        })).build();
    }

    @Bean
    public TaskScheduler taskScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(10);
        scheduler.setThreadNamePrefix("TaskScheduler-");
        scheduler.setAwaitTerminationSeconds(10);
        scheduler.setWaitForTasksToCompleteOnShutdown(true);
        return scheduler;
    }

    @Bean(name = "asyncTaskExecutor")
    public TaskExecutor asyncTaskExecutor(BrokerServerProperties brokerServerProperties) {
        ThreadPoolConfig threadPoolConfig = brokerServerProperties.getAsyncTaskExecutorPoolConfig();
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(threadPoolConfig.getCorePoolSize());
        executor.setMaxPoolSize(threadPoolConfig.getMaxPoolSize());
        executor.setQueueCapacity(threadPoolConfig.getQueueCapacity());
        executor.setThreadNamePrefix("asyncTaskExecutor-");
        executor.setAwaitTerminationSeconds(8);
        executor.setWaitForTasksToCompleteOnShutdown(true);
        return executor;
    }

    @Bean(name = "statisticsTaskExecutor")
    public TaskExecutor statisticsTaskExecutor(BrokerServerProperties brokerServerProperties) {
        ThreadPoolConfig threadPoolConfig = brokerServerProperties.getStatisticsTaskExecutorPoolConfig();
        ThreadPoolTaskExecutor statisticsExecutor = new ThreadPoolTaskExecutor();
        statisticsExecutor.setCorePoolSize(threadPoolConfig.getCorePoolSize());
        statisticsExecutor.setMaxPoolSize(threadPoolConfig.getMaxPoolSize());
        statisticsExecutor.setQueueCapacity(threadPoolConfig.getQueueCapacity());
        statisticsExecutor.setRejectedExecutionHandler(new ThreadPoolExecutor.AbortPolicy());
        statisticsExecutor.setThreadNamePrefix("statisticsThread-");
        statisticsExecutor.setAwaitTerminationSeconds(8);
        statisticsExecutor.setWaitForTasksToCompleteOnShutdown(false);
        return statisticsExecutor;
    }

    @Bean(name = "orgRequestHandleTaskExecutor")
    public TaskExecutor orgRequestHandleTaskExecutor(BrokerServerProperties brokerServerProperties) {
        ThreadPoolConfig threadPoolConfig = brokerServerProperties.getOrgRequestHandleTaskExecutorPoolConfig();
        ThreadPoolTaskExecutor orgRequestHandleExecutor = new ThreadPoolTaskExecutor();
        orgRequestHandleExecutor.setCorePoolSize(threadPoolConfig.getCorePoolSize());
        orgRequestHandleExecutor.setMaxPoolSize(threadPoolConfig.getMaxPoolSize());
        orgRequestHandleExecutor.setQueueCapacity(threadPoolConfig.getQueueCapacity());
        orgRequestHandleExecutor.setRejectedExecutionHandler(new ThreadPoolExecutor.AbortPolicy());
        orgRequestHandleExecutor.setThreadNamePrefix("orgRequestHandleThread-");
        orgRequestHandleExecutor.setAwaitTerminationSeconds(8);
        orgRequestHandleExecutor.setWaitForTasksToCompleteOnShutdown(false);
        return orgRequestHandleExecutor;
    }

    @Bean(name = "userRequestHandleTaskExecutor")
    public TaskExecutor userRequestHandleTaskExecutor(BrokerServerProperties brokerServerProperties) {
        ThreadPoolConfig threadPoolConfig = brokerServerProperties.getUserRequestHandleTaskExecutorPoolConfig();
        ThreadPoolTaskExecutor userRequestHandleExecutor = new ThreadPoolTaskExecutor();
        userRequestHandleExecutor.setCorePoolSize(threadPoolConfig.getCorePoolSize());
        userRequestHandleExecutor.setMaxPoolSize(threadPoolConfig.getMaxPoolSize());
        userRequestHandleExecutor.setQueueCapacity(threadPoolConfig.getQueueCapacity());
        userRequestHandleExecutor.setRejectedExecutionHandler(new ThreadPoolExecutor.AbortPolicy());
        userRequestHandleExecutor.setThreadNamePrefix("userRequestHandleThread-");
        userRequestHandleExecutor.setAwaitTerminationSeconds(8);
        userRequestHandleExecutor.setWaitForTasksToCompleteOnShutdown(true);
        return userRequestHandleExecutor;
    }

    @Bean(name = "mqStreamExecutor")
    public ThreadPoolTaskExecutor mqStreamExecutor() {
        ThreadPoolTaskExecutor streamWorker = new ThreadPoolTaskExecutor();
        streamWorker.setCorePoolSize(5);
        streamWorker.setMaxPoolSize(5);
        streamWorker.setQueueCapacity(500);
        streamWorker.setRejectedExecutionHandler((r, executor) -> log.warn("MqStreamWorker too many tasks, please check the reason!"));
        streamWorker.setThreadNamePrefix("mqStreamWorker_");
        streamWorker.setAllowCoreThreadTimeOut(true);
        streamWorker.setKeepAliveSeconds(60);
        streamWorker.setWaitForTasksToCompleteOnShutdown(true);
        return streamWorker;
    }

    @Bean(name = "kycRequestHandleTaskExecutor")
    public TaskExecutor kycRequestHandleTaskExecutor() {
        ThreadPoolTaskExecutor kycRequestHandleExecutor = new ThreadPoolTaskExecutor();
        kycRequestHandleExecutor.setCorePoolSize(16);
        kycRequestHandleExecutor.setMaxPoolSize(32);
        kycRequestHandleExecutor.setQueueCapacity(128);
        kycRequestHandleExecutor.setRejectedExecutionHandler(new ThreadPoolExecutor.AbortPolicy());
        kycRequestHandleExecutor.setThreadNamePrefix("kycRequestHandleThread-");
        kycRequestHandleExecutor.setAwaitTerminationSeconds(8);
        kycRequestHandleExecutor.setWaitForTasksToCompleteOnShutdown(true);
        return kycRequestHandleExecutor;
    }

    @Bean(name = "airDropRequestHandleTaskExecutor")
    public TaskExecutor airDropRequestHandleTaskExecutor() {
        ThreadPoolTaskExecutor kycRequestHandleExecutor = new ThreadPoolTaskExecutor();
        kycRequestHandleExecutor.setCorePoolSize(16);
        kycRequestHandleExecutor.setMaxPoolSize(32);
        kycRequestHandleExecutor.setQueueCapacity(128);
        kycRequestHandleExecutor.setRejectedExecutionHandler(new ThreadPoolExecutor.AbortPolicy());
        kycRequestHandleExecutor.setThreadNamePrefix("AirDropRequestHandle-");
        kycRequestHandleExecutor.setAwaitTerminationSeconds(8);
        kycRequestHandleExecutor.setWaitForTasksToCompleteOnShutdown(true);
        return kycRequestHandleExecutor;
    }

    @Bean
    public StringRedisTemplate stringRedisTemplate(BrokerServerProperties brokerServerProperties, RedisConnectionFactory redisConnectionFactory) {
        StringRedisTemplate template = new StringRedisTemplate();
        template.setKeySerializer(new StringKeySerializer(brokerServerProperties.getRedisKeyPrefix()));
        template.setHashKeySerializer(new StringKeySerializer(brokerServerProperties.getRedisKeyPrefix()));
        template.setConnectionFactory(redisConnectionFactory);
        return template;
    }

    @Bean("pushTaskRedisTemplate")
    public RedisTemplate<Long, AppPushTaskCache> pushTaskRedisTemplate(BrokerServerProperties brokerServerProperties, RedisConnectionFactory connectionFactory) {
        RedisTemplate<Long, AppPushTaskCache> redisTemplate = new RedisTemplate<>();
        redisTemplate.setKeySerializer(new LongKeySerializer(brokerServerProperties.getRedisKeyPrefix() + "PUSH_TASK:"));
        redisTemplate.setValueSerializer(new GsonValueSerializer<>(AppPushTaskCache.class));
        redisTemplate.setConnectionFactory(connectionFactory);
        return redisTemplate;
    }

    @Bean("pushTaskLimitRedisTemplate")
    public RedisTemplate<String, Long> pushTaskLimitRedisTemplate(BrokerServerProperties brokerServerProperties, RedisConnectionFactory connectionFactory) {
        RedisTemplate<String, Long> redisTemplate = new RedisTemplate<>();
        redisTemplate.setKeySerializer(new StringKeySerializer(brokerServerProperties.getRedisKeyPrefix() + "PUSH_TASK_LIMIT:"));
        redisTemplate.setValueSerializer(new LongValueSerializer());
        redisTemplate.setConnectionFactory(connectionFactory);
        return redisTemplate;
    }

    @Bean("pushBusinessLimitRedisTemplate")
    public RedisTemplate<String, Long> pushBusinessLimitRedisTemplate(BrokerServerProperties brokerServerProperties, RedisConnectionFactory connectionFactory) {
        RedisTemplate<String, Long> redisTemplate = new RedisTemplate<>();
        redisTemplate.setKeySerializer(new StringKeySerializer(brokerServerProperties.getRedisKeyPrefix() + "PUSH_BUSINESS_LIMIT:"));
        redisTemplate.setValueSerializer(new LongValueSerializer());
        redisTemplate.setConnectionFactory(connectionFactory);
        return redisTemplate;
    }

    @Bean
    public JieanApi jieanApi(BrokerServerProperties brokerServerProperties) {
        return new JieanApi(brokerServerProperties.getJiean());
    }

    @Bean
    public TencentApi tencentApi(BrokerServerProperties brokerServerProperties) {
        return new TencentApi(brokerServerProperties.getTencent());
    }

    @Bean
    public ObjectStorage awsObjectStorage(BrokerServerProperties brokerServerProperties) {
        return AwsObjectStorage.buildFromProperties(brokerServerProperties.getAws());
    }

    @Bean
    public PageHelper pageHelper() {
        PageHelper pageHelper = new PageHelper();
        Properties p = new Properties();
        p.setProperty("offsetAsPageNum", "true");
        p.setProperty("rowBoundsWithCount", "true");
        p.setProperty("reasonable", "true");
        //通过设置pageSize=0或者RowBounds.limit = 0就会查询出全部的结果。
        p.setProperty("pageSizeZero", "true");
        pageHelper.setProperties(p);
        return pageHelper;
    }

    @Bean
    public WebServerFactoryCustomizer<ConfigurableServletWebServerFactory> tomcatPoolMetricsSupport(TomcatConnectPoolMetricsSupport tomcatConnectPoolMetricsSupport) {
        return factory -> {
            if (factory instanceof TomcatServletWebServerFactory) {
                log.info("Customizer for add Tomcat connect pool metrics support");
                ((TomcatServletWebServerFactory) factory).addConnectorCustomizers(tomcatConnectPoolMetricsSupport);
            } else {
                log.warn("NOT Tomcat, so do not add connect pool metrics support");
            }
        };
    }

    /**
     * do not use ContextRefreshEvent, because tomcat init executor after that event
     *
     * @param applicationContext
     * @return
     */
    @Bean
    public ApplicationListener<ApplicationReadyEvent> registerPoolMetrics(ApplicationContext applicationContext) {
        return event -> {
            log.info("[INFO] on ApplicationReadyEvent, add all pool to metrics ");
            Map<String, ThreadPoolTaskExecutor> taskExecutorMap = applicationContext.getBeansOfType(ThreadPoolTaskExecutor.class);
            for (String name : taskExecutorMap.keySet()) {
                ThreadPoolTaskExecutor executor = taskExecutorMap.get(name);
                log.info("register to metrics ThreadPoolTaskExecutor: {} = {}", name, executor);
                PrometheusMetricsCollector.registerThreadPoolExecutor(executor.getThreadPoolExecutor(), name);
            }

            Map<String, TomcatConnectPoolMetricsSupport> tomcatMetricsSupports = applicationContext.getBeansOfType(TomcatConnectPoolMetricsSupport.class);
            for (TomcatConnectPoolMetricsSupport support : tomcatMetricsSupports.values()) {
                log.info("addTomcatConnectPoolMetrics for: {} ", support);
                support.addTomcatConnectPoolMetrics();
            }
        };
    }

    public static void main(String[] args) {
        SpringApplication.run(BrokerServerApplication.class, args);
    }

}
