/**********************************
 *@项目名称: broker-proto
 *@文件名称: io.bhex.broker.server
 *@Date 2018/7/27
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
package io.bhex.broker.server;

import io.bhex.broker.common.api.client.jiean.JieanProperties;
import io.bhex.broker.common.api.client.tencent.TencentProperties;
import io.bhex.broker.common.entity.GrpcClientProperties;
import io.bhex.broker.common.objectstorage.AwsObjectStorageProperties;
import io.bhex.broker.server.domain.ThreadPoolConfig;
import io.bhex.broker.server.grpc.server.service.kyc.tencent.WebankProperties;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "broker")
public class BrokerServerProperties {

    private GrpcClientProperties grpcClient = new GrpcClientProperties();
    //    private String quoteRedisUrl = "";
    private JieanProperties jiean = new JieanProperties();
    private TencentProperties tencent = new TencentProperties();
    private WebankProperties webank = new WebankProperties();
    private AwsObjectStorageProperties aws = new AwsObjectStorageProperties();
    private Boolean isPreview = false;
    private String redisKeyPrefix = "broker-server-";
    private String mqGroupPrefix = "mq-group-prefix-";
    private boolean startStream = true; //是否开启stream，默认=true（true 开启， false 关闭）
    private boolean startTokenFxRateSchedule = true; //是否开启token-fxRate-schedule，默认=true（true 开启， false 关闭）

    private boolean proxy = false;

    private ThreadPoolConfig asyncTaskExecutorPoolConfig = new ThreadPoolConfig();
    private ThreadPoolConfig statisticsTaskExecutorPoolConfig = new ThreadPoolConfig();
    private ThreadPoolConfig orgRequestHandleTaskExecutorPoolConfig = new ThreadPoolConfig();
    private ThreadPoolConfig userRequestHandleTaskExecutorPoolConfig = new ThreadPoolConfig();
    private boolean isTencentKyc = false;
    private String slackAlertWebhookUrl = "";

}
