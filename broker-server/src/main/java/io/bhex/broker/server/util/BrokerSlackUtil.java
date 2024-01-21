package io.bhex.broker.server.util;

import com.google.api.client.util.Lists;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import io.bhex.broker.common.util.JsonUtil;
import io.bhex.broker.server.BrokerServerProperties;
import io.bhex.broker.server.grpc.server.service.BaseBizConfigService;
import io.bhex.broker.server.model.BaseConfigInfo;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import okhttp3.*;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.List;
import java.util.Map;

/**
 * 使用步骤：
 * 1. AlertKey增加枚举类型
 * 2. 数据库增加配置，Insert INTO `tb_base_config` (`id`, `org_id`, `conf_group`, `conf_key`, `conf_value`, `extra_value`, `status`, `language`, `admin_user_name`, `created`, `updated`, `new_conf_value`, `new_extra_value`, `new_start_time`, `new_end_time`)
 * VALUES (null, 0, 'broker.alert', '#AlertKey.toLowerCase()#', '<@MemberId>可以@指定人员  通知内容，可以用%s作为替换变量', '', 1, '', 'admin@admin.io', unix_timestamp()*1000, unix_timestamp()*1000, '', '', 0, 0);
 */
@Slf4j
@Component
public class BrokerSlackUtil {

    @Resource
    private BrokerServerProperties brokerServerProperties;
    @Resource
    private BaseBizConfigService baseBizConfigService;

    private static final String CONF_GROUP = "broker.alert";


    private String cluster;
    private String namespace;

    @PostConstruct
    public void init() {
        cluster = Strings.nullToEmpty(System.getenv("KUBE_CLUSTER_NAME"));
        namespace = Strings.nullToEmpty(System.getenv("KUBE_NAMESPACE"));
    }

    public void sendAlertMsg(AlertKey alertKey) {
        sendAlertMsg(alertKey, Lists.newArrayList());
    }

    public void sendAlertMsg(AlertKey alertKey, List<String> params) {
        String webhookUrl = brokerServerProperties.getSlackAlertWebhookUrl();
        if (Strings.isNullOrEmpty(webhookUrl)) {
            return;
        }
        BaseConfigInfo baseConfigInfo = baseBizConfigService.getOneConfig(0, CONF_GROUP, alertKey.name().toLowerCase());
        if (baseConfigInfo == null) {
            log.error("no alert config for alertKey:{}", alertKey);
            return;
        }
        try {
            Map<String, String> contentMap = Maps.newHashMap();
            String msg = String.format(baseConfigInfo.getConfValue(), params);
            contentMap.put("text", msg + " from cluster:" + cluster + " namespace:" + namespace);

            OkHttpClient httpClient = new OkHttpClient();
            final Request request = new Request.Builder()
                    .url(webhookUrl)
                    .post(RequestBody.create(MediaType.parse("application/json;charset=utf-8"),
                            JsonUtil.defaultGson().toJson(contentMap)))
                    .build();

            Call call = httpClient.newCall(request);
            @Cleanup
            Response response = call.execute();
            log.info("{} {}", msg, response.code());
        } catch (Exception e) {
            log.info("sendSlackMsg error {}", e);
        }
    }

    public enum AlertKey {
        JOIN_LEADER_NO_SNAPSHOT,
        V2_FUTURES_TRANSFER_ALERT,
        STAKING_REDEEM_TRANSFER_FAILD;
    }
}
