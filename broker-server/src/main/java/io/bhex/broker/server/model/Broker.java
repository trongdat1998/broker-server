/**********************************
 *@项目名称: broker-server
 *@文件名称: io.bhex.broker.server.model
 *@Date 2018/8/23
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
package io.bhex.broker.server.model;

import com.google.common.base.Strings;
import com.google.gson.reflect.TypeToken;
import io.bhex.broker.common.util.JsonUtil;
import io.bhex.broker.core.domain.GsonIgnore;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import javax.persistence.Id;
import javax.persistence.Table;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "tb_broker")
@Slf4j
@Builder(builderClassName = "Builder", toBuilder = true)
public class Broker {

    @Id
    private Long id;
    private Long orgId;
    private String brokerName;
    private String apiDomain;
    private String backupApiDomain;
    private String huaweiCloudKey;
    private String huaweiCloudDomains;
    private String domainRandomKey;
    @GsonIgnore
    private String privateKey;
    @GsonIgnore
    private String publicKey;
    private Integer keyVersion;
    @GsonIgnore
    private String appRequestSignSalt;
    private String signName;
    private String functions; // {"exchange":true,"otc":true,"pointcard":true,"guild":true,"vol":false,"activity":true,"coupon":true,"option":true,"future":true,"bonus":true,"loan":true}
    private String supportLanguages; // [{}, {}]
    private Integer languageShowType;
    private Integer orgApi; // 券商是否有使用OrgApi的权限
    private Integer loginNeed2fa;
    private Integer status;
    private Long created;
    private Integer superior;
    private String realtimeInterval;
    private String timeZone;
    private Integer filterTopBaseToken;

    @GsonIgnore
    private transient byte[] decryptKey;
    @GsonIgnore
    private transient Map<String, Boolean> functionsMap;
    @GsonIgnore
    private transient List<SupportLanguage> supportLanguageList;

    public void setFunctions(String functions) {
        this.functions = functions;
        if (!Strings.isNullOrEmpty(functions)) {
            try {
                this.functionsMap = JsonUtil.defaultGson().fromJson(functions, new TypeToken<HashMap<String, Boolean>>() {
                }.getType());
            } catch (Exception e) {
                log.warn("set broker function error, {}", functions, e);
            }
        }
    }

    public void setSupportLanguages(String supportLanguages) {
        this.supportLanguages = supportLanguages;
        if (!Strings.isNullOrEmpty(supportLanguages)) {
            this.supportLanguageList = JsonUtil.defaultGson().fromJson(supportLanguages, new TypeToken<List<SupportLanguage>>() {
            }.getType());
        }
    }

}
