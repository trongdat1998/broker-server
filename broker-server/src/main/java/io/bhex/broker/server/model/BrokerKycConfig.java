package io.bhex.broker.server.model;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Id;
import javax.persistence.Table;
import java.util.List;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "tb_broker_kyc_config")
public class BrokerKycConfig {

    @Id
    private Long id;
    private Long orgId;
    private Long countryId;
    private Integer secondKycLevel;
    private String webankAppId;
    private String webankAppSecret;

    public static final Integer DEFAULT_SECOND_KYC_LEVEL = 20;

    /**
     * 支持多包名/license情况
     * JSON格式：[{"packageName": "com.example.app", "license": "iDHHkuy3g6+cEISYroM0BrCljOCRE..."}]
     */
    private String webankAndroidLicense;

    /**
     * 支持多包名/license情况
     * JSON格式：[{"packageName": "com.example.app", "license": "iDHHkuy3g6+cEISYroM0BrCljOCRE..."}]
     */
    private String webankIosLicense;
    private String appName;
    private String companyName;
    private Integer status;
    private Long created;
    private Long updated;

    /**
     * 券商KYC的默认配置
     * 如果数据库中没有这个券商的kyc配置记录，则可以用默认配置代替
     */
    public static BrokerKycConfig newDefaultInstance(Long orgId) {
        return BrokerKycConfig.builder()
                .id(0L)
                .orgId(orgId)
                .secondKycLevel(DEFAULT_SECOND_KYC_LEVEL)
                .status(1)
                .build();
    }

    public String getAndroidLicenseByPackageName(String packageName) {
        return getLicenseByPackageName(webankAndroidLicense, packageName);
    }

    public String getIosLicenseByPackageName(String packageName) {
        return getLicenseByPackageName(webankIosLicense, packageName);
    }

    public List<License> getAndroidLicenses() {
        List<License> licenses = parseLicense(webankAndroidLicense);
        return licenses == null ? Lists.newArrayList() : licenses;
    }

    public List<License> getIosLicenses() {
        List<License> licenses = parseLicense(webankIosLicense);
        return licenses == null ? Lists.newArrayList() : licenses;
    }

    private String getLicenseByPackageName(String licenseText, String packageName) {
        List<License> licenses = parseLicense(licenseText);
        if (licenses == null) {
            // 兼容旧数据：如果解析JSON失败，则默认返回licenseText
            return webankAndroidLicense;
        }

        License license = licenses.stream()
                .filter(l -> l.packageName.equals(packageName))
                .findFirst()
                .orElse(null);
        return license == null ? "" : license.license;
    }

    private static List<License> parseLicense(String licenseText) {
        try {
            Gson gson = new Gson();
            return gson.fromJson(licenseText, new TypeToken<List<License>>() {
            }.getType());
        } catch (Throwable e) {
            return null;
        }
    }

    @Data
    public static class License {
        private String packageName;
        private String license;
    }
}
