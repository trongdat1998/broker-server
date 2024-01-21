/**********************************
 *@项目名称: broker-server
 *@文件名称: io.bhex.broker.server.grpc.server.service
 *@Date 2018/8/23
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
package io.bhex.broker.server.grpc.server.service;

import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.hash.Hashing;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import io.bhex.base.admin.QueryBrokerOrgRequest;
import io.bhex.broker.common.util.JsonUtil;
import io.bhex.broker.grpc.admin.BrokerExtResponse;
import io.bhex.broker.grpc.admin.GetBrokerByBrokerIdRequest;
import io.bhex.broker.grpc.admin.SaveBrokerExtRequest;
import io.bhex.broker.grpc.broker.QueryBrokerResponse;
import io.bhex.broker.grpc.common.BasicRet;
import io.bhex.broker.grpc.common.Header;
import io.bhex.broker.grpc.user.RegisterResponse;
import io.bhex.broker.server.domain.BrokerServerConstants;
import io.bhex.broker.server.domain.CommonIni;
import io.bhex.broker.server.domain.FunctionModule;
import io.bhex.broker.server.domain.RegisterType;
import io.bhex.broker.server.grpc.client.service.GrpcAdminOrgInfoService;
import io.bhex.broker.server.model.*;
import io.bhex.broker.server.primary.mapper.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.DependsOn;
import org.springframework.core.annotation.Order;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import tk.mybatis.mapper.entity.Example;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@Service
public class BrokerService {

    private static final String SPECIAL_USER_EMAIL = "bhop_app@bhex.com";
    private static final String SPECIAL_USER_PASSWORD = Hashing.md5().hashString("Bhoptest123", Charsets.UTF_8).toString();

    @Resource
    private BrokerMapper brokerMapper;

    @Resource
    private SupportLanguageMapper supportLanguageMapper;

    @Resource
    private NoticeTemplateMapper noticeTemplateMapper;

    @Resource
    private NoticeTemplateInitDataMapper noticeInitDataMapper;

    @Resource
    private QuoteTokenInitDataMapper quoteTokenInitDataMapper;

    @Resource
    private QuoteTokenMapper quoteTokenMapper;

    @Resource
    private RegStatisticMapper regStatisticMapper;

    @Resource
    private KycStatisticMapper kycStatisticMapper;

    @Resource
    private UserService userService;

    @Resource
    private CommonIniService commonIniService;

    @Resource
    private UserLevelService userLevelService;

    @Resource
    private RedPacketAdminService redPacketAdminService;
    //@Resource
    //private BrokerExtMapper brokerExtMapper;

    @Resource
    private GrpcAdminOrgInfoService grpcAdminOrgInfoService;

    @Value("${verify-captcha:true}")
    private Boolean verifyCaptcha;

    @Value("${global-notify-type:1}")
    private Integer globalNotifyType;

    private static ImmutableMap<Long, Broker> brokerMap;

    private static final String BROKER_NAME_PLACE_HOLDER = "\\$brokerName\\$";

    private static final String DEFAULT_SUPPORT_LANGUAGE_IDS = "1,2";

    @PostConstruct
    @Scheduled(cron = "0/10 * * * * ?")
    public void loadBrokerCache() {
        List<Broker> brokerList = brokerMapper.selectAll();
        Map<Long, Broker> tmpBrokerMap = Maps.newHashMap();
        for (Broker broker : brokerList) {
            // 状态未启用的，broker_server先不加载
            if (broker.getStatus() != 1) {
                continue;
            }
            tmpBrokerMap.put(broker.getOrgId(), broker);
        }
        brokerMap = ImmutableMap.copyOf(tmpBrokerMap);
    }

    public Boolean createBroker(Broker broker) {
        try {
            initBrokerData(broker.getOrgId(), broker.getBrokerName());
            brokerMapper.insertSelective(broker);
            // set supportLanguage
            initSupportLanguages(broker.getOrgId());
            createWhiteUser(broker.getOrgId());
            return true;
        } catch (Exception e) {
            log.error("createBrokerError {}", broker, e);
            return false;
        }
    }

    public Broker getBrokerById(Long orgId) {
        return brokerMapper.getByOrgIdAndStatus(orgId);
    }

    public Boolean updateBrokerFunctionAndLanguage(Long orgId, String functions, String supportLanguages) {
        if (StringUtils.isNotEmpty(functions)) {
            brokerMapper.updateBrokerFunction(orgId, functions);
            Broker broker = brokerMapper.getByOrgId(orgId);
            Map<String, Boolean> functionMap = broker.getFunctionsMap();
            if (functionMap.getOrDefault("userLevel", false)) {
                userLevelService.addBaseLevelConfig(orgId);
            }
        }
        if (StringUtils.isNotEmpty(supportLanguages)) {
            brokerMapper.updateBrokerLanguage(orgId, supportLanguages);
        }
        try {
            JsonElement functionJson = JsonUtil.defaultJsonParser().parse(functions);
            if (JsonUtil.getBoolean(functionJson, ".red_packet", false)) {
                redPacketAdminService.openRedPacketFunction(orgId);
            }
        } catch (Exception e) {

        }
        return true;
    }

    private void createWhiteUser(Long orgId) {
        try {
            Header header = Header.newBuilder().setOrgId(orgId).setRemoteIp("127.0.0.1").build();
            RegisterResponse response = userService.register(header, "", "", SPECIAL_USER_EMAIL, SPECIAL_USER_PASSWORD, 0L, "", RegisterType.EMAIL, false, false);
            long userId = response.getUser().getUserId();
            {
                CommonIni loginEmailWhiteConfig = CommonIni.builder()
                        .orgId(orgId)
                        .iniName(BrokerServerConstants.LOGIN_WHITE_EMAIL_COMMON_INI_KEY)
                        .iniDesc("login email white list config")
                        .iniValue(SPECIAL_USER_EMAIL)
                        .language("")
                        .build();
                commonIniService.insertOrUpdateCommonIni(loginEmailWhiteConfig);
            }
            {
                CommonIni loginEmailWhiteConfig = CommonIni.builder()
                        .orgId(orgId)
                        .iniName(BrokerServerConstants.LOGIN_WHITE_USER_COMMON_INI_KEY)
                        .iniDesc("login user white list config")
                        .iniValue(Long.toString(userId))
                        .language("")
                        .build();
                commonIniService.insertOrUpdateCommonIni(loginEmailWhiteConfig);
            }
        } catch (Exception e) {
            // ignore
            log.error("create special user error", e);
        }
    }

    /**
     * 初始化券商数据
     * 1.通知消息模板
     * 2.quote token 数据
     *
     * @param brokerId
     * @param brokerName
     */
    public void initBrokerData(Long brokerId, String brokerName) {
        // 1.通知消息模板
        initNoticeTemplate(brokerId, brokerName);
        // 2.quote token 数据
        initQuoteToken(brokerId);
        // 3.init statistics
        initStatistics(brokerId);
    }

    private void initStatistics(Long brokerId) {
        regStatisticMapper.insertSelective(RegStatistic.defaultAggregateInstance(brokerId));
        kycStatisticMapper.insertSelective(KycStatistic.defaultAggregateInstance(brokerId));
    }

    private void initNoticeTemplate(Long brokerId, String brokerName) {
        List<NoticeTemplateInitData> initData = noticeInitDataMapper.selectAll();
        initData.forEach(d -> {
            String templateContent = d.getTemplateContent().replaceAll(BROKER_NAME_PLACE_HOLDER, brokerName);
            NoticeTemplate template = NoticeTemplate.builder()
                    .orgId(brokerId)
                    .noticeType(d.getNoticeType())
                    .businessType(d.getBusinessType())
                    .language(d.getLanguage())
                    .templateId(d.getTemplateId())
                    .templateContent(templateContent)
                    .sendType(d.getSendType())
                    .sign("")
                    .subject("")
                    .created(System.currentTimeMillis())
                    .updated(System.currentTimeMillis())
                    .build();
            noticeTemplateMapper.insertSelective(template);
        });
    }

    private void initQuoteToken(Long brokerId) {
        List<QuoteTokenInitData> initData = quoteTokenInitDataMapper.selectAll();
        initData.forEach(d -> {
            QuoteToken quoteToken = QuoteToken.builder()
                    .orgId(brokerId)
                    .tokenId(d.getTokenId())
                    .tokenName(d.getTokenName())
                    .tokenIcon(d.getTokenIcon())
                    .customOrder(d.getCustomOrder())
                    .status(1)
                    .updated(System.currentTimeMillis())
                    .created(System.currentTimeMillis())
                    .category(1)
                    .build();
            quoteTokenMapper.insertSelective(quoteToken);
        });
    }

    public void initSupportLanguages(Long orgId) {
        try {
            resetSupportLanguages(orgId, DEFAULT_SUPPORT_LANGUAGE_IDS);
        } catch (Exception e) {
            // ignore
        }
    }

    public Boolean updateBroker(Broker broker) {
        int result = brokerMapper.updateByPrimaryKeySelective(broker);
        return result == 1;
    }

    public List<Broker> queryBrokerAll() {
        return brokerMapper.selectAll();
    }

    public List<Broker> queryBrokerList(List<Long> brokerIdList) {
        Example example = Example.builder(Broker.class).build();
        example.createCriteria()
                .andIn("orgId", brokerIdList);
        return brokerMapper.selectByExample(example);
    }

    public Broker getBrokerByOrgId(long orgId) {
        return brokerMapper.getByOrgId(orgId);
    }

    public QueryBrokerResponse queryBrokers() {
        List<Broker> brokerList = brokerMapper.selectAll();
        return QueryBrokerResponse.newBuilder()
                .addAllBrokers(brokerList.stream()
                        .filter(broker -> !Strings.isNullOrEmpty(broker.getApiDomain())
                                && broker.getStatus() == 1)
                        .map(this::getBroker).collect(Collectors.toList()))
                .build();
    }

    private io.bhex.broker.grpc.broker.Broker getBroker(Broker broker) {
        List<io.bhex.broker.grpc.broker.Broker.SupportLanguage> supportLanguageList = Lists.newArrayList();
        if (broker.getSupportLanguageList() != null) {
            supportLanguageList = broker.getSupportLanguageList().stream()
                    .map(supportLanguage -> io.bhex.broker.grpc.broker.Broker.SupportLanguage.newBuilder()
                            .setShowName(supportLanguage.getShowName())
                            .setLanguage(supportLanguage.getLanguage())
                            .setIcon(Strings.nullToEmpty(supportLanguage.getIcon()))
                            .setJsLoadUrl(Strings.nullToEmpty(supportLanguage.getJsLoadUrl()))
                            .setCurrency(supportLanguage.getCurrency())
                            .build())
                    .collect(Collectors.toList());
        }
        QueryBrokerOrgRequest orgRequest = QueryBrokerOrgRequest.newBuilder()
                .setBrokerId(broker.getOrgId())
                .setBrokerName(broker.getBrokerName())
                .build();
        int registerOption = grpcAdminOrgInfoService.queryBrokerOrg(orgRequest).getRegisterOption();
        if (verifyCaptcha && globalNotifyType == 2 || verifyCaptcha && globalNotifyType == 3) {
            //当全局设置仅手机或仅邮箱时，卷商优先使用全局配置
            registerOption = globalNotifyType;
        }
        return io.bhex.broker.grpc.broker.Broker.newBuilder()
                .setId(broker.getId())
                .setOrgId(broker.getOrgId())
                .setBrokerName(broker.getBrokerName())
                .addAllApiDomains(Strings.isNullOrEmpty(broker.getApiDomain()) ? Lists.newArrayList() : Lists.newArrayList(broker.getApiDomain().split(",")))
                .addAllBackupApiDomains(Strings.isNullOrEmpty(broker.getBackupApiDomain()) ? Lists.newArrayList() : Lists.newArrayList(broker.getBackupApiDomain().split(",")))
                .setHuaweiCloudKey(broker.getHuaweiCloudKey())
                .addAllHuaweiCloudDomains(Strings.isNullOrEmpty(broker.getHuaweiCloudDomains()) ? Lists.newArrayList() : Lists.newArrayList(broker.getHuaweiCloudDomains().split(",")))
                .setRandomKey(broker.getDomainRandomKey())
                .setSignName(broker.getSignName())
                .setAppRequestSignSalt(broker.getAppRequestSignSalt())
                .setStatus(broker.getStatus())
                .putAllFunctionsConfig(broker.getFunctionsMap() == null ? Maps.newHashMap() : broker.getFunctionsMap())
                .addAllSupportLanguages(supportLanguageList)
                .setShowType(io.bhex.broker.grpc.broker.Broker.LanguageShowType.forNumber(broker.getLanguageShowType()))
                .setCanCreateOrgApi(broker.getOrgApi() == 1)
                .setLoginForceNeed2Fa(broker.getLoginNeed2fa() == 1)
                .setRegisterOption(registerOption)
                .setRealtimeInterval(broker.getRealtimeInterval())
                .setFilterTopBaseToken(broker.getFilterTopBaseToken() != null && broker.getFilterTopBaseToken() == 1)
                .build();
    }

    public static boolean checkModule(Header header, FunctionModule module) {
        return checkModule(header.getOrgId(), module);
    }

    public static boolean checkModule(Long orgId, FunctionModule module) {
        Broker broker = brokerMap.get(orgId);
        if (broker != null && broker.getFunctionsMap().getOrDefault(module.moduleName(), Boolean.FALSE)) {
            return true;
        }
        return false;
    }

    public static boolean checkHasOrgApi(Long orgId) {
        Broker broker = brokerMap.get(orgId);
        return broker.getOrgApi() == 1;
    }

    public String updateFunctionConfig(Long orgId, FunctionModule functionModule, Boolean value) {
        Broker broker = brokerMapper.getByOrgId(orgId);
        JsonObject functionJson = JsonUtil.defaultJsonParser().parse(broker.getFunctions()).getAsJsonObject();
        if (functionJson.has(functionModule.moduleName())) {
            functionJson.addProperty(functionModule.moduleName(), value);
            Broker updateObj = Broker.builder().id(broker.getId()).functions(JsonUtil.defaultGson().toJson(functionJson)).build();
            brokerMapper.updateByPrimaryKeySelective(updateObj);
            return updateObj.getFunctions();
        } else {
            throw new IllegalArgumentException("functionModule param is wrong, please check");
        }
    }

    public String updateSupportLanguageConfig(Long orgId, SupportLanguage supportLanguage) {
        Broker broker = brokerMapper.getByOrgId(orgId);
        List<SupportLanguage> supportLanguageList = JsonUtil.defaultGson().fromJson(broker.getSupportLanguages(), new TypeToken<List<SupportLanguage>>() {
        }.getType());
        if (supportLanguageList == null || supportLanguageList.isEmpty()) {
            supportLanguageList = Lists.newArrayList(supportLanguage);
        } else {
            for (int i = 0; i < supportLanguageList.size(); i++) {
                if (supportLanguageList.get(i).getLanguage().equals(supportLanguage.getLanguage())) {
                    supportLanguageList.add(i, supportLanguage);
                }
            }
        }
        Broker updateObj = Broker.builder().id(broker.getId()).supportLanguages(JsonUtil.defaultGson().toJson(supportLanguageList)).build();
        brokerMapper.updateByPrimaryKeySelective(updateObj);
        return updateObj.getSupportLanguages();
    }

    public String resetSupportLanguages(Long orgId, String supportLanguageIds) {
//        Example example = new Example(SupportLanguage.class);
//        Example.Criteria criteria = example.createCriteria();
//        criteria.andIn("id", Arrays.stream(supportLanguageIds.split(",")).map(Long::valueOf).collect(Collectors.toList()));
//        List<SupportLanguage> supportLanguageList = supportLanguageMapper.selectByExample(example);
        List<SupportLanguage> supportLanguageList = Lists.newArrayList();
        for (String idStr : supportLanguageIds.split(",")) {
            SupportLanguage supportLanguage = supportLanguageMapper.selectByPrimaryKey(Long.valueOf(idStr));
            if (supportLanguage != null) {
                supportLanguageList.add(supportLanguage);
            }
        }
        return resetSupportLanguages(orgId, supportLanguageList);
    }

    public String resetSupportLanguages(Long orgId, List<SupportLanguage> supportLanguageList) {
        Broker broker = brokerMapper.getByOrgId(orgId);
        String supportLanguages = JsonUtil.defaultGson().toJson(supportLanguageList);
        if (!Strings.isNullOrEmpty(supportLanguages)) {
            Broker updateObj = Broker.builder().id(broker.getId()).supportLanguages(supportLanguages).build();
            brokerMapper.updateByPrimaryKeySelective(updateObj);
            return updateObj.getSupportLanguages();
        }
        return "{}";
    }

    @Deprecated
    public BasicRet saveBrokerExt(SaveBrokerExtRequest request) {
        throw new UnsupportedOperationException("Unsupported this command");

/*        long brokerId=request.getBrokerId();
        if(brokerId==0){
            throw new BrokerException(BrokerErrorCode.PARAM_INVALID);
        }

        long now=System.currentTimeMillis();
        BrokerExt brokerExt= brokerExtMapper.selectByPrimaryKey(brokerId);
        int row=0;
        if(Objects.isNull(brokerExt)){
            brokerExt=BrokerExt.builder()
                    .brokerId(brokerId)
                    .phone(request.getPhone())
                    .createAt(now)
                    .updateAt(now)
                    .build();

            row= brokerExtMapper.insert(brokerExt);
        }else{
            brokerExt=BrokerExt.builder()
                    .brokerId(brokerId)
                    .phone(request.getPhone())
                    .updateAt(now)
                    .build();
            row= brokerExtMapper.updateByPrimaryKeySelective(brokerExt);
        }
        BrokerErrorCode code=BrokerErrorCode.DB_ERROR;
        if(row==1){
            code=BrokerErrorCode.SUCCESS;
        }
        return BasicRet.newBuilder().setCode(code.code()).build();*/
    }

    @Deprecated
    public BrokerExtResponse getBrokerExt(GetBrokerByBrokerIdRequest request) {

        throw new UnsupportedOperationException("Unsupported this command");

/*        long brokerId=request.getBrokerId();
        if(brokerId==0){
            throw new BrokerException(BrokerErrorCode.PARAM_INVALID);
        }

        Broker broker=brokerMapper.getByOrgId(brokerId);
        if(Objects.isNull(broker)){
            throw new BrokerException(BrokerErrorCode.DB_RECORD_NOT_EXISTS);
        }

        BrokerExt brokerInfo= brokerExtMapper.selectByPrimaryKey(brokerId);
        if(Objects.isNull(brokerInfo)){
            return BrokerExtResponse.newBuilder().setRet(BasicRet.newBuilder().setCode(0).build())
                    .setBrokerName(broker.getBrokerName())
                    .build();
            //throw new BrokerException(BrokerErrorCode.DB_RECORD_NOT_EXISTS);
        }

        return BrokerExtResponse.newBuilder()
                .setRet(BasicRet.newBuilder().setCode(0).build())
                .setBrokerId(brokerId)
                .setPhone(brokerInfo.getPhone())
                .setBrokerName(broker.getBrokerName())
                .build();*/
    }
}
