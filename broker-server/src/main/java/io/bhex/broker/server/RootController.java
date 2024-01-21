/**********************************
 *@项目名称: server
 *@文件名称: io.bhex.broker.server
 *@Date 2018/8/10
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
package io.bhex.broker.server;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.common.util.JsonUtil;
import io.bhex.broker.core.domain.BaseResult;
import io.bhex.broker.grpc.account.QueryBalanceFlowResponse;
import io.bhex.broker.grpc.common.AccountTypeEnum;
import io.bhex.broker.grpc.common.AdminSimplyReply;
import io.bhex.broker.grpc.common.Header;
import io.bhex.broker.grpc.common.Platform;
import io.bhex.broker.grpc.deposit.QueryUserDepositAddressResponse;
import io.bhex.broker.grpc.invite.TestInviteFeeBackResponse;
import io.bhex.broker.grpc.user.*;
import io.bhex.broker.server.domain.BrokerLockKeys;
import io.bhex.broker.server.domain.CommonIni;
import io.bhex.broker.server.domain.FunctionModule;
import io.bhex.broker.server.elasticsearch.entity.BalanceFlow;
import io.bhex.broker.server.elasticsearch.entity.TradeDetail;
import io.bhex.broker.server.elasticsearch.service.IBalanceFlowHistoryService;
import io.bhex.broker.server.elasticsearch.service.ITradeDetailHistoryService;
import io.bhex.broker.server.grpc.server.service.*;
import io.bhex.broker.server.grpc.server.service.staking.StakingProductCurrentStrategy;
import io.bhex.broker.server.grpc.server.service.staking.StakingSyncDataService;
import io.bhex.broker.server.grpc.server.service.staking.StakingTransferEvent;
import io.bhex.broker.server.model.*;
import io.bhex.broker.server.util.RedisLockUtils;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.common.TextFormat;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.data.domain.Page;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Controller
@Slf4j
public class RootController {

    @Resource
    private BasicService basicService;

    @Resource
    private AdminTokenService adminTokenService;

    @Resource
    private WithdrawService withdrawService;

    @Resource
    private InviteTaskService inviteTaskService;

    @Resource
    private InviteService inviteService;

    @Resource
    private FinanceProductService financeProductService;

    @Resource
    private FinanceInterestService financeInterestService;

    @Resource
    private FinanceTaskService financeTaskService;

    @Resource
    private FinanceWalletService financeWalletService;

    @Resource
    private UserSecurityService userSecurityService;

    @Resource
    private BrokerService brokerService;

    @Resource(name = "stringRedisTemplate")
    private StringRedisTemplate redisTemplate;

    @Resource
    private OAuthService oAuthService;

    @Resource
    private AppConfigService appConfigService;

    @Resource
    private DepositService depositService;

    @Resource
    private OrgStatisticsService orgStatisticsService;

    @Resource
    private AccountService accountService;

    @Resource
    private UserService userService;

    @Resource
    private CommonIniService commonIniService;

    @Resource
    private SubBusinessSubjectService subBusinessSubjectService;

    @Resource
    private ITradeDetailHistoryService tradeDetailHistoryService;

    @Resource
    private IBalanceFlowHistoryService balanceFlowHistoryService;

    @Resource
    private OtcService otcService;

    @Resource
    private BrokerFeeService brokerFeeService;

    @Resource
    private RedPacketAdminService redPacketAdminService;

    @Resource
    private StakingSyncDataService stakingSyncDataService;

    @Resource
    private StakingProductCurrentStrategy stakingProductCurrentStrategy;

    @Resource
    private OdsService odsService;

    @ResponseBody
    @RequestMapping(value = "/internal/rename_token_name")
    public String renameTokenName(@RequestParam("org_id") Long orgId,
                                  @RequestParam("token_id") String tokenId,
                                  @RequestParam("new_token_name") String newTokenName) {
        try {
            AdminSimplyReply reply = adminTokenService.editTokenName(orgId, tokenId, newTokenName, false);
            return reply.getResult() + "-" + reply.getMessage();
        } catch (Exception e) {
            return "Error:" + Throwables.getStackTraceAsString(e);
        }
    }

    @RequestMapping(value = "internal/metrics", produces = TextFormat.CONTENT_TYPE_004)
    @ResponseBody
    public String metrics(@RequestParam(name = "name[]", required = false) String[] names) throws IOException {
        Set<String> includedNameSet = names == null ? Collections.emptySet() : Sets.newHashSet(names);
        Writer writer = new StringWriter();
        TextFormat.write004(writer, CollectorRegistry.defaultRegistry.filteredMetricFamilySamples(includedNameSet));
        return writer.toString();
    }

    @ResponseBody
    @RequestMapping(value = "/internal/refresh_cache")
    public String initBasicData() {
        try {
            basicService.init();
            return "OK";
        } catch (Exception e) {
            return "Error:" + Throwables.getStackTraceAsString(e);
        }
    }

    @ResponseBody
    @RequestMapping(value = "/internal/refresh_token_convert_rate")
    public String refreshWithdrawTokenConvertRate() {
        try {
            withdrawService.refreshConvertRate();
            return "OK";
        } catch (Exception e) {
            return "Error:" + Throwables.getStackTraceAsString(e);
        }
    }

    @ResponseBody
    @RequestMapping(value = "/internal/refresh_single_token_convert_rate")
    public String refreshSingleWithdrawTokenConvertRate(@RequestParam("org_id") Long orgId,
                                                        @RequestParam("token_id") String tokenId,
                                                        @RequestParam("convert_token_id") String convertTokenId) {
        try {
            withdrawService.refreshTokenConvertRate(orgId, tokenId, convertTokenId);
            return "OK";
        } catch (Exception e) {
            return "Error:" + Throwables.getStackTraceAsString(e);
        }
    }

    @ResponseBody
    @RequestMapping(value = "/internal/update_token_rate_up_ratio")
    public String updateTokenRateUpRatio(@RequestParam("org_id") Long orgId,
                                         @RequestParam("token_id") String tokenId,
                                         @RequestParam("convert_token_id") String convertTokenId,
                                         @RequestParam("rate_up_ratio") BigDecimal rateUpRatio) {
        try {
            withdrawService.updateRateUpRatio(orgId, tokenId, convertTokenId, rateUpRatio);
            return "OK";
        } catch (Exception e) {
            return "Error:" + Throwables.getStackTraceAsString(e);
        }
    }

    @RequestMapping(value = "/internal/redis/set")
    @ResponseBody
    public String internalRedisSetOperation(String key, String value) {
        if (!Strings.isNullOrEmpty(key) && !Strings.isNullOrEmpty(value)) {
            log.info("redisOperation: set {} {}", key, value);
            redisTemplate.opsForValue().set(key, value);
            return "OK";
        } else {
            return "error: check param";
        }
    }

    @RequestMapping(value = "/internal/redis/hset")
    @ResponseBody
    public String internalRedisHSetOperation(String key, String field, String value) {
        if (Stream.of(key, field, value).noneMatch(Strings::isNullOrEmpty)) {
            log.info("redisOperation: hset {} {} {}", key, field, value);
            redisTemplate.opsForHash().put(key, field, value);
            return "OK";
        } else {
            return "error: check param";
        }
    }

    @RequestMapping(value = "/internal/redis/del")
    @ResponseBody
    public String internalRedisDelOperation(String key) {
        if (!Strings.isNullOrEmpty(key)) {
            log.info("redisOperation: del {}", key);
            redisTemplate.delete(key);
            return "OK";
        } else {
            return "error: check param";
        }
    }

    @RequestMapping(value = "/internal/redis/hdel")
    @ResponseBody
    public String internalRedisHDelOperation(String key, String field) {
        if (Stream.of(key, field).noneMatch(Strings::isNullOrEmpty)) {
            log.info("redisOperation: hdel {} {}", key, field);
            redisTemplate.opsForHash().delete(key, field);
            return "OK";
        } else {
            return "error: check param";
        }
    }

    @RequestMapping(value = "/internal/redis/get")
    @ResponseBody
    public String internalRedisGetOperation(String key) {
        if (!Strings.isNullOrEmpty(key)) {
            log.info("redisOperation: get {}", key);
            String value = redisTemplate.opsForValue().get(key);
            return Strings.nullToEmpty(value);
        } else {
            return "error: check param";
        }
    }

    @RequestMapping(value = "/internal/redis/hget")
    @ResponseBody
    public String internalRedisHGetOperation(String key, String field) {
        if (Stream.of(key, field).noneMatch(Strings::isNullOrEmpty)) {
            log.info("redisOperation: hget {} {}", key, field);
            Object value = redisTemplate.opsForHash().get(key, field);
            return value == null ? "" : value.toString();
        } else {
            return "error: check param";
        }
    }

    @RequestMapping(value = "/internal/redis/hgetall")
    @ResponseBody
    public String internalRedisHGetAllOperation(String key) {
        if (!Strings.isNullOrEmpty(key)) {
            log.info("redisOperation: hgetall {}", key);
            Map<Object, Object> valuesMap = redisTemplate.opsForHash().entries(key);
            return JsonUtil.defaultGson().toJson(valuesMap);
        } else {
            return "error: check param";
        }
    }

    @RequestMapping(value = "/internal/redis/sadd")
    @ResponseBody
    public String internalRedisSAddOperation(String key, String values) {
        if (!Strings.isNullOrEmpty(key) && !Strings.isNullOrEmpty(values)) {
            log.info("redisOperation: sadd {}", key);
            return "" + redisTemplate.opsForSet().add(key, values.split(","));
        } else {
            return "error: check param";
        }
    }

    @RequestMapping(value = "/internal/redis/smembers")
    @ResponseBody
    public String internalRedisSMembersOperation(String key) {
        if (!Strings.isNullOrEmpty(key)) {
            log.info("redisOperation: members {}", key);
            return JsonUtil.defaultGson().toJson(redisTemplate.opsForSet().members(key));
        } else {
            return "error: check param";
        }
    }

    @RequestMapping(value = "/internal/redis/srem")
    @ResponseBody
    public String internalRedisSRemoveOperation(String key, String value) {
        if (!Strings.isNullOrEmpty(key) && !Strings.isNullOrEmpty(value)) {
            log.info(" redisOperation: members {}", key);
            return "" + redisTemplate.opsForSet().remove(key, value);
        } else {
            return "error: check param";
        }
    }

    @RequestMapping(value = "/internal/invite/test")
    @ResponseBody
    public String inviteFeeBackTask(Long orgId, String time) {
        String[] timeArray = time.split(",");
        new Thread(() -> {
            for (String timeStr : timeArray) {
                if (StringUtils.isBlank(timeStr)) {
                    continue;
                }
                Long t = Long.valueOf(timeStr);
                TestInviteFeeBackResponse response = inviteTaskService.testInviteFeeBack(orgId, t);
                log.info(" test inviteFeeBackTask :{} ", response);
            }
        }).start();
        return "FINISH";
    }


    @RequestMapping(value = "/internal/invite/test/create")
    @ResponseBody
    public String testExecuteFuturesAdminGrantInviteBonus(Long orgId, String time) {
        String[] timeArray = time.split(",");
        new Thread(() -> {
            for (String timeStr : timeArray) {
                if (StringUtils.isBlank(timeStr)) {
                    continue;
                }
                Long t = Long.valueOf(timeStr);
                TestInviteFeeBackResponse response = inviteTaskService.testExecuteFuturesAdminGrantInviteBonus(orgId, t);
                log.info(" test inviteFeeBackTask :{} ", response);
            }
        }).start();
        return "FINISH";
    }

    @RequestMapping(value = "/internal/invite/grant")
    @ResponseBody
    public String inviteFeeBackGrantTask(Long orgId, String time) {
        String[] timeArray = time.split(",");
        new Thread(() -> {
            for (String timeStr : timeArray) {
                if (StringUtils.isBlank(timeStr)) {
                    continue;
                }
                Long t = Long.valueOf(timeStr);
                int value = inviteTaskService.executeAdminGrantInviteBonus(orgId, t);
                log.info(" test inviteFeeBackTask :{} ", value);
            }
        }).start();
        return "FINISH";
    }

    @RequestMapping(value = "/internal/finance/product/refresh")
    @ResponseBody
    public String reloadProductInfo(@RequestParam(name = "type", required = false, defaultValue = "0") Integer productType) {
        financeProductService.resetFinanceProductCache(productType);
        return "finished";
    }

    /**
     * !!!测试用接口。用来执行以当前资产为快照，某一天的利息发放情况
     */
    @RequestMapping(value = "/internal/finance/test/interest_task")
    @ResponseBody
    @Deprecated
    public String autoDailyTask(@RequestParam(name = "time") String statisticsTime) {
        try {
            boolean lock = RedisLockUtils.tryLock(redisTemplate, BrokerLockKeys.FINANCE_DAILY_TASK_LOCK_KEY, BrokerLockKeys.FINANCE_DAILY_TASK_LOCK_EXPIRE);
            if (!lock) {
                log.warn(" **** Finance **** interestDailyTask cannot get lock");
                return "cannot get lock";
            }
            boolean taskContinue = false;
            // 1、创建interest_data 和 finance_record(interest)
            boolean result = financeWalletService.manualFinanceInterestTask(statisticsTime);
            if (result) {
                taskContinue = true;
            }
            if (taskContinue) {
                // 2、计算七日年化。这个任务重复执行也没啥问题，重复计算几次相同的数据而已
                financeInterestService.computeFinanceProductSevenYearRate(statisticsTime);

                // TODO 5、计算平台总限额数量
                // 这里为什么要计算更新这货？
//                 financeLimitStatisticsService.computeFinanceTotalLimit();

                // 6、发放收益（是否自动？）
                // back
                //financeInterestService.grantFinanceInterest(statisticsTime, true);
                return "finished";
            } else {
                log.error("**** Finance **** interestDailyTask failed, Need to be processed manually");
                return "interrupted";
            }
        } catch (Exception e) {
            log.error("**** Finance **** interestDailyTask failed, catch exception, Need to be processed manually， statisticsTime:{}", statisticsTime, e);
            return "exception";
        }
    }

    @RequestMapping(value = "/internal/invite/update_invite_info")
    @ResponseBody
    public String updateInviteInfo(@RequestParam(name = "org_id") Long orgId,
                                   @RequestParam(name = "user_id") Long userId) {
        try {
            JsonObject dataObj = inviteService.updateInviteInfo(orgId, userId);
            return JsonUtil.defaultGson().toJson(dataObj);
        } catch (Exception e) {
            log.error(" update user invite info error", e);
            return "Exception:" + e.toString();
        }
    }

    @RequestMapping(value = "/internal/user/unbind_mobile")
    @ResponseBody
    public String unbindMobile(@RequestParam(name = "org_id") Long orgId,
                               @RequestParam(name = "user_id") Long userId) {
        try {
            Header header = Header.newBuilder().setOrgId(orgId).setUserId(userId).build();
            UnbindMobileResponse response = userSecurityService.unbindMobile(header);
            return "Success:" + response.getRet();
        } catch (Exception e) {
            log.error(" unbind mobile error", e);
            return "Exception:" + e.toString();
        }
    }

    @RequestMapping(value = "/internal/user/unbind_email")
    @ResponseBody
    public String unbindEmail(@RequestParam(name = "org_id") Long orgId,
                              @RequestParam(name = "user_id") Long userId) {
        try {
            Header header = Header.newBuilder().setOrgId(orgId).setUserId(userId).build();
            UnbindEmailResponse response = userSecurityService.unbindEmail(header);
            return "Success:" + response.getRet();
        } catch (Exception e) {
            log.error(" unbind email error", e);
            return "Exception:" + e.toString();
        }
    }

    @RequestMapping(value = "/internal/user/unbind_ga")
    @ResponseBody
    public String unbindGA(@RequestParam(name = "org_id") Long orgId,
                           @RequestParam(name = "user_id") Long userId) {
        try {
            Header header = Header.newBuilder().setOrgId(orgId).setUserId(userId).build();
            UnbindGAResponse response = userSecurityService.unbindGA(header, userId, false, null);
            return "Success:" + response.getRet();
        } catch (Exception e) {
            log.error(" update user invite info error", e);
            return "Exception:" + e.toString();
        }
    }

    /**
     * 如果利息定时任务执行失败，手动执行该任务
     */
    @RequestMapping(value = "/internal/finance/internal_task")
    @ResponseBody
    public String manualDailyTask() {
        try {
            financeTaskService.dailyInterestTask();
            return "finished, check log";
        } catch (Exception e) {
            log.error("manualDailyTask exception", e);
            return "exception";
        }
    }

    /**
     * 手动某一天发放利息
     */
    @RequestMapping(value = "/internal/finance/grant_interest")
    @ResponseBody
    @Deprecated
    public String grantInterest(@RequestParam(name = "time") String statisticsTime) {
        //financeInterestService.grantFinanceInterest(statisticsTime, false);
        return "finished";
    }

    @RequestMapping(value = "/internal/broker/update_{orgId}_functions")
    @ResponseBody
    public String updateBrokerFunctionConfig(@PathVariable Long orgId,
                                             @RequestParam String module,
                                             @RequestParam Boolean value) {
        try {
            FunctionModule functionModule = FunctionModule.valueOf(module.toUpperCase());
            return "Success:\n" + brokerService.updateFunctionConfig(orgId, functionModule, value);
        } catch (Exception e) {
            log.error("update broker function config error", e);
            return "Exception:" + e.getMessage();
        }
    }

    @RequestMapping(value = "/internal/broker/update_{orgId}_support_language")
    @ResponseBody
    public String updateBrokerSupportLanguageConfig(@PathVariable Long orgId, @RequestBody SupportLanguage supportLanguage) {
        try {
            return "Success:\n" + brokerService.updateSupportLanguageConfig(orgId, supportLanguage);
        } catch (Exception e) {
            log.error("update broker support language config error", e);
            return "Exception:" + e.getMessage();
        }
    }

    @RequestMapping(value = "/internal/broker/reset_{orgId}_support_language")
    @ResponseBody
    public String updateBrokerSupportLanguageConfig(@PathVariable Long orgId, @RequestBody List<SupportLanguage> supportLanguageList) {
        try {
            return "Success:\n" + brokerService.resetSupportLanguages(orgId, supportLanguageList);
        } catch (Exception e) {
            log.error("reset broker support language config error", e);
            return "Exception:" + e.getMessage();
        }
    }

    @RequestMapping(value = "/internal/broker/reset2_{orgId}_support_language")
    @ResponseBody
    public String updateBrokerSupportLanguageConfigByLanguageIds(@PathVariable Long orgId,
                                                                 @RequestParam(name = "language_ids") String languageIds) {
        try {
            return "Success:\n" + brokerService.resetSupportLanguages(orgId, languageIds);
        } catch (Exception e) {
            log.error("reset broker support language config error", e);
            return "Exception:" + e.getMessage();
        }
    }

    @RequestMapping(value = "/internal/add_{orgId}_third_party_app")
    @ResponseBody
    public String addThirdPartyApp(@PathVariable Long orgId,
                                   @RequestParam(name = "app_type") Integer appType,
                                   @RequestParam(name = "app_id") String appId,
                                   @RequestParam(name = "app_name") String appName,
                                   @RequestParam String callback,
                                   @RequestParam String functions) {
        try {
            ThirdPartyApp thirdPartyApp = oAuthService.saveThirdPartyApp(orgId, appType, appId, appName, callback, functions);
            return JsonUtil.defaultGson().toJson(thirdPartyApp);
        } catch (Exception e) {
            log.error("add third party app error", e);
            return "Exception:" + e.getMessage();
        }
    }

    @RequestMapping(value = "/internal/update_{orgId}_third_party_app")
    @ResponseBody
    public String updateThirdPartyApp(@PathVariable Long orgId, Long id, int status) {
        try {
            oAuthService.updateThirdPartyAppStatus(orgId, id, status);
            return "Success";
        } catch (Exception e) {
            log.error("update third party app error", e);
            return "Exception:" + e.getMessage();
        }
    }

    @RequestMapping(value = "/internal/add_{orgId}_app_config")
    @ResponseBody
    public String addNewApp(@PathVariable Long orgId,
                            @RequestParam(name = "app_id") String appId,
                            @RequestParam(name = "app_version") String appVersion,
                            @RequestParam(name = "device_type") String deviceType,
                            @RequestParam(name = "device_version") String deviceVersion,
                            @RequestParam(name = "app_channel") String appChannel,
                            @RequestParam(name = "download_url") String downloadUrl,
                            @RequestParam(name = "download_webview_url") String downloadWebviewUrl,
                            @RequestParam(name = "new_features") String newFeatures,
                            @RequestParam(name = "update_version") boolean updateToThisVersion) {
        try {
            appConfigService.addNewApp(orgId, appId, appVersion, deviceType, deviceVersion, appChannel,
                    downloadUrl, downloadWebviewUrl, newFeatures, updateToThisVersion);
            return "Success";
        } catch (Exception e) {
            log.error("add new app error", e);
            return "Exception:" + e.getMessage();
        }
    }

    @RequestMapping(value = "/internal/update_token_icon")
    @ResponseBody
    public String addTokenIcon(@RequestParam(name = "token_id") String tokenId,
                               @RequestParam(name = "token_icon") String tokenIcon) {
        adminTokenService.updateTokenIcon(tokenId, tokenIcon);
        return "Success";
    }

    @RequestMapping(value = "/internal/query_deposit_address")
    @ResponseBody
    public String queryDepositAddress(@RequestParam(name = "org_id") Long orgId,
                                      @RequestParam(name = "user_id") Long userId) {
        QueryUserDepositAddressResponse response = depositService.queryDepositAddress(orgId, userId);
        return JsonUtil.defaultGson().toJson(response);
    }

    @RequestMapping(value = "/internal/org_balance_summary")
    @ResponseBody
    public String queryDepositAddress(@RequestParam(name = "org_id") Long orgId) {
        List<OrgBalanceSummary> result = orgStatisticsService.orgBalanceSummary(orgId);
        return JsonUtil.defaultGson().toJson(result);
    }

    @RequestMapping(value = "/internal/balance_info")
    @ResponseBody
    public String queryDepositAddress(@RequestParam(name = "org_id") Long orgId,
                                      @RequestParam(name = "user_id") Long userId,
                                      @RequestParam(name = "token_id") String tokenId,
                                      @RequestParam(name = "from_id", required = false, defaultValue = "0") Long fromId,
                                      @RequestParam(name = "last_id", required = false, defaultValue = "0") Long lastId,
                                      @RequestParam(name = "limit", required = false, defaultValue = "100") Integer limit) {
        List<StatisticsBalance> result = orgStatisticsService.queryTokenHoldInfo(orgId, userId, tokenId, fromId, lastId, limit);
        return JsonUtil.defaultGson().toJson(result);
    }

    @RequestMapping(value = "/internal/statistics_symbol_trade_fee")
    @ResponseBody
    public String statisticsSymbolTradeFee(@RequestParam(name = "org_id") Long orgId,
                                           @RequestParam(name = "user_id") Long userId,
                                           @RequestParam(name = "symbol_id", required = false, defaultValue = "") String symbolId,
                                           @RequestParam(name = "start_time", required = false, defaultValue = "0") Long startTime,
                                           @RequestParam(name = "end_time", required = false, defaultValue = "0") Long endTime) {
        List<StatisticsSymbolTradeFee> tradeFeeList = orgStatisticsService.statisticsSymbolTradeFee(orgId, userId, symbolId, startTime, endTime);
        return JsonUtil.defaultGson().toJson(tradeFeeList);
    }

    @RequestMapping(value = "/internal/statistics_trade_fee_top")
    @ResponseBody
    public String statisticsTradeFeeTop(@RequestParam(name = "org_id") Long orgId,
                                        @RequestParam(name = "symbol_id", required = false, defaultValue = "") String symbolId,
                                        @RequestParam(name = "fee_token_id") String feeTokenId,
                                        @RequestParam(name = "start_time", required = false, defaultValue = "0") Long startTime,
                                        @RequestParam(name = "end_time", required = false, defaultValue = "0") Long endTime,
                                        @RequestParam(name = "top", required = false, defaultValue = "50") Integer top) {
        List<StatisticsTradeFeeTop> tradeFeeTopList = orgStatisticsService.statisticsTradeFeeTop(orgId, symbolId, feeTokenId, startTime, endTime, top);
        return JsonUtil.defaultGson().toJson(tradeFeeTopList);
    }

    @RequestMapping(value = "/internal/deposit_orders")
    @ResponseBody
    public String queryOrgDepositOrder(@RequestParam(name = "org_id") Long orgId,
                                       @RequestParam(name = "user_id") Long userId,
                                       @RequestParam(name = "token_id", required = false, defaultValue = "") String tokenId,
                                       @RequestParam(name = "start_time", required = false, defaultValue = "0") Long startTime,
                                       @RequestParam(name = "end_time", required = false, defaultValue = "0") Long endTime,
                                       @RequestParam(name = "from_id", required = false, defaultValue = "0") Long fromId,
                                       @RequestParam(name = "last_id", required = false, defaultValue = "0") Long lastId,
                                       @RequestParam(name = "limit", required = false, defaultValue = "100") Integer limit) {
        List<StatisticsDepositOrder> result = orgStatisticsService.queryOrgDepositOrder(orgId, userId, tokenId, startTime, endTime, fromId, lastId, limit, "", "");
        return JsonUtil.defaultGson().toJson(result);
    }

    @RequestMapping(value = "/internal/withdraw_orders")
    @ResponseBody
    public String queryOrgWithdrawOrder(@RequestParam(name = "org_id") Long orgId,
                                        @RequestParam(name = "user_id") Long userId,
                                        @RequestParam(name = "token_id", required = false, defaultValue = "") String tokenId,
                                        @RequestParam(name = "start_time", required = false, defaultValue = "0") Long startTime,
                                        @RequestParam(name = "end_time", required = false, defaultValue = "0") Long endTime,
                                        @RequestParam(name = "from_id", required = false, defaultValue = "0") Long fromId,
                                        @RequestParam(name = "last_id", required = false, defaultValue = "0") Long lastId,
                                        @RequestParam(name = "limit", required = false, defaultValue = "100") Integer limit) {
        List<StatisticsWithdrawOrder> result = orgStatisticsService.queryOrgWithdrawOrder(orgId, userId, tokenId, startTime, endTime, fromId, lastId, limit, "", "");
        return JsonUtil.defaultGson().toJson(result);
    }

    @RequestMapping(value = "/internal/trade_detail")
    @ResponseBody
    public String queryOrgTradeDetail(@RequestParam(name = "org_id") Long orgId,
                                      @RequestParam(name = "symbol_id", required = false, defaultValue = "") String symbolId,
                                      @RequestParam(name = "start_time", required = false, defaultValue = "0") Long startTime,
                                      @RequestParam(name = "end_time", required = false, defaultValue = "0") Long endTime,
                                      @RequestParam(name = "from_id", required = false, defaultValue = "0") Long fromId,
                                      @RequestParam(name = "last_id", required = false, defaultValue = "0") Long lastId,
                                      @RequestParam(name = "limit", required = false, defaultValue = "100") Integer limit) {
        List<StatisticsTradeDetail> result = orgStatisticsService.queryOrgTradeDetail(orgId, symbolId, startTime, endTime, fromId, lastId, limit);
        return JsonUtil.defaultGson().toJson(result);
    }

    @RequestMapping(value = "/internal/otc_orders")
    @ResponseBody
    public String queryOrgOTCOrder(@RequestParam(name = "org_id") Long orgId,
                                   @RequestParam(name = "user_id") Long userId,
                                   @RequestParam(name = "token_id", required = false, defaultValue = "") String tokenId,
                                   @RequestParam(name = "start_time", required = false, defaultValue = "0") Long startTime,
                                   @RequestParam(name = "end_time", required = false, defaultValue = "0") Long endTime,
                                   @RequestParam(name = "from_id", required = false, defaultValue = "0") Long fromId,
                                   @RequestParam(name = "last_id", required = false, defaultValue = "0") Long lastId,
                                   @RequestParam(name = "limit", required = false, defaultValue = "100") Integer limit) {
        List<StatisticsOTCOrder> result = orgStatisticsService.queryOrgOTCOrder(orgId, userId, tokenId, startTime, endTime, fromId, lastId, limit);
        return JsonUtil.defaultGson().toJson(result);
    }

    @RequestMapping(value = "/internal/transfer")
    @ResponseBody
    public String transfer(@RequestParam(name = "org_id") Long orgId,
                           @RequestParam(name = "client_order_id") String clientOrderId,
                           @RequestParam(name = "source_user_id") Long sourceUserId,
                           @RequestParam(name = "target_user_id") Long targetUserId,
                           @RequestParam(name = "token_id") String tokenId,
                           @RequestParam(name = "amount") String amount,
                           @RequestParam(name = "from_source_lock", required = false, defaultValue = "false") Boolean fromSourceLock,
                           @RequestParam(name = "to_target_lock", required = false, defaultValue = "false") Boolean toTargetLock,
                           @RequestParam(name = "business_type") Integer businessType,
                           @RequestParam(name = "sub_business_type", required = false, defaultValue = "0") Integer subBusinessType,
                           @RequestParam(name = "source_account_id", required = false, defaultValue = "0") Long sourceAccountId,
                           @RequestParam(name = "target_account_id", required = false, defaultValue = "0") Long targetAccountId) {
        Header header = Header.newBuilder().setOrgId(orgId).build();
        try {
            BaseResult baseResult = accountService.balanceTransfer(header, clientOrderId, sourceUserId, targetUserId, tokenId, amount, fromSourceLock, toTargetLock, businessType, subBusinessType,
                    sourceAccountId, targetAccountId);
            return JsonUtil.defaultGson().toJson(baseResult);
        } catch (BrokerException e) {
            log.error("transfer error", e);
            return "Error:" + e.getCode();
        } catch (Exception e) {
            log.error("transfer error", e);
            return "Error";
        }
    }

    @RequestMapping(value = "/internal/mapping")
    @ResponseBody
    public String mapping(@RequestParam(name = "org_id") Long orgId,
                          @RequestParam(name = "client_order_id") String clientOrderId,
                          @RequestParam(name = "source_user_id") Long sourceUserId,
                          @RequestParam(name = "source_token_id") String sourceTokenId,
                          @RequestParam(name = "source_amount") String sourceAmount,
                          @RequestParam(name = "from_source_lock", required = false, defaultValue = "false") Boolean fromSourceLock,
                          @RequestParam(name = "to_target_lock", required = false, defaultValue = "false") Boolean toTargetLock,
                          @RequestParam(name = "target_user_id") Long targetUserId,
                          @RequestParam(name = "target_token_id") String targetTokenId,
                          @RequestParam(name = "target_amount") String targetAmount,
                          @RequestParam(name = "from_target_lock", required = false, defaultValue = "") Boolean fromTargetLock,
                          @RequestParam(name = "to_source_lock", required = false, defaultValue = "") Boolean toSourceLock,
                          @RequestParam(name = "business_type") Integer businessType) {
        Header header = Header.newBuilder().setOrgId(orgId).build();
        try {
            BaseResult baseResult = accountService.balanceMapping(header, clientOrderId, sourceUserId, sourceTokenId, sourceAmount, fromSourceLock, toTargetLock,
                    targetUserId, targetTokenId, targetAmount, fromTargetLock, toSourceLock, businessType);
            return JsonUtil.defaultGson().toJson(baseResult);
        } catch (BrokerException e) {
            log.error("mapping error", e);
            return "Error:" + e.getCode();
        } catch (Exception e) {
            log.error("mapping error", e);
            return "Error";
        }
    }

    @RequestMapping(value = "/internal/register")
    @ResponseBody
    public String importRegister(@RequestParam(name = "org_id") Long orgId,
                                 @RequestParam(name = "national_code", required = false, defaultValue = "") String nationalCode,
                                 @RequestParam(name = "mobile", required = false, defaultValue = "") String mobile,
                                 @RequestParam(name = "email", required = false, defaultValue = "") String email,
                                 @RequestParam(required = false, defaultValue = "") String password,
                                 @RequestParam(name = "invite_code", required = false, defaultValue = "") String inviteCode) {
        try {
            Header header = Header.newBuilder().setOrgId(orgId).setRemoteIp("127.0.0.1").setPlatform(Platform.PC).setLanguage("zh_CN").setUserAgent("Chrome").build();
            RegisterResponse response = userService.importRegister(header, nationalCode, mobile, email, password, inviteCode, 0L, false, false, true);
            return JsonUtil.defaultGson().toJson(response);
        } catch (BrokerException e) {
            log.error("register error", e);
            return "Error:" + e.getCode();
        } catch (Exception e) {
            log.error("register error", e);
            return "Error";
        }
    }

    @RequestMapping(value = "/internal/query_user_list")
    @ResponseBody
    public String queryUserList(@RequestParam(name = "org_id") Long orgId,
                                @RequestParam(required = false, defaultValue = "") String source,
                                @RequestParam(name = "from_id", required = false, defaultValue = "0") Long fromId,
                                @RequestParam(name = "end_id", required = false, defaultValue = "0") Long endId,
                                @RequestParam(name = "start_time", required = false, defaultValue = "0") Long startTime,
                                @RequestParam(name = "end_time", required = false, defaultValue = "0") Long endTime,
                                @RequestParam(required = false, defaultValue = "20") Integer limit) {
        try {
            List<SimpleUserInfo> simpleUserInfoList = userService.querySimpleUserInfoList(orgId, source, fromId, endId,
                    startTime, endTime, limit);
            return JsonUtil.defaultGson().toJson(simpleUserInfoList);
        } catch (BrokerException e) {
            log.error("queryUserList error", e);
            return "Error:" + e.getCode();
        } catch (Exception e) {
            log.error("queryUserList error", e);
            return "Error";
        }
    }

    @RequestMapping(value = "/internal/rebuild_invite_relation")
    @ResponseBody
    public String rebuildInviteRelation(@RequestParam(name = "org_id") Long orgId,
                                        @RequestParam(name = "user_id") Long userId,
                                        @RequestParam(name = "invite_user_id") Long inviteUserId) {
        try {
            userService.rebuildUserInviteRelation(orgId, userId, inviteUserId);
            return "SUCCESS";
        } catch (Exception e) {
            log.error("rebuildInviteRelation error", e);
            return "FAILED";
        }
    }

    @RequestMapping(value = "/internal/rates")
    @ResponseBody
    public String rates(@RequestParam(name = "org_id") Long orgId,
                        @RequestParam(name = "token_id") String tokenId,
                        @RequestParam(name = "target_token_id", required = false, defaultValue = "") String targetTokenId) {
        try {
            String result;
            if (Strings.isNullOrEmpty(targetTokenId)) {
                result = JsonUtil.defaultGson().toJson(basicService.getV3Rate(orgId, tokenId));
            } else {
                result = basicService.getFXRate(orgId, tokenId, targetTokenId).stripTrailingZeros().toPlainString();
            }
            return result;
        } catch (Exception e) {
            log.error("rates error", e);
            return "FAILED";
        }
    }

    @RequestMapping(value = "/internal/quote_rates")
    @ResponseBody
    public String quoteRates(@RequestParam(name = "org_id") Long orgId,
                             @RequestParam(name = "token_id") String tokenId) {
        try {
            JsonObject dataObj = basicService.getOrgTokenRate(orgId, tokenId);
            return JsonUtil.defaultGson().toJson(dataObj);
        } catch (Exception e) {
            log.error("rates error", e);
            return "FAILED";
        }
    }

    @RequestMapping(value = "/internal/get_common_ini")
    @ResponseBody
    public String getCommonIni(@RequestParam(name = "org_id") Long orgId,
                               @RequestParam(name = "ini_name") String iniName,
                               @RequestParam(required = false, defaultValue = "") String language,
                               @RequestParam(name = "from_cache", required = false, defaultValue = "true") Boolean fromCache) {
        CommonIni commonIni;
        if (fromCache) {
            commonIni = commonIniService.getCommonIniFromCache(orgId, iniName, language);
        } else {
            commonIni = commonIniService.getCommonIni(orgId, iniName, language);
        }
        return commonIni == null ? "no data" : commonIni.getIniValue();
    }

    @RequestMapping(value = "/internal/set_common_ini")
    @ResponseBody
    public String replaceCommonIni(@RequestParam(name = "org_id") Long orgId,
                                   @RequestParam(name = "ini_name") String iniName,
                                   @RequestParam(name = "ini_desc") String iniDesc,
                                   @RequestParam(required = false, defaultValue = "") String language,
                                   @RequestParam(name = "ini_value") String iniValue) {
        CommonIni commonIni = CommonIni.builder()
                .orgId(orgId)
                .iniName(iniName)
                .iniDesc(iniDesc)
                .language(Strings.nullToEmpty(language))
                .iniValue(iniValue)
                .build();
        commonIniService.insertOrUpdateCommonIni(commonIni);
        return getCommonIni(orgId, iniName, language, false);
    }

    @RequestMapping(value = "/internal/balance_flow")
    @ResponseBody
    public String queryBalanceFlow(Long orgId, Long userId, Integer limit) {
        Header header = Header.newBuilder().setOrgId(orgId).setUserId(userId).setLanguage("zh_CN").build();
        QueryBalanceFlowResponse response = accountService.queryBalanceFlow(header, AccountTypeEnum.COIN, 0, Lists.newArrayList(), Lists.newArrayList(), 0L, 0L, 0L, 0L, limit, true);
        return JsonUtil.defaultGson().toJson(response);
    }

    @RequestMapping(value = "/internal/query_sub_business_subject")
    @ResponseBody
    public String querySubBusinessSubject(@RequestParam(name = "org_id") Long orgId,
                                          @RequestParam(name = "parent_subject", required = false, defaultValue = "0") Integer parentSubject) {
        List<SubBusinessSubject> subBusinessSubjectList = subBusinessSubjectService.querySubBusinessSubject(orgId, parentSubject);
        Map<String, List<SubBusinessSubject>> subjectMap = subBusinessSubjectList.stream()
                .collect(Collectors.groupingBy(item -> String.format("%s:%s", item.getParentSubject(), item.getSubject())));

        List<io.bhex.broker.grpc.sub_business_subject.SubBusinessSubject> responseItemList = subjectMap.keySet().stream()
                .map(item -> {
                    List<SubBusinessSubject> itemSubjectList = subjectMap.get(item);
                    Map<String, String> subjectNameMap = Maps.newHashMap();
                    for (SubBusinessSubject subject : itemSubjectList) {
                        subjectNameMap.put(subject.getLanguage(), subject.getSubjectName());
                    }
                    return io.bhex.broker.grpc.sub_business_subject.SubBusinessSubject.newBuilder()
                            .setParentSubject(Integer.parseInt(item.split(":")[0]))
                            .setSubject(Integer.parseInt(item.split(":")[1]))
                            .putAllNames(subjectNameMap)
                            .build();
                }).collect(Collectors.toList());
        return JsonUtil.defaultGson().toJson(responseItemList);
    }

    @RequestMapping(value = "/internal/save_sub_business_subject")
    @ResponseBody
    public String saveSubBusinessSubject(@RequestParam(name = "org_id") Long orgId,
                                         @RequestParam(name = "parent_subject") Integer parentSubject,
                                         @RequestParam Integer subject,
                                         @RequestParam String names,
                                         @RequestParam Integer status) {
        Map<String, String> subjectNames = JsonUtil.defaultGson().fromJson(names, new TypeToken<Map<String, String>>() {
        }.getType());
        subBusinessSubjectService.saveSubBusinessSubject(orgId, parentSubject, subject, subjectNames, status);
        return "SUCCESS";
    }

    @RequestMapping(value = "/internal/evict_account_cache")
    @ResponseBody
    public String evictCachedAccountId() {
        accountService.evictAccountIdCache();
        return "This operate will evict all accountId cache.\nSUCCESS\n";
    }

    @RequestMapping(value = "/internal/history_trade_detail")
    @ResponseBody
    public String historyTradeDetail(@RequestParam Long orgId,
                                     @RequestParam(required = false, defaultValue = "0") Long userId,
                                     @RequestParam(required = false, defaultValue = "0") Long accountId,
                                     @RequestParam(required = false, defaultValue = "1") Integer queryDataType,
                                     @RequestParam(required = false, defaultValue = "") String symbolId,
                                     @RequestParam(required = false, defaultValue = "") String quoteTokenId,
                                     @RequestParam(required = false, defaultValue = "") Integer orderSide,
                                     @RequestParam(required = false, defaultValue = "0") Long startTime,
                                     @RequestParam(required = false, defaultValue = "0") Long endTime,
                                     @RequestParam(required = false, defaultValue = "0") Long fromId,
                                     @RequestParam(required = false, defaultValue = "0") Long endId,
                                     @RequestParam(required = false, defaultValue = "false") Boolean withFuturesInfo,
                                     @RequestParam(required = false, defaultValue = "0") Integer page,
                                     @RequestParam(required = false, defaultValue = "20") Integer limit) {
        Header header = Header.newBuilder().setOrgId(orgId).setUserId(userId).build();
        List<TradeDetail> tradeDetailPage = tradeDetailHistoryService.queryWithCondition(header, accountId,
                queryDataType, 0L, symbolId, quoteTokenId, orderSide, startTime, endTime, fromId, endId, limit, withFuturesInfo);
        JsonObject dataObject = new JsonObject();
//        dataObject.addProperty("totalPage", tradeDetailPage.getTotalPages());
//        dataObject.addProperty("totalItems", tradeDetailPage.getTotalElements());
        dataObject.addProperty("items", JsonUtil.defaultGson().toJson(tradeDetailPage));
        return JsonUtil.defaultGson().toJson(dataObject);
    }

    @RequestMapping(value = "/internal/history_balance_flow")
    @ResponseBody
    public String historyBalanceFlow(@RequestParam Long orgId,
                                     @RequestParam(required = false, defaultValue = "0") Long userId,
                                     @RequestParam(required = false, defaultValue = "0") Long accountId,
                                     @RequestParam(required = false, defaultValue = "1") Integer queryDataType,
                                     @RequestParam(required = false, defaultValue = "") String tokenIds,
                                     @RequestParam(required = false, defaultValue = "0") Long startTime,
                                     @RequestParam(required = false, defaultValue = "0") Long endTime,
                                     @RequestParam(required = false, defaultValue = "0") Long fromId,
                                     @RequestParam(required = false, defaultValue = "0") Long endId,
                                     @RequestParam(required = false, defaultValue = "0") Integer page,
                                     @RequestParam(required = false, defaultValue = "20") Integer limit) {
        Header header = Header.newBuilder().setOrgId(orgId).setUserId(userId).build();
        Page<BalanceFlow> tradeDetailPage = balanceFlowHistoryService.queryPageWithCondition(header, accountId,
                Splitter.on(",").omitEmptyStrings().trimResults().splitToList(tokenIds), Lists.newArrayList(),
                startTime, endTime, fromId, endId, page, limit);
        JsonObject dataObject = new JsonObject();
        dataObject.addProperty("totalPage", tradeDetailPage.getTotalPages());
        dataObject.addProperty("totalItems", tradeDetailPage.getTotalElements());
        dataObject.addProperty("items", JsonUtil.defaultGson().toJson(tradeDetailPage.getContent()));

        List<BalanceFlow> balanceFlowList = balanceFlowHistoryService.queryWithCondition(header, accountId,
                Splitter.on(",").omitEmptyStrings().trimResults().splitToList(tokenIds), Lists.newArrayList(),
                startTime, endTime, fromId, endId, limit);
        dataObject.addProperty("criData", JsonUtil.defaultGson().toJson(balanceFlowList));
        return JsonUtil.defaultGson().toJson(dataObject);
    }

    @RequestMapping(value = "/internal/update_balance_proof")
    @ResponseBody
    public String updateBalanceProof(@RequestParam(name = "org_id") Long orgId,
                                     @RequestParam(name = "token_id") Long userId,
                                     @RequestParam(name = "token_id") String tokenId,
                                     String nonce,
                                     String amount,
                                     String json,
                                     @RequestParam("created_at") Long createdAt) {
        Long proofId = userService.updateBalanceProof(orgId, userId, tokenId, nonce, amount, json, createdAt);
        JsonObject dataObject = new JsonObject();
        dataObject.addProperty("proofId", proofId);
        return JsonUtil.defaultGson().toJson(dataObject);
    }

    @RequestMapping(value = "/internal/payment/create")
    @ResponseBody
    public String importPayment(@RequestParam(name = "org_id") Long orgId,
                                @RequestParam(name = "user_id") Long userId,
                                @RequestParam(name = "payment_type") Integer paymentType,
                                @RequestParam(name = "real_name", required = false) String realName,
                                @RequestParam(name = "bank_name", required = false) String bankName,
                                @RequestParam(name = "branch_name", required = false) String branchName,
                                @RequestParam(name = "account_no") String accountNo,
                                @RequestParam(name = "qrcode", required = false) String qrcode) {
        try {
            Header header = Header.newBuilder().setOrgId(orgId).setUserId(userId).setRemoteIp("127.0.0.1").setPlatform(Platform.PC).setLanguage("zh_CN").setUserAgent("Chrome").build();
            otcService.importPayment(header, paymentType, realName, bankName, branchName, accountNo, qrcode);
            JsonObject dataObj = new JsonObject();
            dataObj.addProperty("success", Boolean.TRUE);
            return JsonUtil.defaultGson().toJson(dataObj);
        } catch (BrokerException e) {
            log.error("payment error", e);
            return "Error:" + e.getCode();
        } catch (Exception e) {
            log.error("payment error", e);
            return "Error";
        }
    }

    @RequestMapping(value = "/internal/get_account_trade_fee_config")
    @ResponseBody
    public String getCachedAccountTradeFeeConfig(@RequestParam(name = "org_id") Long orgId,
                                                 @RequestParam(name = "exchange_id") Long exchangeId,
                                                 @RequestParam(name = "symbol_id") String symbolId,
                                                 @RequestParam(name = "account_id") Long accountId) {
        AccountTradeFeeConfig cachedTradeFeeConfig = brokerFeeService.getCachedTradeFeeConfig(orgId, exchangeId, symbolId, accountId);
        if (cachedTradeFeeConfig == null) {
            return "cannot find cached data";
        }
        return JsonUtil.defaultGson().toJson(cachedTradeFeeConfig);
    }

    @RequestMapping(value = "/internal/reset_symbol_fee_config")
    @ResponseBody
    public String resetSymbolFeeConfig(@RequestParam(name = "org_id") Long orgId,
                                       @RequestParam(name = "symbol_id") String symbolId,
                                       @RequestParam(name = "mbfr") String makerBuyFeeRate,
                                       @RequestParam(name = "msfr") String makerSellFeeRate,
                                       @RequestParam(name = "tbfr") String takerBuyFeeRate,
                                       @RequestParam(name = "tsfr") String takerSellFeeRate,
                                       @RequestParam(name = "refresh") Boolean refreshAccountTradeFeeConfig) {
        JsonObject dataObj = brokerFeeService.resetSymbolNegativeMakerFeeRate(orgId, symbolId, makerBuyFeeRate, makerSellFeeRate, takerBuyFeeRate, takerSellFeeRate, refreshAccountTradeFeeConfig);
        return JsonUtil.defaultGson().toJson(dataObj);
    }

    @RequestMapping(value = "/internal/open_red_packet")
    @ResponseBody
    public String openOrgRedPacket(@RequestParam(name = "org_id") Long orgId) {
        try {
            redPacketAdminService.openRedPacketFunction(orgId);
            return "SUCCESS";
        } catch (Exception e) {
            return "ERROR: " + e.getMessage();
        }
    }

    @RequestMapping(value = "/internal/open_margin_account")
    @ResponseBody
    public String openMarginAccount(@RequestParam(name = "org_id") Long orgId,
                                    @RequestParam(name = "user_id") Long userId) {
        try {
            SaveUserContractRequest request = SaveUserContractRequest.newBuilder()
                    .setOrgId(orgId)
                    .setUserId(userId)
                    .setOpen(1)
                    .setName("margin").build();
            userService.saveUserContract(request);
            return "SUCCESS";
        } catch (Exception e) {
            return "ERROR: " + e.getMessage();
        }
    }

    /**
     * 同步原币多多数据
     *
     * @return
     */
    @RequestMapping(value = "/internal/staking/syncdata")
    @ResponseBody
    @Deprecated
    public String stakingSyncData() {
        try {
            Long orgId = 6002L;
            stakingSyncDataService.syncFinanceData(orgId, 1L, 732371163958744350L);
            stakingSyncDataService.syncFinanceData(orgId, 2L, 732371163958744351L);
            stakingSyncDataService.syncFinanceData(orgId, 3L, 732371163958744352L);
            return "SUCCESS";
        } catch (Exception e) {
            return "ERROR: " + e.getMessage();
        }
    }

    /**
     * 手工派息
     *
     * @return
     */
    @RequestMapping(value = "/internal/staking/transfer")
    @ResponseBody
    @Deprecated
    public String stakingTransfer(Long orgId, Long pid, Long rid) {
        try {
            stakingProductCurrentStrategy.dispatchInterest(StakingTransferEvent.builder()
                    .orgId(orgId)
                    .productId(pid)
                    .rebateId(rid)
                    .rebateType(0)
                    .build());
            return "SUCCESS";
        } catch (Exception e) {
            return "ERROR: " + e.getMessage();
        }
    }

    @ResponseBody
    @RequestMapping(value = "/internal/trade_daily_report")
    public String generateTradeDataDailyReport() {
        try {
            odsService.generateTradeData(new Date(), "d");
            return "OK";
        } catch (Exception e) {
            return "Error:" + Throwables.getStackTraceAsString(e);
        }
    }

    @ResponseBody
    @RequestMapping(value = "/internal/common_daily_report")
    public String generateCommonDataDailyReport() {
        try {
            odsService.exec(new Date(), "d");
            return "OK";
        } catch (Exception e) {
            return "Error:" + Throwables.getStackTraceAsString(e);
        }
    }

}
