package io.bhex.broker.server.grpc.server.service;

import com.github.pagehelper.PageHelper;
import com.google.common.collect.Lists;
import com.google.gson.JsonObject;
import io.bhex.base.margin.QueryMarginSymbolReply;
import io.bhex.base.margin.QueryMarginSymbolRequest;
import io.bhex.base.margin.SetSymbolConfigRequest;
import io.bhex.base.proto.BaseRequest;
import io.bhex.base.token.ExchangeSymbolDetail;
import io.bhex.base.token.GetExchangeSymbolsRequest;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.common.util.JsonUtil;
import io.bhex.broker.grpc.admin.*;
import io.bhex.broker.grpc.common.Header;
import io.bhex.broker.server.domain.BrokerLockKeys;
import io.bhex.broker.server.domain.NoticeBusinessType;
import io.bhex.broker.server.grpc.client.service.GrpcMarginService;
import io.bhex.broker.server.grpc.client.service.GrpcSymbolService;
import io.bhex.broker.server.grpc.client.service.GrpcTokenService;
import io.bhex.broker.server.grpc.server.AdminTokenGrpcService;
import io.bhex.broker.server.model.BrokerTaskConfig;
import io.bhex.broker.server.model.Symbol;
import io.bhex.broker.server.model.Token;
import io.bhex.broker.server.primary.mapper.BrokerTaskConfigMapper;
import io.bhex.broker.server.primary.mapper.SymbolMapper;
import io.bhex.broker.server.primary.mapper.TokenMapper;
import io.bhex.broker.server.util.BaseReqUtil;
import io.bhex.broker.server.util.BeanCopyUtils;
import io.bhex.broker.server.util.RedisLockUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MapUtils;
import org.springframework.aop.framework.AopContext;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;
import tk.mybatis.mapper.entity.Example;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @Description: 后台设置定时任务
 * @Date: 2020/1/7 下午2:34
 * @Author: liwei
 * @Copyright（C）: 2018 BlueHelix Inc. All rights reserved.
 */

@Slf4j
@Service
public class AdminBrokerTaskConfigService {


    @Resource
    private SymbolMapper symbolMapper;
    @Resource
    private GrpcSymbolService grpcSymbolService;
    @Resource
    private OrderService orderService;
    @Resource
    private BrokerTaskConfigMapper brokerTaskConfigMapper;
    @Resource(name = "stringRedisTemplate")
    private StringRedisTemplate redisTemplate;
    @Resource
    private NoticeTemplateService noticeTemplateService;
    @Resource
    private TokenMapper tokenMapper;
    @Resource
    private GrpcTokenService grpcTokenService;
    @Resource
    private AdminTokenGrpcService adminTokenGrpcService;
    @Resource
    private AdminTokenService adminTokenService;
    @Resource
    private GrpcMarginService grpcMarginService;


    public GetBrokerTaskConfigsReply getBrokerTaskConfigs(GetBrokerTaskConfigsRequest request) {
        Example example = Example.builder(BrokerTaskConfig.class)
                .orderByDesc("id")
                .build();
        PageHelper.startPage(0, request.getPageSize());
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("orgId", request.getOrgId());
        criteria.andEqualTo("type", request.getType());
        if (request.getTaskId() > 0) {
            criteria.andEqualTo("id", request.getTaskId());
        }
        if (request.getLastId() > 0) {
            criteria.andLessThan("id", request.getLastId());
        }
        List<BrokerTaskConfig> configs = brokerTaskConfigMapper.selectByExample(example);
        if (CollectionUtils.isEmpty(configs)) {
            configs = Lists.newArrayList();
        }
        List<BrokerTaskConfigDetail> result = configs.stream().map(c -> {
            BrokerTaskConfigDetail.Builder builder = BrokerTaskConfigDetail.newBuilder();
            BeanCopyUtils.copyPropertiesIgnoreNull(c, builder);
            Map<String, String> actionMap = JsonUtil.defaultGson().fromJson(c.getActionContent(), Map.class);
            builder.putAllActionContent(actionMap);
            return builder.build();
        }).collect(Collectors.toList());

        return GetBrokerTaskConfigsReply.newBuilder().addAllTaskConfigs(result).build();
    }

    public SaveBrokerTaskConfigReply saveBrokerTaskConfig(SaveBrokerTaskConfigRequest request) {
        BrokerTaskConfigDetail grpcConfig = request.getTaskConfig();

        String actionContent = JsonUtil.defaultGson().toJson(grpcConfig.getActionContentMap());
        BrokerTaskConfig config = brokerTaskConfigMapper.selectForUpdate(grpcConfig.getId());

        if (grpcConfig.getStatus() == 0) { // request status == 0 ,代表取消
            if (config == null) {
                return SaveBrokerTaskConfigReply.newBuilder().setResult(false).setMessage("request.parameter.error").build();
            }
            config.setStatus(0);
            brokerTaskConfigMapper.updateByPrimaryKeySelective(config);
            return SaveBrokerTaskConfigReply.newBuilder().setResult(true).build();
        }

        if (config != null) {
            if (config.getStatus() != 1) { //只能修改原状态是待执行的任务
                return SaveBrokerTaskConfigReply.newBuilder().setResult(false).setMessage("error.status").build();
            }

            config.setUpdated(System.currentTimeMillis());
            config.setActionContent(actionContent);
            config.setActionTime(grpcConfig.getActionTime());
            config.setAdminUserName(grpcConfig.getAdminUserName());
            config.setRemark(grpcConfig.getRemark());
            config.setSymbolId(grpcConfig.getSymbolId());
            config.setTokenId(grpcConfig.getTokenId());
            config.setStatus(1);
            config.setDailyTask(grpcConfig.getDailyTask());
            config.setExchangeId(grpcConfig.getExchangeId());
            brokerTaskConfigMapper.updateByPrimaryKeySelective(config);
        } else {
            config = BrokerTaskConfig.builder().build();
            BeanCopyUtils.copyPropertiesIgnoreNull(grpcConfig, config);
            config.setCreated(System.currentTimeMillis());
            config.setUpdated(System.currentTimeMillis());
            config.setActionContent(actionContent);
            brokerTaskConfigMapper.insertSelective(config);
        }
        return SaveBrokerTaskConfigReply.newBuilder().setResult(true).build();
    }

    //币对定时开盘任务
    @Scheduled(cron = "0/8 * * * * ?")
    public void symbolFixedTime() {

        List<BrokerTaskConfig> configs = getExecTask(1);
        if (CollectionUtils.isEmpty(configs)) {
            return;
        }
        Collections.shuffle(configs);
        for (BrokerTaskConfig config : configs) {
            String lockKey = BrokerLockKeys.BROKER_TASK_CONFIG_LOCK_KEY + config.getId();
            boolean lock = RedisLockUtils.tryLock(redisTemplate, lockKey, BrokerLockKeys.BROKER_TASK_CONFIG_LOCK_KEY_EXPIRE);
            if (!lock) {
                continue;
            }
            try {
                ((AdminBrokerTaskConfigService) AopContext.currentProxy()).execSymbolTask(config.getId());
            } catch (Exception e) {
                log.info("execSymbolTask error", e);
            } finally {
                RedisLockUtils.releaseLock(redisTemplate, lockKey);
            }
        }
    }

    //币种定时开盘任务
    @Scheduled(cron = "0/8 * * * * ?")
    public void tokenFixedTime() {

        List<BrokerTaskConfig> configs = getExecTask(2);
        if (CollectionUtils.isEmpty(configs)) {
            return;
        }
        Collections.shuffle(configs);
        for (BrokerTaskConfig config : configs) {
            String lockKey = BrokerLockKeys.BROKER_TASK_CONFIG_LOCK_KEY + config.getId();
            boolean lock = RedisLockUtils.tryLock(redisTemplate, lockKey, BrokerLockKeys.BROKER_TASK_CONFIG_LOCK_KEY_EXPIRE);
            if (!lock) {
                continue;
            }
            try {
                Token token = ((AdminBrokerTaskConfigService) AopContext.currentProxy()).execTokenTask(config.getId());
                if (token != null) {
                    adminTokenGrpcService.syncBrokerExchangeToken(token.getOrgId(),token.getTokenId());
                }

            } catch (Exception e) {
                log.info("execTokenTask error", e);
            } finally {
                RedisLockUtils.releaseLock(redisTemplate, lockKey);
            }
        }
    }

    private List<BrokerTaskConfig> getExecTask(Integer type) {

        Example example = Example.builder(BrokerTaskConfig.class)
                .orderByAsc("id")
                .build();

        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("status", 1);
        criteria.andEqualTo("type", type);
        criteria.andBetween("actionTime", System.currentTimeMillis() - 3600_000L * 24, System.currentTimeMillis());

        return brokerTaskConfigMapper.selectByExample(example);
    }


    @Transactional(rollbackFor = Exception.class, isolation = Isolation.READ_COMMITTED)
    public void execSymbolTask(long taskId) {
        BrokerTaskConfig config = brokerTaskConfigMapper.selectForUpdate(taskId);
        log.info("config:{}", config);
        if (config.getStatus() != 1 || config.getActionTime() > System.currentTimeMillis()) {
            log.error("symbol task was executed. id:{}", taskId);
            return;
        }
        Map<String, String> actionMap = JsonUtil.defaultGson().fromJson(config.getActionContent(), Map.class);
        if (!publishedInExchange(config.getExchangeId(), config.getSymbolId(), config.getOrgId())) {
            log.error("execSymbolTask {} failed, not publishedInExchange!", taskId);
            sendSymbolTaskFailedNotice(config.getOrgId(), MapUtils.getString(actionMap, "areaCode"),
                    MapUtils.getString(actionMap, "phone"), MapUtils.getString(actionMap, "email"),
                    MapUtils.getString(actionMap, "language"));
            return;
        }

        boolean dailyTask = config.getDailyTask() == 1;


        Symbol symbol = symbolMapper.getBySymbolId(config.getExchangeId(), config.getSymbolId(), config.getOrgId());
        boolean published = MapUtils.getBoolean(actionMap, "published", false);
        boolean showStatus = MapUtils.getBoolean(actionMap, "showStatus", false);
        boolean banSellStatus = MapUtils.getBoolean(actionMap, "banSellStatus", false);
        boolean banBuyStatus = MapUtils.getBoolean(actionMap, "banBuyStatus", false);

        symbol.setStatus(published ? 1 : 0);
        symbol.setShowStatus(showStatus ? 1 : 0);
        symbol.setBanSellStatus(banSellStatus ? 1 : 0);
        symbol.setBanBuyStatus(banBuyStatus ? 1 : 0);
        symbol.setUpdated(System.currentTimeMillis());
        if (banBuyStatus && banSellStatus) {
            symbol.setAllowTrade(0);
        } else {
            symbol.setAllowTrade(1);
        }
        if(!published){
            //下架，关闭币对杠杆交易
            symbol.setAllowMargin(0);
        }
        symbolMapper.updateByPrimaryKeySelective(symbol);

        if (!published) {
//            orderService.orgBatchCancelOrder(Header.newBuilder().setOrgId(config.getOrgId()).build(), config.getSymbolId());
//            log.info("orgBatchCancelOrder org:{} symbol:{}", config.getOrgId(), config.getSymbolId());
        }
        if (!published) { //币对下架
            QueryMarginSymbolRequest marginSymbolRequest = QueryMarginSymbolRequest.newBuilder()
                    .setBaseRequest(BaseRequest.newBuilder().setOrganizationId(symbol.getOrgId()).build())
                    .setSymbolId(symbol.getSymbolId())
                    .build();
            QueryMarginSymbolReply marginSymbolReply = grpcMarginService.queryMarginSymbolReply(marginSymbolRequest);
            //为杠杆币对且开通交易，下架时同步关闭杠杆交易
            if (marginSymbolReply.getSymbolsCount() > 0 && marginSymbolReply.getSymbols(0).getAllowTrade() == 1) {
                SetSymbolConfigRequest setSymbolConfigRequest = SetSymbolConfigRequest.newBuilder()
                        .setBaseRequest(BaseRequest.newBuilder().setOrganizationId(symbol.getOrgId()).build())
                        .setSymbolId(symbol.getSymbolId())
                        .setAllowTrade(2)
                        .build();
                grpcMarginService.setSymbolConfig(setSymbolConfigRequest);
            }
        }

        //周期任务下一次执行为明天这个时间
        if (dailyTask) {
            config.setActionTime(config.getActionTime() + 3600_000L * 24);
        } else {
            config.setStatus(2);
        }
        config.setUpdated(System.currentTimeMillis());
        int row = brokerTaskConfigMapper.updateByPrimaryKeySelective(config);
        if (row != 1) {
            log.error("execSymbolTask {} failed!", taskId);
            throw new BrokerException(BrokerErrorCode.PARAM_INVALID);
        }
        log.info("execSymbolTask {} success", taskId);
    }

    @Transactional(rollbackFor = Exception.class, isolation = Isolation.READ_COMMITTED)
    public Token execTokenTask(long taskId) {

        BrokerTaskConfig config = brokerTaskConfigMapper.selectForUpdate(taskId);
        log.info("config:{}", config);
        if (config.getStatus() != 1 || config.getActionTime() > System.currentTimeMillis()) {
            log.error("token task was executed. id:{}", taskId);
            return null;
        }

        Map<String, String> actionMap = JsonUtil.defaultGson().fromJson(config.getActionContent(), Map.class);

        boolean dailyTask = config.getDailyTask() == 1;

        Token token = tokenMapper.getToken(config.getOrgId(), config.getTokenId());
        boolean published = MapUtils.getBoolean(actionMap, "published", false);

        boolean withdrawStatus = MapUtils.getBoolean(actionMap, "withdrawStatus", false);
        boolean depositStatus = MapUtils.getBoolean(actionMap, "depositStatus", false);
        adminTokenService.publish(token.getTokenId(), published, token.getOrgId());
        adminTokenService.allowDeposit(token.getTokenId(), depositStatus, token.getOrgId());
        adminTokenService.allowWithdraw(token.getTokenId(), withdrawStatus, token.getOrgId());

        //token.setStatus(published ? 1 : 0);
        //token.setAllowDeposit(depositStatus ? 1 : 0);
        //token.setAllowWithdraw(withdrawStatus ? 1 : 0);
        //token.setUpdated(System.currentTimeMillis());
        //tokenMapper.updateByPrimaryKeySelective(token);

        //周期任务下一次执行为明天这个时间
        if (dailyTask) {
            config.setActionTime(config.getActionTime() + 3600_000L * 24);
        } else {
            config.setStatus(2);
        }
        config.setUpdated(System.currentTimeMillis());
        int row = brokerTaskConfigMapper.updateByPrimaryKeySelective(config);
        if (row != 1) {
            log.error("execTokenTask {} failed!", taskId);
            throw new BrokerException(BrokerErrorCode.PARAM_INVALID);
        }
        log.info("execTokenTask {} success", taskId);

        return tokenMapper.getToken(config.getOrgId(), config.getTokenId());
    }


    private boolean publishedInExchange(Long exchangeId, String symbolId, Long orgId) {
        int pageSize = 1000;
        for (int i = 1; i < 10; i++) {
            GetExchangeSymbolsRequest request = GetExchangeSymbolsRequest.newBuilder()
                    .setBaseRequest(BaseReqUtil.getBaseRequest(orgId))
                    .setExchangeId(exchangeId).setCurrent(i).setPageSize(pageSize)
                    .build();
            List<ExchangeSymbolDetail> details = grpcSymbolService.getExchangeSymbols(request);
            if (CollectionUtils.isEmpty(details)) {
                return false;
            }
            for (ExchangeSymbolDetail detail : details) {
                if (detail.getSymbolId().equals(symbolId)) {
                    return detail.getPublished();
                }
            }
            if (details.size() < pageSize) {
                return false;
            }
        }
        return false;
    }

    private void sendSymbolTaskFailedNotice(long orgId, String areaCode, String mobile, String email, String language) {
        JsonObject jsonObject = new JsonObject();

        if (!StringUtils.isEmpty(areaCode) && !StringUtils.isEmpty(mobile)) {
            noticeTemplateService.sendSmsNotice(Header.newBuilder().setOrgId(orgId).setLanguage(language).build(), 0L,
                    NoticeBusinessType.SYMBOL_TASK_FAILED, areaCode, mobile, jsonObject);
        }
        if (!StringUtils.isEmpty(email)) {
            noticeTemplateService.sendEmailNotice(Header.newBuilder().setOrgId(orgId).setLanguage(language).build(), 0L,
                    NoticeBusinessType.SYMBOL_TASK_FAILED, email, jsonObject);
        }

    }

}
