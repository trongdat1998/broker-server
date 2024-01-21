package io.bhex.broker.server.grpc.server.service;

import com.google.common.collect.Lists;
import io.bhex.base.account.*;
import io.bhex.base.idgen.api.ISequenceGenerator;
import io.bhex.broker.common.util.JsonUtil;
import io.bhex.broker.grpc.airdrop.AutoAirdropInfo;
import io.bhex.broker.grpc.airdrop.GetAutoAirdropInfoRequest;
import io.bhex.broker.grpc.airdrop.SaveAutoAirdropInfoRequest;
import io.bhex.broker.grpc.airdrop.SaveAutoAirdropInfoResponse;
import io.bhex.broker.server.domain.BrokerServerConstants;
import io.bhex.broker.server.grpc.client.service.GrpcBatchTransferService;
import io.bhex.broker.server.primary.mapper.AutoAirdropMapper;
import io.bhex.broker.server.primary.mapper.AutoAirdropTransferRecordMapper;
import io.bhex.broker.server.model.AutoAirdrop;
import io.bhex.broker.server.model.AutoAirdropTransferRecord;
import io.bhex.broker.server.util.BaseReqUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import tk.mybatis.mapper.entity.Example;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.util.List;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.grpc.server.service
 * @Author: ming.xu
 * @CreateDate: 30/11/2018 3:15 PM
 * @Copyright（C）: 2018 BHEX Inc. All rights reserved.
 */
@Slf4j
@Service
public class AutoAirdropService {

    @Autowired
    private AutoAirdropMapper autoAirdropMapper;

    @Resource(name = "stringRedisTemplate")
    private StringRedisTemplate redisTemplate;

    @Autowired
    private ISequenceGenerator sequenceGenerator;

    @Resource
    AutoAirdropTransferRecordMapper autoAirdropTransferRecordMapper;

    @Autowired
    GrpcBatchTransferService grpcBatchTransferService;

    public void registerAirdrop(Long brokerId, Long accountId) {
        try {

            AutoAirdrop autoAirdrop = this.getRunningAirdrop(brokerId);
            if (autoAirdrop == null) {
                return;
            }

            // 这里做一下判断， 如果用户已经有过转账记录， 那就拉倒吧。
            AutoAirdropTransferRecord recordCondition = AutoAirdropTransferRecord.builder()
                    .brokerId(brokerId)
                    .accountId(accountId)
                    .airdropType(autoAirdrop.getAirdropType())
                    .build();
            List<AutoAirdropTransferRecord> list = autoAirdropTransferRecordMapper.select(recordCondition);
            if (!CollectionUtils.isEmpty(list)) {
                return;
            }


            // 构建转账请求
            BatchTransferRequest request = this.buildTransferRequest(autoAirdrop, accountId);
            if (request == null) {
                return;
            }

            log.info(" registerAirdrop BatchTransferRequest:{} ", request);
            BatchTransferResponse batchTransferResponse = grpcBatchTransferService.batchTransfer(request);
            log.info("  registerAirdrop brokerId:{} account:{}  get {} = {}  result:{}",
                    brokerId, accountId, autoAirdrop.getTokenId(), autoAirdrop.getAirdropTokenNum().toPlainString(), batchTransferResponse);

        } catch (Exception e) {
            log.error(" registerAirdrop exception: brokerId:{} accountId:{}", brokerId, accountId, e);
        }
    }


    public BatchTransferRequest buildTransferRequest(AutoAirdrop autoAirdrop, Long accountId) {

        // 插入一条记录
        Long clientId = sequenceGenerator.getLong();
        AutoAirdropTransferRecord record = AutoAirdropTransferRecord.builder()
                .clientId(clientId)
                .brokerId(autoAirdrop.getBrokerId())
                .accountId(accountId)
                .airdropType(autoAirdrop.getAirdropType())
                .token(autoAirdrop.getTokenId())
                .amount(autoAirdrop.getAirdropTokenNum())
                .status(0)
                .createdAt(System.currentTimeMillis())
                .updatedAt(System.currentTimeMillis())
                .build();
        autoAirdropTransferRecordMapper.insert(record);

        // 构建转账 target item
        List<BatchTransferItem> itemList = Lists.newArrayList(BatchTransferItem.newBuilder()
                .setTargetAccountId(accountId)
                .setTargetOrgId(autoAirdrop.getBrokerId())
                .setAmount(autoAirdrop.getAirdropTokenNum().toPlainString())
                .setTokenId(autoAirdrop.getTokenId())
                .setTargetAccountType(io.bhex.base.account.AccountType.GENERAL_ACCOUNT)
                .setSubject(BusinessSubject.AIRDROP)
                .build());

        BatchTransferRequest transferRequest = BatchTransferRequest.newBuilder()
                .setBaseRequest(BaseReqUtil.getBaseRequest(autoAirdrop.getBrokerId()))
                .setClientTransferId(clientId)
                .setSourceOrgId(autoAirdrop.getBrokerId())
                .setSourceAccountType(AccountType.OPERATION_ACCOUNT)
                .addAllTransferTo(itemList)
                .setSubject(BusinessSubject.AIRDROP)
                .build();

        return transferRequest;
    }

    public AutoAirdrop getRunningAirdrop(Long brokerId) {

        String cacheKey = BrokerServerConstants.AUTO_AIRDROP_RUNNING_KEY + brokerId;
        String cacheString = redisTemplate.opsForValue().get(cacheKey);
        if (StringUtils.isNotBlank(cacheString)) {
            return JsonUtil.defaultGson().fromJson(cacheString, AutoAirdrop.class);
        }

        AutoAirdrop condition = AutoAirdrop.builder()
                .brokerId(brokerId)
                .status(1)
                .build();

        AutoAirdrop autoAirdrop = autoAirdropMapper.selectOne(condition);
        if (autoAirdrop == null) {
            return null;
        }

        // TODO 这里放了缓存，  记得清
        redisTemplate.opsForValue().set(cacheKey, JsonUtil.defaultGson().toJson(autoAirdrop));
        return autoAirdrop;
    }


    public SaveAutoAirdropInfoResponse saveAutoAirdrop(SaveAutoAirdropInfoRequest request) {
        AutoAirdrop autoAirdrop = getAutoAirdrop(request.getHeader().getOrgId());
        Boolean isNew = false;
        if (null == autoAirdrop) {
            autoAirdrop = new AutoAirdrop();
            autoAirdrop.setCreatedAt(System.currentTimeMillis());
            isNew = true;
        }

        // 无论更新还是新建都有更新的公共部分
        autoAirdrop.setBrokerId(request.getHeader().getOrgId());
        autoAirdrop.setTokenId(request.getAutoAirdrop().getTokenId());
        autoAirdrop.setAirdropType(request.getAutoAirdrop().getAirdropType());
        autoAirdrop.setAccountType(request.getAutoAirdrop().getAccountType());
        autoAirdrop.setStatus(request.getAutoAirdrop().getStatus());
        if (StringUtils.isEmpty(request.getAutoAirdrop().getAirdropTokenNum())) {
            autoAirdrop.setAirdropTokenNum(new BigDecimal(0));
        } else {
            autoAirdrop.setAirdropTokenNum(new BigDecimal(request.getAutoAirdrop().getAirdropTokenNum()));
        }
        autoAirdrop.setUpdatedAt(System.currentTimeMillis());

        Boolean isOk;
        if (isNew) {
            isOk = autoAirdropMapper.insert(autoAirdrop) > 0 ? true : false;
        } else {
            isOk = autoAirdropMapper.updateByPrimaryKey(autoAirdrop) > 0 ? true : false;
        }

        // 如果更新成功，刷新缓存
        if (isOk) {
            String cacheKey = BrokerServerConstants.AUTO_AIRDROP_RUNNING_KEY + request.getHeader().getOrgId();
            redisTemplate.delete(cacheKey);
        }
        SaveAutoAirdropInfoResponse response = SaveAutoAirdropInfoResponse.newBuilder()
                .setRet(isOk)
                .build();
        return response;
    }

    public AutoAirdropInfo getAutoAirdrop(GetAutoAirdropInfoRequest request) {
        AutoAirdrop autoAirdrop = getAutoAirdrop(request.getHeader().getOrgId());
        AutoAirdropInfo.Builder builder = AutoAirdropInfo.newBuilder();
        if (null != autoAirdrop) {
            BeanUtils.copyProperties(autoAirdrop, builder);
            builder.setAirdropTokenNum(autoAirdrop.getAirdropTokenNum().stripTrailingZeros().toPlainString());
        }

        return builder.build();
    }

    private AutoAirdrop getAutoAirdrop(Long brokerId) {
        Example example = new Example(AutoAirdrop.class);
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("brokerId", brokerId);
        return autoAirdropMapper.selectOneByExample(example);
    }
}
