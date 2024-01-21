package io.bhex.broker.server.grpc.server.service;

import io.bhex.broker.server.primary.mapper.BrokerExchangeMapper;
import io.bhex.broker.server.model.BrokerExchange;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.grpc.server.service
 * @Author: ming.xu
 * @CreateDate: 22/10/2018 11:40 AM
 * @Copyright（C）: 2018 BHEX Inc. All rights reserved.
 */
@Service
public class BrokerExchangeService {

    @Autowired
    private BrokerExchangeMapper brokerExchangeMapper;

    public Boolean enableContract(Long brokerId, Long exchangeId, String exchangeName) {
        BrokerExchange contract = brokerExchangeMapper.getContract(brokerId, exchangeId);
        if (contract == null) {
            contract = new BrokerExchange();
            contract.setOrgId(brokerId);
            contract.setExchangeId(exchangeId);
            if (StringUtils.isNotEmpty(exchangeName)) {
                contract.setExchangeName(exchangeName);
            } else {
                contract.setExchangeName(new String());
            }
            contract.setStatus(BrokerExchange.ENABLE_STATUS);
            contract.setCreated(System.currentTimeMillis());
            brokerExchangeMapper.insert(contract);
            return true;
        } else {
            if (StringUtils.isNotEmpty(exchangeName)) {
                contract.setExchangeName(exchangeName);
            }
            contract.setStatus(BrokerExchange.ENABLE_STATUS);
            contract.setCreated(System.currentTimeMillis());
            brokerExchangeMapper.updateByPrimaryKey(contract);
            return true;
        }
    }

    public Boolean disableContract(Long brokerId, Long exchangeId, String exchangeName) {
        BrokerExchange contract = brokerExchangeMapper.getContract(brokerId, exchangeId);
        if (contract != null) {
            if (StringUtils.isNotEmpty(exchangeName)) {
                contract.setExchangeName(exchangeName);
            }
            contract.setStatus(BrokerExchange.DISABLE_STATUS);
            contract.setCreated(System.currentTimeMillis());
            brokerExchangeMapper.updateByPrimaryKey(contract);
        }
        return true;
    }
}
