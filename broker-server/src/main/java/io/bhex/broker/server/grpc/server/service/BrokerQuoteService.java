package io.bhex.broker.server.grpc.server.service;

import io.bhex.broker.server.model.RealQuoteEngineAddress;
import io.bhex.broker.server.primary.mapper.RealQuoteEngineMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import tk.mybatis.mapper.entity.Example;

import javax.annotation.Resource;

/**
 * @author wangsc
 * @description quote服务
 * @date 2020-07-02 14:47
 */
@Slf4j
@Service
public class BrokerQuoteService {

    @Resource
    private RealQuoteEngineMapper realQuoteEngineMapper;

    /**
     * 获取真实的quoteEngine地址
     *
     * @return
     */
    public RealQuoteEngineAddress getRealQuoteEngineAddress(String platform, String host, int port) {
        RealQuoteEngineAddress realQuoteEngineAddress;
        if (platform == null || host == null) {
            realQuoteEngineAddress = null;
            log.info("getRealQuoteEngineAddress param error:platform={},host={},port={}", platform, host, port);
        } else {
            Example example = Example.builder(RealQuoteEngineAddress.class).build();
            Example.Criteria criteria = example.createCriteria();
            criteria.andEqualTo("platform", platform);
            criteria.andEqualTo("host", host);
            criteria.andEqualTo("port", port);
            realQuoteEngineAddress = realQuoteEngineMapper.selectOneByExample(example);
        }
        if (realQuoteEngineAddress == null) {
            //不存在时，按约定返回原有的信息
            realQuoteEngineAddress = new RealQuoteEngineAddress();
            realQuoteEngineAddress.setHost(host);
            realQuoteEngineAddress.setPort(port);
            realQuoteEngineAddress.setRealHost(host);
            realQuoteEngineAddress.setRealPort(port);
        }
        return realQuoteEngineAddress;
    }


}
