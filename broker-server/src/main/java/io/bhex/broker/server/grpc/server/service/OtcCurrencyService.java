package io.bhex.broker.server.grpc.server.service;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.bhex.broker.grpc.otc.OTCCurrency;
import io.bhex.broker.grpc.otc.OTCLanguage;
import io.bhex.broker.server.grpc.client.service.GrpcOtcService;
import io.bhex.broker.server.primary.mapper.OtcCurrencyMapper;
import io.bhex.broker.server.model.OtcCurrency;
import io.bhex.ex.otc.GetOTCCurrencysRequest;
import io.bhex.ex.otc.GetOTCCurrencysResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author lizhen
 * @date 2018-11-04
 */
@Slf4j
@Service
public class OtcCurrencyService {

    //@Autowired
    //private OtcCurrencyMapper otcCurrencyMapper;

    @Resource
    private GrpcOtcService grpcOtcService;

/*    public List<OtcCurrency> getOtcCurrencyList(Long orgId) {
        OtcCurrency example = OtcCurrency.builder()
                .status(OtcCurrency.AVAILABLE)
                .build();
        if (orgId != null && orgId > 0) {
            example.setOrgId(orgId);
        }
        return otcCurrencyMapper.select(example);
    }*/

    public List<io.bhex.ex.otc.OTCCurrency> getOtcCurrencyList(Long orgId) {

        GetOTCCurrencysRequest request=GetOTCCurrencysRequest.newBuilder()
                .setOrgId(Objects.isNull(orgId)?0L:orgId.longValue())
                .build();

        GetOTCCurrencysResponse response=grpcOtcService.getBrokerCurrencies(request);

        List<io.bhex.ex.otc.OTCCurrency> list=Lists.newArrayList();
        if(CollectionUtils.isEmpty(response.getCurrencyList())){
            return list;
        }

        return response.getCurrencyList();
    }

/*    public void createOtcCurrency(OtcCurrency otcCurrency) {
        OtcCurrency queryOtcCurrency = otcCurrencyMapper.queryOtcCurrency(
                otcCurrency.getCode(),
                otcCurrency.getLanguage(),
                otcCurrency.getOrgId());
        if (queryOtcCurrency == null) {
            otcCurrencyMapper.insertSelective(otcCurrency);
        }
    }*/
}
