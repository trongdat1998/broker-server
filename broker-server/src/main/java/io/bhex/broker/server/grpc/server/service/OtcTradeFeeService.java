package io.bhex.broker.server.grpc.server.service;


import io.bhex.broker.server.primary.mapper.OtcTokenMapper;
import io.bhex.broker.server.primary.mapper.OtcTradeFeeRateMapper;
import io.bhex.broker.server.model.OtcTradeFeeRate;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.Date;

@Slf4j
@Service
public class OtcTradeFeeService {

    @Autowired
    private OtcTradeFeeRateMapper otcTradeFeeRateMapper;

    @Autowired
    private OtcTokenMapper otcTokenMapper;

    public OtcTradeFeeRate queryOtcTradeFeeByTokenId(Long orgId, String tokenId) {
        return otcTradeFeeRateMapper.queryOtcTradeFeeByTokenId(orgId, tokenId);
    }

    public void addOtcTradeFee(Long orgId, String tokenId,
                               BigDecimal makerFeeRate, BigDecimal takerFeeRate) {
        otcTradeFeeRateMapper.insert(OtcTradeFeeRate
                .builder()
                .orgId(orgId)
                .tokenId(tokenId)
                .makerFeeRate(makerFeeRate)
                .takerFeeRate(takerFeeRate)
                .createdAt(new Date())
                .updateAt(new Date())
                .deleted(1)
                .build()
        );
    }

    public void updateOtcTradeFee(Integer id, BigDecimal makerFeeRate, BigDecimal takerFeeRate) {
        otcTradeFeeRateMapper.updateByPrimaryKeySelective(
                OtcTradeFeeRate
                        .builder()
                        .id(id)
                        .makerFeeRate(makerFeeRate)
                        .takerFeeRate(takerFeeRate)
                        .updateAt(new Date())
                        .build()
        );
    }
}

