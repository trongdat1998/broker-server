package io.bhex.broker.server.grpc.server.service;

import io.bhex.broker.server.grpc.client.service.GrpcOtcService;
import io.bhex.broker.server.model.OtcSymbol;
import io.bhex.broker.server.primary.mapper.OtcSymbolMapper;
import io.bhex.broker.server.util.CommonUtil;
import io.bhex.ex.otc.GetOTCSymbolsRequest;
import io.bhex.ex.otc.GetOTCSymbolsResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author lizhen
 * @date 2018-11-04
 */
@Slf4j
@Service
public class OtcSymbolService {

    @Resource
    private OtcSymbolMapper otcSymbolMapper;

    @Resource
    private GrpcOtcService grpcOtcService;

    public List<OtcSymbol> getOtcSymbolList(Long exchangeId, Long orgId) {
/*        OtcSymbol example = OtcSymbol.builder()
            .status(OtcSymbol.AVAILABLE)
            .build();
        if (exchangeId != null && exchangeId > 0) {
            example.setExchangeId(exchangeId);
        }
        if (orgId != null && orgId > 0) {
            example.setOrgId(orgId);
        }
        return otcSymbolMapper.select(example);*/
        GetOTCSymbolsRequest request=GetOTCSymbolsRequest.newBuilder()
                .setExchangeId(CommonUtil.nullToDefault(exchangeId,0L))
                .setOrgId(CommonUtil.nullToDefault(orgId,0L))
                .build();

        GetOTCSymbolsResponse response=grpcOtcService.getBrokerSymbols(request);
        return response.getSymbolList().stream().map(i->{
            return OtcSymbol.builder()
                    .id(i.getId())
                    .orgId(i.getOrgId())
                    .tokenId(i.getTokenId())
                    .currencyId(i.getCurrencyId())
                    .exchangeId(i.getExchangeId())
                    .build();

        }).collect(Collectors.toList());
    }
}
