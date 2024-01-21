package io.bhex.broker.server.grpc.server.service;

import io.bhex.broker.grpc.otc.OTCToken;
import io.bhex.broker.server.grpc.client.service.GrpcOtcService;
import io.bhex.broker.server.primary.mapper.OtcTokenMapper;
import io.bhex.broker.server.model.OtcToken;
import io.bhex.ex.otc.GetOTCTokensRequest;
import io.bhex.ex.otc.GetOTCTokensResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author lizhen
 * @date 2018-11-04
 */
@Slf4j
@Service
public class OtcTokenService {

    //@Autowired
    //private OtcTokenMapper otcTokenMapper;

    @Resource
    private GrpcOtcService grpcOtcService;

    public List<OTCToken> getOtcTokenList(Long orgId) {
/*        if (orgId != null && orgId > 0) {
            return otcTokenMapper.queryTokenListByOrgId(orgId);
        }
        return otcTokenMapper.queryAllTokenList();*/
        GetOTCTokensRequest request=GetOTCTokensRequest.newBuilder()
                .setOrgId(Objects.isNull(orgId)?0L:orgId.longValue())
                .build();

        GetOTCTokensResponse response=grpcOtcService.getBrokerTokens(request);
        return response.getTokenList().stream().map(otcToken -> OTCToken.newBuilder()
                .setOrgId(otcToken.getOrgId())
                .setTokenId(otcToken.getTokenId())
                .setScale(otcToken.getScale())
                .setMinQuote(otcToken.getMinQuote())
                .setMaxQuote(otcToken.getMaxQuote())
                .setUpRange(otcToken.getUpRange())
                .setDownRange(otcToken.getDownRange())
                .setStatus(otcToken.getStatus())
                .setTokenName(otcToken.getTokenName())
                .build())
                .collect(Collectors.toList());
    }
}
