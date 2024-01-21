package io.bhex.broker.server.grpc.server.service;

import io.bhex.broker.server.grpc.interceptor.RouteAuthCredentials;
import io.bhex.broker.server.util.OrgConfigUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Set;

/**
 * @author wangsc
 * @description
 * @date 2020-06-06 17:36
 */
@Slf4j
@Service
public class BrokerAuthService {

    public RouteAuthCredentials.BrokerAuth getBrokerAuthByOrgId(Long orgId){
       return RouteAuthCredentials.getBrokerAuthByOrgId(orgId);
    }

    public Set<Long> getALLAuthOrgIds(){
        return OrgConfigUtil.getOrgSets();
    }

}
