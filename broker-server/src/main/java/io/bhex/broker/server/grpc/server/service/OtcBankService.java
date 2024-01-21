package io.bhex.broker.server.grpc.server.service;

import java.util.List;

import io.bhex.broker.server.primary.mapper.OtcBankMapper;
import io.bhex.broker.server.model.OtcBank;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * @author lizhen
 * @date 2018-11-04
 */
@Slf4j
@Service
public class OtcBankService {

    @Autowired
    private OtcBankMapper otcBankMapper;

    public List<OtcBank> getOtcBankList(Long orgId) {
        OtcBank example = OtcBank.builder()
            .status(OtcBank.AVAILABLE)
            .build();
        if (orgId != null && orgId > 0) {
            example.setOrgId(orgId);
        }
        return otcBankMapper.select(example);
    }
}
