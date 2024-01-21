package io.bhex.broker.server.grpc.server.service;


import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.List;

import javax.annotation.Resource;

import io.bhex.broker.server.model.Country;
import io.bhex.broker.server.model.OtcLegalCurrency;
import io.bhex.broker.server.model.User;
import io.bhex.broker.server.primary.mapper.CountryMapper;
import io.bhex.broker.server.primary.mapper.OtcLegalCurrencyMapper;
import io.bhex.broker.server.primary.mapper.UserMapper;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class OtcLegalCurrencyService {

    @Resource
    private UserMapper userMapper;

    @Resource
    private OtcLegalCurrencyMapper otcLegalCurrencyMapper;

    @Resource
    private CountryMapper countryMapper;


    public void initUserLegalCurrency() {
        //获取到所有code 不为空的数据 刷一遍
        List<User> userList = userMapper.getNationalCodeList();
        if (CollectionUtils.isEmpty(userList)) {
            log.info("user list is null");
        }
        userList.forEach(user -> {
            try {
                if (this.otcLegalCurrencyMapper.getLegalCurrencyByUserId(user.getOrgId(), user.getUserId()) == null) {

                    Country country = countryMapper.queryCountryByCode(user.getNationalCode());
                    String currencyCode = country.getCurrencyCode();

                    OtcLegalCurrency otcLegalCurrency
                            = createUserLegalCurrencyV2(user.getOrgId(), user.getUserId(), currencyCode);
                    if (otcLegalCurrency != null) {
                        otcLegalCurrencyMapper.insert(otcLegalCurrency);
                    }
                }
            } catch (Exception ex) {
                log.info("init otc legal currency fail {}", ex);
            }
        });
    }

    public void bindingUserLegalCurrency(Long orgId, Long userId, String nationalCode) {
        OtcLegalCurrency otcLegalCurrency
                = createUserLegalCurrency(orgId, userId, nationalCode);
        if (otcLegalCurrency != null) {
            if (this.otcLegalCurrencyMapper.getLegalCurrencyByUserId(orgId, userId) == null) {
                otcLegalCurrencyMapper.insert(otcLegalCurrency);
            }
        } else {
            log.warn("binding user legal currency fail userId {}", userId);
        }
    }

    public OtcLegalCurrency queryUserLegalCurrencyInfo(Long orgId, Long userId) {
        return this.otcLegalCurrencyMapper.getLegalCurrencyByUserId(orgId, userId);
    }

    public int updateCodeByOrgIdAndUserId(String code, Long orgId, Long userId) {
        return this.otcLegalCurrencyMapper.updateCodeByOrgIdAndUserId(code, orgId, userId);
    }

    //废弃硬编码方式
    @Deprecated
    private OtcLegalCurrency createUserLegalCurrency(Long orgId, Long userId, String nationalCode) {
        OtcLegalCurrency otcLegalCurrency = new OtcLegalCurrency();
        otcLegalCurrency.setOrgId(orgId);
        otcLegalCurrency.setUserId(userId);
        switch (nationalCode) {
            case "86":
                otcLegalCurrency.setCode("CNY");
                break;
            case "886":
                otcLegalCurrency.setCode("CNY");
                break;
            case "853":
                otcLegalCurrency.setCode("CNY");
                break;
            case "852":
                otcLegalCurrency.setCode("CNY");
                break;
            case "1":
                otcLegalCurrency.setCode("USD");
                break;
            case "7":
                otcLegalCurrency.setCode("RUB");
                break;
            case "82":
                otcLegalCurrency.setCode("WON");
                break;
            case "84":
                otcLegalCurrency.setCode("VND");
                break;
            case "81":
                otcLegalCurrency.setCode("JPY");
                break;
            case "66":
                otcLegalCurrency.setCode("THB");
                break;
            default:
                otcLegalCurrency.setCode("USD");
                break;
        }
        return otcLegalCurrency;
    }

    public void bindingUserLegalCurrencyV2(Long orgId, Long userId, String currencyCode) {
        OtcLegalCurrency otcLegalCurrency
                = createUserLegalCurrencyV2(orgId, userId, currencyCode);
        if (otcLegalCurrency != null) {
            OtcLegalCurrency legalCurrency = this.otcLegalCurrencyMapper.getLegalCurrencyByUserId(orgId, userId);
            if (legalCurrency == null) {
                otcLegalCurrencyMapper.insert(otcLegalCurrency);
            } else if (!legalCurrency.getCode().equalsIgnoreCase(otcLegalCurrency.getCode())) {
                otcLegalCurrencyMapper.updateCodeByOrgIdAndUserId(otcLegalCurrency.getCode(), orgId, userId);
            }
        } else {
            log.warn("bindingUserLegalCurrencyV2,binding user legal currency fail userId {}", userId);
        }
    }

    private OtcLegalCurrency createUserLegalCurrencyV2(Long orgId, Long userId, String currencyCode) {
        OtcLegalCurrency otcLegalCurrency = new OtcLegalCurrency();
        otcLegalCurrency.setOrgId(orgId);
        otcLegalCurrency.setUserId(userId);
        otcLegalCurrency.setCode(currencyCode);

        return otcLegalCurrency;
    }
}