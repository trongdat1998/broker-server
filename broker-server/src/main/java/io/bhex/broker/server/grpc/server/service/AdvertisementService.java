package io.bhex.broker.server.grpc.server.service;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import io.bhex.broker.grpc.advertisement.GetAdvertisementOnlineListResponse;
import io.bhex.broker.server.domain.AdvertisementStatus;
import io.bhex.broker.server.primary.mapper.AdvertisementMapper;
import io.bhex.broker.server.model.Advertisement;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import javax.annotation.Resource;
import java.util.List;

@Slf4j
@Service
public class AdvertisementService {


    @Resource
    AdvertisementMapper advertisementMapper;

    public GetAdvertisementOnlineListResponse getAdvertisementOnlineList(Long brokerId, String locale) {
        List<Advertisement> list = this.getAdvertisementList(brokerId, locale, AdvertisementStatus.ONLINE);
        if (CollectionUtils.isEmpty(list)) {
            return GetAdvertisementOnlineListResponse.getDefaultInstance();
        }

        return GetAdvertisementOnlineListResponse.newBuilder()
                .addAllAdvertisements(this.buildAdvertisementList(list))
                .build();
    }


    public List<Advertisement> getAdvertisementList(Long brokerId, String locale, AdvertisementStatus status) {
        Advertisement condition = Advertisement.builder()
                .orgId(brokerId)
                .build();

        if (StringUtils.isNotBlank(locale)) {
            condition.setLocale(locale);
        }

        if (status != null) {
            condition.setStatus(status.getStatus());
        }

        List<Advertisement> list = advertisementMapper.select(condition);
        if (CollectionUtils.isEmpty(list)) {
            return Lists.newArrayList();
        }
        return list;
    }

    public List<io.bhex.broker.grpc.advertisement.Advertisement> buildAdvertisementList(List<Advertisement> list) {
        List<io.bhex.broker.grpc.advertisement.Advertisement> resultList = Lists.newArrayList();
        if (CollectionUtils.isEmpty(list)) {
            return resultList;
        }
        for (Advertisement advertisement : list) {
            resultList.add(this.buildAdvertisement(advertisement));
        }
        return resultList;
    }

    public io.bhex.broker.grpc.advertisement.Advertisement buildAdvertisement(Advertisement advertisement) {
        return io.bhex.broker.grpc.advertisement.Advertisement.newBuilder()
                .setTargetType(advertisement.getTargetType())
                .setPicUrl(advertisement.getPicUrl())
                .setTargetUrl(Strings.nullToEmpty(advertisement.getTargetUrl()))
                .setTargetSign(Strings.nullToEmpty(advertisement.getTargetSign()))
                .setTargetObject(Strings.nullToEmpty(advertisement.getTargetObject()))
                .setLocale(advertisement.getLocale())
                .build();
    }

}
