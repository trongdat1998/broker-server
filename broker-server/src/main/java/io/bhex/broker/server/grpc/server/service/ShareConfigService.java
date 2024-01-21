package io.bhex.broker.server.grpc.server.service;

import io.bhex.broker.grpc.common.Header;
import io.bhex.broker.grpc.order.*;
import io.bhex.broker.server.grpc.server.service.po.ShareConfigPO;
import io.bhex.broker.server.model.ShareConfig;
import io.bhex.broker.server.model.ShareConfigLocale;
import io.bhex.broker.server.primary.mapper.ShareConfigLocaleMapper;
import io.bhex.broker.server.primary.mapper.ShareConfigMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tk.mybatis.mapper.entity.Example;

import java.util.List;
import java.util.Objects;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.grpc.server.service
 * @Author: ming.xu
 * @CreateDate: 2019/6/27 8:50 PM
 * @Copyright（C）: 2019 BHEX Inc. All rights reserved.
 */
@Slf4j
@Service
public class ShareConfigService {

    public final static String DEFAULT_LANGUAGE = "en_US";

    @Autowired
    private ShareConfigMapper shareConfigMapper;

    @Autowired
    private ShareConfigLocaleMapper shareConfigLocaleMapper;

    public ShareConfigInfoByAdminReply shareConfigInfoByAdmin(ShareConfigInfoByAdminRequest request) {
        ShareConfigInfoByAdminReply.Builder builder = ShareConfigInfoByAdminReply.newBuilder();
        ShareConfig shareConfig = getShareConfig(request.getBrokerId());
        if (Objects.nonNull(shareConfig)) {
            builder.setId(shareConfig.getId());
            builder.setBrokerId(shareConfig.getBrokerId());
            builder.setLogoUrl(shareConfig.getLogoUrl());
            builder.setWatermarkImageUrl(shareConfig.getWatermarkImageUrl());
            builder.setStatus(shareConfig.getStatus());
            builder.setType(shareConfig.getType());
            builder.setAdminUserId(shareConfig.getAdminUserId());
            builder.setCreatedTime(shareConfig.getCreatedTime());
            builder.setUpdatedTime(shareConfig.getUpdatedTime());
            List<ShareConfigLocale> localeListcale = listShareConfigLocale(request.getBrokerId());
            localeListcale.forEach(info -> {
                ShareConfigLocaleInfo.Builder localeBuilder = ShareConfigLocaleInfo.newBuilder();
                localeBuilder.setTitle(info.getTitle());
                localeBuilder.setDescription(info.getDescription());
                localeBuilder.setDownloadUrl(info.getDownloadUrl());
                localeBuilder.setLanguage(info.getLanguage());
                builder.addLocaleInfo(localeBuilder);
            });
        }
        return builder.build();
    }

    public QueryShareConfigInfoReply shareConfigInfo(QueryShareConfigInfoRequest request) {
        QueryShareConfigInfoReply.Builder builder = QueryShareConfigInfoReply.newBuilder();
        Header header = request.getHeader();
        List<ShareConfigPO> shareConfigPOList = null;
        if (header.getOrgId() != 0) {
            shareConfigPOList = shareConfigMapper.listShareConfigByOrgId(header.getOrgId());
        } else {
            shareConfigPOList = shareConfigMapper.listAllShareConfig();
        }
        shareConfigPOList.forEach(po -> {
            builder.addShareConfigInfo(ShareConfigInfo.newBuilder()
                    .setBrokerId(po.getBrokerId())
                    .setLogoUrl(po.getLogoUrl())
                    .setWatermarkImageUrl(po.getWatermarkImageUrl())
                    .setTitle(po.getTitle())
                    .setDescription(po.getDescription())
                    .setDownloadUrl(po.getDownloadUrl())
                    .setLanguage(po.getLanguage())
            );
        });
        return builder.build();
    }

    @Transactional(rollbackFor = Exception.class)
    public SaveShareConfigInfoReply saveShareConfigInfo(SaveShareConfigInfoRequest request) {
        // clean old config
        deleteOldConfig(request.getBrokerId());

        ShareConfig shareConfig = new ShareConfig();
        shareConfig.setBrokerId(request.getBrokerId());
        shareConfig.setLogoUrl(request.getLogoUrl());
        shareConfig.setWatermarkImageUrl(request.getWatermarkImageUrl());
        shareConfig.setStatus(ShareConfig.ON_STATUS);
        shareConfig.setType(0);
        shareConfig.setAdminUserId(request.getAdminUserId());
        shareConfig.setCreatedTime(System.currentTimeMillis());
        shareConfig.setUpdatedTime(System.currentTimeMillis());
        shareConfigMapper.insert(shareConfig);

        List<ShareConfigLocaleInfo> localeInfoList = request.getLocaleInfoList();
        localeInfoList.forEach(info -> {
            ShareConfigLocale locale = new ShareConfigLocale();
            locale.setBrokerId(request.getBrokerId());
            locale.setTitle(info.getTitle());
            locale.setDescription(info.getDescription());
            locale.setDownloadUrl(info.getDownloadUrl());
            locale.setLanguage(info.getLanguage());
            locale.setStatus(ShareConfigLocale.ON_STATUS);
            locale.setAdminUserId(request.getAdminUserId());
            locale.setCreatedTime(System.currentTimeMillis());
            locale.setUpdatedTime(System.currentTimeMillis());
            shareConfigLocaleMapper.insert(locale);
        });

        return SaveShareConfigInfoReply.newBuilder().setRet(0).build();
    }

    private void deleteOldConfig(Long brokerId) {
        shareConfigMapper.deleteByBrokerId(brokerId);
        shareConfigLocaleMapper.deleteByBrokerId(brokerId);
    }

    private ShareConfig getShareConfig(Long brokerId) {
        Example example = Example.builder(ShareConfig.class).build();
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("brokerId", brokerId);
        criteria.andEqualTo("status", ShareConfig.ON_STATUS);
        ShareConfig shareConfig = shareConfigMapper.selectOneByExample(example);
        return shareConfig;
    }

    private ShareConfigLocale getShareConfigLocale(Long brokerId, String language) {
        Example example = Example.builder(ShareConfigLocale.class).build();
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("brokerId", brokerId);
        criteria.andEqualTo("status", ShareConfigLocale.ON_STATUS);
        criteria.andEqualTo("language", language);
        ShareConfigLocale shareConfig = shareConfigLocaleMapper.selectOneByExample(example);
        return shareConfig;
    }

    private List<ShareConfigLocale> listShareConfigLocale(Long brokerId) {
        Example example = Example.builder(ShareConfigLocale.class).build();
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("brokerId", brokerId);
        criteria.andEqualTo("status", ShareConfigLocale.ON_STATUS);
        List<ShareConfigLocale> shareConfig = shareConfigLocaleMapper.selectByExample(example);
        return shareConfig;
    }
}
