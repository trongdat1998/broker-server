package io.bhex.broker.server.grpc.server.service;

import io.bhex.broker.grpc.admin.*;
import io.bhex.broker.grpc.announcement.QueryAnnouncementResponse;
import io.bhex.broker.grpc.common.Header;
import io.bhex.broker.server.grpc.server.service.po.AnnouncementPO;
import io.bhex.broker.server.primary.mapper.AnnouncementDetailMapper;
import io.bhex.broker.server.primary.mapper.AnnouncementMapper;
import io.bhex.broker.server.model.Announcement;
import io.bhex.broker.server.util.BeanCopyUtils;
import io.bhex.broker.server.util.PageUtil;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import javax.annotation.Resource;
import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.grpc.server.service
 * @Author: ming.xu
 * @CreateDate: 27/08/2018 3:09 PM
 * @Copyright（C）: 2018 BHEX Inc. All rights reserved.
 */
@Service
public class AnnouncementService {

    @Resource
    private AnnouncementMapper announcementMapper;

    @Resource
    private AnnouncementDetailMapper announcementDetailMapper;

    private static final Integer BANNER_SHOW_COUNT = 6;

    //broker api
    public QueryAnnouncementResponse queryAnnouncement(Header header) {
        Long currentTimeMillis = System.currentTimeMillis();
        List<AnnouncementPO> announcements = announcementMapper.queryOrgAnnouncements(header.getOrgId(), currentTimeMillis);
        QueryAnnouncementResponse.Builder builder = QueryAnnouncementResponse.newBuilder();
        if (!CollectionUtils.isEmpty(announcements)) {
            List<io.bhex.broker.grpc.announcement.AnnouncementDetail> detailList =
                    announcements.stream()
                            .map(b -> io.bhex.broker.grpc.announcement.AnnouncementDetail.newBuilder()
                                    .setOrgId(b.getBrokerId())
                                    .setDeviceType(b.getPlatform())
                                    .setChannel(b.getChannel())
                                    .setLanguage(b.getLocale())
                                    .setTitle(b.getTitle())
                                    .setType(b.getType())
                                    .setPageUrl(b.getPageUrl())
                                    .setCustomOrder(b.getRank())
                                    .setPublisTime(b.getBeginAt())
                                    .build())
                            .sorted(Comparator.comparing(io.bhex.broker.grpc.announcement.AnnouncementDetail::getCustomOrder).reversed())
                            .collect(Collectors.toList());
            builder.addAllAnnouncementDateils(detailList);
        }
        return builder.build();
    }

    public Integer count(Long brokerId, Integer platform) {
        return announcementMapper.countByBrokerId(brokerId, platform);
    }

    public ListAnnouncementReply listAnnouncement(Integer current, Integer pageSize, Long brokerId, Integer platform) {
        ListAnnouncementReply.Builder builder = ListAnnouncementReply.newBuilder();
        Integer total = count(brokerId, platform);
        PageUtil.Page page = PageUtil.pageCount(current, pageSize, total);

        List<Announcement> announcements = announcementMapper.listAnnouncement(page.getStart(), page.getOffset(), brokerId, platform);

        if (!CollectionUtils.isEmpty(announcements)) {
            Map<Long, List<AnnouncementLocaleDetail>> detailMap = new HashMap<>();
            List<Long> announcementIds = new ArrayList<>();
            for (Announcement b : announcements) {
                announcementIds.add(b.getId());
                detailMap.put(b.getId(), new ArrayList());
            }
            List<AnnouncementDetail> result = new ArrayList<>();
            if (!CollectionUtils.isEmpty(announcementIds)) {
                List<io.bhex.broker.server.model.AnnouncementDetail> announcementDetails = announcementDetailMapper.listDetailByAnnouncementIds(announcementIds);
                for (io.bhex.broker.server.model.AnnouncementDetail d : announcementDetails) {
                    AnnouncementLocaleDetail.Builder detailBulider = AnnouncementLocaleDetail.newBuilder();
                    BeanCopyUtils.copyPropertiesIgnoreNull(d, detailBulider);
                    detailMap.get(d.getAnnouncementId()).add(detailBulider.build());
                }

                for (Announcement b : announcements) {
                    AnnouncementDetail.Builder detailBuilder = AnnouncementDetail.newBuilder();
                    BeanUtils.copyProperties(b, detailBuilder);
                    detailBuilder.addAllLocaleDetails(detailMap.get(b.getId()));
                    result.add(detailBuilder.build());
                }
            }
            builder.setCurrent(current)
                    .setPageSize(pageSize)
                    .setTotal(total)
                    .addAllAnnouncementDetails(result);
        }
        return builder.build();
    }

    public AnnouncementDetail getAnnouncementById(Long announcementId, Long brokerId) {
        Announcement announcement = announcementMapper.getAnnouncementById(brokerId, announcementId);

        AnnouncementDetail.Builder replyBuilder = AnnouncementDetail.newBuilder();
        if (null != announcement) {
            BeanUtils.copyProperties(announcement, replyBuilder);

            List<io.bhex.broker.server.model.AnnouncementDetail> bannerDetails = announcementDetailMapper.getDetailByAnnouncementId(announcementId);
            List<AnnouncementLocaleDetail> result = new ArrayList<>();
            if (!CollectionUtils.isEmpty(bannerDetails)) {
                for (io.bhex.broker.server.model.AnnouncementDetail d : bannerDetails) {
                    AnnouncementLocaleDetail.Builder builder = AnnouncementLocaleDetail.newBuilder();
                    BeanUtils.copyProperties(d, builder);
                    result.add(builder.build());
                }
                replyBuilder.addAllLocaleDetails(result);
            }
        }

        return replyBuilder.build();
    }

    public CreateAnnouncementReply createAnnouncement(CreateAnnouncementRequest request) {
        Boolean isOk = false;
        if (!CollectionUtils.isEmpty(request.getLocaleDetailsList())) {
            Announcement announcement = Announcement.builder()
                    .adminUserId(request.getAdminUserId())
                    .brokerId(request.getBrokerId())
                    .status(1)
                    .platform(request.getPlatform())
                    .channel(request.getChannel())
                    .rank(request.getRank())
                    .beginAt(request.getBeginAt())
                    .endAt(request.getEndAt())
                    .createdAt(new Timestamp(System.currentTimeMillis()))
                    .channel(request.getChannel())
                    .build();
            isOk = announcementMapper.insert(announcement) > 0 ? true : false;

            if (isOk) {
                for (AnnouncementLocaleDetail announcementLD : request.getLocaleDetailsList()) {
                    io.bhex.broker.server.model.AnnouncementDetail detail = io.bhex.broker.server.model.AnnouncementDetail.builder()
                            .adminUserId(request.getAdminUserId())
                            .announcementId(announcement.getId())
                            .title(announcementLD.getTitle())
                            .content(announcementLD.getContent())
                            .locale(announcementLD.getLocale())
                            .type(announcementLD.getType())
                            .pageUrl(announcementLD.getPageUrl())
                            .status(1)
                            .isDefault(0)
                            .createdAt(new Timestamp(System.currentTimeMillis()))
                            .build();
                    announcementDetailMapper.insert(detail);
                }
            }
        }
        CreateAnnouncementReply reply = CreateAnnouncementReply.newBuilder()
                .setResult(isOk)
                .build();
        return reply;
    }

    public UpdateAnnouncementReply updateAnnouncement(UpdateAnnouncementRequest request) {
        Boolean isOk = false;
        Announcement announcement = announcementMapper.getAnnouncementById(request.getBrokerId(), request.getId());
        if (!CollectionUtils.isEmpty(request.getLocaleDetailsList()) && null != announcement) {
            announcement.setAdminUserId(request.getAdminUserId());
            announcement.setBrokerId(request.getBrokerId());
            announcement.setStatus(1);
            announcement.setPlatform(request.getPlatform());
            announcement.setChannel(request.getChannel());
            announcement.setRank(request.getRank());
            announcement.setBeginAt(request.getBeginAt());
            announcement.setEndAt(request.getEndAt());
            announcement.setChannel(request.getChannel());
            isOk = announcementMapper.updateByPrimaryKey(announcement) > 0 ? true : false;

            if (isOk) {
                //delete old details
                announcementDetailMapper.deleteByAnnouncementId(request.getId(), request.getAdminUserId());
                for (AnnouncementLocaleDetail announcementLD : request.getLocaleDetailsList()) {
                    io.bhex.broker.server.model.AnnouncementDetail detail = io.bhex.broker.server.model.AnnouncementDetail.builder()
                            .adminUserId(request.getAdminUserId())
                            .announcementId(announcement.getId())
                            .title(announcementLD.getTitle())
                            .content(announcementLD.getContent())
                            .locale(announcementLD.getLocale())
                            .type(announcementLD.getType())
                            .pageUrl(announcementLD.getPageUrl())
                            .status(1)
                            .isDefault(0)
                            .createdAt(new Timestamp(System.currentTimeMillis()))
                            .build();
                    announcementDetailMapper.insert(detail);
                }
            }
        }
        UpdateAnnouncementReply reply = UpdateAnnouncementReply.newBuilder()
                .setResult(isOk)
                .build();
        return reply;
    }

    public PushAnnouncementReply pushAnnouncement(Long announcementId, Long adminUserId, Long brokerId, Integer status) {
        Boolean isOk = announcementMapper.pushAnnouncement(announcementId, status, brokerId, adminUserId) > 0 ? true : false;
        PushAnnouncementReply reply = PushAnnouncementReply.newBuilder()
                .setAnnouncementId(announcementId)
                .setResult(isOk)
                .build();
        return reply;
    }

    public DeleteAnnouncementReply deleteAnnouncement(DeleteAnnouncementRequest request) {
        announcementMapper.deleteByAnnouncementId(request.getAnnouncementId(), request.getAdminUserId());
        announcementDetailMapper.deleteByAnnouncementId(request.getAnnouncementId(), request.getAdminUserId());

        return DeleteAnnouncementReply.newBuilder().setResult(true).build();
    }
}
