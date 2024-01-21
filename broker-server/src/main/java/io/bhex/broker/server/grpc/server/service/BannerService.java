package io.bhex.broker.server.grpc.server.service;

import io.bhex.broker.grpc.admin.*;
import io.bhex.broker.grpc.banner.QueryBannerResponse;
import io.bhex.broker.grpc.common.Header;
import io.bhex.broker.server.grpc.server.service.po.BannerPO;
import io.bhex.broker.server.primary.mapper.BannerDetailMapper;
import io.bhex.broker.server.primary.mapper.BannerMapper;
import io.bhex.broker.server.model.Banner;
import io.bhex.broker.server.util.BeanCopyUtils;
import io.bhex.broker.server.util.PageUtil;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import javax.annotation.Resource;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.grpc.server.service
 * @Author: ming.xu
 * @CreateDate: 28/08/2018 3:59 PM
 * @Copyright（C）: 2018 BHEX Inc. All rights reserved.
 */
@Service
public class BannerService {

    private static final String URL_PREFIX = "https://static.nucleex.com/";

    @Resource
    private BannerMapper bannerMapper;

    @Resource
    private BannerDetailMapper bannerDetailMapper;

    private static final Integer BANNER_SHOW_COUNT = 6;

    //broker api
    public QueryBannerResponse queryBanner(Header header) {
        Long currentTimeMillis = System.currentTimeMillis();
        List<BannerPO> banners = bannerMapper.queryOrgBanners(header.getOrgId(), currentTimeMillis);
        QueryBannerResponse.Builder builder = QueryBannerResponse.newBuilder();
        if (!CollectionUtils.isEmpty(banners)) {
            List<io.bhex.broker.grpc.banner.BannerDetail> detailList =
                    banners.stream()
                            .map(b -> io.bhex.broker.grpc.banner.BannerDetail.newBuilder()
                                    .setOrgId(b.getBrokerId())
                                    .setDeviceType(b.getPlatform())
                                    .setLanguage(b.getLocale())
                                    .setTitle(b.getTitle())
                                    .setContent(b.getContent())
                                    .setImageUrl((b.getImageUrl().startsWith("http://") || b.getImageUrl().startsWith("https://")) ? b.getImageUrl() : URL_PREFIX + b.getImageUrl())
                                    .setH5ImageUrl(b.getH5ImageUrl())
                                    .setType(b.getType())
                                    .setPageUrl(b.getPageUrl())
                                    .setCustomOrder(b.getRank())
                                    .setPublisTime(b.getBeginAt())
                                    .setBannerPosition(b.getBannerPosition())
                                    .build())
                            .collect(Collectors.toList());
            builder.addAllBannerDetails(detailList);
        }
        return builder.build();
    }

    //admin

    public Integer count(Long brokerId, Integer platform, Integer bannerPosition) {
        return bannerMapper.countByBrokerId(brokerId, platform, bannerPosition);
    }

    public ListBannerReply listBanner(Integer current, Integer pageSize, Long brokerId, Integer platform, Integer bannerPosition) {
        Integer total = count(brokerId, platform, bannerPosition);
        PageUtil.Page page = PageUtil.pageCount(current, pageSize, total);

        ListBannerReply.Builder builder = ListBannerReply.newBuilder();
        List<Banner> banners = bannerMapper.listBanner(page.getStart(), page.getOffset(), brokerId, platform, bannerPosition);
        if (!CollectionUtils.isEmpty(banners)) {
            Map<Long, List<BannerLocalDetail>> bannerDetailMap = new HashMap<>();
            List<Long> bannerIds = new ArrayList<>();
            for (Banner b : banners) {
                bannerIds.add(b.getId());
                bannerDetailMap.put(b.getId(), new ArrayList<BannerLocalDetail>());
            }
            List<BannerDetail> result = new ArrayList<>();
            if (!CollectionUtils.isEmpty(bannerIds)) {
                List<io.bhex.broker.server.model.BannerDetail> bannerDetails = bannerDetailMapper.listDetailByBannerIds(bannerIds);
                for (io.bhex.broker.server.model.BannerDetail d : bannerDetails) {
                    BannerLocalDetail.Builder detailBulider = BannerLocalDetail.newBuilder();
                    BeanCopyUtils.copyPropertiesIgnoreNull(d, detailBulider);
                    bannerDetailMap.get(d.getBannerId()).add(detailBulider.build());
                }

                for (Banner b : banners) {
                    BannerDetail.Builder detailBuilder = BannerDetail.newBuilder();
                    BeanUtils.copyProperties(b, detailBuilder);
                    detailBuilder.addAllBannerLocalDetails(bannerDetailMap.get(b.getId()));
                    result.add(detailBuilder.build());
                }
            }
            builder.setCurrent(current)
                    .setPageSize(pageSize)
                    .setTotal(total)
                    .addAllBannerDetails(result);
        }

        return builder.build();
    }

    public BannerDetail getBannerById(Long bannerId, Long brokerId) {
        Banner bannerById = bannerMapper.getBannerById(brokerId, bannerId);

        BannerDetail.Builder replyBuilder = BannerDetail.newBuilder();
        if (null != bannerById) {
            BeanUtils.copyProperties(bannerById, replyBuilder);

            List<io.bhex.broker.server.model.BannerDetail> bannerDetails = bannerDetailMapper.getDetailByBannerId(bannerId);
            List<BannerLocalDetail> result = new ArrayList<>();
            if (!CollectionUtils.isEmpty(bannerDetails)) {
                for (io.bhex.broker.server.model.BannerDetail d : bannerDetails) {
                    BannerLocalDetail.Builder builder = BannerLocalDetail.newBuilder();
                    BeanCopyUtils.copyPropertiesIgnoreNull(d, builder);
                    result.add(builder.build());
                }
                replyBuilder.addAllBannerLocalDetails(result);
            }
        }

        return replyBuilder.build();
    }

    public CreateBannerReply createBanner(CreateBannerRequest request) {
        Boolean isOk = false;
        if (!CollectionUtils.isEmpty(request.getLocaleDetailsList())) {
            Banner banner = Banner.builder()
                    .brokerId(request.getBrokerId())
                    .adminUserId(request.getAdminUserId())
                    .rank(request.getRank())
                    .beginAt(request.getBeginAt())
                    .endAt(request.getEndAt())
                    .remark(request.getRemark())
                    .platform(request.getPlatform())
                    .bannerPosition(request.getBannerPosition())
                    .status(1)
                    .createdAt(new Timestamp(System.currentTimeMillis()))
                    .build();
            isOk = bannerMapper.insertSelective(banner) > 0 ? true : false;
            if (isOk) {
                for (SaveBannerLocalDetail bannerLD : request.getLocaleDetailsList()) {
                    io.bhex.broker.server.model.BannerDetail bannerDetail = io.bhex.broker.server.model.BannerDetail.builder()
                            .adminUserId(request.getAdminUserId())
                            .bannerId(banner.getId())
                            .brokerId(request.getBrokerId())
                            .remark(request.getRemark())
                            .title(bannerLD.getTitle())
                            .content(bannerLD.getContent())
                            .imageUrl(bannerLD.getImageUrl())
                            .h5ImageUrl(bannerLD.getH5ImageUrl())
                            .locale(bannerLD.getLocale())
                            .type(bannerLD.getType())
                            .pageUrl(bannerLD.getPageUrl())
                            .status(1)
                            .isDefault(0)
                            .createdAt(new Timestamp(System.currentTimeMillis()))
                            .build();
                    bannerDetailMapper.insertSelective(bannerDetail);
                }
            }
        }
        CreateBannerReply reply = CreateBannerReply.newBuilder()
                .setResult(isOk)
                .build();
        return reply;
    }

    public UpdateBannerReply updateBanner(UpdateBannerRequest request) {
        Boolean isOk = false;
        Banner banner = bannerMapper.getBannerById(request.getBrokerId(), request.getBannerId());
        if (!CollectionUtils.isEmpty(request.getLocaleDetailsList()) && null != banner) {
            // update banner info
            banner.setAdminUserId(request.getAdminUserId());
            banner.setRank(request.getRank());
            banner.setBeginAt(request.getBeginAt());
            banner.setEndAt(request.getEndAt());
            banner.setRemark(request.getRemark());
            banner.setPlatform(request.getPlatform());
            banner.setBannerPosition(request.getBannerPosition());
            isOk = bannerMapper.updateByPrimaryKey(banner) > 0 ? true : false;

            if (isOk) {
                //delete old banner detail
                bannerDetailMapper.deleteByBannerId(request.getBannerId(), request.getAdminUserId());
                for (SaveBannerLocalDetail bannerLD : request.getLocaleDetailsList()) {
                    io.bhex.broker.server.model.BannerDetail bannerDetail = io.bhex.broker.server.model.BannerDetail.builder()
                            .adminUserId(request.getAdminUserId())
                            .bannerId(banner.getId())
                            .brokerId(request.getBrokerId())
                            .remark(request.getRemark())
                            .title(bannerLD.getTitle())
                            .content(bannerLD.getContent())
                            .imageUrl(bannerLD.getImageUrl())
                            .h5ImageUrl(bannerLD.getH5ImageUrl())
                            .type(bannerLD.getType())
                            .pageUrl(bannerLD.getPageUrl())
                            .locale(bannerLD.getLocale())
                            .status(1)
                            .isDefault(0)
                            .createdAt(new Timestamp(System.currentTimeMillis()))
                            .build();
                    bannerDetailMapper.insert(bannerDetail);
                }
            }

        }
        UpdateBannerReply reply = UpdateBannerReply.newBuilder()
                .setResult(isOk)
                .build();
        return reply;
    }

    public DeleteBannerReply deleteBanner(DeleteBannerRequest request) {
        bannerMapper.deleteByBannerId(request.getBannerId(), request.getAdminUserId());
        bannerDetailMapper.deleteByBannerId(request.getBannerId(), request.getAdminUserId());
        return DeleteBannerReply.newBuilder().setResult(true).build();
    }
}
