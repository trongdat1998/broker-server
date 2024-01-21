package io.bhex.broker.server.grpc.server.service.staking;

import com.github.pagehelper.Page;
import com.github.pagehelper.PageHelper;
import io.bhex.broker.grpc.proto.AdminCommonResponse;
import io.bhex.broker.grpc.staking.*;
import io.bhex.broker.server.grpc.server.service.BasicService;
import io.bhex.broker.server.model.staking.StakingPool;
import io.bhex.broker.server.model.staking.StakingPoolLocal;
import io.bhex.broker.server.model.staking.StakingPoolRebate;
import io.bhex.broker.server.primary.mapper.StakingPoolLocalMapper;
import io.bhex.broker.server.primary.mapper.StakingPoolMapper;
import io.bhex.broker.server.primary.mapper.StakingPoolRebateMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;
import tk.mybatis.mapper.entity.Example;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@Service
public class StakingStakingAdminService {
    private final static String default_lang = "en_US";

    @Autowired
    BasicService basicService;

    @Resource
    StakingPoolMapper stakingPoolMapper;

    @Resource
    StakingPoolLocalMapper stakingPoolLocalMapper;

    @Resource
    StakingPoolRebateMapper stakingPoolRebateMapper;

    @Transactional(rollbackFor = Exception.class)
    public AdminSaveStakingPoolReply saveStakingPool(AdminSaveStakingPoolRequest request) {
        // chenhy todo
        AdminSaveStakingPoolReply.Builder builder = AdminSaveStakingPoolReply.newBuilder();

        return builder.setResult(true).setPoolId(0).build();

    }

    public AdminGetStakingPoolDetailReply getStakingPoolDetail(AdminGetStakingPoolDetailRequest request) {
        AdminGetStakingPoolDetailReply.Builder builder = AdminGetStakingPoolDetailReply.newBuilder();

        Example stakingExample = new Example(StakingPool.class);
        stakingExample.createCriteria().andEqualTo("orgId", request.getOrgId()).andEqualTo("id", request.getPoolId());
        StakingPool stakingPool = stakingPoolMapper.selectOneByExample(stakingExample);

        if (stakingPool == null) {
            return builder.build();
        }

        Example localExample = new Example(StakingPoolLocal.class);
        localExample.createCriteria().andEqualTo("orgId", stakingPool.getOrgId()).andEqualTo("stakingId", stakingPool.getId());
        List<StakingPoolLocal> localList = stakingPoolLocalMapper.selectByExample(localExample);
        //转为proto 的localinfo
        List<StakingPoolLocalInfo> localInfos = localList.stream().map(StakingUtils::convertStakingLocalInfo).collect(Collectors.toList());

        Example rebateExample = new Example(StakingPoolRebate.class);
        rebateExample.createCriteria().andEqualTo("orgId", stakingPool.getOrgId()).andEqualTo("stakingId", stakingPool.getId());
        List<StakingPoolRebate> rebateList = stakingPoolRebateMapper.selectByExample(rebateExample);
        //转为proto 的rebateInfo
        List<StakingPoolRebateInfo> rebateInfos = rebateList.stream().map(StakingUtils::convertStakingRebateInfo).collect(Collectors.toList());

        StakingPoolInfo stakingInfo = StakingPoolInfo.newBuilder()
                .setOrgId(stakingPool.getOrgId())
                .setId(stakingPool.getId())
                .setTokenId(stakingPool.getTokenId())
                .setInterestTokenId(stakingPool.getInterestTokenId())
                .setReferenceApr(stakingPool.getReferenceApr())
                .setLatestRate(stakingPool.getLatestRate().stripTrailingZeros().toPlainString())
                .setLowPosition(stakingPool.getLowPosition().stripTrailingZeros().toPlainString())
                .setAvgPosition(stakingPool.getAvgPosition().stripTrailingZeros().toPlainString())
                .setMaxPosition(stakingPool.getMaxPosition().stripTrailingZeros().toPlainString())
                .setCurPosition(stakingPool.getCurPosition().stripTrailingZeros().toPlainString())
                .setTotalRebateCoinAmount(stakingPool.getTotalRebateCoinAmount().stripTrailingZeros().toPlainString())
                .setSubscribeStartDate(stakingPool.getSubscribeStartDate())
                .setSubscribeEndDate(stakingPool.getSubscribeEndDate())
                .setInterestStartDate(stakingPool.getInterestStartDate())
                .setTags(stakingPool.getTags())
                .setSort(stakingPool.getSort())
                .setType(stakingPool.getType())
                .setDeductRate(stakingPool.getDeductRate().stripTrailingZeros().toPlainString())
                .setRebateCycle(stakingPool.getRebateCycle())
                .setIsShow(stakingPool.getIsShow())
                .setNeedKyc(stakingPool.getNeedKyc())
                .setVipLevel(stakingPool.getVipLevel())
                .setArrposid(stakingPool.getArrposid())
                .setCreatedAt(stakingPool.getCreatedAt())
                .setUpdatedAt(stakingPool.getUpdatedAt())
                .addAllLocalInfos(localInfos)
                .build();

        return builder.setMessage("success").setPoolInfo(stakingInfo).addAllRebates(rebateInfos).build();
    }

    public AdminGetStakingPoolListReply getStakingPoolList(AdminGetStakingPoolListRequest request) {
        AdminGetStakingPoolListReply.Builder builder = AdminGetStakingPoolListReply.newBuilder();

        //查出所有理财矿池
        Example stakingExample = new Example(StakingPool.class);
        stakingExample.orderBy("id").desc();
        stakingExample.createCriteria().andEqualTo("orgId", request.getOrgId());
        Page page = PageHelper.startPage(request.getPageNo(), request.getSize());
        List<StakingPool> stakingList = stakingPoolMapper.selectByExample(stakingExample);
        if (CollectionUtils.isEmpty(stakingList)) {
            return AdminGetStakingPoolListReply.newBuilder()
                    .setCode(0)
                    .setMessage("success")
                    .build();
        }

        List<Long> stakingIds = stakingList.stream().map(StakingPool::getId).collect(Collectors.toList());

        Example localExample = new Example(StakingPoolLocal.class);
        localExample.createCriteria().andEqualTo("orgId", request.getOrgId()).andIn("stakingId", stakingIds);
        List<StakingPoolLocal> localList = stakingPoolLocalMapper.selectByExample(localExample);
        Map<Long, List<StakingPoolLocal>> localMap = localList.stream().collect(Collectors.groupingBy(local -> local.getStakingId()));

        List<StakingPoolProfile> stakingProfileList = new ArrayList<>();

        stakingList.forEach(staking -> {
                    StakingPoolProfile stakingProfile = StakingPoolProfile.newBuilder()
                            .setPoolId(staking.getId())
                            .setPoolName(getStakingPoolName(localMap.get(staking.getId()), request.getLanguage()))
                            .setTokenId(staking.getTokenId())
                            .setTokenName(basicService.getTokenName(staking.getOrgId(), staking.getTokenId()))
                            .setReferenceApr(staking.getReferenceApr())
                            .setStartDate(staking.getSubscribeStartDate())
                            .setEndDate(staking.getSubscribeEndDate())
                            .setType(staking.getType())
                            .setIsShow(staking.getIsShow())
                            .build();
                    stakingProfileList.add(stakingProfile);
                }
        );
        return builder.setCode(0).setMessage("success").addAllPools(stakingProfileList).setTotal((int) page.getTotal()).build();
    }

    public AdminCommonResponse onlineStakingPool(AdminOnlineStakingPoolRequest request) {
        long stakingId = request.getPoolId();
        long orgId = request.getOrgId();
        int isShow = request.getOnlineStatus() ? 1 : 0;

        Example example = new Example(StakingPool.class);
        example.createCriteria().andEqualTo("id", stakingId).andEqualTo("orgId", orgId);

        StakingPool staking = new StakingPool();
        staking.setIsShow(isShow);

        stakingPoolMapper.updateByExampleSelective(staking, example);
        return AdminCommonResponse.newBuilder().setSuccess(true).build();
    }

    private String getStakingPoolName(List<StakingPoolLocal> localList, String language) {
        if (CollectionUtils.isEmpty(localList)) {
            return "";
        } else {
            String firstName = "";
            String languageName = "";
            String defaultName = "";
            for (StakingPoolLocal local : localList) {
                if (firstName.equals("")) {
                    firstName = local.getStakingName();
                }

                if (local.getLanguage().equals(language)) {
                    languageName = local.getStakingName();
                    break;
                }
                if (local.getLanguage().equals(default_lang)) {
                    defaultName = local.getStakingName();
                }
            }
            return !languageName.equals("") ? languageName : (!defaultName.equals("") ? defaultName : firstName);
        }
    }

}
