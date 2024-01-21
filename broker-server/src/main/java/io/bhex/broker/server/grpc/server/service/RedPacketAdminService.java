package io.bhex.broker.server.grpc.server.service;

import io.bhex.base.account.*;
import io.bhex.broker.server.domain.FunctionModule;
import io.bhex.broker.server.grpc.client.service.GrpcAccountService;
import io.bhex.broker.server.model.RedPacket;
import io.bhex.broker.server.model.RedPacketReceiveDetail;
import io.bhex.broker.server.model.RedPacketTheme;
import io.bhex.broker.server.model.RedPacketTokenConfig;
import io.bhex.broker.server.primary.mapper.RedPacketMapper;
import io.bhex.broker.server.primary.mapper.RedPacketReceiveDetailMapper;
import io.bhex.broker.server.primary.mapper.RedPacketThemeMapper;
import io.bhex.broker.server.primary.mapper.RedPacketTokenConfigMapper;
import io.bhex.broker.server.util.BaseReqUtil;
import org.apache.ibatis.session.RowBounds;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import tk.mybatis.mapper.entity.Example;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
public class RedPacketAdminService {

    public static final String RED_PACKET_TRANSIT_USER_ID_SUFFIX = "10001";

    @Resource
    private BrokerService brokerService;

    @Resource
    private BasicService basicService;

    @Resource
    private GrpcAccountService grpcAccountService;

    @Resource
    private RedPacketThemeMapper redPacketThemeMapper;

    @Resource
    private RedPacketTokenConfigMapper redPacketTokenConfigMapper;

    @Resource
    private RedPacketMapper redPacketMapper;

    @Resource
    private RedPacketReceiveDetailMapper redPacketReceiveDetailMapper;

    /**
     * 打开红包功能
     *
     * @param orgId
     * @throws Exception
     */
    public void openRedPacketFunction(Long orgId) throws Exception {
        if (BrokerService.checkModule(orgId, FunctionModule.RED_PACKET)) {
            return;
        }
        long transitUserId = Long.parseLong(orgId + RED_PACKET_TRANSIT_USER_ID_SUFFIX);
        SimpleCreateAccountRequest request = SimpleCreateAccountRequest.newBuilder()
                .setBaseRequest(BaseReqUtil.getBaseRequest(orgId))
                .setOrgId(orgId)
                .setUserId(Long.toString(transitUserId))
                .build();
        SimpleCreateAccountReply reply = grpcAccountService.createAccount(request);
        BindAccountRequest bindAccountRequest = BindAccountRequest.newBuilder()
                .setOrgId(orgId)
                .setAccountId(reply.getAccountId())
                .setAccountType(AccountType.RED_PACKET)
                .build();
        BindAccountReply bindAccountReply = grpcAccountService.bindAccount(bindAccountRequest);
        if (bindAccountReply.getResult() != BindAccountReply.Result.OK
                && bindAccountReply.getResult() != BindAccountReply.Result.BINDING) {
            throw new Exception("bind red packet transit account error: " + bindAccountReply.getResult().name());
        }
//        brokerService.updateFunctionConfig(orgId, FunctionModule.RED_PACKET, true);
    }

    /**
     * 获取主题配置
     *
     * @param orgId
     * @return
     */
    public List<RedPacketTheme> queryAllOrgTheme(Long orgId) {
        List<RedPacketTheme> themes = redPacketThemeMapper.select(RedPacketTheme.builder().orgId(orgId).build());
        if (themes == null || themes.size() == 0) {
            themes = redPacketThemeMapper.select(RedPacketTheme.builder().orgId(0L).build());
            themes = themes.stream().map(theme -> theme.toBuilder().id(0L).build()).collect(Collectors.toList());
        }
        return themes;
    }

    /**
     * 获取token配置
     *
     * @param orgId
     * @return
     */
    public List<RedPacketTokenConfig> queryAllOrgRedPacketTokenConfig(Long orgId) {
        return redPacketTokenConfigMapper.select(RedPacketTokenConfig.builder().orgId(orgId).build());
    }

    /**
     * 更改主题顺序
     *
     * @param orgId
     * @param themeOrderMap
     */
    public void changeThemeOrder(Long orgId, Map<Long, Integer> themeOrderMap) {
        for (Long themeId : themeOrderMap.keySet()) {
            redPacketThemeMapper.updateCustomOrder(orgId, themeId, themeOrderMap.get(themeId), System.currentTimeMillis());
        }
    }

    /**
     * 更改token顺序
     *
     * @param orgId
     * @param tokenConfigOrderMap
     */
    public void changeTokenConfigOrder(Long orgId, Map<Long, Integer> tokenConfigOrderMap) {
        for (Long themeId : tokenConfigOrderMap.keySet()) {
            redPacketTokenConfigMapper.updateCustomOrder(orgId, themeId, tokenConfigOrderMap.get(themeId), System.currentTimeMillis());
        }
    }

    /**
     * 保存主题
     *
     * @param redPacketTheme
     */
    public void saveOrUpdateRedPacketTheme(RedPacketTheme redPacketTheme) {
        if (redPacketTheme.getId() != null && redPacketTheme.getId() > 0) {
            redPacketTheme.setUpdated(System.currentTimeMillis());
            redPacketThemeMapper.updateByPrimaryKeySelective(redPacketTheme);
        } else {
            redPacketTheme.setCreated(System.currentTimeMillis());
            redPacketTheme.setUpdated(System.currentTimeMillis());
            redPacketThemeMapper.insertSelective(redPacketTheme);
        }
    }

    @Transactional(isolation = Isolation.READ_COMMITTED, propagation = Propagation.REQUIRED)
    public void saveOrUpdateRedPacketThemes(List<RedPacketTheme> redPacketThemes) {
        for (RedPacketTheme redPacketTheme : redPacketThemes) {
            if (redPacketTheme.getId() > 0) {
                RedPacketTheme themeInDB = redPacketThemeMapper.selectByPrimaryKey(redPacketTheme.getId());
                if (themeInDB == null || !themeInDB.getOrgId().equals(redPacketTheme.getOrgId())) {
                    continue;
                } else {
                    redPacketTheme.setUpdated(System.currentTimeMillis());
                    redPacketThemeMapper.updateByPrimaryKeySelective(redPacketTheme);
                }
            } else {
                redPacketTheme.setCreated(System.currentTimeMillis());
                redPacketTheme.setUpdated(System.currentTimeMillis());
                redPacketThemeMapper.insertSelective(redPacketTheme);
            }
        }
    }

    /**
     * 保存token配置
     *
     * @param redPacketTokenConfig
     */
    public void saveOrUpdateRedPacketTokenConfig(RedPacketTokenConfig redPacketTokenConfig) {
        redPacketTokenConfig.setTokenName(basicService.getTokenName(redPacketTokenConfig.getOrgId(), redPacketTokenConfig.getTokenId()));
        if (redPacketTokenConfig.getId() != null && redPacketTokenConfig.getId() > 0) {
            redPacketTokenConfig.setUpdated(System.currentTimeMillis());
            redPacketTokenConfigMapper.updateByPrimaryKeySelective(redPacketTokenConfig);
        } else {
            redPacketTokenConfig.setCreated(System.currentTimeMillis());
            redPacketTokenConfig.setUpdated(System.currentTimeMillis());
            redPacketTokenConfigMapper.insertSelective(redPacketTokenConfig);
        }
    }

    /**
     * 获取红包列表
     *
     * @param orgId
     * @param userId
     * @param fromId
     * @param limit
     * @return
     */
    public List<RedPacket> queryRedPacketList(Long orgId, Long userId, Long fromId, Integer limit) {
        Example example = Example.builder(RedPacket.class).orderByDesc("id").build();
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("orgId", orgId);
        if (userId > 0) {
            criteria.andEqualTo("userId", userId);
        }
        if (fromId > 0) {
            criteria.andLessThan("id", fromId);
        }
        return redPacketMapper.selectByExampleAndRowBounds(example, new RowBounds(0, limit));
    }

    /**
     * 获取红包收取列表
     *
     * @param orgId
     * @param receiverUserId
     * @param redPacketId
     * @param fromId
     * @param limit
     * @return
     */
    public List<RedPacketReceiveDetail> queryRedPacketReceiveDetailList(Long orgId, Long receiverUserId, Long redPacketId, Long fromId, Integer limit) {
        Example example = Example.builder(RedPacketReceiveDetail.class).orderByDesc("created").build();
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("orgId", orgId);
        criteria.andGreaterThan("receiverUserId", 0);
        if (receiverUserId > 0) {
            criteria.andEqualTo("receiverUserId", receiverUserId);
        }
        if (redPacketId > 0) {
            criteria.andEqualTo("redPacketId", redPacketId);
        }
        if (fromId > 0) {
            criteria.andLessThan("id", fromId);
        }
        return redPacketReceiveDetailMapper.selectByExampleAndRowBounds(example, new RowBounds(0, limit));
    }

}
