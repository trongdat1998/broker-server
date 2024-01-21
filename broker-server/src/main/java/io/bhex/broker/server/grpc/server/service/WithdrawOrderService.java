package io.bhex.broker.server.grpc.server.service;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import io.bhex.base.account.WithdrawalBrokerAuditEnum;
import io.bhex.broker.grpc.admin.BrokerDetail;
import io.bhex.broker.grpc.admin.VerfiedWithdrawOrder;
import io.bhex.broker.grpc.common.Header;
import io.bhex.broker.server.domain.NoticeBusinessType;
import io.bhex.broker.server.domain.UserVerifyStatus;
import io.bhex.broker.server.model.LoginLog;
import io.bhex.broker.server.model.User;
import io.bhex.broker.server.primary.mapper.LoginLogMapper;
import io.bhex.broker.server.primary.mapper.UserMapper;
import io.bhex.broker.server.primary.mapper.WithdrawOrderMapper;
import io.bhex.broker.server.primary.mapper.WithdrawOrderVerifyHistoryMapper;
import io.bhex.broker.server.model.WithdrawOrder;
import io.bhex.broker.server.model.WithdrawOrderVerifyHistory;
import io.bhex.broker.server.util.BeanCopyUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;
import tk.mybatis.mapper.entity.Example;

import javax.annotation.Resource;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @Description:
 * @Date: 2018/9/19 下午4:59
 * @Author: liwei
 * @Copyright（C）: 2018 BlueHelix Inc. All rights reserved.
 */

@Slf4j
@Service
public class WithdrawOrderService {

    @Resource
    private UserMapper userMapper;
    @Resource
    private WithdrawOrderMapper withdrawOrderMapper;
    @Resource
    private NoticeTemplateService noticeTemplateService;
    @Resource
    private WithdrawOrderVerifyHistoryMapper withdrawOrderVerifyHistoryMapper;
    @Resource
    LoginLogMapper loginLogMapper;

    public List<WithdrawOrder> queryOrders(long brokerId, long accountId, long fromOrderId,
                                           long endOrderId, String tokenId, long limit, Integer status) {
        return withdrawOrderMapper.queryOrders(brokerId, accountId, fromOrderId, endOrderId, tokenId, limit, status);
    }

    public WithdrawOrder queryOrderByBhOrderId(Long bhWithdrawalOrderId) {
        return withdrawOrderMapper.getByOrderId(bhWithdrawalOrderId);
    }

    @Transactional
    public boolean addVerifyHistory(Long orgId, Long userId, Long accountId,
                                    Long brokerWithdrawOrderId, String adminUserName
            , String remark, boolean passed, Integer failedReason, String refuseReason) {
        int verifyStatus = passed ? WithdrawalBrokerAuditEnum.PASS_VALUE : WithdrawalBrokerAuditEnum.NO_PASS_VALUE;
        WithdrawOrderVerifyHistory history = new WithdrawOrderVerifyHistory();
        history.setWithdrawOrderId(brokerWithdrawOrderId);
        history.setOrgId(orgId);
        history.setAdminUserName(adminUserName);
        history.setRemark(remark);
        history.setUserId(userId);
        history.setAccountId(accountId);
        history.setVerifyStatus(verifyStatus);
        history.setVerifyTime(new Timestamp(System.currentTimeMillis()));
        history.setFailedReason(failedReason);
        history.setRefuseReason(refuseReason);
        withdrawOrderVerifyHistoryMapper.insert(history);

        withdrawOrderMapper.updateStatusById(brokerWithdrawOrderId, verifyStatus, System.currentTimeMillis());
        return true;
    }

    public void sendVerifyNotify(WithdrawOrder withdrawOrder) {
        if (withdrawOrder.getStatus() != WithdrawalBrokerAuditEnum.NO_PASS_VALUE) {
            return;
        }
        User user = userMapper.getByOrgAndUserId(withdrawOrder.getOrgId(), withdrawOrder.getUserId());
        NoticeBusinessType businessType =  getVerifyNotifyBusinessType(withdrawOrder);
        Header header = Header.newBuilder().setOrgId(withdrawOrder.getOrgId()).setUserId(withdrawOrder.getUserId()).build();
        List<LoginLog> loginLogs = loginLogMapper.queryLastLoginLogs(withdrawOrder.getUserId(), 1);
        if (!CollectionUtils.isEmpty(loginLogs)) {
            header = header.toBuilder().setLanguage(Strings.nullToEmpty(loginLogs.get(0).getLanguage())).build();
        }
        if (!StringUtils.isEmpty(user.getEmail())) {
            noticeTemplateService.sendEmailNotice(header, withdrawOrder.getUserId(), businessType, user.getEmail(), null);
        }

        if (!StringUtils.isEmpty(user.getNationalCode()) && !StringUtils.isEmpty(user.getMobile())) {
            noticeTemplateService.sendSmsNotice(header, withdrawOrder.getUserId(), businessType, user.getNationalCode(), user.getMobile(), null);
        }

        noticeTemplateService.sendBizPushNotice(header, withdrawOrder.getUserId(), businessType,
                "withdraw_verify_" + withdrawOrder.getOrderId(), null, null);
    }

    /**
     * 101.提币失败：登录地址异常，无法确认提币安全性，请联系客服。
     * 102.提币失败：提币失败：客服致电未接通，请注意接听电话。
     * 103.提币失败：2FA绑定不全，无法确认提币安全性，请联系客服。
     * 104.提币失败：客服致电，接听人反馈不是本人操作。
     * 105.提币失败：为了保障您的资产安全，请联系客服。
     *
     * @param withdrawOrder
     * @return
     */
    private NoticeBusinessType getVerifyNotifyBusinessType(WithdrawOrder withdrawOrder){
        NoticeBusinessType businessType;
        switch (withdrawOrder.getFailedReason()){
            case 101:
                businessType = NoticeBusinessType.WITHDRAW_VERIFY_REJECTED_1;
                break;
            case 102:
                businessType = NoticeBusinessType.WITHDRAW_VERIFY_REJECTED_2;
                break;
            case 103:
                businessType = NoticeBusinessType.WITHDRAW_VERIFY_REJECTED_3;
                break;
            case 104:
                businessType = NoticeBusinessType.WITHDRAW_VERIFY_REJECTED_4;
                break;
            case 105:
                businessType = NoticeBusinessType.WITHDRAW_VERIFY_REJECTED_5;
                break;
            default:
                businessType = NoticeBusinessType.WITHDRAW_VERIFY_REJECTED;
                break;
        }
        return businessType;
    }

    public List<VerfiedWithdrawOrder> queryVerfiedOrders(long brokerId, long accountId, long fromOrderId,
                                                         long endOrderId, String tokenId, long limit, long bhWithdrawOrderId) {
        List<WithdrawOrderVerifyHistory> histories = withdrawOrderVerifyHistoryMapper.queryOrdersHistory(brokerId,
                accountId, fromOrderId, endOrderId, tokenId, limit, null, bhWithdrawOrderId);
        if (CollectionUtils.isEmpty(histories)) {
            return new ArrayList<>();
        }
        List<Long> orderIds = histories.stream().map(WithdrawOrderVerifyHistory::getWithdrawOrderId).collect(Collectors.toList());
        Example example = new Example(WithdrawOrder.class);
        example.createCriteria().andIn("id", orderIds);
        Map<Long, WithdrawOrder> withdrawOrderMap = new HashMap<>();
        List<WithdrawOrder> withdrawOrders = withdrawOrderMapper.selectByExample(example);
        if (!CollectionUtils.isEmpty(withdrawOrders)) {
            for (WithdrawOrder withdrawOrder : withdrawOrders) {
                withdrawOrderMap.put(withdrawOrder.getId(), withdrawOrder);
            }
        }
        List<VerfiedWithdrawOrder> grpcOrders = histories.stream().map(item -> {
            WithdrawOrder withdrawOrder = withdrawOrderMap.get(item.getWithdrawOrderId());

            VerfiedWithdrawOrder.Builder builder = VerfiedWithdrawOrder.newBuilder();
            if (withdrawOrder != null) {
                BeanCopyUtils.copyPropertiesIgnoreNull(withdrawOrder, builder);
                builder.setBhOrderId(withdrawOrder.getOrderId());
                builder.setQuantity(withdrawOrder.getQuantity().stripTrailingZeros().toPlainString() + "");
                builder.setArrivalQuantity(withdrawOrder.getArrivalQuantity().stripTrailingZeros().toPlainString() + "");
            }

            builder.setVerifyTime(item.getVerifyTime().getTime());
            builder.setAdminUserName(item.getAdminUserName());
            builder.setVerifyStatus(item.getVerifyStatus());
            builder.setFailedReason(item.getFailedReason());
            builder.setRefuseReason(item.getRefuseReason());
            builder.setRemark(item.getRemark());

            return builder.build();
        }).collect(Collectors.toList());

        return grpcOrders;
    }


}
