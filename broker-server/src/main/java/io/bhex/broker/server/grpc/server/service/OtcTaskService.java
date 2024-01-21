package io.bhex.broker.server.grpc.server.service;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.JsonObject;

import io.bhex.ex.proto.OrderSideEnum;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.security.SecureRandom;
import java.util.*;
import java.util.stream.Collectors;

import javax.annotation.Resource;

import io.bhex.broker.grpc.common.Header;
import io.bhex.broker.server.domain.OTCMsgCode;
import io.bhex.broker.server.grpc.client.service.GrpcOtcService;
import io.bhex.broker.server.model.Broker;
import io.bhex.broker.server.model.OtcSymbol;
import io.bhex.broker.server.model.OtcWhiteList;
import io.bhex.broker.server.model.User;
import io.bhex.broker.server.model.UserVerify;
import io.bhex.broker.server.primary.mapper.OtcWhiteListMapper;
import io.bhex.broker.server.primary.mapper.UserVerifyMapper;
import io.bhex.broker.server.util.OrgConfigUtil;
import io.bhex.ex.otc.BrokerExt;
import io.bhex.ex.otc.OTCGetMessageRequest;
import io.bhex.ex.otc.OTCGetMessageResponse;
import io.bhex.ex.otc.OTCResult;
import io.bhex.ex.proto.BaseRequest;
import lombok.extern.slf4j.Slf4j;
import tk.mybatis.mapper.util.StringUtil;

@Slf4j
@Service
public class OtcTaskService {

    @Autowired
    OtcSymbolService otcSymbolService;

    @Autowired
    BrokerService brokerService;

    @Autowired
    GrpcOtcService grpcOtcService;

    @Autowired
    UserService userService;

    @Autowired
    NoticeTemplateService noticeTemplateService;

    @Resource
    UserVerifyMapper userVerifyMapper;

    @Resource
    private OtcWhiteListMapper otcWhiteListMapper;

    //列表内短信邮件只给用户发
    private static final List<OTCMsgCode> OTC_MSG_CODE_LIMIT_LIST = Arrays.asList(
            OTCMsgCode.BUY_APPEAL_MSG_TO_BUYER, OTCMsgCode.BUY_APPEAL_MSG_TO_SELLER,
            OTCMsgCode.CANCEL_MSG_TO_BUYER, OTCMsgCode.CANCEL_MSG_TO_SELLER,
            OTCMsgCode.SELL_APPEAL_MSG_TO_BUYER, OTCMsgCode.SELL_APPEAL_MSG_TO_SELLER,
            OTCMsgCode.ORDER_AUTO_CANCEL, OTCMsgCode.ORDER_AUTO_APPEAL_TO_BUYER,
            OTCMsgCode.ORDER_AUTO_APPEAL_TO_SELLER);

    @Scheduled(cron = "0/10 * * * * ?")
    public void handleMessages() {
        try {
            //TODO 临时的,后面考虑代理为true时合并数据
            Long orgId = 0L;
            List<OtcSymbol> otcSymbolList = otcSymbolService.getOtcSymbolList(null, orgId);
            if (CollectionUtils.isEmpty(otcSymbolList)) {
                return;
            }
            Set<Long> brokerIdSet = otcSymbolList.stream().map(OtcSymbol::getOrgId).collect(Collectors.toSet());
            List<Broker> brokerList = brokerService.queryBrokerList(Lists.newArrayList(brokerIdSet));
            if (CollectionUtils.isEmpty(brokerList)) {
                return;
            }
            for (Broker broker : brokerList) {
                while (true) {
                    OTCGetMessageResponse response = grpcOtcService.getMessage(OTCGetMessageRequest.newBuilder()
                            .setBaseRequest(BaseRequest.newBuilder().setOrgId(broker.getOrgId()).build())
                            .build());
                    if (response == null || response.getResult() == OTCResult.NO_DATA) {
                        break;
                    }
                    sendMessage(response);
                }
            }
        } catch (Exception e) {
            log.error("handle failed orders error", e);
        }
    }

    public void sendMessage(OTCGetMessageResponse response) {
        this.sendMessage(response, null);
    }

    public void sendMessage(OTCGetMessageResponse response, String locale) {
        OTCMsgCode otcMsgCode = OTCMsgCode.fromCode(response.getMsgCode());
        if (otcMsgCode == null) {
            return;
        }

        User user = userService.getUser(response.getUserId());
        if (user == null) {
            return;
        }
        if (!user.getOrgId().equals(response.getOrgId())) {
            return;
        }
        UserVerify userVerify = UserVerify.decrypt(userVerifyMapper.getByUserId(response.getUserId()));
        if (userVerify == null || !userVerify.isSeniorVerifyPassed()) {
            return;
        }

        String language = StringUtils.isNoneBlank(locale) ? locale : (userVerify.getNationality().equals(1L) ? Locale.CHINA.toString() : Locale.US.toString());
        Header header = Header.newBuilder()
                .setOrgId(response.getOrgId())
                .setUserId(user.getUserId())
                .setLanguage(language)
                .build();

        JsonObject paramJson = this.getOTCMessageJson(otcMsgCode, response);
        log.info("sendMessage orderId {} info {} messageCode {} contains {} ", response.getOrderId(), new Gson().toJson(paramJson), otcMsgCode.getName(), OTC_MSG_CODE_LIMIT_LIST.contains(otcMsgCode));
        if (paramJson == null) {
            return;
        }

        Map<String, String> pushUrlData = Maps.newHashMap();
        if (response.getOrderId() > 0 && response.getSide() != OrderSideEnum.UNKNOWN) {
            pushUrlData.put("ORDER_ID", response.getOrderId() + "");
            pushUrlData.put("ORDER_DIR", response.getSide() == OrderSideEnum.BUY ? "BUY" : "SELL");
        }

        String reqOrderId = response.getOrderId() > 0 ? response.getOrderId() + "_" + new SecureRandom().nextInt(10000) : "";
        OtcWhiteList otcWhiteList = this.otcWhiteListMapper.queryByUserId(response.getOrgId(), response.getUserId());
        //判断当前用户是不是商家 列表的只发普通用户
        if (OTC_MSG_CODE_LIMIT_LIST.contains(otcMsgCode)) {
            if (otcWhiteList == null) {
                if (StringUtil.isNotEmpty(user.getMobile())) {
                    noticeTemplateService.sendSmsNotice(header, user.getUserId(), otcMsgCode.getNoticeBusinessType(), user.getNationalCode(), user.getMobile(), paramJson, "otc");
                }
                if (StringUtil.isNotEmpty(user.getEmail())) {
                    noticeTemplateService.sendEmailNotice(header, user.getUserId(), otcMsgCode.getNoticeBusinessType(), user.getEmail(), paramJson, "otc");
                }
                noticeTemplateService.sendBizPushNotice(header, user.getUserId(), otcMsgCode.getNoticeBusinessType(), reqOrderId, paramJson, pushUrlData);
            }
        } else {
            if (StringUtil.isNotEmpty(user.getMobile())) {
                noticeTemplateService.sendSmsNotice(header, user.getUserId(), otcMsgCode.getNoticeBusinessType(), user.getNationalCode(), user.getMobile(), paramJson, "otc");
            }
            if (StringUtil.isNotEmpty(user.getEmail())) {
                noticeTemplateService.sendEmailNotice(header, user.getUserId(), otcMsgCode.getNoticeBusinessType(), user.getEmail(), paramJson, "otc");
            }
            noticeTemplateService.sendBizPushNotice(header, user.getUserId(), otcMsgCode.getNoticeBusinessType(), reqOrderId, paramJson, pushUrlData);
        }
    }

    public JsonObject getOTCMessageJson(OTCMsgCode otcMsgCode, OTCGetMessageResponse response) {
        BrokerExt brokerExt = BasicService.brokerExtMap.get(response.getOrgId());
        int cancelTime = 15;
        if (brokerExt != null && brokerExt.getCancelTime() > 0 && brokerExt.getIsShare() == 0) {
            cancelTime = brokerExt.getCancelTime();
        }
        log.info("getOTCMessageJson getCancelTime orgId {} cancelTime {} isShare {} otcCancelTime {} otcAppealTime {}",
                response.getOrgId(), brokerExt != null ? brokerExt.getCancelTime() : 15, brokerExt != null ? brokerExt.getIsShare() : 0, response.getCancelTime(), response.getAppealTime());
        JsonObject jsonObject = new JsonObject();
        switch (otcMsgCode) {
            case BUY_CREATE_MSG_TO_BUYER:
                jsonObject.addProperty("seller", response.getSeller());
                jsonObject.addProperty("amount", response.getAmount());
                jsonObject.addProperty("currency", response.getCurrencyId());
                jsonObject.addProperty("orderId", response.getOrderId());
                break;
            case BUY_CREATE_MSG_TO_SELLER:
//                jsonObject.addProperty("buyer", response.getBuyer());
//                jsonObject.addProperty("quantity", response.getQuantity());
//                jsonObject.addProperty("token", response.getTokenId());
                return null;
            case PAY_MSG_TO_SELLER:
                jsonObject.addProperty("buyer", response.getBuyer());
                jsonObject.addProperty("amount", response.getAmount());
                jsonObject.addProperty("currency", response.getCurrencyId());
                jsonObject.addProperty("orderId", response.getOrderId());
                break;
            case BUY_APPEAL_MSG_TO_BUYER:
                break;
            case BUY_APPEAL_MSG_TO_SELLER:
                jsonObject.addProperty("buyer", response.getBuyer());
                jsonObject.addProperty("orderId", response.getOrderId());
                break;
            case CANCEL_MSG_TO_BUYER:
                jsonObject.addProperty("quantity", response.getQuantity());
                jsonObject.addProperty("token", response.getTokenId());
                jsonObject.addProperty("orderId", response.getOrderId());
                break;
            case CANCEL_MSG_TO_SELLER:
                jsonObject.addProperty("buyer", response.getBuyer());
                jsonObject.addProperty("quantity", response.getQuantity());
                jsonObject.addProperty("token", response.getTokenId());
                jsonObject.addProperty("orderId", response.getOrderId());
                break;
            case FINISH_MSG_TO_BUYER:
                jsonObject.addProperty("seller", response.getSeller());
                jsonObject.addProperty("amount", response.getAmount());
                jsonObject.addProperty("currency", response.getCurrencyId());
                jsonObject.addProperty("quantity", response.getQuantity());
                jsonObject.addProperty("token", response.getTokenId());
                jsonObject.addProperty("orderId", response.getOrderId());
                break;
            case FINISH_MSG_TO_SELLER:
//                jsonObject.addProperty("quantity", response.getQuantity());
//                jsonObject.addProperty("token", response.getTokenId());
                return null;
            case SELL_CREATE_MSG_TO_BUYER:
                jsonObject.addProperty("seller", response.getSeller());
                jsonObject.addProperty("amount", response.getAmount());
                jsonObject.addProperty("currency", response.getCurrencyId());
                jsonObject.addProperty("orderId", response.getOrderId());
                break;
            case SELL_CREATE_MSG_TO_SELLER:
//                jsonObject.addProperty("quantity", response.getQuantity());
//                jsonObject.addProperty("token", response.getTokenId());
//                jsonObject.addProperty("buyer", response.getBuyer());
//                break;
                return null;
            case SELL_APPEAL_MSG_TO_BUYER:
                jsonObject.addProperty("seller", response.getSeller());
                jsonObject.addProperty("orderId", response.getOrderId());
                break;
            case BUY_CREATE_MSG_TO_BUYER_TIME:
//                jsonObject.addProperty("cancelTime", cancelTime);
//                jsonObject.addProperty("seller", response.getSeller());
//                jsonObject.addProperty("amount", response.getAmount());
//                jsonObject.addProperty("currency", response.getCurrencyId());
                return null;
            case SELL_CREATE_MSG_TO_BUYER_TIME:
                jsonObject.addProperty("cancelTime", cancelTime);
                jsonObject.addProperty("seller", response.getSeller());
                jsonObject.addProperty("amount", response.getAmount());
                jsonObject.addProperty("currency", response.getCurrencyId());
                jsonObject.addProperty("orderId", response.getOrderId());
                break;
            case SELL_APPEAL_MSG_TO_SELLER:
                break;
            case ORDER_AUTO_CANCEL:
                break;
            case ORDER_AUTO_APPEAL_TO_BUYER:
                break;
            case ORDER_AUTO_APPEAL_TO_SELLER:
                break;
            case ITEM_AUTO_OFFLINE_SMALL_QUANTITY:
                return null;
            default:
                break;
        }
        return jsonObject;
    }

}
