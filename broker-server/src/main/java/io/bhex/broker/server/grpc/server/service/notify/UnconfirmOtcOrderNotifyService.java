package io.bhex.broker.server.grpc.server.service.notify;

import com.github.pagehelper.PageHelper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.bhex.broker.server.grpc.client.service.GrpcOtcService;
import io.bhex.broker.server.primary.mapper.UnconfirmOtcOrderMapper;
import io.bhex.broker.server.model.UnconfirmOtcOrder;
import io.bhex.broker.server.util.RedisLockUtils;
import io.bhex.ex.otc.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.aop.framework.AopContext;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;
import tk.mybatis.mapper.entity.Example;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

@Slf4j
@Service
public class UnconfirmOtcOrderNotifyService extends AbstractAspect {

    private final static String UN_CONFIRM = "otc-unconfirm";

    @Resource
    private UnconfirmOtcOrderMapper unpaymentOtcOrderMapper;

    @Resource
    private GrpcOtcService grpcOtcService;

    private List<Integer> openStatus = Lists.newArrayList(OTCOrderStatusEnum.OTC_ORDER_APPEAL_VALUE,
            OTCOrderStatusEnum.OTC_ORDER_UNCONFIRM_VALUE);


    @Scheduled(initialDelay = 10000L,fixedRate = 30000L)
    public void diffOtcOrderStatus() {

        if(!prodProfile.contains(runEnv)){
            log.info("runEnv={},Doesn't match production profile.",runEnv);
            return;
        }

        Boolean locked = RedisLockUtils.tryLock(redisTemplate, "otc-order-status-sync", 10000L);
        if (!locked) {
            return;
        }

        try {

            int size=100;
            int page=0;

            List<UnconfirmOtcOrder> notifyOrder = Lists.newArrayList();
            Map<Long, OTCOrderStatusEnum> unmatchedStatusMap= Maps.newHashMap();

            do{
                page++;

                PageHelper.startPage(page,size);
                List<UnconfirmOtcOrder> list = listOpenOtcOrder();
                if (CollectionUtils.isEmpty(list)) {
                    break;
                }

                Map<Long, UnconfirmOtcOrder> orderMap = list.stream().collect(Collectors.toMap(i -> i.getOrderId(), i -> i));

                FindOrdersByOrderIdsRequest req = FindOrdersByOrderIdsRequest.newBuilder()
                        .addAllOrderIds(Lists.newArrayList(orderMap.keySet()))
                        .build();

                FindOrdersByOrderIdsReponse resp = grpcOtcService.listOtcOrderByOrderIds(req);
                if (resp.getResult() != OTCResult.SUCCESS) {
                    break;
                }

                if (CollectionUtils.isEmpty(resp.getOrdersList())) {
                    break;
                }

                List<OrderBrief> orders = resp.getOrdersList();
                Map<Long, OTCOrderStatusEnum> statusMap = orders.stream()
                        .collect(Collectors.toMap(i -> i.getOrderId(), i -> i.getOrderStatus()));

                statusMap.forEach((id, status) -> {
                    UnconfirmOtcOrder uoo = orderMap.get(id);
                    if(Objects.isNull(uoo)){
                        return;
                    }
                    if (!uoo.getNotifiedBoolean() &&
                            uoo.getStatusEnum() == status &&
                            status == OTCOrderStatusEnum.OTC_ORDER_UNCONFIRM) {

                        //时间比较
                        long dura=System.currentTimeMillis() - uoo.getCreateDate();
                        //10分钟未确认订单
                        if(dura>=1000*60*10){
                            notifyOrder.add(uoo);
                        }
                    }else{
                        if(uoo.getStatusEnum()!=status){
                            unmatchedStatusMap.put(uoo.getId(),status);
                        }
                    }
                });

            }while(true);


            //异步同步本地状态
            ((UnconfirmOtcOrderNotifyService)AopContext.currentProxy()).syncOtcOrderStatus(unmatchedStatusMap);

            Map<Long, List<UnconfirmOtcOrder>> notifyGroup = notifyOrder.stream().collect(Collectors.groupingBy(i -> i.getOrgId()));

            notifyGroup.forEach((brokerId, tmpList) -> {
                String key = buildKey(UN_CONFIRM, brokerId.toString());
                log.info("NotifyService,UN_CONFIRM,Notify key={}", key);
                redisTemplate.opsForValue().set(key, tmpList.size() + "");
                cacheNotify(UN_CONFIRM, brokerId.toString());
            });

            //更新通知标记
            notifyOrder.forEach(order->{
                UnconfirmOtcOrder no = UnconfirmOtcOrder.builder()
                        .id(order.getId())
                        .notified((byte)1)
                        .updateDate(System.currentTimeMillis())
                        .build();

                unpaymentOtcOrderMapper.updateByPrimaryKeySelective(no);
            });

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            RedisLockUtils.releaseLock(redisTemplate, "otc-order-status-sync");
        }

    }


    //@Transactional用于生成切面对象
    @Transactional(rollbackFor = Exception.class)
    @Async
    public void syncOtcOrderStatus(Map<Long, OTCOrderStatusEnum> statusMap) {

        statusMap.forEach((id, status) -> {
            UnconfirmOtcOrder order = UnconfirmOtcOrder.builder()
                    .id(id)
                    .status(status.getNumber())
                    .updateDate(System.currentTimeMillis())
                    .build();

            unpaymentOtcOrderMapper.updateByPrimaryKeySelective(order);
        });
    }

    private List<UnconfirmOtcOrder> listOpenOtcOrder() {

        Example exp = new Example(UnconfirmOtcOrder.class);
        exp.createCriteria().andIn("status", openStatus);

        return unpaymentOtcOrderMapper.selectByExample(exp);
    }
}
