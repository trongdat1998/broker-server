package io.bhex.broker.server.grpc.server.service;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.bhex.base.DateUtil;
import io.bhex.base.account.ConvertRequest;
import io.bhex.base.account.ConvertResponse;
import io.bhex.base.idgen.api.ISequenceGenerator;
import io.bhex.base.proto.DecimalUtil;
import io.bhex.base.quote.GetKLineReply;
import io.bhex.base.quote.GetLatestKLineRequest;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.grpc.account.Balance;
import io.bhex.broker.grpc.common.Header;
import io.bhex.broker.grpc.convert.*;
import io.bhex.broker.grpc.user.GetUserInfoResponse;
import io.bhex.broker.grpc.user.User;
import io.bhex.broker.server.domain.BrokerLockKeys;
import io.bhex.broker.server.domain.ConvertOrderStatus;
import io.bhex.broker.server.domain.ConvertSymbolPriceType;
import io.bhex.broker.server.domain.ConvertSymbolStatus;
import io.bhex.broker.server.grpc.client.service.GrpcBatchTransferService;
import io.bhex.broker.server.grpc.client.service.GrpcQuoteService;
import io.bhex.broker.server.model.ConvertOrder;
import io.bhex.broker.server.model.ConvertSymbol;
import io.bhex.broker.server.model.*;
import io.bhex.broker.server.primary.mapper.*;
import io.bhex.broker.server.util.BaseReqUtil;
import io.bhex.broker.server.util.RedisLockUtils;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.session.RowBounds;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import tk.mybatis.mapper.entity.Example;
import tk.mybatis.mapper.util.StringUtil;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author cookie.yuan
 * @description
 * @date 2020-08-13
 */
@Slf4j
@Service
public class ConvertService {
    @Resource
    private ConvertSymbolMapper convertSymbolMapper;

    @Resource
    private ConvertOrderMapper convertOrderMapper;

    @Resource
    private ConvertUserRecordMapper convertUserRecordMapper;

    @Resource
    private ConvertBrokerRecordMapper convertBrokerRecordMapper;

    @Resource
    private SymbolMapper symbolMapper;

    @Resource
    private ISequenceGenerator iSequenceGenerator;

    @Resource
    GrpcQuoteService grpcQuoteService;

    @Resource
    UserService userService;

    @Resource
    AccountService accountService;

    @Resource
    TokenMapper tokenMapper;

    @Resource
    ConvertService convertService;

    @Resource
    private GrpcBatchTransferService grpcBatchTransferService;

    @Resource(name = "stringRedisTemplate")
    private StringRedisTemplate redisTemplate;

    // 单次申购任务订单执行数量
    private static final int PURCHASE_TASK_LIMIT = 100;
    // 数量精度值
    private static final int DECIMAL_SCALE = 18;

    private static Map<Long, ConvertSymbol> convertSymbolMap = new ConcurrentHashMap<>();

    /**
     * key: exchangeId_symbolId_klineId
     */
    private Cache<String, BigDecimal> cacheBuilder = CacheBuilder.newBuilder()
            .expireAfterWrite(2, TimeUnit.MINUTES)
            .build();

    private BigDecimal getKlineConvertPrice(Long orgId, Long exchangeId, String symbolId, Long kLineId) {
        String cacheKey = exchangeId + "_" + symbolId + "_" + kLineId;
        BigDecimal price = cacheBuilder.getIfPresent(cacheKey);
        if (price == null) {
            GetLatestKLineRequest getLatestKLineRequest = GetLatestKLineRequest.newBuilder()
                    .setBaseRequest(BaseReqUtil.getBaseRequest(orgId))
                    .setExchangeId(exchangeId)
                    .setSymbol(symbolId)
                    .setInterval("1m")
                    .setLimitCount(2)
                    .build();
            GetKLineReply kLineReply = grpcQuoteService.getLatestKLine(getLatestKLineRequest);
            kLineReply.getKlineList().stream().filter(kLine -> kLine.getId() == kLineId)
                    .findFirst()
                    .ifPresent(kline -> cacheBuilder.put(cacheKey, DecimalUtil.toBigDecimal(kline.getClose())));
            price = cacheBuilder.getIfPresent(cacheKey);
        }
        return price;
    }

    ConvertSymbol getConvertSymbol(Long convertSymbolId, Long brokerId) {
        ConvertSymbol symbol = convertSymbolMap.get(convertSymbolId);
        if (symbol != null && symbol.getBrokerId().equals(brokerId)) {
            return symbol;
        }
        symbol = convertSymbolMapper.getByConvertSymbolId(convertSymbolId, brokerId);
        if (symbol != null) {
            convertSymbolMap.put(symbol.getId(), symbol);
        }
        return symbol;
    }

    @Scheduled(cron = "0 0/1 * * * ?")
    public void loadConvertSymbolInfo() {
        // 定时从数据库种读取闪兑币对信息
        List<ConvertSymbol> symbolList = convertSymbolMapper.selectAll();
        symbolList.stream().forEach(symbol -> {
            convertSymbolMap.put(symbol.getId(), symbol);
        });
    }

    @Scheduled(cron = "0/5 * * * * ?")
    public void dealConvertTransfer() {
        // 定时调用平台进行闪兑订单转账
        boolean lock = RedisLockUtils.tryLock(redisTemplate, BrokerLockKeys.CONVERT_PURCHASE_LOCK_KEY,
                BrokerLockKeys.CONVERT_PURCHASE_LOCK_EXPIRE);
        if (!lock) {
            return;
        }
        try {
            List<ConvertOrder> orderList = convertOrderMapper.queryByStatus(ConvertOrderStatus.UNPAID.getStatus(), PURCHASE_TASK_LIMIT);
            for (ConvertOrder order : orderList) {
                ConvertSymbol convertSymbol = getConvertSymbol(order.getConvertSymbolId(), order.getBrokerId());
                if (convertSymbol == null) {
                    continue;
                }
                convertService.transferConvertOrder(order, convertSymbol);
            }
        } catch (Exception e) {
            log.error("dealConvertTransfer catch exception.", e);
        } finally {
            RedisLockUtils.releaseLock(redisTemplate, BrokerLockKeys.CONVERT_PURCHASE_LOCK_KEY);
        }
    }


    public List<ConvertSymbol> getConvertSymbols(Header header) {
        List<ConvertSymbol> symbolList = convertSymbolMapper.querySymbolsByBrokerId(header.getOrgId());
        return symbolList;
    }

    public List<String> queryOfferingsTokens(Header header) {
        List<String> offeringsTokenList = convertSymbolMapper.queryOfferingsTokens(header.getOrgId(),
                ConvertSymbolStatus.ENABLE.getStatus());
        return offeringsTokenList.stream().distinct().collect(Collectors.toList());
    }

    public List<String> queryPurchaseTokens(Header header) {
        List<String> purchaseTokenList = convertSymbolMapper.queryPurchaseTokens(header.getOrgId(),
                ConvertSymbolStatus.ENABLE.getStatus());
        return purchaseTokenList.stream().distinct().collect(Collectors.toList());
    }

    public GetConvertPriceResponse getConvertPrice(Header header, Long convertSymbolId) {
        ConvertSymbol convertSymbol = getConvertSymbol(convertSymbolId, header.getOrgId());
        if (convertSymbol == null) {
            throw new BrokerException(BrokerErrorCode.CONVERT_SYMBOL_NOT_FOUND);
        }
        if (convertSymbol.getPriceType().equals(ConvertSymbolPriceType.FIXED.getType())) {
            // 固定价格
            return GetConvertPriceResponse.newBuilder()
                    .setPriceType(convertSymbol.getPriceType())
                    .setPrice(DecimalUtil.toTrimString(convertSymbol.getPriceValue()))
                    .build();
        } else {
            // 浮动价格
            Symbol symbol = symbolMapper.getOrgSymbol(convertSymbol.getBrokerId(), convertSymbol.getSymbolId());
            if (symbol == null) {
                throw new BrokerException(BrokerErrorCode.CONVERT_SYMBOL_NOT_FOUND);
            }
            Long kLineId = getKLineId();
            BigDecimal convertPrice = getKlineConvertPrice(symbol.getOrgId(), symbol.getExchangeId(), symbol.getSymbolId(), kLineId);
            if (convertPrice == null) {
                // 未获取到上一分钟的Kline收盘价
                throw new BrokerException(BrokerErrorCode.CONVERT_SYMBOL_PRICE_IS_NULL);
            }
            // 判断是否需要颠倒价格
            BigDecimal priceRatio = convertSymbol.getPriceValue().divide(new BigDecimal(100), DECIMAL_SCALE, RoundingMode.DOWN);
            if (symbol.getBaseTokenId().equalsIgnoreCase(convertSymbol.getOfferingsTokenId())
                    && symbol.getQuoteTokenId().equalsIgnoreCase(convertSymbol.getPurchaseTokenId())) {
                // 如果发售币种为基础币种，且申购币种为计价币种，则不需要颠倒价格
                convertPrice = convertPrice.multiply(priceRatio).setScale(DECIMAL_SCALE);
            } else {
                // 颠倒价格计算兑换的价格比例
                convertPrice = convertPrice.compareTo(BigDecimal.ZERO) == 0 ? BigDecimal.ZERO : BigDecimal.ONE.divide(convertPrice, DECIMAL_SCALE, RoundingMode.DOWN)
                        .multiply(priceRatio).setScale(DECIMAL_SCALE);
            }
            // 价格有效时间
            int second = 60 - Calendar.getInstance().get(Calendar.SECOND);
            return GetConvertPriceResponse.newBuilder()
                    .setPriceType(convertSymbol.getPriceType())
                    .setPrice(DecimalUtil.toTrimString(convertPrice))
                    .setTime(second)
                    .build();
        }
    }

    public CreateConvertOrderResponse createConvertOrder(Header header, Long convertSymbolId, String clientOrderId,
                                                         String purchaseQuantity, String offeringsQuantity,
                                                         String price, String orderTokenId) {
        // 根据header获取用户信息
        GetUserInfoResponse userInfoResponse = userService.getUserInfo(header);
        if (userInfoResponse.getUser() == null) {
            throw new BrokerException(BrokerErrorCode.USER_NOT_EXIST);
        }
        User user = userInfoResponse.getUser();
        Long userAccountId = user.getDefaultAccountId();
        Long brokerId = header.getOrgId();
        // 判断用户是否已经存在对应的外部订单,如果已经存在则返回已有的订单号
        ConvertOrder oldOrder = convertOrderMapper.getByClientOrderId(brokerId, userAccountId, clientOrderId);
        if (oldOrder != null) {
            return CreateConvertOrderResponse.newBuilder()
                    .setOrderId(oldOrder.getOrderId())
                    .setSuccess(true)
                    .build();
        }
        ConvertSymbol convertSymbol = getConvertSymbol(convertSymbolId, brokerId);
        if (convertSymbol == null) {
            throw new BrokerException(BrokerErrorCode.CONVERT_SYMBOL_NOT_FOUND);
        }
        if (convertSymbol.getStatus().equals(ConvertSymbolStatus.DISABLE.getStatus())) {
            throw new BrokerException(BrokerErrorCode.CONVERT_SYMBOL_STATUS_FORBIDDEN);
        }
        // 闪电兑换不为固定价格，需要校验币对是否存在
        Symbol symbol = symbolMapper.getOrgSymbol(convertSymbol.getBrokerId(), convertSymbol.getSymbolId());
        if (!convertSymbol.getPriceType().equals(ConvertSymbolPriceType.FIXED.getType())) {
            // 不存在币对或币对为下架状态，则返回报错
            if (symbol == null || symbol.getStatus().equals(0)) {
                throw new BrokerException(BrokerErrorCode.ORDER_REQUEST_SYMBOL_INVALID);
            }
        }

        BigDecimal purchaseQty = new BigDecimal(purchaseQuantity);
        BigDecimal offeringsQty = new BigDecimal(offeringsQuantity);
        BigDecimal orderPrice = new BigDecimal(price);
        // 判断兑换订单是否有效
        checkOrder(user, convertSymbol, symbol, orderTokenId, offeringsQty, purchaseQty);
        // 查询用户资金是否够用
        Balance userBalance = accountService.queryTokenBalance(brokerId, userAccountId,
                convertSymbol.getPurchaseTokenId());
        if (userBalance != null) {
            BigDecimal userFreeQty = new BigDecimal(userBalance.getFree());
            if (userFreeQty.compareTo(purchaseQty) < 0) {
                throw new BrokerException(BrokerErrorCode.CONVERT_PURCHASE_INSUFFICIENT_BALANCE);
            }
        } else {
            throw new BrokerException(BrokerErrorCode.CONVERT_PURCHASE_INSUFFICIENT_BALANCE);
        }

        // 查询券商指定账户资金是否够用
        Balance brokerBalance = accountService.queryTokenBalance(convertSymbol.getBrokerId(),
                convertSymbol.getBrokerAccountId(), convertSymbol.getOfferingsTokenId());
        if (brokerBalance != null) {
            BigDecimal brokerFreeQty = new BigDecimal(brokerBalance.getFree());
            if (brokerFreeQty.compareTo(offeringsQty) < 0) {
                throw new BrokerException(BrokerErrorCode.CONVERT_OFFERINGS_INSUFFICIENT_BALANCE);
            }
        } else {
            throw new BrokerException(BrokerErrorCode.CONVERT_OFFERINGS_INSUFFICIENT_BALANCE);
        }

        // 插入订单前先插入对应user_record和broker_record，防止处理数据时悲观锁未锁到数据导致全表锁定
        try {
            Long curTime = System.currentTimeMillis();
            Long curDate = DateUtil.startOfDay(curTime);
            // 如果tb_convert_broker_record表不存在对应记录，则新增记录
            ConvertBrokerRecord brokerRecord = convertBrokerRecordMapper.getByConvertSymbolId(brokerId, convertSymbolId);
            if (brokerRecord == null) {
                brokerRecord = new ConvertBrokerRecord();
                brokerRecord.setBrokerId(brokerId);
                brokerRecord.setConvertSymbolId(convertSymbolId);
                brokerRecord.setCurrDate(curDate);
                brokerRecord.setCurrentQuantity(BigDecimal.ZERO);
                brokerRecord.setCreated(curTime);
                brokerRecord.setUpdated(curTime);
                convertBrokerRecordMapper.insertRecord(brokerRecord);
            }
            // 如果tb_convert_user_record表不存在对应记录，则新增记录
            ConvertUserRecord userRecord = convertUserRecordMapper.getByAccountId(brokerId, convertSymbolId, userAccountId);
            if (userRecord == null) {
                userRecord = new ConvertUserRecord();
                userRecord.setBrokerId(brokerId);
                userRecord.setConvertSymbolId(convertSymbolId);
                userRecord.setAccountId(userAccountId);
                userRecord.setCurrDate(curDate);
                userRecord.setCurrentQuantity(BigDecimal.ZERO);
                userRecord.setTotalQuantity(BigDecimal.ZERO);
                userRecord.setCreated(curTime);
                userRecord.setUpdated(curTime);
                convertUserRecordMapper.insertRecord(userRecord);
            }
        } catch (Exception e) {
            log.info("Insert record failed:{}", e.getMessage());
        }
        return convertService.insertNewOrder(clientOrderId, brokerId, user, convertSymbol, userAccountId, offeringsQty, purchaseQty, orderPrice);
    }

    @Transactional(isolation = Isolation.READ_COMMITTED, propagation = Propagation.REQUIRED, rollbackFor = Throwable.class)
    public CreateConvertOrderResponse insertNewOrder(String clientOrderId, Long brokerId, User user, ConvertSymbol convertSymbol,
                                                     Long userAccountId, BigDecimal offeringsQty, BigDecimal purchaseQty,
                                                     BigDecimal orderPrice) {
        Long curTime = System.currentTimeMillis();
        Long curDate = DateUtil.startOfDay(curTime);
        // 增加或更新用户兑换记录表
        ConvertUserRecord userRecord = convertUserRecordMapper.lockByAccountId(brokerId, convertSymbol.getId(), userAccountId);
        if (userRecord == null) {
            userRecord = new ConvertUserRecord();
            userRecord.setBrokerId(brokerId);
            userRecord.setConvertSymbolId(convertSymbol.getId());
            userRecord.setAccountId(userAccountId);
            userRecord.setCurrDate(curDate);
            userRecord.setCurrentQuantity(offeringsQty);
            userRecord.setTotalQuantity(offeringsQty);
            userRecord.setCreated(curTime);
            userRecord.setUpdated(curTime);
            convertUserRecordMapper.insertSelective(userRecord);
        } else {
            if (userRecord.getCurrDate().equals(curDate)) {
                // 订单数量加上用户今日已兑换数量大于每日限额则不再下单
                if (convertSymbol.getAccountDailyLimit().compareTo(BigDecimal.ZERO) > 0) {
                    if (offeringsQty.add(userRecord.getCurrentQuantity())
                            .compareTo(convertSymbol.getAccountDailyLimit()) > 0) {
                        throw new BrokerException(BrokerErrorCode.CONVERT_ORDER_AMOUNT_OVER_ACCOUNT_DAILY_LIMIT);
                    }
                }
                // 如果已存在今日申购数量则累加数量
                userRecord.setCurrentQuantity(userRecord.getCurrentQuantity().add(offeringsQty));
            } else {
                // 更新今日申购日期和申购数量
                userRecord.setCurrDate(curDate);
                userRecord.setCurrentQuantity(offeringsQty);
            }

            // 订单数量加上用户已兑换的数量，如果超出总额则不再下单
            if (convertSymbol.getAccountTotalLimit().compareTo(BigDecimal.ZERO) > 0) {
                if (offeringsQty.add(userRecord.getTotalQuantity())
                        .compareTo(convertSymbol.getAccountTotalLimit()) > 0) {
                    throw new BrokerException(BrokerErrorCode.CONVERT_ORDER_AMOUNT_OVER_ACCOUNT_DAILY_LIMIT);
                }
            }
            // 增加用户的总兑换数量
            userRecord.setTotalQuantity(userRecord.getTotalQuantity().add(offeringsQty));
            userRecord.setUpdated(curTime);
            convertUserRecordMapper.updateByPrimaryKeySelective(userRecord);
        }
        // 增加或更新币对兑换记录表
        ConvertBrokerRecord brokerRecord = convertBrokerRecordMapper.lockByConvertSymbolId(convertSymbol.getBrokerId(), convertSymbol.getId());
        if (brokerRecord == null) {
            brokerRecord = new ConvertBrokerRecord();
            brokerRecord.setBrokerId(brokerId);
            brokerRecord.setConvertSymbolId(convertSymbol.getId());
            brokerRecord.setCurrDate(curDate);
            brokerRecord.setCurrentQuantity(offeringsQty);
            brokerRecord.setCreated(curTime);
            brokerRecord.setUpdated(curTime);
            convertBrokerRecordMapper.insertSelective(brokerRecord);
        } else {
            if (brokerRecord.getCurrDate().equals(curDate)) {
                // 订单数量加上币对今日已兑换数量大于币对每日限额则不再下单
                if (convertSymbol.getSymbolDailyLimit().compareTo(BigDecimal.ZERO) > 0) {
                    if (offeringsQty.add(brokerRecord.getCurrentQuantity())
                            .compareTo(convertSymbol.getSymbolDailyLimit()) > 0) {
                        throw new BrokerException(BrokerErrorCode.CONVERT_ORDER_AMOUNT_OVER_SYMBOL_DAILY_LIMIT);
                    }
                }
                // 如果已存在今日申购数量则累加数量
                brokerRecord.setCurrentQuantity(brokerRecord.getCurrentQuantity().add(offeringsQty));
            } else {
                // 更新今日申购日期和申购数量
                brokerRecord.setCurrDate(curDate);
                brokerRecord.setCurrentQuantity(offeringsQty);
            }
            brokerRecord.setUpdated(curTime);
            convertBrokerRecordMapper.updateByPrimaryKeySelective(brokerRecord);
        }

        //新增闪兑订单
        Long orderId = iSequenceGenerator.getLong();
        ConvertOrder newOrder = new ConvertOrder();
        newOrder.setBrokerId(brokerId);
        newOrder.setBrokerAccountId(convertSymbol.getBrokerAccountId());
        newOrder.setUserId(user.getUserId());
        newOrder.setAccountId(user.getDefaultAccountId());
        newOrder.setOrderId(orderId);
        newOrder.setClientOrderId(clientOrderId);
        newOrder.setConvertSymbolId(convertSymbol.getId());
        newOrder.setOfferingsQuantity(offeringsQty);
        newOrder.setPurchaseQuantity(purchaseQty);
        newOrder.setPrice(orderPrice);
        newOrder.setStatus(ConvertOrderStatus.UNPAID.getStatus());
        newOrder.setErrorMessage("");
        newOrder.setCreated(curTime);
        newOrder.setUpdated(curTime);
        convertOrderMapper.insertRecord(newOrder);
        // 闪兑订单转账，并根据结果更新订单状态
        // convertService.transferConvertOrder(newOrder, convertSymbol);
        return CreateConvertOrderResponse.newBuilder()
                .setOrderId(orderId)
                .setSuccess(true)
                .build();

    }

    @Transactional(isolation = Isolation.READ_COMMITTED, propagation = Propagation.REQUIRED, rollbackFor = Throwable.class)
    public void transferConvertOrder(ConvertOrder newOrder, ConvertSymbol convertSymbol) {
        Long curTime = System.currentTimeMillis();
        Long curDate = DateUtil.startOfDay(curTime);
        ConvertResponse response;
        try {
            ConvertRequest request = ConvertRequest.newBuilder()
                    .setBaseRequest(BaseReqUtil.getBaseRequest(convertSymbol.getBrokerId()))
                    .setClientReqId(newOrder.getOrderId())
                    .setTakerAccountId(newOrder.getAccountId())
                    .setTakerOrgId(convertSymbol.getBrokerId())
                    .setMakerAccountId(convertSymbol.getBrokerAccountId())
                    .setMakerOrgId(convertSymbol.getBrokerId())
                    .setTakerTokenId(convertSymbol.getPurchaseTokenId())
                    .setTakerAmount(DecimalUtil.toTrimString(newOrder.getPurchaseQuantity()))
                    .setMakerTokenId(convertSymbol.getOfferingsTokenId())
                    .setMakerAmount(DecimalUtil.toTrimString(newOrder.getOfferingsQuantity()))
                    .build();
            // 向平台请求闪兑转账
            response = grpcBatchTransferService.convert(request);
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() != Status.Code.DEADLINE_EXCEEDED) {
                // 平台转账异常不为超时，则认为转账失败，更新闪兑订单状态
                newOrder.setStatus(ConvertOrderStatus.FAILED.getStatus());
                newOrder.setErrorMessage(e.getMessage());
                newOrder.setUpdated(curTime);
                convertOrderMapper.updateByPrimaryKeySelective(newOrder);

                // 转账失败处理,回减用户兑换数量
                ConvertUserRecord userRecord = convertUserRecordMapper.lockByAccountId(convertSymbol.getBrokerId(),
                        convertSymbol.getId(), newOrder.getAccountId());
                if (userRecord != null) {
                    if (userRecord.getCurrDate().equals(curDate)) {
                        // 如果已存在今日申购数量则累加数量
                        userRecord.setCurrentQuantity(userRecord.getCurrentQuantity().subtract(newOrder.getOfferingsQuantity()));
                    }
                    // 回减用户的总兑换数量
                    userRecord.setTotalQuantity(userRecord.getTotalQuantity().subtract(newOrder.getOfferingsQuantity()));
                    userRecord.setUpdated(curTime);
                    convertUserRecordMapper.updateByPrimaryKeySelective(userRecord);
                }
                // 转账失败处理,回减币对兑换数量
                ConvertBrokerRecord brokerRecord = convertBrokerRecordMapper.lockByConvertSymbolId(convertSymbol.getBrokerAccountId(),
                        convertSymbol.getId());
                if (brokerRecord != null) {
                    if (brokerRecord.getCurrDate().equals(curDate)) {
                        brokerRecord.setCurrentQuantity(brokerRecord.getCurrentQuantity().subtract(newOrder.getOfferingsQuantity()));
                    }
                    brokerRecord.setUpdated(curTime);
                    convertBrokerRecordMapper.updateByPrimaryKeySelective(brokerRecord);
                }
            }
            return;
        } catch (Exception e) {
            // 其他异常认为转账失败，更新订单状态，回减对应订单数量
            newOrder.setStatus(ConvertOrderStatus.FAILED.getStatus());
            newOrder.setErrorMessage(e.getMessage());
            newOrder.setUpdated(curTime);
            convertOrderMapper.updateByPrimaryKeySelective(newOrder);

            // 转账失败处理,回减用户兑换数量
            ConvertUserRecord userRecord = convertUserRecordMapper.lockByAccountId(convertSymbol.getBrokerId(),
                    convertSymbol.getId(), newOrder.getAccountId());
            if (userRecord != null) {
                if (userRecord.getCurrDate().equals(curDate)) {
                    // 如果已存在今日申购数量则累加数量
                    userRecord.setCurrentQuantity(userRecord.getCurrentQuantity().subtract(newOrder.getOfferingsQuantity()));
                }
                // 回减用户的总兑换数量
                userRecord.setTotalQuantity(userRecord.getTotalQuantity().subtract(newOrder.getOfferingsQuantity()));
                userRecord.setUpdated(curTime);
                convertUserRecordMapper.updateByPrimaryKeySelective(userRecord);
            }
            // 转账失败处理,回减币对兑换数量
            ConvertBrokerRecord brokerRecord = convertBrokerRecordMapper.lockByConvertSymbolId(convertSymbol.getBrokerId(), convertSymbol.getId());
            if (brokerRecord != null) {
                if (brokerRecord.getCurrDate().equals(curDate)) {
                    brokerRecord.setCurrentQuantity(brokerRecord.getCurrentQuantity().subtract(newOrder.getOfferingsQuantity()));
                }
                brokerRecord.setUpdated(curTime);
                convertBrokerRecordMapper.updateByPrimaryKeySelective(brokerRecord);
            }
            return;
        }
        if (response.getCode() == ConvertResponse.ResponseCode.SUCCESS) {
            // 转账成功，更新订单状态
            newOrder.setStatus(ConvertOrderStatus.PAID.getStatus());
            newOrder.setUpdated(curTime);
            convertOrderMapper.updateByPrimaryKeySelective(newOrder);
        } else if (response.getCode() == ConvertResponse.ResponseCode.PROCESSING) {
            // 如果平台返回转账中，则不处理闪兑订单状态
        } else {
            // 转账应答不为成功和处理中，则任务转账失败，更新闪兑订单状态为失败
            newOrder.setStatus(ConvertOrderStatus.FAILED.getStatus());
            newOrder.setErrorMessage(response.getMsg());
            newOrder.setUpdated(curTime);
            convertOrderMapper.updateByPrimaryKeySelective(newOrder);

            // 转账失败处理,回减用户兑换数量
            ConvertUserRecord userRecord = convertUserRecordMapper.lockByAccountId(convertSymbol.getBrokerId(), convertSymbol.getId(), newOrder.getAccountId());
            if (userRecord != null) {
                if (userRecord.getCurrDate().equals(curDate)) {
                    // 如果已存在今日申购数量则累加数量
                    userRecord.setCurrentQuantity(userRecord.getCurrentQuantity().subtract(newOrder.getOfferingsQuantity()));
                }
                // 回减用户的总兑换数量
                userRecord.setTotalQuantity(userRecord.getTotalQuantity().subtract(newOrder.getOfferingsQuantity()));
                userRecord.setUpdated(curTime);
                convertUserRecordMapper.updateByPrimaryKeySelective(userRecord);
            }
            // 转账失败处理,回减币对兑换数量
            ConvertBrokerRecord brokerRecord = convertBrokerRecordMapper.lockByConvertSymbolId(convertSymbol.getBrokerId(), convertSymbol.getId());
            if (brokerRecord != null) {
                if (brokerRecord.getCurrDate().equals(curDate)) {
                    brokerRecord.setCurrentQuantity(brokerRecord.getCurrentQuantity().subtract(newOrder.getOfferingsQuantity()));
                }
                brokerRecord.setUpdated(curTime);
                convertBrokerRecordMapper.updateByPrimaryKeySelective(brokerRecord);
            }
        }
    }

    public List<io.bhex.broker.grpc.convert.ConvertOrder> queryOrders(Header header,
                                                                      Long convertSymbolId, Integer status,
                                                                      Long beginTime, Long endTime,
                                                                      Long fromOrderId, Integer limit) {
        Example example = new Example(ConvertOrder.class);
        Example.Criteria criteria = example.createCriteria();
        criteria.andEqualTo("brokerId", header.getOrgId());
        criteria.andEqualTo("userId", header.getUserId());
        if (fromOrderId != null && !fromOrderId.equals(0L)) {
            criteria.andLessThan("orderId", fromOrderId);
        }
        if (convertSymbolId != null && !convertSymbolId.equals(0L)) {
            criteria.andEqualTo("convertSymbolId", convertSymbolId);
        }
        if (status != null && !status.equals(0)) {
            criteria.andEqualTo("status", status);
        }
        if (beginTime != null && !beginTime.equals(0L)) {
            criteria.andGreaterThan("created", beginTime);
        }
        if (endTime != null && !endTime.equals(0L)) {
            criteria.andLessThan("created", endTime);
        }
        if (limit == null || limit == 0) {
            limit = 50;
        } else if (limit > 500) {
            limit = 500;
        }
        example.setOrderByClause("`id` DESC");
        List<ConvertOrder> convertOrderList = convertOrderMapper.selectByExampleAndRowBounds(example, new RowBounds(0, limit));
        return convertOrderList.stream().map(this::buildConvertOrder).collect(Collectors.toList());
    }

    public List<io.bhex.broker.grpc.convert.ConvertOrder> adminQueryOrders(Header header, Long userId,
                                                                           Long convertSymbolId, Integer status,
                                                                           Long beginTime, Long endTime,
                                                                           Long fromOrderId, Integer limit) {
        Example example = new Example(ConvertOrder.class);
        Example.Criteria criteria = example.createCriteria();
        Long brokerId = header.getOrgId();
        criteria.andEqualTo("brokerId", brokerId);
        if (userId != null && !userId.equals(0L)) {
            criteria.andEqualTo("userId", userId);
        }
        if (fromOrderId != null && !fromOrderId.equals(0L)) {
            criteria.andLessThan("orderId", fromOrderId);
        }
        if (convertSymbolId != null && !convertSymbolId.equals(0L)) {
            criteria.andEqualTo("convertSymbolId", convertSymbolId);
        }
        if (status != null && !status.equals(0)) {
            criteria.andEqualTo("status", status);
        }
        if (beginTime != null && !beginTime.equals(0L)) {
            criteria.andGreaterThan("created", beginTime);
        }
        if (endTime != null && !endTime.equals(0L)) {
            criteria.andLessThan("created", endTime);
        }
        if (limit == null || limit == 0) {
            limit = 50;
        } else if (limit > 500) {
            limit = 500;
        }
        example.setOrderByClause("`id` DESC");
        List<ConvertOrder> convertOrderList = convertOrderMapper.selectByExampleAndRowBounds(example, new RowBounds(0, limit));
        return convertOrderList.stream().map(this::buildConvertOrder).collect(Collectors.toList());
    }

    private io.bhex.broker.grpc.convert.ConvertOrder buildConvertOrder(ConvertOrder convertOrder) {
        ConvertSymbol symbol = getConvertSymbol(convertOrder.getConvertSymbolId(), convertOrder.getBrokerId());
        if (symbol != null) {
            return io.bhex.broker.grpc.convert.ConvertOrder.newBuilder()
                    .setId(convertOrder.getId())
                    .setOrderId(convertOrder.getOrderId())
                    .setConvertSymbolId(convertOrder.getConvertSymbolId())
                    .setPurchaseTokenId(symbol.getPurchaseTokenId())
                    .setPurchaseTokenName(symbol.getPurchaseTokenName())
                    .setOfferingsTokenId(symbol.getOfferingsTokenId())
                    .setOfferingsTokenName(symbol.getOfferingsTokenName())
                    .setUserId(convertOrder.getUserId())
                    .setAccountId(convertOrder.getAccountId())
                    .setBrokerAccountId(convertOrder.getBrokerAccountId())
                    .setPurchaseQuantity(DecimalUtil.toTrimString(convertOrder.getPurchaseQuantity()))
                    .setOfferingsQuantity(DecimalUtil.toTrimString(convertOrder.getOfferingsQuantity()))
                    .setPrice(DecimalUtil.toTrimString(convertOrder.getPrice()))
                    .setStatus(convertOrder.getStatus())
                    .setErrorMessage(convertOrder.getErrorMessage())
                    .setCreated(convertOrder.getCreated())
                    .setUpdated(convertOrder.getUpdated())
                    .build();
        } else {
            return io.bhex.broker.grpc.convert.ConvertOrder.newBuilder()
                    .setId(convertOrder.getId())
                    .setOrderId(convertOrder.getOrderId())
                    .setConvertSymbolId(convertOrder.getConvertSymbolId())
                    .setUserId(convertOrder.getUserId())
                    .setAccountId(convertOrder.getAccountId())
                    .setPurchaseQuantity(DecimalUtil.toTrimString(convertOrder.getPurchaseQuantity()))
                    .setOfferingsQuantity(DecimalUtil.toTrimString(convertOrder.getOfferingsQuantity()))
                    .setPrice(DecimalUtil.toTrimString(convertOrder.getPrice()))
                    .setStatus(convertOrder.getStatus())
                    .setErrorMessage(convertOrder.getErrorMessage())
                    .setCreated(convertOrder.getCreated())
                    .setUpdated(convertOrder.getUpdated())
                    .build();
        }
    }

    public AddConvertSymbolResponse addConvertSymbol(Header header, String symbolId, Long brokerAccountId,
                                                     String purchaseTokenId, String offeringsTokenId,
                                                     Integer purchasePrecision, Integer offeringsPrecision,
                                                     Integer priceType, String priceValue,
                                                     String minQuantity, String maxQuantity,
                                                     String accountDailyLimit, String accountTotalLimit,
                                                     String symbolDailyLimit, Boolean verifyKyc,
                                                     Boolean verifyMobile, Integer verifyVipLevel, Integer status) {
        log.info("addConvertSymbol ==> orgId:{},symbolId:{},purchaseTokenId:{},offeringsTokenId:{}",
                header.getOrgId(), symbolId, purchaseTokenId, offeringsTokenId);
        Long orgId = header.getOrgId();
        Token offeringsToken = tokenMapper.getToken(orgId, offeringsTokenId);
        if (offeringsToken == null) {
            throw new BrokerException(BrokerErrorCode.CONVERT_OFFERINGS_TOKEN_NOT_FOUND);
        }
        Token purchaseToken = tokenMapper.getToken(orgId, purchaseTokenId);
        if (purchaseToken == null) {
            throw new BrokerException(BrokerErrorCode.CONVERT_PURCHASE_TOKEN_NOT_FOUND);
        }
        // 固定价格拼装symbolId，浮动价格校验symbolId是否存在
        if (priceType.equals(ConvertSymbolPriceType.FIXED.getType())) {
            symbolId = offeringsTokenId + purchaseTokenId;
        } else {
            if (StringUtil.isEmpty(symbolId)) {
                throw new BrokerException(BrokerErrorCode.CONVERT_SYMBOL_NOT_FOUND);
            } else {
                Symbol symbol = symbolMapper.getOrgSymbol(orgId, symbolId);
                if (symbol == null) {
                    throw new BrokerException(BrokerErrorCode.CONVERT_SYMBOL_NOT_FOUND);
                }
            }
        }
        ConvertSymbol convertSymbol = convertSymbolMapper.getByBrokerAndToken(orgId, purchaseTokenId, offeringsTokenId);
        if (convertSymbol != null) {
            throw new BrokerException(BrokerErrorCode.CONVERT_SYMBOL_IS_EXIST);
        }

        Long curTime = System.currentTimeMillis();
        ConvertSymbol newSymbol = ConvertSymbol.builder()
                .brokerId(header.getOrgId())
                .symbolId(symbolId)
                .brokerAccountId(brokerAccountId)
                .purchaseTokenId(purchaseTokenId)
                .purchaseTokenName(purchaseToken.getTokenName())
                .offeringsTokenId(offeringsTokenId)
                .offeringsTokenName(offeringsToken.getTokenName())
                .purchasePrecision(purchasePrecision)
                .offeringsPrecision(offeringsPrecision)
                .priceType(priceType)
                .priceValue(new BigDecimal(priceValue))
                .minQuantity(new BigDecimal(minQuantity))
                .maxQuantity(new BigDecimal(maxQuantity))
                .accountDailyLimit(new BigDecimal(accountDailyLimit))
                .accountTotalLimit(new BigDecimal(accountTotalLimit))
                .symbolDailyLimit(new BigDecimal(symbolDailyLimit))
                .verifyKyc(verifyKyc)
                .verifyMobile(verifyMobile)
                .verifyVipLevel(verifyVipLevel)
                .status(status)
                .created(curTime)
                .updated(curTime)
                .build();
        convertSymbolMapper.insertSelective(newSymbol);
        return AddConvertSymbolResponse.newBuilder()
                .setSuccess(true)
                .build();
    }

    public ModifyConvertSymbolResponse modifyConvertSymbol(Header header, Long convertSymbolId, String symbolId,
                                                           String purchaseTokenId, String offeringsTokenId,
                                                           Integer purchasePrecision, Integer offeringsPrecision,
                                                           Long brokerAccountId,
                                                           Integer priceType, String priceValue,
                                                           String minQuantity, String maxQuantity,
                                                           String accountDailyLimit, String accountTotalLimit,
                                                           String symbolDailyLimit,
                                                           Boolean verifyKyc, Boolean verifyMobile,
                                                           Integer verifyVipLevel) {
        ConvertSymbol modifySymbol = getConvertSymbol(convertSymbolId, header.getOrgId());
        if (modifySymbol == null) {
            throw new BrokerException(BrokerErrorCode.CONVERT_SYMBOL_NOT_FOUND);
        }
        // 修改币对时不允许改变symbolId
        if (!modifySymbol.getSymbolId().equalsIgnoreCase(symbolId)) {
            throw new BrokerException(BrokerErrorCode.CONVERT_SYMBOL_NOT_FOUND);
        }
        if (brokerAccountId != null && !brokerAccountId.equals(0L)) {
            modifySymbol.setBrokerAccountId(brokerAccountId);
        }
        if (StringUtil.isNotEmpty(purchaseTokenId)) {
            modifySymbol.setPurchaseTokenId(purchaseTokenId);
            Token purchaseToken = tokenMapper.getToken(modifySymbol.getBrokerId(), purchaseTokenId);
            if (purchaseToken != null) {
                modifySymbol.setPurchaseTokenName(purchaseToken.getTokenName());
            }
        }
        if (StringUtil.isNotEmpty(offeringsTokenId)) {
            modifySymbol.setOfferingsTokenId(offeringsTokenId);
            Token offeringsToken = tokenMapper.getToken(modifySymbol.getBrokerId(), offeringsTokenId);
            if (offeringsToken != null) {
                modifySymbol.setOfferingsTokenName(offeringsToken.getTokenName());
            }
        }
        if (purchasePrecision != null) {
            modifySymbol.setPurchasePrecision(purchasePrecision);
        }
        if (offeringsPrecision != null) {
            modifySymbol.setOfferingsPrecision(offeringsPrecision);
        }
        if (priceType != null && !priceType.equals(0)) {
            modifySymbol.setPriceType(priceType);
        }
        if (StringUtil.isNotEmpty(priceValue)) {
            modifySymbol.setPriceValue(new BigDecimal(priceValue));
        }
        if (StringUtil.isNotEmpty(minQuantity)) {
            modifySymbol.setMinQuantity(new BigDecimal(minQuantity));
        }
        if (StringUtil.isNotEmpty(maxQuantity)) {
            modifySymbol.setMaxQuantity(new BigDecimal(maxQuantity));
        }
        if (StringUtil.isNotEmpty(accountDailyLimit)) {
            modifySymbol.setAccountDailyLimit(new BigDecimal(accountDailyLimit));
        }
        if (StringUtil.isNotEmpty(accountTotalLimit)) {
            modifySymbol.setAccountTotalLimit(new BigDecimal(accountTotalLimit));
        }
        if (StringUtil.isNotEmpty(symbolDailyLimit)) {
            modifySymbol.setSymbolDailyLimit(new BigDecimal(symbolDailyLimit));
        }
        if (verifyKyc != null) {
            modifySymbol.setVerifyKyc(verifyKyc);
        }
        if (verifyMobile != null) {
            modifySymbol.setVerifyMobile(verifyMobile);
        }
        if (verifyVipLevel != null) {
            modifySymbol.setVerifyVipLevel(verifyVipLevel);
        }
        convertSymbolMapper.updateByPrimaryKeySelective(modifySymbol);
        convertSymbolMap.put(modifySymbol.getId(), modifySymbol);
        return ModifyConvertSymbolResponse.newBuilder()
                .setSuccess(true)
                .build();
    }

    public UpdateConvertSymbolStatusResponse updateConvertSymbolStatus(Header header, Long convertSymbolId, Integer status) {
        ConvertSymbol updateSymbol = convertSymbolMapper.getByConvertSymbolId(convertSymbolId, header.getOrgId());
        if (updateSymbol == null) {
            throw new BrokerException(BrokerErrorCode.CONVERT_SYMBOL_NOT_FOUND);
        }
        // 判断当前状态是否已等于更新状态，如果相等直接返回
        if (updateSymbol.getStatus().equals(status)) {
            return UpdateConvertSymbolStatusResponse.newBuilder()
                    .setSuccess(true)
                    .build();
        }
        updateSymbol.setStatus(status);
        updateSymbol.setUpdated(System.currentTimeMillis());
        convertSymbolMapper.updateByPrimaryKeySelective(updateSymbol);
        convertSymbolMap.put(updateSymbol.getId(), updateSymbol);
        return UpdateConvertSymbolStatusResponse.newBuilder()
                .setSuccess(true)
                .build();
    }

    private void checkOrder(User queryUser, ConvertSymbol convertSymbol, Symbol symbol,
                            String orderTokenId, BigDecimal offeringsQty, BigDecimal purchaseQty) {
        // 验证kyc
        if (convertSymbol.getVerifyKyc()) {
            if (queryUser.getKycLevel() == 0) {
                throw new BrokerException(BrokerErrorCode.NEED_KYC);
            }
        }
        // 验证手机号
        if (convertSymbol.getVerifyMobile()) {
            if (StringUtil.isEmpty(queryUser.getMobile())) {
                throw new BrokerException(BrokerErrorCode.MOBILE_CANNOT_BE_NULL);
            }
        }
        // 验证用户是否为vip
//        if (convertSymbol.getVerifyVipLevel() > 0) {
//            if (user.getIsVip().equals(0)) {
//                log.info("用户不是vip，无法进行闪电兑付");
//                throw new BrokerException(BrokerErrorCode.CONVERT_SYMBOL_NOT_FOUND);
//            }
//        }
        // 校验订单最小数量
        if (convertSymbol.getMinQuantity().compareTo(BigDecimal.ZERO) > 0) {
            if (offeringsQty.compareTo(convertSymbol.getMinQuantity()) < 0) {
                throw new BrokerException(BrokerErrorCode.ORDER_QUANTITY_TOO_SMALL);
            }
        }
        // 校验订单最大数量
        if (convertSymbol.getMaxQuantity().compareTo(BigDecimal.ZERO) > 0) {
            if (offeringsQty.compareTo(convertSymbol.getMaxQuantity()) > 0) {
                throw new BrokerException(BrokerErrorCode.ORDER_QUANTITY_TOO_BIG);
            }
        }
//        // 校验用户兑付是否超过当日可购买最大数量
//        if (convertSymbol.getAccountDailyLimit().compareTo(BigDecimal.ZERO) > 0) {
//            ConvertUserRecord userRecord = convertUserRecordMapper.getByAccountId(convertSymbol.getId(),
//                    queryUser.getDefaultAccountId());
//            if (userRecord != null) {
//                Long date = DateUtil.startOfDay(System.currentTimeMillis());
//                if (userRecord.getCurrDate().compareTo(date) == 0) {
//                    if (userRecord.getCurrentQuantity().add(convertOrderPo.getOfferingsQty())
//                            .compareTo(convertSymbol.getAccountDailyLimit()) > 0) {
//                        log.info("用户兑付数量大于单账户每日可兑换最大数量");
//                        throw new BrokerException(BrokerErrorCode.CONVERT_ORDER_AMOUNT_OVER_ACCOUNT_DAILY_LIMIT);
//                    }
//                } else {
//                    if (convertOrderPo.getOfferingsQty().compareTo(convertSymbol.getAccountDailyLimit()) > 0) {
//                        log.info("用户兑付数量大于单账户每日可兑换最大数量");
//                        throw new BrokerException(BrokerErrorCode.CONVERT_ORDER_AMOUNT_OVER_ACCOUNT_DAILY_LIMIT);
//                    }
//                }
//            } else {
//                if (convertOrderPo.getOfferingsQty().compareTo(convertSymbol.getAccountDailyLimit()) > 0) {
//                    log.info("用户兑付数量大于单账户每日可兑换最大数量");
//                    throw new BrokerException(BrokerErrorCode.CONVERT_ORDER_AMOUNT_OVER_ACCOUNT_DAILY_LIMIT);
//                }
//            }
//        }
//        // 校验用户兑付是否超过单账户最大可兑换数量
//        if (convertSymbol.getAccountTotalLimit().compareTo(BigDecimal.ZERO) > 0) {
//            ConvertUserRecord userRecord = convertUserRecordMapper.getByAccountId(convertSymbol.getId(),
//                    queryUser.getDefaultAccountId());
//            if (userRecord != null) {
//                if (userRecord.getTotalQuantity().add(convertOrderPo.getOfferingsQty())
//                        .compareTo(convertSymbol.getAccountTotalLimit()) > 0) {
//                    log.info("用户兑付数量大于单账号可兑换最大数量");
//                    throw new BrokerException(BrokerErrorCode.CONVERT_ORDER_AMOUNT_OVER_ACCOUNT_TOTAL_LIMIT);
//                }
//            } else {
//                if (convertOrderPo.getOfferingsQty().compareTo(convertSymbol.getAccountTotalLimit()) > 0) {
//                    log.info("用户兑付数量大于单账号可兑换最大数量");
//                    throw new BrokerException(BrokerErrorCode.CONVERT_ORDER_AMOUNT_OVER_ACCOUNT_TOTAL_LIMIT);
//                }
//            }
//        }
//        // 校验用户兑付是否超过币对当日的最大可兑换数量
//        if (convertSymbol.getSymbolDailyLimit().compareTo(BigDecimal.ZERO) > 0) {
//            ConvertBrokerRecord brokerRecord = convertBrokerRecordMapper.getByConvertSymbolId(convertSymbol.getId());
//            if (brokerRecord != null) {
//                Long date = DateUtil.startOfDay(System.currentTimeMillis());
//                if (brokerRecord.getCurrDate().compareTo(date) == 0) {
//                    if (brokerRecord.getCurrentQuantity().add(convertOrderPo.getOfferingsQty())
//                            .compareTo(convertSymbol.getSymbolDailyLimit()) > 0) {
//                        throw new BrokerException(BrokerErrorCode.CONVERT_ORDER_AMOUNT_OVER_SYMBOL_DAILY_LIMIT);
//                    }
//                } else {
//                    if (convertOrderPo.getOfferingsQty().compareTo(convertSymbol.getSymbolDailyLimit()) > 0) {
//                        throw new BrokerException(BrokerErrorCode.CONVERT_ORDER_AMOUNT_OVER_SYMBOL_DAILY_LIMIT);
//                    }
//                }
//            } else {
//                if (convertOrderPo.getOfferingsQty().compareTo(convertSymbol.getSymbolDailyLimit()) > 0) {
//                    throw new BrokerException(BrokerErrorCode.CONVERT_ORDER_AMOUNT_OVER_SYMBOL_DAILY_LIMIT);
//                }
//            }
//        }

        // 判断当前数量是否大于用户每日限额
        if (convertSymbol.getAccountDailyLimit().compareTo(BigDecimal.ZERO) > 0) {
            if (offeringsQty.compareTo(convertSymbol.getAccountDailyLimit()) > 0) {
                throw new BrokerException(BrokerErrorCode.CONVERT_ORDER_AMOUNT_OVER_ACCOUNT_DAILY_LIMIT);
            }
        }
        // 判断当前数量是否大于用户总限额
        if (convertSymbol.getAccountTotalLimit().compareTo(BigDecimal.ZERO) > 0) {
            if (offeringsQty.compareTo(convertSymbol.getAccountTotalLimit()) > 0) {
                throw new BrokerException(BrokerErrorCode.CONVERT_ORDER_AMOUNT_OVER_ACCOUNT_DAILY_LIMIT);
            }
        }
        // 判断当前数量是否大于币对每日总限额
        if (convertSymbol.getSymbolDailyLimit().compareTo(BigDecimal.ZERO) > 0) {
            if (offeringsQty.compareTo(convertSymbol.getSymbolDailyLimit()) > 0) {
                throw new BrokerException(BrokerErrorCode.CONVERT_ORDER_AMOUNT_OVER_SYMBOL_DAILY_LIMIT);
            }
        }

        // 获取闪兑币对的当前价格
        BigDecimal convertPrice;
        if (convertSymbol.getPriceType().equals(ConvertSymbolPriceType.FIXED.getType())) {
            convertPrice = convertSymbol.getPriceValue();
        } else {
            if (symbol == null) {
                throw new BrokerException(BrokerErrorCode.ORDER_REQUEST_SYMBOL_INVALID);
            }
            convertPrice = getKlineConvertPrice(symbol.getOrgId(), symbol.getExchangeId(), symbol.getSymbolId(), getKLineId());
            if (convertPrice == null) {
                // 未获取到上一分钟的Kline收盘价
                throw new BrokerException(BrokerErrorCode.CONVERT_SYMBOL_PRICE_IS_NULL);
            }
            // 判断是否需要颠倒价格
            BigDecimal priceRatio = convertSymbol.getPriceValue().divide(new BigDecimal(100), DECIMAL_SCALE, RoundingMode.DOWN);
            if (symbol.getBaseTokenId().equalsIgnoreCase(convertSymbol.getOfferingsTokenId())
                    && symbol.getQuoteTokenId().equalsIgnoreCase(convertSymbol.getPurchaseTokenId())) {
                // 如果发售币种为基础币种，且申购币种为计价币种，则不需要颠倒价格
                convertPrice = convertPrice.multiply(priceRatio).setScale(DECIMAL_SCALE);
            } else {
                // 颠倒价格计算兑换的价格比例
                convertPrice = convertPrice.compareTo(BigDecimal.ZERO) == 0 ? BigDecimal.ZERO : BigDecimal.ONE.divide(convertPrice, DECIMAL_SCALE, RoundingMode.DOWN)
                        .multiply(priceRatio).setScale(DECIMAL_SCALE);
            }
        }
        if (convertPrice.compareTo(BigDecimal.ZERO) <= 0) {
            throw new BrokerException(BrokerErrorCode.CONVERT_SYMBOL_PRICE_IS_NULL);
        }

        // 校验订单数量数量是否有效
        if (!checkOrderQty(convertSymbol, orderTokenId, offeringsQty, purchaseQty, convertPrice)) {
            throw new BrokerException(BrokerErrorCode.ORDER_AMOUNT_ILLEGAL);
        }
    }

    private boolean checkOrderQty(ConvertSymbol convertSymbol, String orderTokenId, BigDecimal offeringsQty,
                                  BigDecimal purchaseQty, BigDecimal convertPrice) {
        if (orderTokenId.compareTo(convertSymbol.getOfferingsTokenId()) == 0) {
            // 如果标准数量是发售币种，则通过发售币种和当前价格计算收购币种数量
            BigDecimal calcPurchaseQty = offeringsQty.multiply(convertPrice)
                    .setScale(convertSymbol.getPurchasePrecision(), RoundingMode.UP);
            if (calcPurchaseQty.compareTo(purchaseQty) != 0) {
                log.info("checkOrderQty ==> calcPurchaseQty:{}, getPurchaseQty:{}", calcPurchaseQty, purchaseQty);
                return false;
            }
        } else if (orderTokenId.compareTo(convertSymbol.getPurchaseTokenId()) == 0) {
            BigDecimal calcOfferingsQty = purchaseQty
                    .divide(convertPrice, convertSymbol.getOfferingsPrecision(), RoundingMode.DOWN);
            if (calcOfferingsQty.compareTo(offeringsQty) != 0) {
                log.info("checkOrderQty ==> calcOfferingsQty:{}, getOfferingsQty:{}",
                        calcOfferingsQty, offeringsQty);
                return false;
            }
        } else {
            return false;
        }
        return true;
    }

    public Long getKLineId() {
        Long time = System.currentTimeMillis();
        Long intervalTimeMills = 60000L;
        return time / intervalTimeMills * intervalTimeMills - intervalTimeMills;
    }

}
