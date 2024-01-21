package io.bhex.broker.server.grpc.server.service;

import com.google.common.reflect.TypeToken;
import io.bhex.broker.common.api.client.okhttp.OkHttpPrometheusInterceptor;
import io.bhex.broker.common.util.JsonUtil;
import io.bhex.broker.server.grpc.server.service.po.MoonpayCurrency;
import io.bhex.broker.server.grpc.server.service.po.MoonpayPrice;
import io.bhex.broker.server.grpc.server.service.po.MoonpayTransaction;
import io.bhex.broker.server.model.OtcThirdParty;
import io.bhex.broker.server.util.SignUtils;
import lombok.extern.slf4j.Slf4j;
import okhttp3.*;
import org.springframework.stereotype.Service;
import tk.mybatis.mapper.util.StringUtil;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @author cookie.yuan
 * @description
 * @date 2020-08-13
 */
@Slf4j
@Service
public class MoonpayService {
    private static final String BUY_URL = "https://buy.moonpay.com/";
    private static final String SELL_URL = "https://sell.moonpay.com/";
    private static final int HTTP_OK_CODE = 200;
    private static OkHttpClient client = new OkHttpClient.Builder()
            .addInterceptor(OkHttpPrometheusInterceptor.getInstance())
            .connectTimeout(5, TimeUnit.SECONDS)
            .writeTimeout(5, TimeUnit.SECONDS)
            .readTimeout(5, TimeUnit.SECONDS)
            .build();

    public String getRequest(OtcThirdParty thirdParty, String url, Map<String, String> params) {
        try {
            String thirdPartyApiKey = SignUtils.decryptDataWithAES(thirdParty.getOrgId().toString(), thirdParty.getApiKey());
            //String thirdPartySecret = SignUtils.decryptDataWithAES(thirdParty.getOrgId().toString(), thirdParty.getSecret());
            HttpUrl httpUrl = HttpUrl.parse(thirdParty.getEndPoint() + url);
            HttpUrl.Builder builder = httpUrl.newBuilder();
            // Get请求增加apiKey参数
            builder.addQueryParameter("apiKey", thirdPartyApiKey);
            if (params != null) {
                for (String key : params.keySet()) {
                    builder.addQueryParameter(key, params.get(key));
                }
            }

            Request request = new Request.Builder()
                    .url(builder.build())
                    .build();

            Response response = client.newCall(request).execute();
            if (response.code() == HTTP_OK_CODE) {
                return response.body().string();
            } else {
                log.info("moonpayGet: response.code:{}, body:{}", response.code(), response.body().string());
            }
        } catch (IOException e) {
            log.info("moonpayGet: IOException:" + e.getMessage(), e);
        } catch (Exception ex) {
            log.info("moonpayGet: Exception:" + ex.getMessage(), ex);
        }
        return null;
    }

    // 查询支持币种
    public String getCurrencies(OtcThirdParty thirdParty) {
        String url = "/v3/currencies";
        Map<String, String> params = new HashMap<>();
        return getRequest(thirdParty, url, params);
    }

    public List<MoonpayCurrency> queryCurrencies(OtcThirdParty thirdParty) {
        String result = getCurrencies(thirdParty);
        if (StringUtil.isEmpty(result)) {
            return null;
        }
        try {
            Type type = new TypeToken<ArrayList<MoonpayCurrency>>() {
            }.getType();
            return JsonUtil.defaultGson().fromJson(result, type);
        } catch (Throwable e) {
            return null;
        }
    }

    public MoonpayPrice getPrice(OtcThirdParty thirdParty, String tokenId, String currencyId, String paymentMethodType,
                                 String tokenAmount, String currencyAmount, Integer side) {
        try {
            log.info("moonpayGetPrice: tokenId:{},currencyId:{},tokenAmount:{},currencyAmount:{},paymentMethodType:{},side:{}.",
                    tokenId, currencyId, tokenAmount, currencyAmount, paymentMethodType, side);
            String result = getOrderPrice(thirdParty, tokenId, currencyId, paymentMethodType, tokenAmount, currencyAmount, side);
            log.info("moonpayGetPrice: result:{}.", result);
            return JsonUtil.defaultGson().fromJson(result, MoonpayPrice.class);
        } catch (Throwable e) {
            log.info("moonpayGetPrice fail:" + e.getMessage(), e);
            return null;
        }
    }

    // 查询价格
    public String getOrderPrice(OtcThirdParty thirdParty, String tokenId, String currencyId, String paymentMethodType,
                                String tokenAmount, String currencyAmount, Integer side) {
        if (side == 1) {
            String url = "/v3/currencies/" + tokenId.toLowerCase() + "/sell_quote";
            Map<String, String> params = new HashMap<>();
            params.put("baseCurrencyAmount", tokenAmount);
            params.put("quoteCurrencyCode", currencyId.toLowerCase());
            return getRequest(thirdParty, url, params);
        } else {
            String url = "/v3/currencies/" + tokenId.toLowerCase() + "/buy_quote";
            Map<String, String> params = new HashMap<>();
            params.put("baseCurrencyAmount", currencyAmount);
            params.put("baseCurrencyCode", currencyId.toLowerCase());
            // todo 如果需要开通银行转账买币需要传入对应的映射关系，目前不支持，默认是信用卡买币：credit_debit_card
            //params.put("paymentMethod", paymentMethodType);
            // 下单时不支持包含费用的模式，因为下单时的订单最小金额不包含费用
            //params.put("areFeesIncluded","true");
            return getRequest(thirdParty, url, params);
        }
    }

    public MoonpayTransaction getTransaction(OtcThirdParty thirdParty, String transactionId, Integer side) {
        try {
            String result = getOrder(thirdParty, transactionId, side);
            return JsonUtil.defaultGson().fromJson(result, MoonpayTransaction.class);
        } catch (Throwable e) {
            return null;
        }
    }

    // 查询指定订单
    public String getOrder(OtcThirdParty thirdParty, String transactionId, Integer side) {
        String url;
        if (side == 1) {
            url = "/v1/sell_transactions/" + transactionId;
        } else {
            url = "/v1/transactions/" + transactionId;
        }
        Map<String, String> params = new HashMap<>();
        return getRequest(thirdParty, url, params);
    }


    public String placeOrder(OtcThirdParty thirdParty, String userId, String orderId, String walletAddress, String walletAddressTag,
                             String paymentType, String tokenId, String currencyId, String tokenAmount, String currencyAmount,
                             Integer side, String successUrl) {
        try {
            String thirdPartyApiKey = SignUtils.decryptDataWithAES(thirdParty.getOrgId().toString(), thirdParty.getApiKey());
            String thirdPartySecret = SignUtils.decryptDataWithAES(thirdParty.getOrgId().toString(), thirdParty.getSecret());
            Map<String, String> params = new HashMap<>();
            params.put("externalCustomerId", userId);
            params.put("externalTransactionId", orderId);
            params.put("lockAmount", "true");
            params.put("showWalletAddressForm", "true");
            String url;
            if (side == 1) {
                url = SELL_URL;
                params.put("refundWalletAddress", walletAddress);
                params.put("baseCurrencyCode", tokenId.toLowerCase());
                params.put("quoteCurrencyCode", currencyId.toLowerCase());
                params.put("baseCurrencyAmount", tokenAmount);
            } else {
                url = BUY_URL;
                params.put("walletAddress", walletAddress);
                if (StringUtil.isNotEmpty(walletAddressTag)) {
                    params.put("walletAddressTag", walletAddressTag);
                }
                params.put("enabledPaymentMethods", paymentType);
                params.put("baseCurrencyAmount", currencyAmount);
                params.put("currencyCode", tokenId.toLowerCase());
                params.put("baseCurrencyCode", currencyId.toLowerCase());
                params.put("redirectURL", successUrl);
            }
            HttpUrl httpUrl = HttpUrl.parse(url);
            HttpUrl.Builder builder = httpUrl.newBuilder();
            // Get请求增加apiKey参数
            builder.addQueryParameter("apiKey", thirdPartyApiKey);
            if (params != null) {
                for (String key : params.keySet()) {
                    builder.addQueryParameter(key, params.get(key));
                }
            }
            builder.addQueryParameter("signature", signGet(thirdPartySecret, builder.toString().substring(url.length())));
            return builder.toString();
        } catch (Throwable e) {
            log.info("placeOrder Throwable:{} {}", e.getMessage(), e);
            return null;
        }
    }

    private String signGet(String secret, String param) {
        try {
            Mac sha256_HMAC = Mac.getInstance("HmacSHA256");
            SecretKeySpec secretKeySpec = new SecretKeySpec(secret.getBytes(), "HmacSHA256");
            sha256_HMAC.init(secretKeySpec);
            return Base64.getEncoder().encodeToString(sha256_HMAC.doFinal(param.getBytes()));
        } catch (Exception e) {
            throw new RuntimeException("Unable to sign message.", e);
        }
    }

}
