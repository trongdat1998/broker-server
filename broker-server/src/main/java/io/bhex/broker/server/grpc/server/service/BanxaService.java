package io.bhex.broker.server.grpc.server.service;

import com.google.common.base.Joiner;
import io.bhex.broker.common.api.client.okhttp.OkHttpPrometheusInterceptor;
import io.bhex.broker.common.util.JsonUtil;
import io.bhex.broker.server.grpc.server.service.po.BanxaOrderResult;
import io.bhex.broker.server.grpc.server.service.po.BanxaPaymentResult;
import io.bhex.broker.server.grpc.server.service.po.BanxaPriceResult;
import io.bhex.broker.server.model.OtcThirdParty;
import io.bhex.broker.server.util.SignUtils;
import lombok.extern.slf4j.Slf4j;
import okhttp3.*;
import org.apache.commons.codec.binary.Hex;
import org.springframework.stereotype.Service;
import tk.mybatis.mapper.util.StringUtil;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author cookie.yuan
 * @description
 * @date 2020-08-13
 */
@Slf4j
@Service
public class BanxaService {

    private static final int HTTP_OK_CODE = 200;
    private static final MediaType JSON = MediaType.get("application/json; charset=utf-8");
    private static OkHttpClient client = new OkHttpClient.Builder()
            .addInterceptor(OkHttpPrometheusInterceptor.getInstance())
            .connectTimeout(5, TimeUnit.SECONDS)
            .writeTimeout(5, TimeUnit.SECONDS)
            .readTimeout(5, TimeUnit.SECONDS)
            .build();

    public String getRequest(OtcThirdParty thirdParty, String url, Map<String, String> params) {
        try {
            String thirdPartyApiKey = SignUtils.decryptDataWithAES(thirdParty.getOrgId().toString(), thirdParty.getApiKey());
            String thirdPartySecret = SignUtils.decryptDataWithAES(thirdParty.getOrgId().toString(), thirdParty.getSecret());

            HttpUrl httpUrl = HttpUrl.parse(thirdParty.getEndPoint() + url);
            HttpUrl.Builder builder = httpUrl.newBuilder();
            if (params != null) {
                for (String key : params.keySet()) {
                    builder.addQueryParameter(key, params.get(key));
                }
            }
            Request request = new Request.Builder()
                    .url(builder.build())
                    .addHeader("Authorization", signGet(thirdPartyApiKey, thirdPartySecret, url, params))
                    .build();
            Response response = client.newCall(request).execute();
            if (response.code() == HTTP_OK_CODE) {
                return response.body().string();
            } else {
                log.info("banxaGet: response.code:{}, body:{}", response.code(), response.body().string());
            }
        } catch (IOException e) {
            log.info("banxaGet: IOException:" + e.getMessage(), e);
        } catch (Exception ex) {
            log.info("banxaGet: Exception:" + ex.getMessage(), ex);
        }
        return null;
    }

    private String signGet(String apiKey, String secret, String url, Map<String, String> params) {
        try {
            String signString = "Bearer " + apiKey + ":";
            long timeStamp = System.currentTimeMillis() / 1000;
            Mac sha256_HMAC = Mac.getInstance("HmacSHA256");
            SecretKeySpec secretKeySpec = new SecretKeySpec(secret.getBytes(), "HmacSHA256");
            sha256_HMAC.init(secretKeySpec);
            StringBuffer originalStr = new StringBuffer();
            originalStr.append("GET" + "\n");
            originalStr.append(url);
            if (params != null && params.size() > 0) {
                originalStr.append("?");
                originalStr.append(Joiner.on("&").withKeyValueSeparator("=").join(params));
            }
            originalStr.append("\n");
            originalStr.append(timeStamp);
            signString = signString + new String(Hex.encodeHex(sha256_HMAC.doFinal(new String(originalStr).getBytes())));
            signString = signString + ":" + timeStamp;
            return signString;
        } catch (Exception e) {
            throw new RuntimeException("Unable to sign message.", e);
        }
    }

    /**
     * post请求 参数是map格式
     */
    public String postRequest(OtcThirdParty thirdParty, String url, Map<String, String> params) {
        try {
            String thirdPartyApiKey = SignUtils.decryptDataWithAES(thirdParty.getOrgId().toString(), thirdParty.getApiKey());
            String thirdPartySecret = SignUtils.decryptDataWithAES(thirdParty.getOrgId().toString(), thirdParty.getSecret());
            String json = JsonUtil.defaultGson().toJson(params);
            RequestBody body = RequestBody.create(JSON, json);
            Request request = new Request.Builder()
                    .url(thirdParty.getEndPoint() + url)
                    .addHeader("Content-Type", "application/json")
                    .addHeader("Authorization", signPost(thirdPartyApiKey, thirdPartySecret, url, params))
                    .post(body)
                    .build();

            Response response = client.newCall(request).execute();
            if (response.code() == HTTP_OK_CODE) {
                return response.body().string();
            } else {
                log.info("banxaPost: response.code:{}, body:{}", response.code(), response.body().string());
            }
        } catch (IOException e) {
            log.info("banxaPost: IOException: ", e);
        } catch (Exception ex) {
            log.info("banxaPost: Exception:" + ex.getMessage(), ex);
        }
        return null;
    }


    private String signPost(String apiKey, String secret, String url, Map<String, String> params) {
        try {
            String signString = "Bearer " + apiKey + ":";
            long timeStamp = System.currentTimeMillis() / 1000;
            Mac sha256_HMAC = Mac.getInstance("HmacSHA256");
            SecretKeySpec secretKeySpec = new SecretKeySpec(secret.getBytes(), "HmacSHA256");
            sha256_HMAC.init(secretKeySpec);
            StringBuffer originalStr = new StringBuffer();
            originalStr.append("POST" + "\n");
            originalStr.append(url + "\n");
            originalStr.append(timeStamp + "\n");
            if (params != null && params.size() > 0) {
                originalStr.append(JsonUtil.defaultGson().toJson(params));
            }
            signString = signString + new String(Hex.encodeHex(sha256_HMAC.doFinal(new String(originalStr).getBytes())));
            signString = signString + ":" + timeStamp;
            return signString;
        } catch (Exception e) {
            throw new RuntimeException("Unable to sign message.", e);
        }
    }

    // 查询法币币种
    public String getFiats(OtcThirdParty thirdParty) {
        String url = "/api/fiats";
        Map<String, String> params = new HashMap<>();
        return getRequest(thirdParty, url, params);
    }

    // 查询法币币种
    public String getCoins(OtcThirdParty thirdParty) {
        String url = "/api/coins";
        Map<String, String> params = new HashMap<>();
        return getRequest(thirdParty, url, params);
    }

    // 查询支付方式
    public String getPayment(OtcThirdParty thirdParty) {
        String url = "/api/payment-methods";
        Map<String, String> params = new HashMap<>();
        return getRequest(thirdParty, url, params);
    }

    // 查询价格
    public String getPrice(OtcThirdParty thirdParty, String source, String target,
                           String sourceAmount, String targetAmount, Long paymentMethodId) {
        String url = "/api/prices";
        Map<String, String> params = new HashMap<>();
        params.put("source", source);
        params.put("target", target);
        params.put("payment_method_id", paymentMethodId.toString());
        if (StringUtil.isNotEmpty(sourceAmount)) {
            params.put("source_amount", sourceAmount);
        } else {
            if (StringUtil.isNotEmpty(targetAmount)) {
                params.put("target_amount", targetAmount);
            }
        }
        log.info("banxaGetPrice: source:{},target:{},sourceAmount:{},paymentMethodId:{}",
                source, target, sourceAmount, paymentMethodId);
        return getRequest(thirdParty, url, params);
    }

    // 查询指定订单
    public String getOrder(OtcThirdParty thirdParty, String orderId) {
        String url = "/api/orders";
        url = url + "/" + orderId;
        Map<String, String> params = new HashMap<>();
        return getRequest(thirdParty, url, params);
    }

    // 创建订单
    public String createOrder(OtcThirdParty thirdParty, String accountReference, Long paymentMethodId, String source, String target,
                              String sourceAmount, String targetAmount, String walletAddress, String walletAddressTag,
                              String successUrl, String failureUrl, String cancelledUrl) {
        String url = "/api/orders";
        Map<String, String> params = new HashMap<>();
        params.put("account_reference", accountReference);
        params.put("payment_method_id", paymentMethodId.toString());
        params.put("source", source);
        params.put("target", target);
        params.put("source_amount", sourceAmount);
        params.put("target_amount", targetAmount);
        params.put("return_url_on_success", successUrl);
        if (StringUtil.isNotEmpty(failureUrl)) {
            params.put("return_url_on_failure", failureUrl);
        }
        if (StringUtil.isNotEmpty(cancelledUrl)) {
            params.put("return_url_on_cancelled", cancelledUrl);
        }
        params.put("wallet_address", walletAddress);
        if (StringUtil.isNotEmpty(walletAddressTag)) {
            params.put("wallet_address_tag", walletAddressTag);
        }
        return postRequest(thirdParty, url, params);
    }

    public BanxaOrderResult placeOrder(OtcThirdParty thirdParty, String walletAddress, String walletAddressTag,
                                       String accountReference, Long paymentId, Integer side, String tokenId, String currencyId,
                                       String tokenAmount, String currencyAmount, String successUrl, String failureUrl,
                                       String cancelledUrl) {
        try {
            // 根据买卖方向传给banxa对应的字段
            String source, target, sourceAmount, targetAmount;
            if (side.equals(0)) {
                source = currencyId;
                target = tokenId;
                sourceAmount = currencyAmount;
                targetAmount = tokenAmount;
            } else {
                source = tokenId;
                target = currencyId;
                sourceAmount = tokenAmount;
                targetAmount = currencyAmount;
            }
            String orderResult = createOrder(thirdParty, accountReference, paymentId,
                    source, target, sourceAmount, targetAmount, walletAddress, walletAddressTag, successUrl, failureUrl, cancelledUrl);
            log.info("banxaCreateOrder: priceResult:{}.", orderResult);
            return JsonUtil.defaultGson().fromJson(orderResult, BanxaOrderResult.class);
        } catch (Throwable e) {
            return null;
        }
    }

    public BanxaPriceResult getPrice(OtcThirdParty thirdParty, String tokenId, String currencyId, Long paymentId,
                                     String tokenAmount, String currencyAmount, Integer side) {
        try {
            log.info("banxa getPrice: tokenId:{},currencyId:{},tokenAmount:{},currencyAmount:{},paymentId:{},side:{}.",
                    tokenId, currencyId, tokenAmount, currencyAmount, paymentId, side);
            // 根据买卖方向传给banxa对应的字段
            String source, target, sourceAmount, targetAmount;
            if (side.equals(0)) {
                source = currencyId;
                target = tokenId;
                sourceAmount = currencyAmount;
                targetAmount = tokenAmount;
            } else {
                source = tokenId;
                target = currencyId;
                sourceAmount = tokenAmount;
                targetAmount = currencyAmount;
            }
            String priceResult = getPrice(thirdParty, source, target, sourceAmount, targetAmount, paymentId);
            log.info("banxaGetPrice: priceResult:{}.", priceResult);
            return JsonUtil.defaultGson().fromJson(priceResult, BanxaPriceResult.class);
        } catch (Throwable e) {
            log.info("banxaGetPrice fail:" + e.getMessage(), e);
            return null;
        }
    }

    public BanxaPaymentResult queryPayment(OtcThirdParty thirdParty) {
        try {
            String paymentResult = getPayment(thirdParty);
            // log.info("banxaQueryPayment response:{}", paymentResult);
            return JsonUtil.defaultGson().fromJson(paymentResult, BanxaPaymentResult.class);
        } catch (Throwable e) {
            log.info("banxaQueryPayment fail:" + e.getMessage(), e);
            return null;
        }
    }

    public BanxaOrderResult queryOrder(OtcThirdParty thirdParty, String orderId) {
        try {
            String orderResponse = getOrder(thirdParty, orderId);
            log.info("banxaQueryOrder response:{}", orderResponse);
            return JsonUtil.defaultGson().fromJson(orderResponse, BanxaOrderResult.class);
        } catch (Throwable e) {
            log.info("banxaQueryOrder fail:" + e.getMessage(), e);
            return null;
        }
    }

}
