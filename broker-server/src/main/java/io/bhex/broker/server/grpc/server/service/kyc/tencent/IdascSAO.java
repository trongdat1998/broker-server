package io.bhex.broker.server.grpc.server.service.kyc.tencent;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.common.util.JsonUtil;
import io.bhex.broker.server.BrokerServerProperties;
import io.bhex.broker.server.domain.kyc.tencent.*;
import io.bhex.broker.server.util.UrlBuilder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.*;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

@Slf4j
@Service
public class IdascSAO {

    private static final String RESP_SUCCESS = "0";

    @Resource
    private RestTemplate restTemplate;

    @Resource
    private BrokerServerProperties brokerServerProperties;

    private static final String DEFAULT_IDASC_URL_BASE = "https://idasc.webank.com";
    private static final String DEFAULT_IDA_URL_BASE = "https://ida.webank.com";

    private static String idascUrlBase;
    private static String idaUrlBase;

    private static GsonBuilder printGsonBuilder = new GsonBuilder();

    static {
        printGsonBuilder.registerTypeAdapter(GetFaceIdRequest.class, new GetFaceIdRequest.PrintJsonSerializer());
        printGsonBuilder.registerTypeAdapter(FaceCompareObject.class, new FaceCompareObject.PrintJsonSerializer());
        printGsonBuilder.registerTypeAdapter(IDCardOcrRequest.class, new IDCardOcrRequest.PrintJsonSerializer());
    }

    @PostConstruct
    public void init() {
        idascUrlBase = brokerServerProperties.getWebank().getIdascUrl();
        idaUrlBase = brokerServerProperties.getWebank().getIdaUrl();
    }

    @Scheduled(cron = "0/30 * * * * ?")
    public void detectWebankUrl() {
        String propIdascUrlBase = brokerServerProperties.getWebank().getIdascUrl();
        String propIdaUrlBase = brokerServerProperties.getWebank().getIdaUrl();

        // 如果配置的webank的url地址和默认的地址不一样，说明是走的专线代理，需要探测地址是否可用（防止专线掉线）
        if (!propIdascUrlBase.startsWith(DEFAULT_IDASC_URL_BASE)) {
            if (detectIdascUrl(propIdascUrlBase)) {
                idascUrlBase = propIdascUrlBase;
            } else {
                idascUrlBase = DEFAULT_IDASC_URL_BASE;
            }
            log.warn("detectWebankUrl set idascUrlBase: {}", idascUrlBase);
        }

        if (!propIdaUrlBase.startsWith(DEFAULT_IDA_URL_BASE)) {
            if (detectIdaUrl(propIdaUrlBase)) {
                idaUrlBase = propIdaUrlBase;
            } else {
                idaUrlBase = DEFAULT_IDA_URL_BASE;
            }
            log.warn("detectWebankUrl set idaUrlBase: {}", idaUrlBase);
        }
    }

    /**
     * 检测idaUrl是否可用
     */
    private boolean detectIdaUrl(String urlBase) {
        String url = UrlBuilder.fromUrlTemplate(urlBase + "/api/paas/idcardocrapp").build();
        try {
            ResponseEntity<String> response = restTemplate.getForEntity(url, String.class);
            return response.getStatusCode() == HttpStatus.METHOD_NOT_ALLOWED;
        } catch (RestClientException e) {
            if (e.getMessage() != null && e.getMessage().contains("405")) {
                return true;
            } else {
                log.warn(String.format("detectUrl: %s error: %s", url, e.getMessage()));
                return false;
            }
        }
    }

    /**
     * 检测idascUrl是否可用
     */
    private boolean detectIdascUrl(String urlBase) {
        String url = UrlBuilder.fromUrlTemplate(urlBase + "/api/oauth2/access_token").build();
        try {
            GetAccessTokenResponse response = restTemplate.getForObject(url, GetAccessTokenResponse.class);
            return response != null && response.getCode().equals("400101");
        } catch (RestClientException e) {
            log.warn(String.format("detectUrl: %s error: %s", url, e.getMessage()));
            return false;
        }
    }

    public GetAccessTokenResponse getAccessToken(GetAccessTokenRequest request) {
        String url = UrlBuilder.fromUrlTemplate(idascUrlBase + "/api/oauth2/access_token")
                .addParams("app_id", request.getAppId())
                .addParams("secret", request.getSecret())
                .addParams("grant_type", request.getGrantType())
                .addParams("version", request.getVersion()).build();
        try {
            log.info("[Webank] request url: {}", url);
            GetAccessTokenResponse response = restTemplate.getForObject(url, GetAccessTokenResponse.class);
            log.info("[Webank] response: {}", JsonUtil.defaultGson().toJson(response));
            checkResponse(response);
            return response;
        } catch (RestClientException e) {
            log.error(String.format("Request url: %s error", url), e);
            throw new BrokerException(BrokerErrorCode.TENCENT_KYC_REQUEST_ERROR, e.getMessage());
        }
    }

    public GetApiTicketResponse getApiTicket(GetApiTicketRequest request) {
        String url = UrlBuilder.fromUrlTemplate(idascUrlBase + "/api/oauth2/api_ticket")
                .addParams("app_id", request.getAppId())
                .addParams("access_token", request.getAccessToken())
                .addParams("type", request.getType())
                .addParams("version", request.getVersion())
                .addParams("user_id", request.getUserId()).build();
        try {
            log.info("[Webank] request url: {}", url);
            GetApiTicketResponse response = restTemplate.getForObject(url, GetApiTicketResponse.class);
            log.info("[Webank] response: {}", JsonUtil.defaultGson().toJson(response));
            checkResponse(response);
            return response;
        } catch (RestClientException e) {
            log.error(String.format("Request url: %s error", url), e);
            throw new BrokerException(BrokerErrorCode.TENCENT_KYC_REQUEST_ERROR, e.getMessage());
        }
    }

    public QueryResultResponse queryFaceCompareResult(QueryResultRequest request) {
        String url = UrlBuilder.fromUrlTemplate(idascUrlBase + "/api/server/sync")
                .addParams("app_id", request.getAppId())
                .addParams("version", request.getVersion())
                .addParams("nonce", request.getNonce())
                .addParams("order_no", request.getOrderNo())
                .addParams("sign", request.getSign())
                .addParams("get_file", request.getGetFile())
                .build();
        try {
            log.info("[Webank] request url: {}", url);
            QueryResultResponse response = restTemplate.getForObject(url, QueryResultResponse.class);
            log.info("[Webank] response: {}", toPrintJson(response));
            checkResponse(response);
            return response;
        } catch (RestClientException e) {
            log.error(String.format("Request url: %s error", url), e);
            throw new BrokerException(BrokerErrorCode.TENCENT_KYC_REQUEST_ERROR, e.getMessage());
        }
    }

    public GetFaceIdResponse getFaceId(GetFaceIdRequest request) {
        String url = UrlBuilder.fromUrlTemplate(idascUrlBase + "/api/server/getfaceid").build();
        HttpHeaders headers = new HttpHeaders();
        MediaType contentType = MediaType.parseMediaType("application/json; charset=UTF-8");
        headers.setContentType(contentType);
        String requestJson = JsonUtil.defaultGson().toJson(request);
        HttpEntity<String> entity = new HttpEntity<>(requestJson,headers);

        try {
            log.info("[Webank] request url: {} params: {}", url, toPrintJson(request));
            GetFaceIdResponse response = restTemplate.postForObject(url, entity, GetFaceIdResponse.class);
            log.info("[Webank] response: {}", toPrintJson(response));
            checkResponse(response);
            return response;
        } catch (RestClientException e) {
            log.error(String.format("Request url: %s error", url), e);
            throw new BrokerException(BrokerErrorCode.TENCENT_KYC_REQUEST_ERROR, e.getMessage());
        }
    }

    public IDCardOcrResponse idCardOcr(IDCardOcrRequest request) {
        String url = UrlBuilder.fromUrlTemplate(idaUrlBase + "/api/paas/idcardocrapp").build();
        HttpHeaders headers = new HttpHeaders();
        MediaType contentType = MediaType.parseMediaType("application/json; charset=UTF-8");
        headers.setContentType(contentType);
        String requestJson = JsonUtil.defaultGson().toJson(request);
        HttpEntity<String> entity = new HttpEntity<>(requestJson,headers);

        try {
            log.info("[Webank] request url: {} params: {}", url, toPrintJson(request));
            IDCardOcrResponse response = restTemplate.postForObject(url, entity, IDCardOcrResponse.class);
            log.info("[Webank] response: {}", toPrintJson(response));
            checkResponse(response);
            return response;
        } catch (RestClientException e) {
            log.error(String.format("Request url: %s error", url), e);
            throw new BrokerException(BrokerErrorCode.TENCENT_KYC_REQUEST_ERROR, e.getMessage());
        }
    }

    private void checkResponse(WebankBaseResponse response) {
        if (response == null) {
            throw new WebankKycException("response is NULL", new NullPointerException());
        } else if (!response.getCode().equals(RESP_SUCCESS)) {
            throw new WebankKycException(response.getCode(), response.getMsg());
        }
    }

    public static String toPrintJson(Object object) {
        Gson printJson = printGsonBuilder.create();
        return printJson.toJson(object);
    }

}
