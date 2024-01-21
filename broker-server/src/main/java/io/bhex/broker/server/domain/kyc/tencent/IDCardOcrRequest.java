package io.bhex.broker.server.domain.kyc.tencent;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import io.bhex.broker.server.util.CommonUtil;
import lombok.Data;

import java.lang.reflect.Type;

@Data
public class IDCardOcrRequest {
    private String webankAppId;
    private String orderNo;
    private String userId;
    private String version;
    private String nonce;
    private String sign;
    private String cardType;
    private String idcardStr;

    public static class PrintJsonSerializer implements JsonSerializer<IDCardOcrRequest> {

        @Override
        public JsonElement serialize(IDCardOcrRequest request, Type type, JsonSerializationContext context) {
            final JsonObject jsonObject = new JsonObject();
            jsonObject.addProperty("webankAppId", request.getWebankAppId());
            jsonObject.addProperty("orderNo", request.getOrderNo());
            jsonObject.addProperty("userId", request.getUserId());
            jsonObject.addProperty("version", request.getVersion());
            jsonObject.addProperty("nonce", request.getNonce());
            jsonObject.addProperty("sign", request.getSign());
            jsonObject.addProperty("cardType", request.getCardType());
            jsonObject.addProperty("idcardStr", CommonUtil.getBriefString(request.getIdcardStr(), 32));
            return jsonObject;
        }
    }
}
