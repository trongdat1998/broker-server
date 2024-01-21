package io.bhex.broker.server.domain.kyc.tencent;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import io.bhex.broker.server.util.CommonUtil;
import lombok.Data;

import java.lang.reflect.Type;

@Data
public class GetFaceIdRequest {
    private String webankAppId;
    private String orderNo;
    private String name;
    private String idNo;
    private String userId;
    private String sourcePhotoStr;
    private String sourcePhotoType;
    private String version;
    private String nonce;
    private String sign;

    public static class PrintJsonSerializer implements JsonSerializer<GetFaceIdRequest> {

        @Override
        public JsonElement serialize(GetFaceIdRequest request, Type type, JsonSerializationContext context) {
            final JsonObject jsonObject = new JsonObject();
            jsonObject.addProperty("webankAppId", request.getWebankAppId());
            jsonObject.addProperty("orderNo", request.getOrderNo());
            jsonObject.addProperty("name", request.getName());
            jsonObject.addProperty("idNo", request.getIdNo());
            jsonObject.addProperty("userId", request.getUserId());
            jsonObject.addProperty("sourcePhotoStr", CommonUtil.getBriefString(request.getSourcePhotoStr(), 32));
            jsonObject.addProperty("sourcePhotoType", request.getSourcePhotoType());
            jsonObject.addProperty("version", request.getVersion());
            jsonObject.addProperty("nonce", request.getNonce());
            jsonObject.addProperty("sign", request.getSign());
            return jsonObject;
        }
    }
}
