package io.bhex.broker.server.domain.kyc.tencent;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import io.bhex.broker.server.util.CommonUtil;
import lombok.Data;

import java.lang.reflect.Type;

@Data
public class FaceCompareObject {
    private String orderNo;
    private String idNo;
    private String idType;
    private String name;
    private String video;
    private String photo;
    private String liveRate;
    private String similarity;
    private String occurredTime;

    public static class PrintJsonSerializer implements JsonSerializer<FaceCompareObject> {

        @Override
        public JsonElement serialize(FaceCompareObject src, Type type, JsonSerializationContext context) {
            final JsonObject jsonObject = new JsonObject();

            String briefVideo = CommonUtil.getBriefString(src.getVideo(), 32);
            String briefPhoto = CommonUtil.getBriefString(src.getPhoto(), 32);

            jsonObject.addProperty("orderNo", src.getOrderNo());
            jsonObject.addProperty("idNo", src.getIdNo());
            jsonObject.addProperty("idType", src.getIdType());
            jsonObject.addProperty("name", src.getName());
            jsonObject.addProperty("video", briefVideo);
            jsonObject.addProperty("photo", briefPhoto);
            jsonObject.addProperty("liveRate", src.getLiveRate());
            jsonObject.addProperty("similarity", src.getSimilarity());
            jsonObject.addProperty("occurredTime", src.getOccurredTime());

            return jsonObject;
        }
    }
}
