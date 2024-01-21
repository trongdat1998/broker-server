package io.bhex.broker.server.domain;

import com.google.common.collect.Lists;
import com.google.gson.JsonObject;
import io.bhex.broker.common.util.JsonUtil;

import java.util.Iterator;

/**********************************
 *@项目名称: server-parent
 *@文件名称: io.bhex.broker.server.domain
 *@Date 2019-03-15
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
public enum FunctionModule {

    ORG_API(0, "org_api", "OrgApi权限"),
    EXCHANGE(1, "exchange", "币币"),
    OTC(2, "otc", "法币"),
    POINTCARD(3, "pointcard", "点卡"),
    GUILD(4, "guild", "公会"),
    VOL(5, "vol", "波动性基金"),
    ACTIVITY(6, "activity", "活动"),
    COUPON(7, "coupon", "卡券"),
    OPTION(8, "option", "期权"),
    EXPLORE(9, "explore", "期权体验区"),
    FUTURES(10, "futures", "期货"),
    BONUS(11, "bonus", "理财"),
    LOAN(12, "loan", "借贷"),
    RED_PACKET(13, "red_packet", "红包"),
    MARGIN(14,"margin","杠杆");


    private int id;
    private String moduleName;
    private String desc;

    FunctionModule(int id, String moduleName, String desc) {
        this.id = id;
        this.moduleName = moduleName;
        this.desc = desc;
    }

    public String moduleName() {
        return moduleName;
    }

    public static String getDefaultFunctionConfig() {
        JsonObject functions = new JsonObject();
        Iterator<FunctionModule> moduleIterator = Lists.newArrayList(FunctionModule.values()).iterator();
        while (moduleIterator.hasNext()) {
            functions.addProperty(moduleIterator.next().moduleName(), Boolean.FALSE);
        }
        functions.addProperty(EXCHANGE.moduleName(), Boolean.TRUE);
        functions.addProperty(OTC.moduleName(), Boolean.FALSE);
        return JsonUtil.defaultGson().toJson(functions);
    }

}
