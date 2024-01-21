package io.bhex.broker.server.primary.mapper;


import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;

public class DBToolsSqlProvider {

    public String fetchOne(Map<String, Object> parameter) {
        StringBuilder stringBuilder = new StringBuilder("select ");
        stringBuilder.append(parameter.get("fields"));
        stringBuilder.append(" from ").append(parameter.get("table")).append(" ");
        if (parameter.containsKey("conditions")) {
            List<Map<String,String>> conditions = (List<Map<String,String>>) parameter.get("conditions");
            if (conditions.size() > 0) {
                stringBuilder.append(" where ");
                for (int i = 0; i < conditions.size(); i++) {
                    if (i > 0) {
                        stringBuilder.append(" and ");
                    }
                    stringBuilder.append(conditions.get(i).get("name")).append(" ")
                            .append(conditions.get(i).get("condition")).append(" ")
                            .append(String.format("#{conditions[%d].value}", i));
                }
            }
        }
        stringBuilder.append(" limit 1;");
        return stringBuilder.toString();
    }
}
