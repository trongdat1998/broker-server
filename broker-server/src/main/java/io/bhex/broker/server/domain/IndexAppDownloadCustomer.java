package io.bhex.broker.server.domain;

import lombok.Builder;
import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
@Builder(builderClassName = "Builder", toBuilder = true)
public class IndexAppDownloadCustomer {

    private String title;

    private String image;

    private String background;

    private String desc;

    private List<Map<String, String>> list;

}
