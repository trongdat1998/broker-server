package io.bhex.broker.server.model;


import com.google.common.base.Splitter;
import lombok.Data;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

@Data
@Table(name = "tb_activity_lock_interest_white_list")
public class ActivityLockInterestWhiteList {

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;

    private Long brokerId;

    private Long projectId;

    private String userIdStr;

    public HashSet<String> queryUserIds() {
        List<String> ids = Splitter.on(",").omitEmptyStrings().splitToList(userIdStr).stream().map(String::trim).collect(Collectors.toList());
        return new HashSet<>(ids);
    }
}
