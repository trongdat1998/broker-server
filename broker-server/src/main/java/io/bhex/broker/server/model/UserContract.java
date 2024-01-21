package io.bhex.broker.server.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * @ProjectName: broker
 * @Package: io.bhex.broker.server.model
 * @Author: ming.xu
 * @CreateDate: 2019/1/31 5:47 PM
 * @Copyright（C）: 2019 BHEX Inc. All rights reserved.
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "tb_user_contract")
public class UserContract {

    public final static Integer ON_OPEN_TYPE = 1;
    public final static Integer OFF_OPEN_TYPE = 0;

    public final static String OPTION_NAME = "option";
    public final static String FUTURES_NAME = "futures";
    public final static String MARGIN_NAME = "margin";

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;
    private String name;
    private Long userId;
    private Long orgId;
    private Integer open;
    private Long updated;
    private Long created;
}
