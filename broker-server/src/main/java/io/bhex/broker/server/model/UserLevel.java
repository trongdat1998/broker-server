package io.bhex.broker.server.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Id;
import javax.persistence.Table;
import java.math.BigDecimal;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "tb_user_level")
public class UserLevel {

    public static final int WHITE_LIST_STATUS = 3;

    @Id
    private Long id;

    private Long orgId;

    private Long levelConfigId;

    private Long userId;

    private String levelData;

    private Integer status; //-1 levelconfig被删除 0-不可用 1-ok 2-preview 3-白名单状态

    private Long created;

    private Long updated;
}
